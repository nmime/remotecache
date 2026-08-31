import {
  accessForbidden,
  badRequest,
  conflictError,
  internalServerError,
  okResponse,
  payloadTooLargeError,
} from '../responses';
import { CacheFile } from './cache-file.interface';
import { TokenPermission } from '../token/token-interfaces';
import { logger } from '../logger';
import { CacheEntryExistsError } from './storage-strategy/storage-strategy.interface';

const validateContentLengthHeader = (headerContentLength: string) => {
  // Content-Length must be a non-negative decimal integer per the HTTP spec;
  // this also rejects scientific notation like '1e308' that Number() would
  // otherwise accept as a finite value.
  if (!/^\d+$/.test(headerContentLength)) return null;
  const contentLength = Number(headerContentLength);
  if (!Number.isFinite(contentLength) || contentLength <= 0) return null;
  return contentLength;
};

const toReadableStream = (
  body: ReadableStream<Uint8Array> | Blob | null,
): ReadableStream<Uint8Array> | null => {
  if (body instanceof ReadableStream) return body;
  if (body instanceof Blob) return body.stream();
  return null;
};

class ContentLengthExceededError extends Error {}
class ContentLengthMismatchError extends Error {}

/** Validates and streams one append-only cache upload. */
export async function writeCache(
  cacheFile: Pick<CacheFile, 'exists' | 'writeStream' | 'valid'>,
  tokenPermission: TokenPermission | null,
  body: ReadableStream<Uint8Array> | Blob | null,
  headerContentLength: string,
  maxUploadBytes: number,
) {
  const canWrite = tokenPermission === 'full';

  if (!canWrite) {
    return accessForbidden();
  }

  if (!cacheFile.valid()) {
    return badRequest('Invalid hash');
  }

  try {
    if (await cacheFile.exists()) {
      return conflictError('Cannot override an existing record');
    }
  } catch (error) {
    logger.error(error);
    return internalServerError('Failed to check cache');
  }

  const expectedLength = validateContentLengthHeader(headerContentLength);
  const sourceStream = toReadableStream(body);
  if (!expectedLength || !sourceStream) {
    return badRequest('Invalid Content-Length header');
  }

  if (expectedLength > maxUploadBytes) {
    return payloadTooLargeError(
      `Upload exceeds the maximum allowed size of ${maxUploadBytes} bytes`,
    );
  }

  let total = 0;
  // Keep the request body on Bun's native pipe path. A hand-built ReadableStream
  // around request.body is not consumed correctly when Bun fetch() forwards it
  // to an S3-compatible backend, which makes valid uploads look truncated.
  const counter = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      total += chunk.byteLength;
      if (total > expectedLength) {
        throw new ContentLengthExceededError();
      }
      controller.enqueue(chunk);
    },
    flush() {
      if (total !== expectedLength) {
        throw new ContentLengthMismatchError();
      }
    },
  });
  // Keep request-body consumption in the downstream pull chain. Awaiting a
  // separate pipeTo promise can stay pending after S3 has committed the object,
  // leaving the client PUT open indefinitely on Bun.
  const countedStream = sourceStream.pipeThrough(counter);
  let cancellationStarted = false;
  let cancellation: Promise<void> | undefined;
  const cancelCountedStream = () => {
    if (cancellationStarted || countedStream.locked) return cancellation;
    cancellationStarted = true;
    cancellation = countedStream.cancel().catch(() => {
      // Preserve the original storage error and response mapping.
    });
    return cancellation;
  };

  try {
    await cacheFile.writeStream(countedStream, expectedLength);
    if (total !== expectedLength) throw new ContentLengthMismatchError();
    return okResponse({ message: null });
  } catch (error) {
    cancelCountedStream();
    // Let standard stream cancellation reach the request body before returning,
    // without waiting for a slow or stuck source cancel callback to settle.
    await Promise.resolve();
    const failure = error;
    if (failure instanceof CacheEntryExistsError) {
      return conflictError('Cannot override an existing record');
    }
    if (
      failure instanceof ContentLengthExceededError ||
      failure instanceof ContentLengthMismatchError
    ) {
      return badRequest('Invalid Content-Length header');
    }
    logger.error(failure);
    return internalServerError('Failed to write to cache');
  }
}
