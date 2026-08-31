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
class ClientDisconnectedError extends Error {}

/** Validates and streams one append-only cache upload. */
export async function writeCache(
  cacheFile: Pick<CacheFile, 'exists' | 'writeStream' | 'valid'>,
  tokenPermission: TokenPermission | null,
  body: ReadableStream<Uint8Array> | Blob | null,
  headerContentLength: string,
  maxUploadBytes: number,
  requestSignal?: AbortSignal,
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
  // Keep request-body consumption in the downstream pull chain. A separately
  // awaited pipeTo can remain pending after S3 commits and deadlock the client
  // response. The route's AbortSignal below handles disconnects explicitly.
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
    if (requestSignal?.aborted) throw new ClientDisconnectedError();
    const storageWrite = cacheFile.writeStream(countedStream, expectedLength);
    if (requestSignal) {
      let rejectDisconnected: ((error: ClientDisconnectedError) => void) | undefined;
      const disconnected = new Promise<never>((_resolve, reject) => {
        rejectDisconnected = reject;
      });
      const onAbort = () => rejectDisconnected?.(new ClientDisconnectedError());
      requestSignal.addEventListener('abort', onAbort, { once: true });
      try {
        await Promise.race([storageWrite, disconnected]);
      } finally {
        requestSignal.removeEventListener('abort', onAbort);
      }
    } else {
      await storageWrite;
    }
    if (total !== expectedLength) throw new ContentLengthMismatchError();
    await cancelCountedStream();
    // Yield one Bun event-loop turn after the downstream fetch consumes the
    // request stream. Returning the route response in the same turn can leave
    // the client socket waiting even though the S3 object is already committed.
    await Bun.sleep(0);
    return okResponse({ message: null });
  } catch (error) {
    const cancellation = cancelCountedStream();
    const failure = error;
    // Let standard stream cancellation reach the request body before returning,
    // without waiting for a slow or stuck source cancel callback to settle.
    if (failure instanceof ClientDisconnectedError) {
      await Promise.race([cancellation ?? Promise.resolve(), Bun.sleep(0)]);
    } else {
      await Promise.resolve();
    }
    if (failure instanceof CacheEntryExistsError) {
      return conflictError('Cannot override an existing record');
    }
    if (
      failure instanceof ContentLengthExceededError ||
      failure instanceof ContentLengthMismatchError ||
      failure instanceof ClientDisconnectedError
    ) {
      return badRequest('Invalid Content-Length header');
    }
    logger.error(failure);
    return internalServerError('Failed to write to cache');
  }
}
