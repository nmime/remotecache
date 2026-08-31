import {
  accessForbidden,
  notFoundError,
  okResponse,
  internalServerError,
  badRequest,
} from '../responses';
import { CacheFile } from './cache-file.interface';
import { TokenPermission } from '../token/token-interfaces';
import { logger } from '../logger';

const readPermissions: TokenPermission[] = ['readonly', 'full'];

export async function getCache(
  cacheFile: Pick<CacheFile, 'exists' | 'stream' | 'size' | 'valid'>,
  tokenPermission: TokenPermission | null,
) {
  const canRead = !!tokenPermission && readPermissions.includes(tokenPermission);
  if (!canRead) {
    return accessForbidden();
  }

  if (!cacheFile.valid()) {
    return badRequest('Invalid hash');
  }

  try {
    if (!(await cacheFile.exists())) {
      return notFoundError('The record was not found');
    }

    // Resolve metadata before opening the body stream. Issuing a second S3
    // operation after creating an unread Bun S3 stream can leave that stream
    // stalled against SeaweedFS.
    const contentLength = await cacheFile.size();
    const message = await cacheFile.stream();
    return okResponse({ message, contentType: 'application/octet-stream', contentLength });
  } catch (error) {
    logger.error(error);
    return internalServerError('Failed to read cache');
  }
}
