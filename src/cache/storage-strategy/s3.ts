import { S3Client, type S3Options } from 'bun';
import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { Readable } from 'node:stream';
import { CacheEntryExistsError, CacheStorageStrategy } from './storage-strategy.interface';

type StaticCredentials = { accessKeyId: string; secretAccessKey: string; sessionToken?: string };
type ResolvedCredentials = StaticCredentials & { expiration?: Date };
type CredentialProvider = () => Promise<ResolvedCredentials>;
type UploadResult = { status: number; detail: string };
export type S3Upload = (
  url: string,
  stream: ReadableStream<Uint8Array>,
  contentLength: number,
  signal?: AbortSignal,
) => Promise<UploadResult>;

export interface S3StrategyOptions {
  bucket: string;
  region?: string;
  endpoint?: string;
  credentials: StaticCredentials | CredentialProvider;
  upload?: S3Upload;
}

const REFRESH_WINDOW_MS = 5 * 60 * 1000;

/** True when temporary credentials are missing or within the refresh window of expiry. */
export function shouldRefreshCredentials(expiration: number | null, now: number): boolean {
  if (expiration === null) return false;
  return now >= expiration - REFRESH_WINDOW_MS;
}

/** Streams one upload over a dedicated, non-pooled HTTP connection. */
export const isolatedS3Upload: S3Upload = (url, stream, contentLength, signal) =>
  new Promise((resolve, reject) => {
    const target = new URL(url);
    const request = target.protocol === 'https:' ? httpsRequest : httpRequest;
    const source = Readable.fromWeb(stream as unknown as Parameters<typeof Readable.fromWeb>[0]);
    let settled = false;
    const cleanup = () => signal?.removeEventListener('abort', onAbort);
    const accept = (result: UploadResult) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(result);
    };
    const fail = (error: unknown) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(error);
    };
    const req = request(
      target,
      {
        method: 'PUT',
        // agent:false creates a one-shot Agent/socket. Unlike fetch pooling
        // hints, destroying this request on abort cannot contaminate a later
        // cache lookup or upload in the Bun process.
        agent: false,
        headers: {
          Connection: 'close',
          'Content-Length': String(contentLength),
          'If-None-Match': '*',
        },
      },
      (response) => {
        let detail = '';
        response.setEncoding('utf8');
        response.on('data', (chunk) => {
          if (detail.length < 512) detail += String(chunk).slice(0, 512 - detail.length);
        });
        response.on('end', () => accept({ status: response.statusCode ?? 0, detail }));
        response.on('aborted', () => fail(new Error('S3 upload response was aborted')));
        response.on('error', fail);
      },
    );
    function onAbort() {
      const error = signal?.reason instanceof Error ? signal.reason : new Error('Upload aborted');
      source.destroy(error);
      req.destroy(error);
    }
    req.on('error', fail);
    source.on('error', (error) => req.destroy(error));
    signal?.addEventListener('abort', onAbort, { once: true });
    if (signal?.aborted) onAbort();
    else source.pipe(req);
  });

export class S3Strategy implements CacheStorageStrategy {
  readonly #bucket: string;
  readonly #region?: string;
  readonly #endpoint?: string;
  readonly #provider?: CredentialProvider;
  readonly #upload: S3Upload;
  #client: Bun.S3Client | null = null;
  #expiration: number | null = null;
  #refreshPromise: Promise<void> | null = null;

  constructor(options: S3StrategyOptions) {
    this.#bucket = options.bucket;
    this.#region = options.region;
    this.#endpoint = options.endpoint;
    this.#upload = options.upload ?? isolatedS3Upload;
    if (typeof options.credentials === 'function') {
      this.#provider = options.credentials;
    } else {
      this.#client = this.#build(options.credentials);
    }
  }

  #build(creds: StaticCredentials): Bun.S3Client {
    const opts: S3Options = {
      bucket: this.#bucket,
      accessKeyId: creds.accessKeyId,
      secretAccessKey: creds.secretAccessKey,
      ...(creds.sessionToken ? { sessionToken: creds.sessionToken } : {}),
      ...(this.#region ? { region: this.#region } : {}),
      ...(this.#endpoint ? { endpoint: this.#endpoint } : {}),
    };
    return new S3Client(opts);
  }

  async #getClient(): Promise<Bun.S3Client> {
    if (!this.#provider) return this.#client as Bun.S3Client;
    if (!this.#client || shouldRefreshCredentials(this.#expiration, Date.now())) {
      // Coalesce concurrent refreshes so a credential-expiry window triggers a
      // single provider (e.g. STS AssumeRole) call instead of one per in-flight
      // request. The promise is cleared once settled so the next cycle refreshes.
      this.#refreshPromise ??= this.#provider()
        .then((creds) => {
          this.#client = this.#build(creds);
          this.#expiration = creds.expiration ? creds.expiration.getTime() : null;
        })
        .finally(() => {
          this.#refreshPromise = null;
        });
      await this.#refreshPromise;
    }
    return this.#client as Bun.S3Client;
  }

  async exists(hash: string): Promise<boolean> {
    const client = await this.#getClient();
    // Some S3-compatible backends (notably SeaweedFS) close the connection
    // when a HEAD miss tries to include an XML error body. Bun's S3 exists()
    // uses HEAD, so a normal Nx cache miss can otherwise stall until the HTTP
    // idle timeout. A one-byte ranged GET is portable, cheap, and keeps the
    // missing-object response on the regular GET error path.
    const response = await fetch(client.presign(hash, { method: 'GET' }), {
      method: 'GET',
      headers: { Range: 'bytes=0-0' },
    });

    if (response.ok) {
      await response.arrayBuffer();
      return true;
    }
    const detail = (await response.text().catch(() => '')).slice(0, 512);
    if (response.status === 404) return false;
    throw new Error(`S3 existence probe failed with HTTP ${response.status}: ${detail}`);
  }

  async checkReady(): Promise<void> {
    await (await this.#getClient()).list({ maxKeys: 1 });
  }

  async getStream(hash: string): Promise<ReadableStream> {
    const reader = (await this.#getClient()).file(hash).stream().getReader();
    let readerReleased = false;
    const releaseReader = () => {
      if (readerReleased) return;
      readerReleased = true;
      reader.releaseLock();
    };

    let pendingRead: Awaited<ReturnType<typeof reader.read>> | undefined;
    try {
      // Prime the S3 request so an upstream read failure reaches getCache before
      // it commits a 200 response, and so every response owns one explicit
      // reader lifecycle.
      pendingRead = await reader.read();
    } catch (error) {
      releaseReader();
      throw error;
    }

    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const result = pendingRead ?? (await reader.read());
          pendingRead = undefined;
          if (result.done) {
            releaseReader();
            controller.close();
            return;
          }
          controller.enqueue(result.value);
        } catch (error) {
          releaseReader();
          throw error;
        }
      },
      async cancel(reason) {
        try {
          await reader.cancel(reason);
        } finally {
          releaseReader();
        }
      },
    });
  }

  async getSize(hash: string): Promise<number> {
    return (await this.#getClient()).size(hash);
  }

  async writeStream(
    hash: string,
    stream: ReadableStream<Uint8Array>,
    contentLength: number,
    signal?: AbortSignal,
  ): Promise<void> {
    const client = await this.#getClient();
    // Single attempt, deliberately: the body is a one-shot stream, so a retry
    // would have to buffer the whole upload, and Nx treats remote-cache
    // failures as soft (the client falls back to a cache miss).
    const response = await this.#upload(
      client.presign(hash, { method: 'PUT' }),
      stream,
      contentLength,
      signal,
    );

    if (response.status >= 200 && response.status < 300) return;
    if (response.status === 409 || response.status === 412) {
      throw new CacheEntryExistsError(hash);
    }
    const detail = response.detail.slice(0, 512);
    if (response.status === 501) {
      throw new Error(
        `S3 write failed with HTTP 501: the backend does not support conditional writes (If-None-Match), which remotecache requires for append-only uploads. Use AWS S3 or another backend with S3 conditional-write support — see the storage-strategies guide. Backend response: ${detail}`,
      );
    }
    throw new Error(`S3 write failed with HTTP ${response.status}: ${detail}`);
  }
}
