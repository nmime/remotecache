import { afterEach, describe, expect, it } from 'bun:test';
import { S3Client } from 'bun';
import { S3Strategy, shouldRefreshCredentials } from './s3';

type S3ClientPrototype = {
  file(path: string, options?: Bun.S3Options): Bun.S3File;
  list(input?: Bun.S3ListObjectsOptions | null): Promise<Bun.S3ListObjectsResponse>;
};

const s3Prototype = S3Client.prototype as unknown as S3ClientPrototype;
const originalFile = s3Prototype.file;
const originalList = s3Prototype.list;
const originalFetch = globalThis.fetch;

afterEach(() => {
  s3Prototype.file = originalFile;
  s3Prototype.list = originalList;
  globalThis.fetch = originalFetch;
});

describe('shouldRefreshCredentials', () => {
  const now = 1_000_000_000_000;

  it('never refreshes when expiration is null (static credentials)', () => {
    expect(shouldRefreshCredentials(null, now)).toBe(false);
  });

  it('refreshes within five minutes of expiry', () => {
    expect(shouldRefreshCredentials(now + 4 * 60 * 1000, now)).toBe(true);
  });

  it('does not refresh comfortably before expiry', () => {
    expect(shouldRefreshCredentials(now + 30 * 60 * 1000, now)).toBe(false);
  });

  it('refreshes when already expired', () => {
    expect(shouldRefreshCredentials(now - 1000, now)).toBe(true);
  });
});

describe('S3Strategy', () => {
  const createStrategy = () =>
    new S3Strategy({
      bucket: 'bucket',
      credentials: { accessKeyId: 'access', secretAccessKey: 'secret' },
    });

  it('coalesces concurrent credential resolution', async () => {
    let providerCalls = 0;
    const { promise: credentialsGate, resolve: releaseCredentials } = Promise.withResolvers<void>();
    globalThis.fetch = Object.assign(async () => new Response('missing', { status: 404 }), {
      preconnect: originalFetch.preconnect,
    });
    const strategy = new S3Strategy({
      bucket: 'bucket',
      credentials: async () => {
        providerCalls++;
        await credentialsGate;
        return {
          accessKeyId: 'access',
          secretAccessKey: 'secret',
          expiration: new Date(Date.now() + 30 * 60 * 1000),
        };
      },
    });

    const requests = Array.from({ length: 32 }, () => strategy.exists('hash'));
    expect(providerCalls).toBe(1);
    releaseCredentials();

    await expect(Promise.all(requests)).resolves.toEqual(Array<boolean>(32).fill(false));
    expect(providerCalls).toBe(1);
  });

  it('retries credential resolution after a provider failure', async () => {
    let providerCalls = 0;
    globalThis.fetch = Object.assign(async () => new Response('x', { status: 206 }), {
      preconnect: originalFetch.preconnect,
    });
    const strategy = new S3Strategy({
      bucket: 'bucket',
      credentials: async () => {
        providerCalls++;
        if (providerCalls === 1) {
          throw new Error('STS unavailable');
        }
        return { accessKeyId: 'access', secretAccessKey: 'secret' };
      },
    });

    await expect(strategy.exists('hash')).rejects.toThrow(/^STS unavailable$/);
    await expect(strategy.exists('hash')).resolves.toBe(true);
    expect(providerCalls).toBe(2);
  });

  it('uses a one-byte ranged GET for portable existence checks', async () => {
    const requests: Array<{ method: string | undefined; range: string | null }> = [];
    let call = 0;
    globalThis.fetch = Object.assign(
      async (_input: string | URL | Request, init?: RequestInit) => {
        requests.push({
          method: init?.method,
          range: new Headers(init?.headers).get('Range'),
        });
        call++;
        return call === 1
          ? new Response('x', { status: 206 })
          : new Response('missing', { status: 404 });
      },
      { preconnect: originalFetch.preconnect },
    );

    await expect(createStrategy().exists('present')).resolves.toBe(true);
    await expect(createStrategy().exists('missing')).resolves.toBe(false);
    expect(requests).toEqual([
      { method: 'GET', range: 'bytes=0-0' },
      { method: 'GET', range: 'bytes=0-0' },
    ]);
  });

  it('surfaces unexpected existence-probe failures', async () => {
    globalThis.fetch = Object.assign(
      async () => new Response('backend unavailable', { status: 503 }),
      { preconnect: originalFetch.preconnect },
    );

    await expect(createStrategy().exists('hash')).rejects.toThrow(
      'S3 existence probe failed with HTTP 503: backend unavailable',
    );
  });

  it('rejects writes when the backend does not support conditional writes', async () => {
    globalThis.fetch = Object.assign(
      async () => new Response('conditional writes unsupported', { status: 501 }),
      { preconnect: originalFetch.preconnect },
    );

    await expect(
      createStrategy().writeStream('hash', new Blob(['data']).stream(), 4),
    ).rejects.toThrow('does not support conditional writes');
  });

  it('forwards cancellation to the outbound S3 upload', async () => {
    let forwardedSignal: AbortSignal | null | undefined;
    globalThis.fetch = Object.assign(
      async (_input: string | URL | Request, init?: RequestInit) => {
        forwardedSignal = init?.signal;
        return await new Promise<Response>((_resolve, reject) => {
          if (init?.signal?.aborted) {
            reject(init.signal.reason);
            return;
          }
          init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), {
            once: true,
          });
        });
      },
      { preconnect: originalFetch.preconnect },
    );
    const controller = new AbortController();
    const upload = createStrategy().writeStream(
      'hash',
      new Blob(['data']).stream(),
      4,
      controller.signal,
    );

    controller.abort(new Error('client disconnected'));

    await expect(upload).rejects.toThrow('client disconnected');
    expect(forwardedSignal).toBe(controller.signal);
  });

  it('isolates each streamed upload from the fetch connection pool', async () => {
    let forwardedHeaders: HeadersInit | undefined;
    globalThis.fetch = Object.assign(
      async (_input: string | URL | Request, init?: RequestInit) => {
        forwardedHeaders = init?.headers;
        return new Response(null, { status: 200 });
      },
      { preconnect: originalFetch.preconnect },
    );

    await createStrategy().writeStream('hash', new Blob(['data']).stream(), 4);

    expect(new Headers(forwardedHeaders).get('Connection')).toBe('close');
  });

  it('releases the S3 response reader after the streamed body reaches EOF', async () => {
    const source = new Blob(['payload']).stream();
    const getReader = source.getReader.bind(source);
    let releaseCalls = 0;
    Object.defineProperty(source, 'getReader', {
      value: () => {
        const reader = getReader();
        const releaseLock = reader.releaseLock.bind(reader);
        Object.defineProperty(reader, 'releaseLock', {
          value: () => {
            releaseCalls++;
            releaseLock();
          },
        });
        return reader;
      },
    });
    s3Prototype.file = () => ({ stream: () => source }) as Bun.S3File;

    const stream = await createStrategy().getStream('hash');

    expect(await new Response(stream).text()).toBe('payload');
    expect(releaseCalls).toBe(1);
  });

  it('checks bucket readiness by listing at most one object', async () => {
    let listInput: Bun.S3ListObjectsOptions | null | undefined;
    s3Prototype.list = (input) => {
      listInput = input;
      return Promise.resolve({});
    };

    await expect(createStrategy().checkReady()).resolves.toBeUndefined();

    expect(listInput).toEqual({ maxKeys: 1 });
  });

  it('fails readiness when the bucket list probe fails', async () => {
    s3Prototype.list = () => Promise.reject(new Error('NoSuchBucket'));

    await expect(createStrategy().checkReady()).rejects.toThrow('NoSuchBucket');
  });
});
