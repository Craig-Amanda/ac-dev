/**
 * A KnackContext for tests: real caches, real policy code, no network and no disk.
 * Stub `request`, `requestWithRetry` and `getRuntimeMetadata` by assignment.
 */
import type { AppConfig, ServerOptions } from '../config.js';
import { KnackContext } from '../context.js';
import type { KnackApiResult } from '../http.js';
import { getCacheEntry, makeCacheEntry } from '../lib/cache.js';
import type { ToolResult } from '../response.js';
import type { RuntimeMetadata } from '../types.js';

export function makeApp(overrides: Partial<AppConfig> = {}): AppConfig {
    return {
        appKey: 'Demo',
        appName: 'Demo',
        appId: '000000000000000000000000',
        apiBase: 'https://api.knack.com/v1',
        appFolder: '/tmp/KnackApps/Demo',
        readonly: false,
        allowViewMutation: true,
        allowDelete: true,
        allowDiagnostics: true,
        ...overrides,
    };
}

export type RequestLog = Array<{
    apiPath: string;
    method: string;
    body: unknown;
}>;

export type FakeContextInput = {
    apps?: AppConfig[];
    options?: ServerOptions;
    secrets?: Record<string, string>;
    /** Runtime metadata per appKey; null means the fetch fails. */
    runtimeMetadata?: Record<string, RuntimeMetadata | null>;
    /** Answer for each REST call, by "METHOD /path" or a function of the request. */
    responses?:
        | Record<string, KnackApiResult>
        | ((
              apiPath: string,
              init?: RequestInit,
          ) => KnackApiResult | Promise<KnackApiResult>);
};

export function makeFakeContext(input: FakeContextInput = {}): {
    ctx: KnackContext;
    requests: RequestLog;
    /** appKeys fetched "over the wire" on a cache miss, in order — a cache hit does not append. */
    runtimeMetadataFetches: string[];
} {
    const apps = input.apps ?? [makeApp()];
    const ctx = new KnackContext({
        knackAppsDir: '/tmp/KnackApps',
        apps,
        secrets:
            input.secrets ??
            Object.fromEntries(apps.map((app) => [app.appKey, 'test-key'])),
        options: input.options,
        discover: () => apps,
        readSecrets: () =>
            input.secrets ??
            Object.fromEntries(apps.map((app) => [app.appKey, 'test-key'])),
    });
    const requests: RequestLog = [];
    const runtimeMetadataFetches: string[] = [];

    // Goes through the same cache map production uses, so a handler or guard that
    // clears ctx.caches.runtimeMetadata before reading it — the whole point of "fresh
    // metadata before a mutation" — actually causes a second fetch here, and a test can
    // assert on runtimeMetadataFetches rather than trust that the call was made.
    ctx.getRuntimeMetadata = async (app) => {
        const cached = getCacheEntry(ctx.caches.runtimeMetadata, app.appKey);
        if (cached) return cached.value;

        runtimeMetadataFetches.push(app.appKey);
        const metadata = input.runtimeMetadata?.[app.appKey] ?? null;
        if (metadata) {
            ctx.caches.runtimeMetadata.set(
                app.appKey,
                makeCacheEntry(metadata, 'runtime'),
            );
        }
        return metadata;
    };

    ctx.request = async (app, apiPath, init) => {
        ctx.getApiKey(app.appKey);
        const method = (init?.method || 'GET').toUpperCase();
        requests.push({
            apiPath,
            method,
            body:
                typeof init?.body === 'string'
                    ? JSON.parse(init.body)
                    : (init?.body ?? null),
        });
        const responses = input.responses;
        if (typeof responses === 'function') return responses(apiPath, init);
        const keyed =
            responses?.[`${method} ${apiPath}`] ?? responses?.[apiPath];
        return (
            keyed ?? {
                ok: false,
                status: 404,
                body: { error: `no fake response for ${method} ${apiPath}` },
            }
        );
    };
    ctx.requestWithRetry = (app, apiPath, init) =>
        ctx.request(app, apiPath, init);

    return { ctx, requests, runtimeMetadataFetches };
}

/** The first text block of a tool result, parsed as JSON. */
export function payloadOf(result: ToolResult): Record<string, unknown> {
    return JSON.parse(result.content[0].text) as Record<string, unknown>;
}

/** The trailing prose note of a tool result, if any. */
export function noteOf(result: ToolResult): string | null {
    return result.content[1]?.text ?? null;
}
