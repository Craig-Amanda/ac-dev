import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, describe, it } from 'node:test';

import { KnackContext } from './context.js';
import { makeApp, makeFakeContext } from './testing/fake-context.js';

const RUNTIME = {
    application: {
        objects: [
            {
                key: 'object_1',
                name: 'Clients',
                fields: [
                    { key: 'field_1', name: 'Name', type: 'short_text' },
                    { key: 'field_2', name: 'Email', type: 'email' },
                ],
            },
        ],
        scenes: [{ key: 'scene_1', name: 'Home', slug: 'home', views: [] }],
    },
};

describe('KnackContext app selection', () => {
    it('uses the session app when none is passed, and names the fix when there is none', () => {
        const { ctx } = makeFakeContext();
        assert.throws(() => ctx.getApp(), /knack_set_context or pass appKey/);
        ctx.state.activeAppKey = 'Demo';
        assert.equal(ctx.getApp().appKey, 'Demo');
        assert.throws(() => ctx.getApp('Nope'), /Unknown appKey: Nope/);
    });

    it('infers an app from a path inside its folder, a segment alias, or the basename', () => {
        const apps = [
            makeApp({
                appKey: 'ARC',
                appName: 'Arc Portal',
                appFolder: '/work/KnackApps/ARC',
            }),
            makeApp({
                appKey: 'HR',
                appName: 'People',
                appFolder: '/work/KnackApps/HR',
            }),
        ];
        const { ctx } = makeFakeContext({ apps });
        assert.deepEqual(
            ctx.inferAppKeyFromPath('/work/KnackApps/ARC/js/app.js'),
            {
                appKey: 'ARC',
                inferenceMode: 'direct-folder',
                candidateAppKeys: ['ARC'],
            },
        );
        assert.equal(
            ctx.inferAppKeyFromPath('/elsewhere/arc-portal/x.js').appKey,
            'ARC',
        );
        assert.equal(
            ctx.inferAppKeyFromPath('/elsewhere/notes/people.md').appKey,
            'HR',
        );
        assert.equal(
            ctx.inferAppKeyFromPath('/elsewhere/notes/todo.md').appKey,
            null,
        );
    });

    it('rescan picks up new apps and re-reads secrets', () => {
        let apps = [makeApp()];
        const ctx = new KnackContext({
            knackAppsDir: '/x',
            apps,
            secrets: {},
            discover: () => apps,
            readSecrets: () => ({ Demo: 'k', New: 'k2' }),
        });
        assert.throws(() => ctx.getApiKey('Demo'), /No API key/);
        apps = [makeApp(), makeApp({ appKey: 'New' })];
        assert.equal(ctx.rescanApps().length, 2);
        assert.equal(ctx.getApiKey('New'), 'k2');
        assert.equal(ctx.findApp('New')?.appKey, 'New');
    });
});

describe('KnackContext metadata caches', () => {
    it('serves the schema from runtime metadata and caches it', async () => {
        const { ctx } = makeFakeContext({ runtimeMetadata: { Demo: RUNTIME } });
        const app = ctx.getApp('Demo');
        const first = await ctx.getSchema(app);
        assert.equal(first.source, 'runtime');
        assert.equal(first.schema?.objects?.[0].fields?.length, 2);
        ctx.getRuntimeMetadata = async () => {
            throw new Error('must not refetch');
        };
        assert.equal((await ctx.getSchema(app)).source, 'runtime');
        ctx.invalidate('Demo');
        await assert.rejects(ctx.getSchema(app), /must not refetch/);
    });

    it('falls back to schema.json on disk when the runtime fetch fails', async () => {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'knack-ctx-'));
        after(() => fs.rmSync(dir, { recursive: true, force: true }));
        fs.mkdirSync(path.join(dir, 'schema'));
        fs.writeFileSync(
            path.join(dir, 'schema', 'schema.json'),
            JSON.stringify({
                objects: [{ key: 'object_9', name: 'Disk', fields: [] }],
            }),
        );
        const { ctx } = makeFakeContext({
            apps: [makeApp({ appFolder: dir })],
        });
        const result = await ctx.getSchema(ctx.getApp('Demo'));
        assert.equal(result.source, 'file');
        assert.equal(result.schema?.objects?.[0].key, 'object_9');
        await assert.rejects(
            ctx.requireSchema(
                makeApp({ appKey: 'Other', appFolder: '/nowhere' }),
            ),
            /No schema available/,
        );
    });

    it('finds a field owner through the schema', async () => {
        const { ctx } = makeFakeContext({ runtimeMetadata: { Demo: RUNTIME } });
        const app = ctx.getApp('Demo');
        assert.deepEqual(await ctx.findFieldOwner(app, 'field_2'), {
            objectKey: 'object_1',
            objectName: 'Clients',
            fieldName: 'Email',
        });
        assert.equal(await ctx.findFieldOwner(app, 'field_99'), null);
    });
});

describe('KnackContext HTTP', () => {
    it('adds the Knack auth headers and the app base to every request', async () => {
        const app = makeApp({ apiBase: 'https://eu.example/v1' });
        const ctx = new KnackContext({
            knackAppsDir: '/x',
            apps: [app],
            secrets: { Demo: 'secret' },
        });
        const seen: Array<{ url: string; init: RequestInit }> = [];
        const realFetch = globalThis.fetch;
        globalThis.fetch = (async (
            url: string | URL | Request,
            init?: RequestInit,
        ) => {
            seen.push({ url: String(url), init: init ?? {} });
            return new Response('{"ok":1}', { status: 200 });
        }) as typeof fetch;
        try {
            const result = await ctx.request(app, '/objects/object_1/records', {
                method: 'GET',
            });
            assert.equal(result.ok, true);
            assert.deepEqual(result.body, { ok: 1 });
            assert.equal(
                seen[0].url,
                'https://eu.example/v1/objects/object_1/records',
            );
            const headers = seen[0].init.headers as Record<string, string>;
            assert.equal(headers['X-Knack-Application-Id'], app.appId);
            assert.equal(headers['X-Knack-REST-API-Key'], 'secret');
        } finally {
            globalThis.fetch = realFetch;
        }
    });

    it('shares one in-flight runtime metadata fetch across concurrent callers', async () => {
        const app = makeApp({ apiBase: 'https://eu.example/v1' });
        const ctx = new KnackContext({
            knackAppsDir: '/x',
            apps: [app],
            secrets: { Demo: 'secret' },
        });
        let fetchCount = 0;
        const realFetch = globalThis.fetch;
        globalThis.fetch = (async (url: string | URL | Request) => {
            if (String(url).includes('/v1/applications/')) fetchCount += 1;
            return new Response(JSON.stringify(RUNTIME), { status: 200 });
        }) as typeof fetch;
        try {
            const [a, b] = await Promise.all([
                ctx.getRuntimeMetadata(app),
                ctx.getRuntimeMetadata(app),
            ]);
            assert.equal(fetchCount, 1);
            assert.equal(a, b);
            assert.equal(
                (a?.application as { objects: Array<Record<string, unknown>> })
                    .objects[0].key,
                'object_1',
            );

            ctx.invalidate('Demo');
            await ctx.getRuntimeMetadata(app);
            assert.equal(fetchCount, 2);
        } finally {
            globalThis.fetch = realFetch;
        }
    });

    it('retries 429 for any method, 5xx only for non-POST, and infers a delete after 5xx→404', async () => {
        const { ctx } = makeFakeContext();
        const app = ctx.getApp('Demo');
        const statuses: Record<string, number[]> = {
            POST: [429, 200],
            'POST-5xx': [500, 200],
            PUT: [502, 200],
            DELETE: [503, 404],
        };
        const calls: string[] = [];
        const fakeCtx = ctx as KnackContext & {
            request: KnackContext['request'];
        };
        let scenario = 'POST';
        fakeCtx.request = async (_app, _path, init) => {
            calls.push(scenario);
            const queue = statuses[scenario];
            const status = queue.shift() ?? 200;
            return { ok: status < 300, status, body: { method: init?.method } };
        };
        // requestWithRetry was stubbed by the fake; restore the real one for this test.
        fakeCtx.requestWithRetry = KnackContext.prototype.requestWithRetry;

        assert.equal(
            (await fakeCtx.requestWithRetry(app, '/x', { method: 'POST' }))
                .status,
            200,
        );
        scenario = 'POST-5xx';
        assert.equal(
            (await fakeCtx.requestWithRetry(app, '/x', { method: 'POST' }))
                .status,
            500,
        );
        scenario = 'PUT';
        assert.equal(
            (await fakeCtx.requestWithRetry(app, '/x', { method: 'PUT' }))
                .status,
            200,
        );
        scenario = 'DELETE';
        const inferred = await fakeCtx.requestWithRetry(app, '/x', {
            method: 'DELETE',
        });
        assert.equal(inferred.ok, true);
        assert.equal(
            (inferred.body as { inferredSuccess: boolean }).inferredSuccess,
            true,
        );
    });
});

describe('KnackContext runtime metadata needs an accepted API key', () => {
    const app = makeApp({ apiBase: 'https://eu.example/v1' });
    const APP_URL =
        'https://eu.example/v1/applications/000000000000000000000000';
    const OBJECT_URL = 'https://eu.example/v1/objects/object_1';

    type Call = { url: string; key?: string };

    async function withFetch<T>(
        answer: (url: string) => Response,
        run: (seen: Call[]) => Promise<T>,
    ): Promise<T> {
        const seen: Call[] = [];
        const realFetch = globalThis.fetch;
        globalThis.fetch = (async (
            url: string | URL | Request,
            init?: RequestInit,
        ) => {
            const headers = (init?.headers ?? {}) as Record<string, string>;
            seen.push({
                url: String(url),
                key: headers['X-Knack-REST-API-Key'],
            });
            return answer(String(url));
        }) as typeof fetch;
        try {
            return await run(seen);
        } finally {
            globalThis.fetch = realFetch;
        }
    }

    const okRuntime = () =>
        new Response(JSON.stringify(RUNTIME), { status: 200 });
    const objectAnswer =
        (status: number) =>
        (url: string): Response =>
            url.includes('/applications/')
                ? okRuntime()
                : new Response('{}', { status });

    function contextWith(
        secrets: Record<string, string>,
        target = app,
    ): { ctx: KnackContext; secrets: Record<string, string> } {
        const ctx = new KnackContext({
            knackAppsDir: '/x',
            apps: [target],
            secrets,
            discover: () => [target],
            readSecrets: () => secrets,
        });
        return { ctx, secrets };
    }

    function tempAppFolder(files: Record<string, unknown>): string {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'knack-key-'));
        fs.mkdirSync(path.join(dir, 'schema'));
        for (const [name, body] of Object.entries(files)) {
            fs.writeFileSync(
                path.join(dir, 'schema', name),
                JSON.stringify(body),
            );
        }
        return dir;
    }

    it('withholds the metadata before any request when the app has no key', async () => {
        const { ctx } = contextWith({});
        await withFetch(okRuntime, async (seen) => {
            assert.equal(await ctx.getRuntimeMetadata(app), null);
            assert.match(
                ctx.metadataRefusal(app) ?? '',
                /No API key found for appKey "Demo"/,
            );
            await assert.rejects(
                ctx.requireSchema(app),
                /No schema available for "Demo".*No API key found/,
            );
            assert.equal(ctx.apiKeyStatus('Demo'), 'missing');
            assert.equal(seen.length, 0);
        });
    });

    it('checks the key once, with the key it captured, then serves the cache', async () => {
        const { ctx } = contextWith({ Demo: 'secret' });
        await withFetch(objectAnswer(200), async (seen) => {
            assert.ok(await ctx.getRuntimeMetadata(app));
            assert.deepEqual(
                seen.map((call) => call.url),
                [APP_URL, OBJECT_URL],
            );
            assert.equal(seen[1].key, 'secret');
            assert.equal(ctx.apiKeyStatus('Demo'), 'accepted');

            ctx.caches.runtimeMetadata.delete('Demo');
            await ctx.getRuntimeMetadata(app);
            assert.equal(seen.length, 3, 'an accepted key is not re-checked');
        });
    });

    it('keeps the warm cache across a rescan when the key is unchanged', async () => {
        const { ctx } = contextWith({ Demo: 'secret' });
        await withFetch(objectAnswer(200), async (seen) => {
            await ctx.getRuntimeMetadata(app);
            ctx.rescanApps();
            await ctx.getRuntimeMetadata(app);
            await ctx.getSchema(app);
            assert.equal(seen.length, 2);
        });
    });

    it('remembers a rejected key, so the payload is not downloaded again', async () => {
        const { ctx } = contextWith({ Demo: 'placeholder' });
        await withFetch(objectAnswer(401), async (seen) => {
            assert.equal(await ctx.getRuntimeMetadata(app), null);
            assert.match(
                ctx.metadataRefusal(app) ?? '',
                /Knack rejected the REST API key for appKey "Demo"/,
            );
            assert.equal(ctx.caches.runtimeMetadata.has('Demo'), false);
            assert.equal(ctx.apiKeyStatus('Demo'), 'rejected');

            await ctx.getRuntimeMetadata(app);
            await ctx.getFieldMap(app);
            assert.equal(seen.length, 2);
        });
    });

    it('serves the payload on a transient failure and checks the key again next time', async () => {
        const { ctx } = contextWith({ Demo: 'secret' });
        let objectStatus = 429;
        await withFetch(
            (url) =>
                url.includes('/applications/')
                    ? okRuntime()
                    : new Response('{}', { status: objectStatus }),
            async (seen) => {
                const realSetTimeout = globalThis.setTimeout;
                globalThis.setTimeout = ((fn: () => void) =>
                    realSetTimeout(fn, 0)) as typeof setTimeout;
                try {
                    assert.ok(await ctx.getRuntimeMetadata(app));
                } finally {
                    globalThis.setTimeout = realSetTimeout;
                }
                assert.equal(ctx.apiKeyStatus('Demo'), 'unchecked');

                objectStatus = 200;
                const before = seen.length;
                assert.ok(await ctx.getRuntimeMetadata(app));
                assert.deepEqual(
                    seen.slice(before).map((call) => call.url),
                    [OBJECT_URL],
                    'the cached payload is re-checked, not downloaded again',
                );
                assert.equal(ctx.apiKeyStatus('Demo'), 'accepted');
            },
        );
    });

    it('checks the key against the first object that has a key', async () => {
        const { ctx } = contextWith({ Demo: 'secret' });
        const runtime = {
            application: {
                objects: [{ name: 'No key' }, ...RUNTIME.application.objects],
                scenes: [],
            },
        };
        await withFetch(
            (url) =>
                url.includes('/applications/')
                    ? new Response(JSON.stringify(runtime), { status: 200 })
                    : new Response('{}', { status: 200 }),
            async (seen) => {
                assert.ok(await ctx.getRuntimeMetadata(app));
                assert.equal(seen[1].url, OBJECT_URL);
            },
        );

        const { ctx: none } = contextWith({ Demo: 'secret' });
        const unusable = {
            application: { objects: [{ name: 'No key' }], scenes: [] },
        };
        await withFetch(
            () => new Response(JSON.stringify(unusable), { status: 200 }),
            async (seen) => {
                assert.equal(await none.getRuntimeMetadata(app), null);
                assert.equal(seen.length, 1);
                assert.equal(none.apiKeyStatus('Demo'), 'unchecked');
            },
        );
    });

    it('drops views built with a key once the key is removed', async () => {
        const { ctx, secrets } = contextWith({ Demo: 'secret' });
        await withFetch(objectAnswer(200), async () => {
            assert.equal((await ctx.getSchema(app)).source, 'runtime');
            delete secrets.Demo;
            ctx.rescanApps();
            assert.deepEqual(await ctx.getSchema(app), {
                schema: null,
                source: null,
            });
        });
    });

    it('keeps every disk fallback working when there is no key', async () => {
        const dir = tempAppFolder({
            'fieldMap.json': { 'object_9.name': 'field_90' },
            'fieldReferenceIndex.json': {
                field_90: { fieldKey: 'field_90', references: [] },
            },
        });
        const local = makeApp({ appFolder: dir });
        const { ctx } = contextWith({}, local);
        try {
            await withFetch(okRuntime, async (seen) => {
                const { fieldMap, source } = await ctx.getFieldMap(local);
                assert.equal(source, 'file');
                assert.ok(fieldMap && Object.keys(fieldMap).length);

                assert.deepEqual(await ctx.getScenes(local), []);
                const { index } = await ctx.getFieldReferenceIndex(local);
                assert.ok(index);
                assert.equal(seen.length, 0);
            });
        } finally {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    it('falls back to the schema file on disk when there is no key', async () => {
        const dir = tempAppFolder({
            'schema.json': {
                objects: [{ key: 'object_9', name: 'Local', fields: [] }],
            },
        });
        const local = makeApp({ appFolder: dir });
        const { ctx } = contextWith({}, local);
        try {
            await withFetch(okRuntime, async (seen) => {
                const { schema, source } = await ctx.getSchema(local);
                assert.equal(source, 'file');
                assert.equal(schema?.objects?.[0]?.key, 'object_9');
                assert.equal(seen.length, 0);
            });
        } finally {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });
});
