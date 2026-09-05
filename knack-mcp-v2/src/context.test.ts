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
        assert.equal(first.schema?.objects[0].fields?.length, 2);
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
        assert.equal(result.schema?.objects[0].key, 'object_9');
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
