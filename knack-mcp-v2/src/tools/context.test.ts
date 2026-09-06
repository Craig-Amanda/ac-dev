import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, it } from 'node:test';

import { getCacheEntry } from '../lib/cache.js';
import {
    makeApp,
    makeFakeContext,
    noteOf,
    payloadOf,
} from '../testing/fake-context.js';
import type { RuntimeMetadata } from '../types.js';
import { cache, contextTools, listApps, setContext } from './context.js';

/** A small runtime payload: enough for every derived cache to load from 'runtime'. */
const RUNTIME_METADATA: RuntimeMetadata = {
    application: { name: 'Demo', slug: 'demo', account: { slug: 'acct' } },
    objects: [
        {
            key: 'object_1',
            name: 'Companies',
            fields: [
                { key: 'field_1', name: 'Company Name', type: 'short_text' },
            ],
        },
        {
            key: 'object_2',
            name: 'Contacts',
            fields: [
                { key: 'field_2', name: 'Full Name', type: 'short_text' },
                {
                    key: 'field_3',
                    name: 'Company',
                    type: 'connection',
                    relationship: {
                        object: 'object_1',
                        has: 'one',
                        belongs_to: 'many',
                    },
                },
            ],
        },
    ],
    scenes: [
        {
            key: 'scene_1',
            name: 'Home',
            slug: 'home',
            views: [
                {
                    key: 'view_1',
                    name: 'Companies',
                    type: 'table',
                    source: { object: 'object_1' },
                    columns: [{ field: { key: 'field_1' } }],
                },
            ],
        },
    ],
};

describe('contextTools catalogue', () => {
    it('lists the three tools in order', () => {
        assert.deepEqual(
            contextTools.map((tool) => tool.name),
            ['knack_list_apps', 'knack_set_context', 'knack_cache'],
        );
        assert.ok(contextTools.every((tool) => tool.access === 'read'));
    });
});

describe('knack_list_apps', () => {
    it('re-scans apps and reports permissions plus a human-readable note', async () => {
        const apps = [
            makeApp(),
            makeApp({
                appKey: 'Other',
                appName: 'Other',
                readonly: true,
                allowDelete: false,
            }),
        ];
        const { ctx } = makeFakeContext({ apps });
        ctx.state.activeAppKey = 'Demo';

        const result = await listApps.handler({}, ctx);
        const payload = payloadOf(result);

        assert.equal(payload.ok, true);
        assert.equal(payload.knackAppsDir, '/tmp/KnackApps');
        assert.equal(payload.activeAppKey, 'Demo');
        assert.deepEqual(payload.humanConfirmation, {
            available: false,
            client: null,
            message:
                'This client did not advertise the elicitation capability, so no human can be prompted. Any mutation that would delete child pages is refused, with no override. Make such changes in the Knack builder.',
        });
        assert.ok(
            typeof (payload.cascadeDeleteBehaviour as { mode: string }).mode ===
                'string',
        );
        assert.equal(
            (payload.serverBuild as { name: string }).name,
            'knack-mcp',
        );
        assert.deepEqual(payload.apps, [
            {
                appKey: 'Demo',
                appName: 'Demo',
                appId: '000000000000000000000000',
                readonly: false,
                allowViewMutation: true,
                allowDelete: true,
                allowDiagnostics: true,
            },
            {
                appKey: 'Other',
                appName: 'Other',
                appId: '000000000000000000000000',
                readonly: true,
                allowViewMutation: true,
                allowDelete: false,
                allowDiagnostics: true,
            },
        ]);

        const note = noteOf(result);
        assert.ok(note);
        assert.match(
            note,
            /^Knack apps: 2 discovered in \/tmp\/KnackApps\. Active app: Demo\./,
        );
        assert.match(note, /Writable: Demo\./);
        assert.match(note, /Cascade deletes: refused\./);
        assert.match(note, /Build: knack-mcp/);
    });

    it('reports enforced read-only mode in the note', async () => {
        const { ctx } = makeFakeContext({ options: { readOnly: true } });
        const result = await listApps.handler({}, ctx);
        assert.equal(
            (payloadOf(result).serverBuild as { mode: string }).mode,
            'readonly',
        );
        assert.match(noteOf(result) ?? '', /Writes: none\./);
    });
});

describe('knack_set_context', () => {
    it('activates an explicit appKey and records the context path', async () => {
        const { ctx } = makeFakeContext();
        const result = await setContext.handler(
            { appKey: 'Demo', contextPath: '/work/notes.md' },
            ctx,
        );
        assert.deepEqual(payloadOf(result), {
            ok: true,
            activeAppKey: 'Demo',
            contextPath: '/work/notes.md',
            inferenceMode: 'explicit-appkey',
        });
        assert.equal(ctx.state.activeAppKey, 'Demo');
        assert.equal(ctx.state.lastContextPath, '/work/notes.md');
    });

    it('refuses an unknown appKey and lists the available ones', async () => {
        const { ctx } = makeFakeContext();
        const result = await setContext.handler({ appKey: 'Nope' }, ctx);
        assert.deepEqual(payloadOf(result), {
            ok: false,
            message: 'Unknown appKey: Nope',
            availableApps: ['Demo'],
        });
        assert.equal(ctx.state.activeAppKey, null);
    });

    it('requires either appKey or contextPath', async () => {
        const { ctx } = makeFakeContext();
        const result = await setContext.handler({}, ctx);
        assert.deepEqual(payloadOf(result), {
            ok: false,
            message: 'Provide either appKey or contextPath.',
            availableApps: ['Demo'],
        });
    });

    it('infers the app from a path inside its folder', async () => {
        const { ctx } = makeFakeContext();
        const result = await setContext.handler(
            { contextPath: '/tmp/KnackApps/Demo/js/app.js' },
            ctx,
        );
        assert.deepEqual(payloadOf(result), {
            ok: true,
            activeAppKey: 'Demo',
            contextPath: '/tmp/KnackApps/Demo/js/app.js',
            inferenceMode: 'direct-folder',
        });
        assert.equal(
            ctx.state.lastContextPath,
            '/tmp/KnackApps/Demo/js/app.js',
        );
    });

    it('explains when no app can be inferred from the path', async () => {
        const { ctx } = makeFakeContext();
        const result = await setContext.handler(
            { contextPath: '/elsewhere/thing.ts' },
            ctx,
        );
        const payload = payloadOf(result);
        assert.equal(payload.ok, false);
        assert.equal(
            payload.message,
            'Could not infer appKey from the given contextPath.',
        );
        assert.equal(payload.contextPath, '/elsewhere/thing.ts');
        assert.match(String(payload.hint), /KnackApps\/<AppKey>/);
        assert.deepEqual(payload.candidateAppKeys, []);
        assert.deepEqual(payload.availableApps, ['Demo']);
        assert.equal(ctx.state.activeAppKey, null);
    });
});

describe('knack_cache (status)', () => {
    it('reports file candidates and empty caches for a cold app', async () => {
        const { ctx } = makeFakeContext();
        const result = await cache.handler(
            { appKey: 'Demo', refresh: false, warm: false, persistFiles: true },
            ctx,
        );
        const payload = payloadOf(result);
        assert.equal(payload.ok, true);
        assert.equal(payload.appKey, 'Demo');
        assert.equal(payload.activeAppKey, null);
        assert.equal(payload.lastContextPath, null);
        assert.equal(typeof payload.cacheTtlMs, 'number');

        const files = payload.files as Record<string, unknown>;
        assert.equal(
            files.schemaPath,
            '/tmp/KnackApps/Demo/schema/schema.json',
        );
        assert.equal(files.schemaExists, false);
        assert.deepEqual(files.schemaPathCandidates, [
            '/tmp/KnackApps/Demo/schema/schema.json',
            '/tmp/KnackApps/Demo/schema.json',
        ]);
        assert.equal(files.fieldMapExists, false);
        assert.equal(files.viewMapExists, false);
        assert.equal(files.fieldReferenceIndexExists, false);
        assert.equal(
            files.fieldReferenceIndexPath,
            '/tmp/KnackApps/Demo/schema/fieldReferenceIndex.json',
        );

        assert.deepEqual(payload.cache, {
            schema: { cached: false },
            fieldMap: { cached: false },
            viewMap: { cached: false },
            runtimeMetadata: { cached: false },
            fieldReferences: { cached: false },
        });
    });

    it('describes warm cache entries with source and expiry', async () => {
        const { ctx } = makeFakeContext({
            runtimeMetadata: { Demo: RUNTIME_METADATA },
        });
        const app = ctx.getApp('Demo');
        await ctx.getSchema(app);
        await ctx.getFieldReferenceIndex(app);

        const payload = payloadOf(
            await cache.handler(
                {
                    appKey: 'Demo',
                    refresh: false,
                    warm: false,
                    persistFiles: true,
                },
                ctx,
            ),
        );
        const status = payload.cache as Record<string, Record<string, unknown>>;
        assert.equal(status.schema.cached, true);
        assert.equal(status.schema.source, 'runtime');
        assert.match(String(status.schema.loadedAt), /^\d{4}-\d{2}-\d{2}T/);
        assert.match(String(status.schema.expiresAt), /^\d{4}-\d{2}-\d{2}T/);
        assert.ok((status.schema.expiresInMs as number) > 0);
        assert.equal(status.fieldReferences.cached, true);
        assert.equal(status.fieldReferences.source, 'runtime');
        // getSchema's own fetch warms this cache too, same as production. No `source`
        // here — runtime metadata has no file-backed alternative to distinguish it from.
        assert.equal(status.runtimeMetadata.cached, true);
        assert.equal(status.runtimeMetadata.source, undefined);
        assert.match(
            String(status.runtimeMetadata.loadedAt),
            /^\d{4}-\d{2}-\d{2}T/,
        );
    });

    it('needs an app when reporting status', async () => {
        const { ctx } = makeFakeContext();
        await assert.rejects(
            cache.handler(
                { refresh: false, warm: false, persistFiles: true },
                ctx,
            ),
            /No app selected/,
        );
    });
});

describe('knack_cache (refresh)', () => {
    const tempDirs: string[] = [];
    afterEach(() => {
        for (const dir of tempDirs.splice(0))
            fs.rmSync(dir, { recursive: true, force: true });
    });

    it('clears one app and reports sizes before and after', async () => {
        const { ctx } = makeFakeContext({
            apps: [makeApp(), makeApp({ appKey: 'Other', appName: 'Other' })],
            runtimeMetadata: {
                Demo: RUNTIME_METADATA,
                Other: RUNTIME_METADATA,
            },
        });
        await ctx.getSchema(ctx.getApp('Demo'));
        await ctx.getSchema(ctx.getApp('Other'));

        const payload = payloadOf(
            await cache.handler(
                {
                    appKey: 'Demo',
                    refresh: true,
                    warm: false,
                    persistFiles: false,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.target, 'Demo');
        assert.equal(payload.warm, false);
        assert.equal(payload.persistFiles, false);
        assert.equal(payload.persistSkipped, undefined);
        assert.equal(payload.appCount, 1);
        assert.equal((payload.beforeSizes as { schema: number }).schema, 2);
        assert.equal((payload.afterSizes as { schema: number }).schema, 1);
        assert.deepEqual(payload.warmed, []);
        assert.equal(ctx.caches.schema.has('Demo'), false);
        assert.equal(ctx.caches.schema.has('Other'), true);
    });

    it('explains that persistFiles without warm writes nothing', async () => {
        const { ctx } = makeFakeContext();
        const payload = payloadOf(
            await cache.handler(
                { refresh: true, warm: false, persistFiles: true },
                ctx,
            ),
        );
        assert.equal(payload.target, 'all');
        assert.match(
            String(payload.persistSkipped),
            /^Nothing was written\. persistFiles only takes effect with warm: true/,
        );
        assert.deepEqual(payload.warmed, []);
    });

    it('warms every cache and persists file names into the app folder', async () => {
        const appFolder = fs.mkdtempSync(
            path.join(os.tmpdir(), 'knack-mcp-cache-'),
        );
        tempDirs.push(appFolder);
        const { ctx } = makeFakeContext({
            apps: [makeApp({ appFolder })],
            runtimeMetadata: { Demo: RUNTIME_METADATA },
        });

        const payload = payloadOf(
            await cache.handler(
                {
                    appKey: 'Demo',
                    refresh: true,
                    warm: true,
                    persistFiles: true,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.warm, true);
        assert.equal(payload.persistSkipped, undefined);
        assert.deepEqual(payload.warmed, [
            {
                appKey: 'Demo',
                ok: true,
                runtimeMetadataLoaded: true,
                sources: {
                    schema: 'runtime',
                    fieldMap: 'runtime',
                    viewMap: 'runtime',
                    fieldReferences: 'runtime',
                },
                persisted: [
                    'schema',
                    'fieldMap',
                    'viewMap',
                    'fieldReferenceIndex',
                ],
            },
        ]);
        assert.equal((payload.afterSizes as { schema: number }).schema, 1);
        assert.equal(
            (payload.afterSizes as { fieldReferences: number }).fieldReferences,
            1,
        );

        for (const name of [
            'schema.json',
            'fieldMap.json',
            'viewMap.json',
            'fieldReferenceIndex.json',
        ]) {
            const filePath = path.join(appFolder, 'schema', name);
            assert.ok(fs.existsSync(filePath), `${name} written`);
        }
        const schema = JSON.parse(
            fs.readFileSync(
                path.join(appFolder, 'schema', 'schema.json'),
                'utf8',
            ),
        );
        assert.equal(schema.objects.length, 2);
    });

    it('warms without persisting when persistFiles is false', async () => {
        const appFolder = fs.mkdtempSync(
            path.join(os.tmpdir(), 'knack-mcp-cache-'),
        );
        tempDirs.push(appFolder);
        const { ctx } = makeFakeContext({
            apps: [makeApp({ appFolder })],
            runtimeMetadata: { Demo: RUNTIME_METADATA },
        });
        const payload = payloadOf(
            await cache.handler(
                { refresh: true, warm: true, persistFiles: false },
                ctx,
            ),
        );
        const warmed = payload.warmed as Array<Record<string, unknown>>;
        assert.equal(warmed.length, 1);
        assert.equal(warmed[0].persisted, undefined);
        assert.equal(fs.existsSync(path.join(appFolder, 'schema')), false);
    });

    it('reports a per-app failure when warming finds no metadata', async () => {
        const { ctx } = makeFakeContext({ runtimeMetadata: { Demo: null } });
        const payload = payloadOf(
            await cache.handler(
                {
                    appKey: 'Demo',
                    refresh: true,
                    warm: true,
                    persistFiles: true,
                },
                ctx,
            ),
        );
        assert.deepEqual(payload.warmed, [
            {
                appKey: 'Demo',
                ok: true,
                runtimeMetadataLoaded: false,
                sources: {
                    schema: null,
                    fieldMap: null,
                    viewMap: null,
                    fieldReferences: null,
                },
                persisted: [],
            },
        ]);
    });

    it('warms multiple apps concurrently rather than one at a time, in stable order', async () => {
        const appA = makeApp({
            appKey: 'A',
            appFolder: fs.mkdtempSync(
                path.join(os.tmpdir(), 'knack-mcp-cache-'),
            ),
        });
        const appB = makeApp({
            appKey: 'B',
            appFolder: fs.mkdtempSync(
                path.join(os.tmpdir(), 'knack-mcp-cache-'),
            ),
        });
        tempDirs.push(appA.appFolder, appB.appFolder);
        const { ctx } = makeFakeContext({
            apps: [appA, appB],
            runtimeMetadata: { A: RUNTIME_METADATA, B: RUNTIME_METADATA },
        });
        const DELAY_MS = 60;
        const fakeGetRuntimeMetadata = ctx.getRuntimeMetadata.bind(ctx);
        // Only the real cache-miss fetch is slow — a cache hit (the other four loaders
        // that warmOneApp also calls per app) must stay instant, exactly as production
        // behaves, or this would time every call rather than only the network fetch.
        ctx.getRuntimeMetadata = async (app) => {
            const cached = getCacheEntry(
                ctx.caches.runtimeMetadata,
                app.appKey,
            );
            if (cached) return fakeGetRuntimeMetadata(app);
            await new Promise((resolve) => setTimeout(resolve, DELAY_MS));
            return fakeGetRuntimeMetadata(app);
        };

        const start = Date.now();
        const payload = payloadOf(
            await cache.handler(
                { refresh: true, warm: true, persistFiles: false },
                ctx,
            ),
        );
        const elapsed = Date.now() - start;

        const warmed = payload.warmed as Array<Record<string, unknown>>;
        assert.deepEqual(
            warmed.map((entry) => entry.appKey),
            ['A', 'B'],
        );
        assert.ok(
            elapsed < DELAY_MS * 2,
            `expected concurrent warming to take under ${DELAY_MS * 2}ms, took ${elapsed}ms`,
        );
    });

    it('rejects an unknown appKey', async () => {
        const { ctx } = makeFakeContext();
        await assert.rejects(
            cache.handler(
                {
                    appKey: 'Nope',
                    refresh: true,
                    warm: false,
                    persistFiles: true,
                },
                ctx,
            ),
            /Unknown appKey: Nope/,
        );
    });
});
