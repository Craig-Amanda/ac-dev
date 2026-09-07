import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import { ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';

import type { KnackApiResult } from '../http.js';
import {
    type RequestLog,
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import { buildProfileNameIndex } from '../lib/page-access.js';
import {
    askHumanToConfirmPageDeletion,
    describeAudienceConsequence,
} from '../view-mutation.js';
import type { RuntimeMetadata, SceneInfo } from '../types.js';
import {
    copyView,
    createView,
    deleteView,
    moveView,
    updateView,
    updateViewOrder,
} from './view-mutations.js';

/**
 * These drive the mutation tools end to end: real guard, real snapshot to a temp folder,
 * fake REST transport. The fake context has no MCP server, so no human can be asked —
 * every cascade-risky mutation must therefore be refused with nothing sent.
 */

/** A table whose one link column owns scene_2 (Knack writes the parent as a slug). */
const TABLE_VIEW = {
    key: 'view_1',
    name: 'Contacts table',
    type: 'table',
    title: 'Contacts',
    source: {
        object: 'object_1',
        criteria: { match: 'all', rules: [], groups: [] },
        sort: [],
        limit: '',
    },
    columns: [
        { type: 'field', field: { key: 'field_1' }, header: 'Name' },
        { type: 'link', header: 'Edit', scene: 'edit-contact' },
    ],
    links: [],
    groups: [],
    inputs: [],
    no_data_text: 'No Contact Records',
};

const MENU_VIEW = {
    key: 'view_2',
    name: 'Nav',
    type: 'menu',
    links: [{ name: 'Reports', type: 'scene', scene: 'reports' }],
};

const RICH_TEXT_VIEW = {
    key: 'view_3',
    name: 'Notes',
    type: 'rich_text',
    content: '<p>Hi</p>',
};

function makeMetadata(): RuntimeMetadata {
    return {
        application: {
            name: 'Demo',
            slug: 'demo',
            objects: [
                {
                    key: 'object_1',
                    name: 'Contact',
                    fields: [
                        { key: 'field_1', name: 'Name', type: 'short_text' },
                    ],
                },
            ],
            scenes: [
                {
                    key: 'scene_1',
                    name: 'Contacts',
                    slug: 'contacts',
                    views: [TABLE_VIEW, MENU_VIEW, RICH_TEXT_VIEW],
                },
                {
                    key: 'scene_2',
                    name: 'Edit contact',
                    slug: 'edit-contact',
                    parent: 'contacts',
                    views: [{ key: 'view_4', name: 'Edit form', type: 'form' }],
                },
                { key: 'scene_3', name: 'Reports', slug: 'reports', views: [] },
            ],
        },
    };
}

let tmpDir: string;

before(() => {
    tmpDir = fs.mkdtempSync(
        path.join(os.tmpdir(), 'knack-mcp-v2-view-mutations-'),
    );
});

after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
});

function makeCtx(
    responses?: Record<string, KnackApiResult>,
    metadata: RuntimeMetadata | null = makeMetadata(),
): { ctx: ReturnType<typeof makeFakeContext>['ctx']; requests: RequestLog } {
    const app = makeApp({ appFolder: tmpDir });
    return makeFakeContext({
        apps: [app],
        runtimeMetadata: { [app.appKey]: metadata },
        responses,
    });
}

function snapshotFiles(): string[] {
    const dir = path.join(tmpDir, 'schema', 'snapshots');
    return fs.existsSync(dir) ? fs.readdirSync(dir) : [];
}

describe('knack_create_view', () => {
    it('posts the caller payload verbatim and reports the created view', async () => {
        const before = snapshotFiles().length;
        const payload = JSON.stringify({
            name: 'New notes',
            type: 'rich_text',
            content: '<p>x</p>',
            pageGroups: [{ columns: [{ keys: ['new'], width: 100 }] }],
        });
        const { ctx, requests } = makeCtx({
            'POST /scenes/scene_3/views': {
                ok: true,
                status: 200,
                body: {
                    view: { key: 'view_10', name: 'New notes' },
                    changes: {
                        inserts: { scenes: [], views: [{ key: 'view_10' }] },
                    },
                },
            },
        });

        const result = payloadOf(
            await createView.handler(
                { appKey: 'Demo', sceneKey: 'scene_3', payload },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.action, 'create_view');
        assert.equal(result.sceneKey, 'scene_3');
        assert.equal(requests.length, 1);
        assert.equal(requests[0].method, 'POST');
        assert.equal(requests[0].apiPath, '/scenes/scene_3/views');
        assert.deepEqual(requests[0].body, JSON.parse(payload));
        assert.deepEqual((result.body as Record<string, unknown>).changes, {
            inserts: { views: ['view_10'] },
        });
        assert.ok(typeof result.cacheNote === 'string');
        // A create destroys nothing, so the guard takes no snapshot before it — but
        // the view it made is snapshotted afterwards, so a page tree built by creates
        // and copies alone can still be rebuilt (recovery drill, 6 September).
        assert.equal(snapshotFiles().length, before + 1);
        assert.match(
            path.basename(String(result.snapshotPath)),
            /-create_view-view_10-\d+\.json$/,
        );
        const snapshot = JSON.parse(
            fs.readFileSync(String(result.snapshotPath), 'utf8'),
        );
        assert.equal(snapshot.action, 'create_view');
        assert.equal(snapshot.sceneKey, 'scene_3');
        assert.equal(snapshot.viewKey, 'view_10');
        assert.deepEqual(snapshot.view, { key: 'view_10', name: 'New notes' });
        assert.equal('snapshotNote' in result, false);
    });

    it('refuses an unparseable payload and sends nothing', async () => {
        const { ctx, requests } = makeCtx();
        const result = payloadOf(
            await createView.handler(
                { appKey: 'Demo', sceneKey: 'scene_3', payload: '{not json' },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.equal(result.error, 'INVALID_UPDATES_JSON');
        assert.equal(requests.length, 0);
    });
});

describe('knack_update_view_order', () => {
    it('posts the parsed order and pageGroups to the sort route', async () => {
        const order = ['view_3', 'view_1', 'view_2'];
        const pageGroups = [{ columns: [{ keys: order, width: 100 }] }];
        const { ctx, requests } = makeCtx({
            'POST /scenes/scene_1/views/sort': {
                ok: true,
                status: 200,
                body: { ok: true },
            },
        });

        const result = payloadOf(
            await updateViewOrder.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    order: JSON.stringify(order),
                    pageGroups: JSON.stringify(pageGroups),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.action, 'update_view_order');
        assert.equal(requests.length, 1);
        assert.equal(requests[0].apiPath, '/scenes/scene_1/views/sort');
        assert.deepEqual(requests[0].body, { order, pageGroups });
    });

    it('takes the order as an array and derives pageGroups when none is given', async () => {
        // `pageGroups` was required and both inputs were JSON strings only, so a caller
        // passing the array, or leaving the layout alone, failed schema validation
        // before this handler ran (6 September). Knack's sort route needs both, so a
        // missing layout becomes one full-width row per view, in the order given.
        const { ctx, requests } = makeCtx({
            'POST /scenes/scene_1/views/sort': {
                ok: true,
                status: 200,
                body: { ok: true },
            },
        });
        const parsed = z.object(updateViewOrder.input).parse({
            appKey: 'Demo',
            sceneKey: 'scene_1',
            order: ['view_2', 'view_1'],
        });
        const result = payloadOf(await updateViewOrder.handler(parsed, ctx));
        assert.equal(result.ok, true, JSON.stringify(result));
        assert.deepEqual(requests[0].body, {
            order: ['view_2', 'view_1'],
            pageGroups: [
                { columns: [{ keys: ['view_2'], width: 100 }] },
                { columns: [{ keys: ['view_1'], width: 100 }] },
            ],
        });
    });

    it('refuses an empty or malformed order list in either form before anything is sent', async () => {
        const { ctx, requests } = makeCtx();
        for (const order of [[], '[]', ['view_1', ''], '["view_1", 3]']) {
            const parsed = z.object(updateViewOrder.input).parse({
                appKey: 'Demo',
                sceneKey: 'scene_1',
                order,
            });
            await assert.rejects(
                updateViewOrder.handler(parsed, ctx),
                /order must be a non-empty array of view keys/,
                JSON.stringify(order),
            );
        }
        assert.equal(requests.length, 0);
    });

    it('refuses an empty order before anything is sent', async () => {
        const { ctx, requests } = makeCtx();
        await assert.rejects(
            updateViewOrder.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    order: '   ',
                    pageGroups: '[]',
                },
                ctx,
            ),
            /order cannot be empty/,
        );
        assert.equal(requests.length, 0);
    });
});

describe('knack_update_view', () => {
    it('warns when the sent body adds a link to a page that does not exist', async () => {
        // Measured 6 September (A3's precondition): a link column pointing at a slug no
        // page has was stored by Knack with an empty change set, and the guard said
        // nothing — it only asks about links a mutation removes. The page it points at
        // opens nothing, so the caller is told, with the ref and where it sits.
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' }, changes: {} },
            },
        });
        const columns = [
            ...TABLE_VIEW.columns,
            { type: 'link', header: 'Nowhere', scene: 'no-such-page' },
        ];
        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    updates: JSON.stringify({ columns }),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(requests.length, 1, 'still sent: nothing is destroyed');
        assert.deepEqual(result.danglingLinks, [
            { ref: 'no-such-page', sourcePaths: ['$.columns[2]'] },
        ]);
        assert.match(String(result.warning), /no-such-page/);
        assert.match(String(result.warning), /opens nothing/);
        // The kept link to the real child page is not reported.
        assert.doesNotMatch(String(result.warning), /edit-contact/);
    });

    it('does not count a menu entry that points outside the app as dangling', async () => {
        const { ctx } = makeCtx({
            'PUT /scenes/scene_1/views/view_2': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_2' }, changes: {} },
            },
        });
        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_2',
                    updates: JSON.stringify({
                        links: [
                            ...MENU_VIEW.links,
                            {
                                name: 'Docs',
                                type: 'url',
                                url: 'https://example.com',
                            },
                        ],
                    }),
                },
                ctx,
            ),
        );
        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal('danglingLinks' in result, false);
    });

    it('sends a title-only change as one PUT of the merged body, with no prompt', async () => {
        const before = snapshotFiles().length;
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1', title: 'Renamed' } },
            },
        });

        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    updates: JSON.stringify({ title: 'Renamed' }),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.action, 'update_view');
        assert.equal(result.viewKey, 'view_1');
        assert.equal(requests.length, 1);
        assert.equal(requests[0].method, 'PUT');
        assert.equal(requests[0].apiPath, '/scenes/scene_1/views/view_1');

        // The body is the live definition with the one change applied: every link
        // column re-sent, identifiers stripped.
        const sent = requests[0].body as Record<string, unknown>;
        assert.equal(sent.title, 'Renamed');
        assert.equal(sent.name, 'Contacts table');
        assert.deepEqual(sent.columns, TABLE_VIEW.columns);
        assert.deepEqual(sent.source, TABLE_VIEW.source);
        assert.equal('key' in sent, false);
        assert.equal('_id' in sent, false);

        // Nothing was at stake, so nothing is reported as deleted or moved, and the
        // re-sent link resolves to a page, so nothing dangles.
        assert.equal('pagesExpectedToBeDeleted' in result, false);
        assert.equal('linksRemovedPagesKept' in result, false);
        assert.equal('danglingLinks' in result, false);
        assert.equal('warning' in result, false);

        // A restore point was written first.
        assert.ok(
            String(result.snapshotPath).startsWith(
                path.join(tmpDir, 'schema', 'snapshots'),
            ),
        );
        assert.equal(snapshotFiles().length, before + 1);
        const snapshot = JSON.parse(
            fs.readFileSync(String(result.snapshotPath), 'utf8'),
        );
        assert.equal(snapshot.action, 'update_view');
        assert.equal(snapshot.view.key, 'view_1');
    });

    it('refuses an update dropping the sole link to a child page when no human can be asked', async () => {
        const before = snapshotFiles().length;
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    updates: JSON.stringify({
                        columns: [
                            {
                                type: 'field',
                                field: { key: 'field_1' },
                                header: 'Name',
                            },
                        ],
                    }),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        assert.match(String(result.message), /cannot prompt a human/);
        const childPages = result.childPages as Array<Record<string, unknown>>;
        assert.equal(childPages.length, 1);
        assert.equal(childPages[0].sceneKey, 'scene_2');
        assert.equal(requests.length, 0);
        // Refused before the snapshot step, so nothing new on disk either.
        assert.equal(snapshotFiles().length, before);
    });

    it('refuses a payload that changes source.object', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    updates: JSON.stringify({
                        source: { ...TABLE_VIEW.source, object: 'object_2' },
                    }),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'SOURCE_OBJECT_CHANGE_REFUSED');
        assert.equal(result.currentObject, 'object_1');
        assert.equal(result.incomingObject, 'object_2');
        assert.equal(requests.length, 0);
    });

    it('refuses the removed confirmDestructive flag', async () => {
        const { ctx, requests } = makeCtx();
        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_2',
                    updates: JSON.stringify({ links: [] }),
                    confirmDestructive: true,
                },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.equal(result.error, 'CONFIRMATION_UPGRADE_REQUIRED');
        assert.equal(requests.length, 0);
    });

    it('refuses when the runtime metadata cannot be read', async () => {
        const { ctx, requests } = makeCtx(undefined, null);
        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    updates: JSON.stringify({ title: 'x' }),
                },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.equal(result.error, 'COULD_NOT_VERIFY_VIEW');
        assert.equal(requests.length, 0);
    });
});

describe('knack_copy_view', () => {
    it('sharePages false posts to copyview with action copy and the real view key', async () => {
        const { ctx, requests } = makeCtx({
            'POST /scenes/scene_1/copyview': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_11' } },
            },
        });

        const result = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_3',
                    sharePages: false,
                    completeViewSchema: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.action, 'copy_view');
        assert.equal(result.sourceSceneKey, 'scene_1');
        assert.equal(result.targetSceneKey, 'scene_3');
        assert.equal(result.sceneKey, 'scene_1');
        assert.equal(requests.length, 1);
        assert.equal(requests[0].method, 'POST');
        assert.equal(requests[0].apiPath, '/scenes/scene_1/copyview');
        assert.deepEqual(requests[0].body, {
            action: 'copy',
            target_scene_key: 'scene_3',
            view_key: 'view_1',
            completeViewSchema: false,
        });
        // A copy destroys nothing, so nothing is snapshotted before it and it never
        // prompts — even though view_1 owns a child page. The copy it made is
        // snapshotted afterwards, from the response when that carries the view.
        assert.match(
            path.basename(String(result.snapshotPath)),
            /-copy_view-view_11-\d+\.json$/,
        );
        const snapshot = JSON.parse(
            fs.readFileSync(String(result.snapshotPath), 'utf8'),
        );
        assert.equal(snapshot.action, 'copy_view');
        assert.equal(snapshot.viewKey, 'view_11');
        assert.deepEqual(snapshot.view, { key: 'view_11' });
    });

    it('snapshots a copy Knack names only in changes.inserts, read back from metadata', async () => {
        // Knack's real copyview response returns the SOURCE scene and names the new
        // view only under changes.inserts.views (measured 6 September: view_53 made
        // by copying view_51). The definition has to come from a fresh metadata read.
        const metadata = makeMetadata();
        const scenes = (metadata.application as Record<string, unknown>)
            .scenes as Array<Record<string, unknown>>;
        const copied = {
            ...TABLE_VIEW,
            key: 'view_11',
            name: 'Contacts table',
        };
        scenes.find((scene) => scene.key === 'scene_3')!.views = [copied];
        const before = snapshotFiles().length;
        const { ctx } = makeCtx(
            {
                'POST /scenes/scene_1/copyview': {
                    ok: true,
                    status: 200,
                    body: {
                        scene: { key: 'scene_1', views: [TABLE_VIEW] },
                        changes: {
                            inserts: { scenes: [], views: ['view_11'] },
                            updates: { scenes: ['scene_3'] },
                        },
                    },
                },
            },
            metadata,
        );

        const result = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_3',
                    sharePages: false,
                    completeViewSchema: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(snapshotFiles().length, before + 1);
        const snapshot = JSON.parse(
            fs.readFileSync(String(result.snapshotPath), 'utf8'),
        );
        assert.equal(snapshot.action, 'copy_view');
        assert.equal(snapshot.viewKey, 'view_11');
        // Filed under the page the copy landed on, not the source page.
        assert.equal(snapshot.sceneKey, 'scene_3');
        assert.equal(snapshot.view.key, 'view_11');
        assert.deepEqual(snapshot.view.columns, TABLE_VIEW.columns);
        assert.equal('snapshotNote' in result, false);
    });

    it('snapshots a copy whose inserts entry wraps the view, without a metadata read', async () => {
        // Knack's copyview can list the inserted view as `{ view: {...} }` — the shape
        // compactKnackChanges already unwraps. The definition is right there, so no
        // second metadata fetch is needed to file it.
        const copied = { ...TABLE_VIEW, key: 'view_14' };
        const app = makeApp({ appFolder: tmpDir });
        const { ctx, runtimeMetadataFetches } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { [app.appKey]: makeMetadata() },
            responses: {
                'POST /scenes/scene_1/copyview': {
                    ok: true,
                    status: 200,
                    body: {
                        scene: { key: 'scene_1', views: [TABLE_VIEW] },
                        changes: {
                            inserts: { scenes: [], views: [{ view: copied }] },
                            updates: { scenes: ['scene_3'] },
                        },
                    },
                },
            },
        });
        const result = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_3',
                    sharePages: false,
                    completeViewSchema: false,
                },
                ctx,
            ),
        );
        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal('snapshotNote' in result, false);
        const snapshot = JSON.parse(
            fs.readFileSync(String(result.snapshotPath), 'utf8'),
        );
        assert.equal(snapshot.viewKey, 'view_14');
        assert.deepEqual(snapshot.view, copied);
        // One read for the guard; the snapshot's tree needs a second, the view did not.
        assert.deepEqual(runtimeMetadataFetches, ['Demo', 'Demo']);
    });

    it('says so when the copied view is not yet in metadata, and still snapshots the tree', async () => {
        const { ctx } = makeCtx({
            'POST /scenes/scene_1/copyview': {
                ok: true,
                status: 200,
                body: {
                    scene: { key: 'scene_1' },
                    changes: { inserts: { views: ['view_99'] } },
                },
            },
        });
        const result = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_3',
                    sharePages: false,
                    completeViewSchema: false,
                },
                ctx,
            ),
        );
        assert.equal(result.ok, true);
        assert.match(
            path.basename(String(result.snapshotPath)),
            /-copy_view-view_99-\d+\.json$/,
        );
        assert.match(String(result.snapshotNote), /could not be read back/);
        const snapshot = JSON.parse(
            fs.readFileSync(String(result.snapshotPath), 'utf8'),
        );
        assert.equal(snapshot.view, null);
        assert.ok(Array.isArray(snapshot.scenes) && snapshot.scenes.length > 0);
    });

    it('sharePages false requires the source scene', async () => {
        const { ctx, requests } = makeCtx();
        await assert.rejects(
            copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    targetSceneKey: 'scene_3',
                    sharePages: false,
                    completeViewSchema: false,
                },
                ctx,
            ),
            /sourceSceneKey is required when sharePages is false/,
        );
        assert.equal(requests.length, 0);
    });

    it('sharePages true creates from the source definition and verifies the pages were shared', async () => {
        const copyAttributes = {
            ...TABLE_VIEW,
            key: 'view_12',
            name: 'Contacts table Copy',
        };
        const { ctx, requests } = makeCtx({
            'POST /scenes/scene_3/views': {
                ok: true,
                status: 200,
                body: {
                    view: copyAttributes,
                    changes: {
                        inserts: { scenes: [], views: [{ key: 'view_12' }] },
                    },
                },
            },
        });

        const result = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    targetSceneKey: 'scene_3',
                    sharePages: true,
                    completeViewSchema: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.action, 'copy_view_sharing_pages');
        assert.equal(result.performedAs, 'create_view');
        assert.equal(result.sourceSceneKey, 'scene_1');
        assert.equal(result.sourceViewKey, 'view_1');
        assert.equal(result.targetSceneKey, 'scene_3');
        assert.equal(result.sceneKey, 'scene_3');
        assert.deepEqual(result.sharedPages, [
            {
                ref: 'edit-contact',
                sceneKey: 'scene_2',
                sceneName: 'Edit contact',
            },
        ]);
        assert.equal(result.sharedPagesVerified, true);
        assert.equal('sharedPagesProblems' in result, false);
        assert.equal('layoutWarning' in result, false);

        assert.equal(requests.length, 1);
        assert.equal(requests[0].apiPath, '/scenes/scene_3/views');
        const sent = requests[0].body as Record<string, unknown>;
        assert.equal('key' in sent, false);
        assert.equal('_id' in sent, false);
        assert.equal(sent.name, 'Contacts table Copy');
        assert.deepEqual(sent.columns, TABLE_VIEW.columns);
        assert.deepEqual(sent.pageGroups, [
            { columns: [{ keys: ['new'], width: 100 }] },
        ]);
    });

    it('sharePages true fetches runtime metadata once before the create, and once after for the snapshot', async () => {
        const copyAttributes = {
            ...TABLE_VIEW,
            key: 'view_12',
            name: 'Contacts table Copy',
        };
        const app = makeApp({ appFolder: tmpDir });
        const { ctx, runtimeMetadataFetches } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { [app.appKey]: makeMetadata() },
            responses: {
                'POST /scenes/scene_3/views': {
                    ok: true,
                    status: 200,
                    body: {
                        view: copyAttributes,
                        changes: {
                            inserts: {
                                scenes: [],
                                views: [{ key: 'view_12' }],
                            },
                        },
                    },
                },
            },
        });

        const result = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    targetSceneKey: 'scene_3',
                    sharePages: true,
                    completeViewSchema: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        // One read resolves the source and feeds the guard; the second, after Knack
        // answered, is the scene tree for the created view's snapshot. Any page the
        // create made is only in that second read, so the pre-mutation tree would be
        // the wrong one to file.
        assert.deepEqual(runtimeMetadataFetches, ['Demo', 'Demo']);
        assert.match(
            path.basename(String(result.snapshotPath)),
            /-create_view-view_12-\d+\.json$/,
        );
    });

    it('sharePages true reports a copy Knack did not share, and warns about a short layout', async () => {
        const { ctx } = makeCtx({
            'POST /scenes/scene_1/views': {
                ok: true,
                status: 200,
                body: {
                    view: {
                        ...TABLE_VIEW,
                        key: 'view_13',
                        columns: [
                            TABLE_VIEW.columns[0],
                            {
                                type: 'link',
                                header: 'Edit',
                                scene: 'edit-contact-2',
                            },
                        ],
                    },
                    changes: {
                        inserts: {
                            scenes: [
                                {
                                    key: 'scene_9',
                                    name: 'Edit contact',
                                    slug: 'edit-contact-2',
                                    parent: 'contacts',
                                },
                            ],
                            views: [{ key: 'view_13' }],
                        },
                    },
                },
            },
        });

        const result = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_1',
                    sharePages: true,
                    completeViewSchema: false,
                    existingViewKeys: ['view_1'],
                    name: 'Second table',
                    title: 'Again',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.sharedPagesVerified, false);
        assert.ok(Array.isArray(result.sharedPagesProblems));
        assert.match(
            String(result.warning),
            /did not come back as a shared-page copy/,
        );
        assert.match(
            String(result.layoutWarning),
            /existingViewKeys omits 2 view\(s\)/,
        );
        assert.deepEqual(result.pagesCreated, [
            {
                sceneKey: 'scene_9',
                sceneName: 'Edit contact',
                sceneSlug: 'edit-contact-2',
                parentRef: 'contacts',
            },
        ]);
    });

    it('sharePages true refuses a menu and an unknown view without sending anything', async () => {
        const { ctx, requests } = makeCtx();

        const menu = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_2',
                    targetSceneKey: 'scene_3',
                    sharePages: true,
                    completeViewSchema: false,
                },
                ctx,
            ),
        );
        assert.equal(menu.ok, false);
        assert.equal(menu.error, 'UNSUPPORTED_VIEW_TYPE');
        assert.equal(menu.sourceSceneKey, 'scene_1');

        const missing = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_99',
                    targetSceneKey: 'scene_3',
                    sharePages: true,
                    completeViewSchema: false,
                },
                ctx,
            ),
        );
        assert.equal(missing.ok, false);
        assert.equal(missing.error, 'VIEW_NOT_FOUND');

        assert.equal(requests.length, 0);
    });
});

describe('knack_move_view', () => {
    it('posts to copyview with action move and the real view key, after a snapshot', async () => {
        const before = snapshotFiles().length;
        const { ctx, requests } = makeCtx({
            'POST /scenes/scene_1/copyview': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_3' } },
            },
        });

        const result = payloadOf(
            await moveView.handler(
                {
                    appKey: 'Demo',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_3',
                    viewKey: 'view_3',
                    completeViewSchema: true,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.action, 'move_view');
        assert.equal(result.sourceSceneKey, 'scene_1');
        assert.equal(result.targetSceneKey, 'scene_3');
        assert.equal(requests.length, 1);
        assert.equal(requests[0].apiPath, '/scenes/scene_1/copyview');
        assert.deepEqual(requests[0].body, {
            action: 'move',
            target_scene_key: 'scene_3',
            view_key: 'view_3',
            completeViewSchema: true,
        });
        assert.equal(snapshotFiles().length, before + 1);
        assert.match(
            path.basename(String(result.snapshotPath)),
            /-move_view-view_3-\d+\.json$/,
        );
    });

    it('refuses to move a view whose links own a child page when no human can be asked', async () => {
        const { ctx, requests } = makeCtx();
        const result = payloadOf(
            await moveView.handler(
                {
                    appKey: 'Demo',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_3',
                    viewKey: 'view_1',
                    completeViewSchema: false,
                },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.equal(result.error, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        assert.equal(requests.length, 0);
    });
});

describe('knack_delete_view', () => {
    it('deletes a view with no page links, after a snapshot', async () => {
        const before = snapshotFiles().length;
        const { ctx, requests } = makeCtx({
            'DELETE /scenes/scene_1/views/view_3': {
                ok: true,
                status: 200,
                body: {
                    changes: {
                        deletes: { scenes: [], views: [{ key: 'view_3' }] },
                    },
                },
            },
        });

        const result = payloadOf(
            await deleteView.handler(
                { appKey: 'Demo', sceneKey: 'scene_1', viewKey: 'view_3' },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.action, 'delete_view');
        assert.equal(requests.length, 1);
        assert.equal(requests[0].method, 'DELETE');
        assert.equal(requests[0].apiPath, '/scenes/scene_1/views/view_3');
        assert.equal(snapshotFiles().length, before + 1);
        assert.equal('pagesKnackReportsDeleted' in result, false);
    });

    it('refuses to delete a view with an owned child page when no human can be asked', async () => {
        const { ctx, requests } = makeCtx();
        const result = payloadOf(
            await deleteView.handler(
                { appKey: 'Demo', sceneKey: 'scene_1', viewKey: 'view_1' },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        const childPages = result.childPages as Array<Record<string, unknown>>;
        assert.deepEqual(
            childPages.map((page) => page.sceneKey),
            ['scene_2'],
        );
        assert.equal(requests.length, 0);
    });

    it('refuses to delete a menu whose link is the only route to a page', async () => {
        const { ctx, requests } = makeCtx();
        const result = payloadOf(
            await deleteView.handler(
                { appKey: 'Demo', sceneKey: 'scene_1', viewKey: 'view_2' },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.equal(result.error, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        assert.equal(requests.length, 0);
    });
});

describe('a missing API key is refused before the guard does any I/O', () => {
    it('refuses knack_delete_view with no metadata fetch, snapshot, or request', async () => {
        const app = makeApp({ appFolder: tmpDir });
        const { ctx, requests, runtimeMetadataFetches } = makeFakeContext({
            apps: [app],
            secrets: {},
            runtimeMetadata: { [app.appKey]: makeMetadata() },
        });
        const before = snapshotFiles().length;

        await assert.rejects(
            deleteView.handler(
                { appKey: 'Demo', sceneKey: 'scene_1', viewKey: 'view_1' },
                ctx,
            ),
            /No API key found for appKey "Demo"/,
        );

        assert.equal(requests.length, 0);
        assert.equal(runtimeMetadataFetches.length, 0);
        assert.equal(snapshotFiles().length, before);
    });

    it('refuses knack_update_view the same way, even on a cascade-free title edit', async () => {
        const app = makeApp({ appFolder: tmpDir });
        const { ctx, requests, runtimeMetadataFetches } = makeFakeContext({
            apps: [app],
            secrets: {},
            runtimeMetadata: { [app.appKey]: makeMetadata() },
        });

        await assert.rejects(
            updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_3',
                    updates: JSON.stringify({ title: 'Renamed' }),
                },
                ctx,
            ),
            /No API key found for appKey "Demo"/,
        );

        assert.equal(requests.length, 0);
        assert.equal(runtimeMetadataFetches.length, 0);
    });
});

describe('an unanswered cascade prompt is told apart from a client that cannot ask', () => {
    /**
     * Both are refusals and neither ever writes, so this is about what the refusal
     * says. The SDK cancels an overdue elicitation with ErrorCode.RequestTimeout;
     * everything else that throws is a real failure and stays `supported: false`.
     */
    function contextThatElicits(
        behaviour: (request?: unknown) => Promise<unknown>,
    ) {
        const { ctx } = makeFakeContext();
        ctx.server = {
            server: {
                getClientCapabilities: () => ({ elicitation: {} }),
                getClientVersion: () => ({ name: 'test', version: '1' }),
                elicitInput: behaviour,
            },
        } as unknown as typeof ctx.server;
        return ctx;
    }

    const input = {
        action: 'update_view',
        sceneKey: 'scene_1',
        viewKey: 'view_1',
        childPages: [{ sceneKey: 'scene_2', sceneName: 'Child', depth: 0 }],
        unresolvedLinkCount: 0,
    };

    it('matches the JSON-RPC code the SDK actually sends', () => {
        // The cases below build their error from ErrorCode.RequestTimeout, so they
        // check the predicate against the SDK's constant rather than a magic number.
        // That cannot notice the constant being renumbered, which would stop the
        // predicate matching real timeouts — so pin the wire value once, here.
        assert.equal(ErrorCode.RequestTimeout, -32001);
    });

    it('warns that a move destroys rather than re-parents, and only for a move', async () => {
        // Measured 6 September: an accepted move deleted the owned child page and
        // Knack made a new one under the target. A prompt that only says "delete"
        // lets someone approve it believing the page travels.
        const seen: string[] = [];
        const ctx = contextThatElicits(async (request?: unknown) => {
            seen.push(
                String(
                    (request as { message?: string } | undefined)?.message ??
                        '',
                ),
            );
            return { action: 'decline' };
        });

        await askHumanToConfirmPageDeletion(ctx, makeApp(), {
            ...input,
            action: 'move_view',
        });
        await askHumanToConfirmPageDeletion(ctx, makeApp(), {
            ...input,
            action: 'update_view',
        });

        assert.match(seen[0], /not a re-parent/i);
        assert.match(seen[0], /NEW key/);
        assert.doesNotMatch(seen[1], /re-parent/i);
    });

    it('asks about the audience on a move and on a transfer, not otherwise', async () => {
        // A page's login and permitted roles follow its parent, so anything that
        // changes parentage can change who can reach it. Not computed — the scene
        // parser does not read permissions — so the prompt asks rather than answers.
        const seen: string[] = [];
        const ctx = contextThatElicits(async (request?: unknown) => {
            seen.push(
                String(
                    (request as { message?: string } | undefined)?.message ??
                        '',
                ),
            );
            return { action: 'decline' };
        });

        await askHumanToConfirmPageDeletion(ctx, makeApp(), {
            ...input,
            action: 'move_view',
        });
        await askHumanToConfirmPageDeletion(ctx, makeApp(), {
            ...input,
            transferredPages: [
                {
                    sceneKey: 'scene_9',
                    sceneName: null,
                    otherReferrers: [
                        { sceneKey: 'scene_7', viewKey: 'view_67' },
                    ],
                },
            ],
        });
        await askHumanToConfirmPageDeletion(ctx, makeApp(), { ...input });

        assert.match(seen[0], /CHECK THE AUDIENCE/);
        assert.match(seen[1], /CHECK THE AUDIENCE/);
        // A plain delete changes no parentage, so it must not carry the warning —
        // a caution on every prompt is a caution nobody reads.
        assert.doesNotMatch(seen[2], /CHECK THE AUDIENCE/);
    });

    it('names where a transferred page is expected to land, and hedges it', async () => {
        // Settled 7 September: whichever surviving referrer comes first in the app's
        // page order takes it. Listing all three left the person deciding to go and
        // find the page afterwards; naming one without hedging would overstate three
        // observations as a guarantee.
        const seen: string[] = [];
        const ctx = contextThatElicits(async (request?: unknown) => {
            seen.push(
                String(
                    (request as { message?: string } | undefined)?.message ??
                        '',
                ),
            );
            return { action: 'decline' };
        });

        await askHumanToConfirmPageDeletion(ctx, makeApp(), {
            ...input,
            transferredPages: [
                {
                    sceneKey: 'scene_9',
                    sceneName: null,
                    otherReferrers: [
                        { sceneKey: 'scene_7', viewKey: 'view_67' },
                        { sceneKey: 'scene_6', viewKey: 'view_68' },
                    ],
                },
            ],
        });

        assert.match(seen[0], /expected to land under view_67/);
        assert.match(seen[0], /measured, not guaranteed/);
        assert.match(seen[0], /view_68 still linking to it/);
    });

    it('keeps the move warning when no page could be named', async () => {
        // Raised in review, and the stronger case: a move prompted only by unreadable
        // links already warns that pages may die unlisted, and "move" is the word that
        // would make someone read that as survivable. Gating the note on a named count
        // dropped it exactly there.
        const seen: string[] = [];
        const ctx = contextThatElicits(async (request?: unknown) => {
            seen.push(
                String(
                    (request as { message?: string } | undefined)?.message ??
                        '',
                ),
            );
            return { action: 'decline' };
        });

        await askHumanToConfirmPageDeletion(ctx, makeApp(), {
            ...input,
            action: 'move_view',
            childPages: [],
            unresolvedLinkCount: 2,
        });

        assert.match(seen[0], /not a re-parent/i);
        assert.match(seen[0], /NEW key/);
    });

    it('reports a request timeout as an unanswered prompt', async () => {
        const timeout = Object.assign(new Error('Request timed out'), {
            code: ErrorCode.RequestTimeout,
        });
        const ctx = contextThatElicits(async () => {
            throw timeout;
        });

        const result = await askHumanToConfirmPageDeletion(
            ctx,
            makeApp(),
            input,
        );

        assert.deepEqual(result, {
            supported: true,
            accepted: false,
            outcome: 'timeout',
        });
    });

    it('still reports any other elicitation failure as unavailable', async () => {
        const ctx = contextThatElicits(async () => {
            throw new Error('transport closed');
        });

        const result = await askHumanToConfirmPageDeletion(
            ctx,
            makeApp(),
            input,
        );

        assert.equal(result.supported, false);
        assert.match(
            result.supported === false ? (result.reason ?? '') : '',
            /transport closed/,
        );
    });

    it('never turns a timeout into an acceptance', async () => {
        // The property that matters more than the wording: no failure path may return
        // `accepted: true`, because the caller acts on that alone.
        for (const thrown of [
            Object.assign(new Error('Request timed out'), {
                code: ErrorCode.RequestTimeout,
            }),
            new Error('transport closed'),
        ]) {
            const ctx = contextThatElicits(async () => {
                throw thrown;
            });
            const result = await askHumanToConfirmPageDeletion(
                ctx,
                makeApp(),
                input,
            );
            assert.notEqual(
                result.supported === true ? result.accepted : false,
                true,
            );
        }
    });
});

describe('describeAudienceConsequence', () => {
    /**
     * Two roots: scene_1 public with child scene_2; scene_8 an authentication scene
     * (login view_8, one role) with child scene_9. Parents are slugs, as Knack writes
     * them. scene_9 carries `authenticated: false` exactly as the live page did.
     */
    const scenes: SceneInfo[] = [
        {
            sceneKey: 'scene_1',
            sceneName: 'Public root',
            sceneSlug: 'public-root',
            parentRef: undefined,
            sceneType: 'page',
            authenticated: false,
            views: [
                { viewKey: 'view_1', viewName: undefined, viewType: 'table' },
            ],
        },
        {
            sceneKey: 'scene_2',
            sceneName: 'Public child',
            sceneSlug: 'public-child',
            parentRef: 'public-root',
            views: [],
        },
        {
            sceneKey: 'scene_8',
            sceneName: undefined,
            sceneSlug: 'gate',
            parentRef: undefined,
            sceneType: 'authentication',
            views: [
                {
                    viewKey: 'view_8',
                    viewName: undefined,
                    viewType: 'login',
                    allowedProfiles: ['profile_9'],
                    limitProfileAccess: true,
                },
            ],
        },
        {
            sceneKey: 'scene_9',
            sceneName: 'Protected page',
            sceneSlug: 'protected-page',
            parentRef: 'gate',
            sceneType: 'page',
            authenticated: false,
            views: [
                { viewKey: 'view_9', viewName: undefined, viewType: 'table' },
            ],
        },
    ];
    const profileNames = buildProfileNameIndex({
        application: {
            objects: [
                { key: 'object_9', name: 'Staff', profile_key: 'profile_9' },
            ],
        },
    });
    const base = {
        action: 'move_view' as const,
        sceneKey: 'scene_9',
        viewKey: 'view_9',
        childPages: [
            { sceneKey: 'scene_9', sceneName: 'Protected page', depth: 0 },
        ],
        externalPages: [],
        transferredPages: [],
        unresolvedLinkCount: 0,
    };

    it('says nothing when no page changes parent', () => {
        assert.equal(
            describeAudienceConsequence(
                { ...base, action: 'delete_view' },
                { scenes, profileNames },
            ),
            '',
        );
    });

    it('asks, in the old words, when it has no tree or no target', () => {
        assert.match(
            describeAudienceConsequence(base, undefined),
            /CHECK THE AUDIENCE/,
        );
        assert.match(
            describeAudienceConsequence(base, { scenes: null, profileNames }),
            /CHECK THE AUDIENCE/,
        );
        // A move whose target is unknown here cannot say who reaches the replacement.
        assert.match(
            describeAudienceConsequence(base, { scenes, profileNames }),
            /CHECK THE AUDIENCE/,
        );
    });

    it('names both audiences and says CHANGES when a protected page would be rebuilt under a public one', () => {
        const text = describeAudienceConsequence(base, {
            scenes,
            profileNames,
            targetSceneKey: 'scene_1',
        });
        assert.match(text, /^\n\nAUDIENCE CHANGES/);
        assert.match(
            text,
            /scene_9: now only Staff \[profile_9\] \(login on scene_8\)/,
        );
        assert.match(
            text,
            /under scene_1: anyone \(no login above it\) → CHANGES/,
        );
        assert.doesNotMatch(text, /CHECK THE AUDIENCE/);
    });

    it('says unchanged when the target sits under the same login', () => {
        const text = describeAudienceConsequence(base, {
            scenes,
            profileNames,
            targetSceneKey: 'scene_8',
        });
        assert.match(text, /Audience unchanged/);
        assert.match(text, /→ unchanged/);
    });

    it('describes unreadable-link moves through the source page, not silence', () => {
        const text = describeAudienceConsequence(
            { ...base, childPages: [], unresolvedLinkCount: 2 },
            { scenes, profileNames, targetSceneKey: 'scene_1' },
        );
        assert.match(text, /pages owned through scene_9's unreadable links/);
        assert.match(text, /→ CHANGES/);
    });

    it('resolves a transfer against its expected destination, and never rounds unknown to unchanged', () => {
        const transfer = {
            ...base,
            action: 'update_view' as const,
            sceneKey: 'scene_1',
            viewKey: 'view_1',
            childPages: [],
            transferredPages: [
                {
                    ref: 'protected-page',
                    sceneKey: 'scene_9',
                    sceneName: 'Protected page',
                    sceneSlug: 'protected-page',
                    classification: 'transferred' as const,
                    parentSceneKey: 'scene_8',
                    otherReferrers: [
                        { sceneKey: 'scene_2', viewKey: 'view_2' },
                    ],
                    reason: '',
                },
            ],
        };
        const changed = describeAudienceConsequence(transfer, {
            scenes,
            profileNames,
        });
        assert.match(changed, /AUDIENCE CHANGES/);
        assert.match(changed, /under scene_2 \(expected destination\): anyone/);

        const orphan = describeAudienceConsequence(
            {
                ...transfer,
                transferredPages: [
                    {
                        ...transfer.transferredPages[0],
                        otherReferrers: [
                            { sceneKey: 'scene_404', viewKey: 'view_404' },
                        ],
                    },
                ],
            },
            { scenes, profileNames },
        );
        assert.match(orphan, /CHECK THE AUDIENCE/);
        assert.match(orphan, /UNKNOWN — verify in the builder/);
        assert.doesNotMatch(orphan, /unchanged/);
    });

    it('reaches the elicitation prompt through askHumanToConfirmPageDeletion', async () => {
        const seen: string[] = [];
        const { ctx } = makeFakeContext();
        ctx.server = {
            server: {
                getClientCapabilities: () => ({ elicitation: {} }),
                getClientVersion: () => ({ name: 'test', version: '1' }),
                elicitInput: async (request?: unknown) => {
                    seen.push(
                        String(
                            (request as { message?: string } | undefined)
                                ?.message ?? '',
                        ),
                    );
                    return { action: 'decline' };
                },
            },
        } as unknown as typeof ctx.server;

        await askHumanToConfirmPageDeletion(ctx, makeApp(), base, {
            scenes,
            profileNames,
            targetSceneKey: 'scene_1',
        });
        assert.match(seen[0], /AUDIENCE CHANGES/);
        assert.match(seen[0], /only Staff \[profile_9\]/);
    });
});
