import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import type { KnackApiResult } from '../http.js';
import {
    type RequestLog,
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import type { RuntimeMetadata } from '../types.js';
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
        // A create destroys nothing, so it takes no snapshot.
        assert.equal('snapshotPath' in result, false);
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

        // Nothing was at stake, so nothing is reported as deleted or moved.
        assert.equal('pagesExpectedToBeDeleted' in result, false);
        assert.equal('linksRemovedPagesKept' in result, false);

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
        // A copy destroys nothing, so it is not snapshotted and never prompts — even
        // though view_1 owns a child page.
        assert.equal('snapshotPath' in result, false);
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
