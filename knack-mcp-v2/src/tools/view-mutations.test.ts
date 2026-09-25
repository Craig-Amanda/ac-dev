import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';
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
import type {
    ChildPage,
    ClassifiedLinkTarget,
    ViewMutationAction,
} from '../lib/view-safety.js';
import type { RuntimeMetadata, SceneInfo } from '../types.js';
import {
    addActionLink,
    addPageLinkColumn,
    addViewColumns,
    addViewLinks,
    addViewRules,
    editViewRules,
    assertFlatSpliceIsClean,
    assertNestedSpliceIsClean,
    copyView,
    createView,
    deleteView,
    moveView,
    updateView,
    updateViewOrder,
} from './view-mutations.js';
import { SdkErrorCode } from '@modelcontextprotocol/server';

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

/** One width-block, one group, one sub-column, one field — the simplest nested shape. */
const DETAILS_VIEW = {
    key: 'view_20',
    name: 'Contact details',
    type: 'details',
    title: 'Contact',
    source: {
        object: 'object_1',
        criteria: { match: 'all', rules: [], groups: [] },
    },
    columns: [
        {
            width: 100,
            groups: [
                {
                    columns: [
                        [
                            {
                                key: 'field_1',
                                type: 'field',
                                name: 'Name',
                                format: { label_format: 'left' },
                            },
                        ],
                    ],
                },
            ],
        },
    ],
    links: [],
    groups: [],
    inputs: [],
};

/** Same shape as DETAILS_VIEW; a list view carries columns identically. */
const LIST_VIEW = {
    ...DETAILS_VIEW,
    key: 'view_21',
    name: 'Contact list',
    type: 'list',
};

/** Carries a trailing KTL keyword cluster on its title, for keyword-guard coverage. */
const KEYWORD_VIEW = {
    key: 'view_6',
    name: 'Keyword rich text',
    type: 'rich_text',
    title: 'Contacts _ktlHide _notes=Craig on 2026-09-01',
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
                // The other fields the fixture views and rules name. They exist in
                // the app, so the unknown-field check passes, but not on object_1,
                // so a header for one still falls back to its key.
                {
                    key: 'object_9',
                    name: 'Elsewhere',
                    fields: ['field_2', 'field_3', 'field_4', 'field_5'].map(
                        (key) => ({ key, name: key, type: 'short_text' }),
                    ),
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
                // Its own scene so KEYWORD_VIEW never changes the view count any other
                // scene's tests (e.g. knack_copy_view's short-layout warning) depend on.
                {
                    key: 'scene_4',
                    name: 'Keywords',
                    slug: 'keywords',
                    views: [KEYWORD_VIEW],
                },
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

    describe('KTL keyword guard on title/description', () => {
        it('refuses a title replacement that would drop existing keywords', async () => {
            const { ctx, requests } = makeCtx();
            const result = payloadOf(
                await updateView.handler(
                    {
                        appKey: 'Demo',
                        sceneKey: 'scene_4',
                        viewKey: 'view_6',
                        updates: JSON.stringify({ title: 'Contacts renamed' }),
                    },
                    ctx,
                ),
            );
            assert.equal(result.ok, false);
            assert.equal(result.error, 'KTL_KEYWORDS_WOULD_BE_DROPPED');
            assert.deepEqual(result.droppedKtlKeywords, {
                title: ['_ktlHide', '_notes'],
            });
            assert.equal(requests.length, 0);
        });

        it('allows dropping keywords only with confirmRemoveKtlKeywords', async () => {
            const { ctx, requests } = makeCtx({
                'PUT /scenes/scene_4/views/view_6': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_6' } },
                },
            });
            const result = payloadOf(
                await updateView.handler(
                    {
                        appKey: 'Demo',
                        sceneKey: 'scene_4',
                        viewKey: 'view_6',
                        updates: JSON.stringify({ title: 'Contacts renamed' }),
                        confirmRemoveKtlKeywords: true,
                    },
                    ctx,
                ),
            );
            assert.equal(result.ok, true, JSON.stringify(result));
            assert.equal(requests.length, 1);
            const sent = requests[0].body as Record<string, unknown>;
            assert.equal(sent.title, 'Contacts renamed');
        });

        it('keywordEdits updates an existing keyword in place, no updates needed', async () => {
            const { ctx, requests } = makeCtx({
                'PUT /scenes/scene_4/views/view_6': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_6' } },
                },
            });
            const result = payloadOf(
                await updateView.handler(
                    {
                        appKey: 'Demo',
                        sceneKey: 'scene_4',
                        viewKey: 'view_6',
                        keywordEdits: JSON.stringify({
                            title: { _notes: 'Craig on 2026-09-07' },
                        }),
                    },
                    ctx,
                ),
            );
            assert.equal(result.ok, true, JSON.stringify(result));
            assert.equal(requests.length, 1);
            const sent = requests[0].body as Record<string, unknown>;
            assert.equal(
                sent.title,
                'Contacts _ktlHide _notes=Craig on 2026-09-07',
            );
        });

        it('keywordEdits appends a brand-new keyword at the end of the cluster', async () => {
            const { ctx, requests } = makeCtx({
                'PUT /scenes/scene_4/views/view_6': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_6' } },
                },
            });
            const result = payloadOf(
                await updateView.handler(
                    {
                        appKey: 'Demo',
                        sceneKey: 'scene_4',
                        viewKey: 'view_6',
                        keywordEdits: JSON.stringify({
                            title: { _showFor: 'admin' },
                        }),
                    },
                    ctx,
                ),
            );
            assert.equal(result.ok, true, JSON.stringify(result));
            assert.equal(requests.length, 1);
            const sent = requests[0].body as Record<string, unknown>;
            assert.equal(
                sent.title,
                'Contacts _ktlHide _notes=Craig on 2026-09-01 _showFor=admin',
            );
        });

        it('refuses malformed keywordEdits JSON before any request', async () => {
            const { ctx, requests } = makeCtx();
            const result = payloadOf(
                await updateView.handler(
                    {
                        appKey: 'Demo',
                        sceneKey: 'scene_4',
                        viewKey: 'view_6',
                        keywordEdits: '{not json',
                    },
                    ctx,
                ),
            );
            assert.equal(result.ok, false);
            assert.equal(result.error, 'INVALID_KEYWORD_EDITS_JSON');
            assert.equal(requests.length, 0);
        });

        it('refuses keywordEdits with an unrecognised top-level key', async () => {
            const { ctx, requests } = makeCtx();
            const result = payloadOf(
                await updateView.handler(
                    {
                        appKey: 'Demo',
                        sceneKey: 'scene_4',
                        viewKey: 'view_6',
                        keywordEdits: JSON.stringify({
                            content: { _foo: 'bar' },
                        }),
                    },
                    ctx,
                ),
            );
            assert.equal(result.ok, false);
            assert.equal(result.error, 'INVALID_KEYWORD_EDITS_JSON');
            assert.match(String(result.message), /unrecognised top-level key/);
            assert.equal(requests.length, 0);
        });

        it('refuses keywordEdits whose property map is not an object', async () => {
            const { ctx, requests } = makeCtx();
            const result = payloadOf(
                await updateView.handler(
                    {
                        appKey: 'Demo',
                        sceneKey: 'scene_4',
                        viewKey: 'view_6',
                        keywordEdits: JSON.stringify({ title: '_notes=x' }),
                    },
                    ctx,
                ),
            );
            assert.equal(result.ok, false);
            assert.equal(result.error, 'INVALID_KEYWORD_EDITS_JSON');
            assert.match(String(result.message), /must be a JSON object/);
            assert.equal(requests.length, 0);
        });

        it('refuses a keyword name that is not underscore-prefixed', async () => {
            const { ctx, requests } = makeCtx();
            const result = payloadOf(
                await updateView.handler(
                    {
                        appKey: 'Demo',
                        sceneKey: 'scene_4',
                        viewKey: 'view_6',
                        keywordEdits: JSON.stringify({
                            title: { notes: 'Craig on 2026-09-07' },
                        }),
                    },
                    ctx,
                ),
            );
            assert.equal(result.ok, false);
            assert.equal(result.error, 'INVALID_KEYWORD_EDITS_JSON');
            assert.match(
                String(result.message),
                /invalid keyword name "notes"/,
            );
            assert.equal(requests.length, 0);
        });

        it('refuses a keyword value that is neither a string nor null', async () => {
            const { ctx, requests } = makeCtx();
            const result = payloadOf(
                await updateView.handler(
                    {
                        appKey: 'Demo',
                        sceneKey: 'scene_4',
                        viewKey: 'view_6',
                        keywordEdits: JSON.stringify({
                            title: { _showFor: { role: 'admin' } },
                        }),
                    },
                    ctx,
                ),
            );
            assert.equal(result.ok, false);
            assert.equal(result.error, 'INVALID_KEYWORD_EDITS_JSON');
            assert.match(
                String(result.message),
                /must be a string or null, not object/,
            );
            assert.equal(requests.length, 0);
        });
    });
});

describe('knack_update_view previewOnly reports audience', () => {
    /**
     * The rule lives in `describePreviewAudience` and is unit tested in the incident
     * suite. These pin the wiring instead: the preview reads its pages out of the
     * refusal's `details`, which is `Record<string, unknown>`, so naming a key wrong
     * compiles cleanly and reports an empty audience for every preview — which is
     * exactly the silence the fix existed to remove.
     */
    it('carries the audience key even when nothing re-parents', async () => {
        const { ctx } = makeCtx();

        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_3',
                    updates: JSON.stringify({ content: '<p>Changed</p>' }),
                    confirmRemoveKtlKeywords: false,
                    previewOnly: true,
                },
                ctx,
            ),
        );

        assert.equal(result.error, 'PREVIEW_ONLY');
        // Present and empty, not absent. A reader cannot otherwise tell "checked,
        // nothing re-parents" from "never looked".
        assert.ok('audienceChanges' in result);
        assert.deepEqual(result.audienceChanges, []);
        assert.equal(result.audienceWarning, undefined);
    });

    it('names the page whose audience would change, and where it goes', async () => {
        // Two views linking to one page, so dropping one link re-parents it rather
        // than destroying it - the quiet case that destroys nothing and still changes
        // who can reach a page.
        const metadata = makeMetadata();
        // RuntimeMetadata is Record<string, unknown>, so reaching into it is a cast
        // whatever we do. One narrow named cast beats `!` chains, which assert nothing.
        const application = metadata.application as {
            scenes: Array<Record<string, unknown>>;
        };
        application.scenes.push({
            key: 'scene_5',
            name: 'Other',
            slug: 'other',
            views: [
                {
                    key: 'view_5',
                    name: 'Other table',
                    type: 'table',
                    columns: [
                        { type: 'link', header: 'Edit', scene: 'edit-contact' },
                    ],
                },
            ],
        });

        const { ctx } = makeCtx(undefined, metadata);

        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    // Drops the link to edit-contact, keeps the plain field column.
                    updates: JSON.stringify({
                        columns: [
                            {
                                type: 'field',
                                field: { key: 'field_1' },
                                header: 'Name',
                            },
                        ],
                    }),
                    confirmRemoveKtlKeywords: false,
                    previewOnly: true,
                },
                ctx,
            ),
        );

        assert.equal(result.error, 'PREVIEW_ONLY');
        // Destroys nothing: the page transfers to the view that still links to it.
        assert.deepEqual(result.childPages, []);

        const rows = result.audienceChanges as Array<Record<string, unknown>>;
        assert.equal(rows.length, 1, JSON.stringify(result.audienceChanges));
        assert.equal(rows[0].sceneKey, 'scene_2');
        assert.equal(rows[0].destinationSceneKey, 'scene_5');
    });
});

describe('knack_update_view previewOnly reports links that point at no page', () => {
    /**
     * `findDanglingLinks` ran only on `outcome.result.ok`, so the one route that exists
     * to look before leaping was the one route that could not see a link pointing at a
     * page that does not exist. Measured 11 September on the test app: a preview whose
     * effective body still carried a known-dangling menu link said nothing about it.
     *
     * The check needs the merged body, which is why the guard now returns it. That was
     * the third of PR #52's claims, withdrawn earlier as having no use; this is the use.
     */
    it('names the link and the page it cannot find', async () => {
        const { ctx } = makeCtx();

        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    // Keeps the real link and adds one to a slug no page has, so the
                    // warning is not confounded with a page being put at risk.
                    updates: JSON.stringify({
                        columns: [
                            {
                                type: 'field',
                                field: { key: 'field_1' },
                                header: 'Name',
                            },
                            {
                                type: 'link',
                                header: 'Edit',
                                scene: 'edit-contact',
                            },
                            {
                                type: 'link',
                                header: 'Ghost',
                                scene: 'no-such-page',
                            },
                        ],
                    }),
                    confirmRemoveKtlKeywords: false,
                    previewOnly: true,
                },
                ctx,
            ),
        );

        assert.equal(result.error, 'PREVIEW_ONLY');
        assert.deepEqual(result.childPages, []);

        const dangling = result.danglingLinks as Array<{
            ref: string;
            sourcePaths: string[];
        }>;
        assert.equal(dangling.length, 1, JSON.stringify(result.danglingLinks));
        assert.equal(dangling[0].ref, 'no-such-page');
        assert.deepEqual(dangling[0].sourcePaths, ['$.columns[2]']);
        assert.match(
            String(result.danglingLinkWarning),
            /would store each one and it would open nothing/,
        );
    });

    it('says nothing when every link resolves', async () => {
        // A caution on every preview is a caution nobody reads.
        const { ctx } = makeCtx();

        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    updates: JSON.stringify({ title: 'Contacts' }),
                    confirmRemoveKtlKeywords: false,
                    previewOnly: true,
                },
                ctx,
            ),
        );

        assert.equal(result.error, 'PREVIEW_ONLY');
        assert.equal(result.danglingLinks, undefined);
        assert.equal(result.danglingLinkWarning, undefined);
    });

    it('returns the body it evaluated, merged', async () => {
        // The caller reads the same object every decision above was made against, and
        // it is a merge: a title-only patch still carries the columns it did not touch.
        const { ctx } = makeCtx();

        const result = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    updates: JSON.stringify({ title: 'Renamed' }),
                    confirmRemoveKtlKeywords: false,
                    previewOnly: true,
                },
                ctx,
            ),
        );

        const body = result.effectiveBody as Record<string, unknown>;
        assert.equal(body.title, 'Renamed');
        assert.equal(
            (body.columns as unknown[]).length,
            2,
            'the merge keeps what the patch did not mention',
        );
    });
});

describe('knack_add_view_columns', () => {
    it('appends a new column and keeps every existing one, verbatim', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.action, 'add_view_columns');
        assert.deepEqual(result.addedFieldKeys, ['field_2']);
        assert.equal(result.columnCountBefore, 2);
        assert.equal(result.columnCountAfter, 3);

        assert.equal(requests.length, 1);
        assert.equal(requests[0].method, 'PUT');
        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.equal(sentColumns.length, 3);
        // The two original columns, including the link column, re-sent unchanged.
        assert.deepEqual(sentColumns.slice(0, 2), TABLE_VIEW.columns);
        assert.equal(
            (sentColumns[2].field as Record<string, unknown>).key,
            'field_2',
        );
        // Not in the schema fixture, so the header falls back to the field key.
        assert.equal(sentColumns[2].header, 'field_2');
        // Everything else on the view came through the same merge knack_update_view
        // uses, untouched.
        assert.equal(sent.name, 'Contacts table');
        assert.deepEqual(sent.source, TABLE_VIEW.source);

        assert.match(String(result.note), /not found in the object's schema/);
    });

    it('resolves the new header from the schema instead of falling back to the key', async () => {
        const metadata = makeMetadata();
        const objects = (
            metadata.application as { objects: Array<Record<string, unknown>> }
        ).objects;
        (objects[0].fields as unknown[]).push({
            key: 'field_2',
            name: 'Email',
            type: 'email',
        });

        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_1/views/view_1': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_1' } },
                },
            },
            metadata,
        );

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal('note' in result, false);
        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.equal(sentColumns[2].header, 'Email');
    });

    it('refuses a field that already has a column on the view', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2', 'field_1'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'FIELD_ALREADY_A_COLUMN');
        assert.match(String(result.message), /field_1/);
        assert.equal(requests.length, 0);
    });

    it('refuses a view type it does not build a nested layout for', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_2',
                    viewKey: 'view_4',
                    fieldKeys: ['field_2'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'UNSUPPORTED_VIEW_TYPE');
        assert.match(String(result.message), /form/);
        assert.equal(requests.length, 0);
    });

    it('accepts a stored view type with different casing or whitespace', async () => {
        // Regression: the view-type check used to read attributes.type raw instead of
        // through getViewType (which trims/lowercases), so a stored type this server
        // itself normalizes everywhere else would have been wrongly refused here as
        // UNSUPPORTED_VIEW_TYPE.
        const metadata = makeMetadata();
        const scenes = (
            metadata.application as { scenes: Array<Record<string, unknown>> }
        ).scenes;
        const scene1 = scenes.find((scene) => scene.key === 'scene_1');
        const views = scene1?.views as Array<Record<string, unknown>>;
        const tableView = views.find((view) => view.key === 'view_1');
        if (tableView) tableView.type = ' Table ';

        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_1/views/view_1': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_1' } },
                },
            },
            metadata,
        );

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(requests.length, 1);
    });

    it('refuses conflicting placement and sends nothing', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2'],
                    insertAfterFieldKey: 'field_1',
                    insertBeforeFieldKey: 'field_1',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'CONFLICTING_PLACEMENT');
        assert.equal(requests.length, 0);
    });

    it('refuses a field name in place of a field key, before the write', async () => {
        // `fieldKeys` took any non-empty string, so a caller reaching for a label rather
        // than a key — the object's schema gives both, and only the label is readable —
        // had the label written straight into the live view as `field: { key: "Email
        // Address" }`. Knack stored it, the tool reported ok, and the only signal was a
        // note saying the header had fallen back to the key. A column naming something
        // that is not a field shows nothing, and undoing it is another write.
        // columnConnections in this same tool has always enforced the pattern.
        for (const fieldKeys of [
            ['Email Address'],
            ['field_2', 'Email Address'],
            ['object_1.field_2'],
            ['2'],
            // Real Knack field keys are lower-case only. FIELD_KEY_PATTERN itself is
            // case-insensitive (other callers match a possibly-mistyped-case key against
            // a lower-case field map), so this guard uses its own case-sensitive pattern
            // instead of that shared one.
            ['FIELD_2'],
        ]) {
            const parsed = z.object(addViewColumns.input).safeParse({
                appKey: 'Demo',
                sceneKey: 'scene_1',
                viewKey: 'view_1',
                fieldKeys,
            });
            assert.equal(parsed.success, false, JSON.stringify(fieldKeys));
        }

        // A real key still parses, so the guard has not closed the tool's front door.
        assert.equal(
            z.object(addViewColumns.input).safeParse({
                appKey: 'Demo',
                sceneKey: 'scene_1',
                viewKey: 'view_1',
                fieldKeys: ['field_2'],
            }).success,
            true,
        );
    });

    it('refuses fieldKeys naming the same field twice', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2', 'field_2'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'DUPLICATE_FIELD_KEY');
        assert.equal(requests.length, 0);
    });

    it('refuses an anchor that is not an existing column', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2'],
                    insertAfterFieldKey: 'field_99',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'ANCHOR_NOT_FOUND');
        assert.equal(requests.length, 0);
    });

    it('places the new column directly before its anchor', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        await addViewColumns.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_1',
                viewKey: 'view_1',
                fieldKeys: ['field_2'],
                insertBeforeFieldKey: 'field_1',
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.equal(
            (sentColumns[0].field as Record<string, unknown>).key,
            'field_2',
        );
        assert.deepEqual(sentColumns.slice(1), TABLE_VIEW.columns);
    });

    it('places the new column directly after its anchor', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        await addViewColumns.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_1',
                viewKey: 'view_1',
                fieldKeys: ['field_2'],
                insertAfterFieldKey: 'field_1',
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.deepEqual(sentColumns[0], TABLE_VIEW.columns[0]);
        assert.equal(
            (sentColumns[1].field as Record<string, unknown>).key,
            'field_2',
        );
        assert.deepEqual(sentColumns[2], TABLE_VIEW.columns[1]);
    });

    it('sets connection on the new column from columnConnections', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        await addViewColumns.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_1',
                viewKey: 'view_1',
                fieldKeys: ['field_2'],
                columnConnections: JSON.stringify({ field_2: 'field_1' }),
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.deepEqual(sentColumns[2].connection, { key: 'field_1' });
    });

    it('rejects columnConnections naming something other than a field key', async () => {
        const { ctx } = makeCtx();

        await assert.rejects(
            addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2'],
                    columnConnections: JSON.stringify({ field_2: 'Contact' }),
                },
                ctx,
            ),
            /must be a connection field key/,
        );
    });

    it('rejects a mis-cased columnConnections key rather than silently dropping it', async () => {
        // Regression: columnConnections used to validate its key against the
        // case-insensitive FIELD_KEY_PATTERN, so "FIELD_2" passed validation, but the
        // lookup against it (`parsedColumnConnections[field.key]`) is always lower-case
        // — so the connection was silently never applied, with no error and no note.
        const { ctx } = makeCtx();

        await assert.rejects(
            addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2'],
                    columnConnections: JSON.stringify({ FIELD_2: 'field_1' }),
                },
                ctx,
            ),
            /must be a field key like "field_10"/,
        );
    });

    it('rejects a mis-cased columnConnections value rather than silently dropping it', async () => {
        const { ctx } = makeCtx();

        await assert.rejects(
            addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2'],
                    columnConnections: JSON.stringify({ field_2: 'FIELD_1' }),
                },
                ctx,
            ),
            /must be a connection field key/,
        );
    });

    it('previewOnly sends nothing and reports no page at risk', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    fieldKeys: ['field_2'],
                    previewOnly: true,
                },
                ctx,
            ),
        );

        assert.equal(result.error, 'PREVIEW_ONLY');
        assert.deepEqual(result.childPages, []);
        assert.equal(requests.length, 0);
    });
});

/** The field-item array at columns[blockIndex].groups[groupIndex].columns[subColumnIndex]. */
function nestedSubColumn(
    columns: unknown,
    blockIndex = 0,
    groupIndex = 0,
    subColumnIndex = 0,
): Array<Record<string, unknown>> {
    const block = (columns as Array<Record<string, unknown>>)[blockIndex];
    const group = (block.groups as Array<Record<string, unknown>>)[groupIndex];
    return (group.columns as Array<Array<Record<string, unknown>>>)[
        subColumnIndex
    ];
}

/** DETAILS_VIEW/LIST_VIEW on their own scene, so nothing else's fixture is disturbed. */
function metadataWithNestedView(
    view: Record<string, unknown>,
): RuntimeMetadata {
    const metadata = makeMetadata();
    (
        metadata.application as { scenes: Array<Record<string, unknown>> }
    ).scenes.push({
        key: 'scene_10',
        name: 'Nested test scene',
        slug: 'nested-test',
        views: [view],
    });
    return metadata;
}

describe('knack_add_view_columns on details/list views', () => {
    it('appends a new field to a details view, keeping the existing field and layout', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_20': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_20' } },
                },
            },
            metadataWithNestedView(DETAILS_VIEW),
        );

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_10',
                    viewKey: 'view_20',
                    fieldKeys: ['field_2'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.columnCountBefore, 1);
        assert.equal(result.columnCountAfter, 2);
        assert.deepEqual(result.addedFieldKeys, ['field_2']);

        const sent = requests[0].body as Record<string, unknown>;
        const subColumn = nestedSubColumn(sent.columns);
        assert.equal(subColumn.length, 2);
        assert.deepEqual(
            subColumn[0],
            DETAILS_VIEW.columns[0].groups[0].columns[0][0],
        );
        assert.equal(subColumn[1].key, 'field_2');
        // Everything else on the view, untouched.
        assert.equal(sent.name, 'Contact details');
        assert.deepEqual(sent.source, DETAILS_VIEW.source);
    });

    it('treats a list view the same as details', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_21': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_21' } },
                },
            },
            metadataWithNestedView(LIST_VIEW),
        );

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_10',
                    viewKey: 'view_21',
                    fieldKeys: ['field_2'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        const sent = requests[0].body as Record<string, unknown>;
        assert.equal(nestedSubColumn(sent.columns).length, 2);
    });

    it('appends to the last sub-column of the last group of the last width-block only', async () => {
        const multiBlockView = {
            ...DETAILS_VIEW,
            key: 'view_22',
            columns: [
                // First block: left untouched by an append with no anchor.
                {
                    width: 50,
                    groups: [
                        { columns: [[{ key: 'field_1', type: 'field' }]] },
                    ],
                },
                // Second (last) block, two groups, second group has two sub-columns.
                {
                    width: 50,
                    groups: [
                        { columns: [[{ key: 'field_3', type: 'field' }]] },
                        {
                            columns: [
                                [{ key: 'field_4', type: 'field' }],
                                [{ key: 'field_5', type: 'field' }],
                            ],
                        },
                    ],
                },
            ],
        };
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_22': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_22' } },
                },
            },
            metadataWithNestedView(multiBlockView),
        );

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_10',
                    viewKey: 'view_22',
                    fieldKeys: ['field_2'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.columnCountBefore, 4);
        assert.equal(result.columnCountAfter, 5);

        const sent = requests[0].body as Record<string, unknown>;
        const columns = sent.columns as unknown[];
        // First block untouched.
        assert.deepEqual(
            nestedSubColumn(columns, 0, 0, 0),
            multiBlockView.columns[0].groups[0].columns[0],
        );
        // Last block's first group untouched.
        assert.deepEqual(
            nestedSubColumn(columns, 1, 0, 0),
            multiBlockView.columns[1].groups[0].columns[0],
        );
        // Last block's last group's first sub-column untouched.
        assert.deepEqual(
            nestedSubColumn(columns, 1, 1, 0),
            multiBlockView.columns[1].groups[1].columns[0],
        );
        // Only the very last sub-column gained the new field.
        const targetSubColumn = nestedSubColumn(columns, 1, 1, 1);
        assert.equal(targetSubColumn.length, 2);
        assert.equal(targetSubColumn[0].key, 'field_5');
        assert.equal(targetSubColumn[1].key, 'field_2');
    });

    it('refuses a field already present anywhere in the nested layout, not just the last sub-column', async () => {
        const twoBlockView = {
            ...DETAILS_VIEW,
            key: 'view_23',
            columns: [
                {
                    width: 50,
                    groups: [
                        { columns: [[{ key: 'field_9', type: 'field' }]] },
                    ],
                },
                {
                    width: 50,
                    groups: [
                        { columns: [[{ key: 'field_1', type: 'field' }]] },
                    ],
                },
            ],
        };
        const { ctx, requests } = makeCtx(
            undefined,
            metadataWithNestedView(twoBlockView),
        );

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_10',
                    viewKey: 'view_23',
                    fieldKeys: ['field_9'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'FIELD_ALREADY_A_COLUMN');
        assert.equal(requests.length, 0);
    });

    it('places a new field directly after an anchor buried earlier in the layout', async () => {
        const twoBlockView = {
            ...DETAILS_VIEW,
            key: 'view_24',
            columns: [
                {
                    width: 50,
                    groups: [
                        {
                            columns: [
                                [
                                    { key: 'field_1', type: 'field' },
                                    { key: 'field_5', type: 'field' },
                                ],
                            ],
                        },
                    ],
                },
                {
                    width: 50,
                    groups: [
                        { columns: [[{ key: 'field_3', type: 'field' }]] },
                    ],
                },
            ],
        };
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_24': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_24' } },
                },
            },
            metadataWithNestedView(twoBlockView),
        );

        await addViewColumns.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_10',
                viewKey: 'view_24',
                fieldKeys: ['field_2'],
                insertAfterFieldKey: 'field_1',
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        // Spliced into the first block's sub-column, not appended to the last block.
        const targetSubColumn = nestedSubColumn(sent.columns, 0, 0, 0);
        assert.deepEqual(
            targetSubColumn.map((item) => item.key),
            ['field_1', 'field_2', 'field_5'],
        );
        assert.deepEqual(
            nestedSubColumn(sent.columns, 1, 0, 0),
            twoBlockView.columns[1].groups[0].columns[0],
        );
    });

    it('places a new field directly before an anchor', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_20': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_20' } },
                },
            },
            metadataWithNestedView(DETAILS_VIEW),
        );

        await addViewColumns.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_10',
                viewKey: 'view_20',
                fieldKeys: ['field_2'],
                insertBeforeFieldKey: 'field_1',
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        assert.deepEqual(
            nestedSubColumn(sent.columns).map((item) => item.key),
            ['field_2', 'field_1'],
        );
    });

    it('refuses an anchor that is not in the nested layout', async () => {
        const { ctx, requests } = makeCtx(
            undefined,
            metadataWithNestedView(DETAILS_VIEW),
        );

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_10',
                    viewKey: 'view_20',
                    fieldKeys: ['field_2'],
                    insertAfterFieldKey: 'field_99',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'ANCHOR_NOT_FOUND');
        assert.equal(requests.length, 0);
    });

    it('declines to invent a layout for a view with an empty columns array', async () => {
        const emptyView = { ...DETAILS_VIEW, key: 'view_25', columns: [] };
        const { ctx, requests } = makeCtx(
            undefined,
            metadataWithNestedView(emptyView),
        );

        const result = payloadOf(
            await addViewColumns.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_10',
                    viewKey: 'view_25',
                    fieldKeys: ['field_2'],
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'EMPTY_LAYOUT');
        assert.equal(requests.length, 0);
    });

    it('sets connection on a new nested field from columnConnections', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_20': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_20' } },
                },
            },
            metadataWithNestedView(DETAILS_VIEW),
        );

        await addViewColumns.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_10',
                viewKey: 'view_20',
                fieldKeys: ['field_2'],
                columnConnections: JSON.stringify({ field_2: 'field_1' }),
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        const subColumn = nestedSubColumn(sent.columns);
        assert.deepEqual(subColumn[1].connection, { key: 'field_1' });
    });
});

describe('splice self-checks (assertFlatSpliceIsClean / assertNestedSpliceIsClean)', () => {
    /**
     * No legitimate call to add_action_link, add_view_rules, add_view_links or
     * add_view_columns can make these fail — every one of those tools builds `after`
     * itself, deterministically, from `before`. These tests exist to prove the checks
     * themselves would catch it if that ever stopped being true: the exact failure
     * mode measured in the GAP-Track incident, an existing item silently altered
     * alongside a legitimate insertion.
     */
    it('passes when only the inserted range differs', () => {
        const before = [{ a: 1 }, { a: 2 }, { a: 3 }];
        const inserted = [{ a: 'new' }];
        const after = [before[0], before[1], inserted[0], before[2]];
        assert.deepEqual(assertFlatSpliceIsClean(before, after, 2, 1), {
            ok: true,
        });
    });

    it('fails when an item outside the inserted range was altered', () => {
        const before = [{ a: 1 }, { a: 2 }, { a: 3 }];
        const inserted = [{ a: 'new' }];
        // Item 0 corrupted alongside the legitimate insertion at index 2 — the shape of
        // the GAP-Track "Docs" column drift, reproduced deliberately here.
        const after = [{ a: 'corrupted' }, before[1], inserted[0], before[2]];
        const result = assertFlatSpliceIsClean(before, after, 2, 1);
        assert.equal(result.ok, false);
        assert.match(
            (result as { ok: false; message: string }).message,
            /no longer matches what was read from Knack/,
        );
    });

    it('fails when the claimed insertion position is wrong', () => {
        const before = [{ a: 1 }, { a: 2 }];
        const after = [before[0], before[1], { a: 'new' }]; // really inserted at index 2
        // Claiming it was inserted at index 0 instead: removing "the wrong window"
        // leaves before[0] out and the new item in, which cannot match `before`.
        const result = assertFlatSpliceIsClean(before, after, 0, 1);
        assert.equal(result.ok, false);
    });

    const NESTED_LOCATION = {
        blockIndex: 0,
        groupIndex: 0,
        subColumnIndex: 0,
    };

    function nestedColumns(items: unknown[]): unknown[] {
        return [{ groups: [{ columns: [items] }] }];
    }

    it('passes for a clean nested splice', () => {
        const before = nestedColumns([{ key: 'field_1' }, { key: 'field_2' }]);
        const after = nestedColumns([
            { key: 'field_1' },
            { type: 'action_link' },
            { key: 'field_2' },
        ]);
        assert.deepEqual(
            assertNestedSpliceIsClean(before, after, NESTED_LOCATION, 1, 1),
            { ok: true },
        );
    });

    it('fails when a nested sibling item was altered', () => {
        const before = nestedColumns([{ key: 'field_1' }, { key: 'field_2' }]);
        const after = nestedColumns([
            { key: 'field_1', label: 'corrupted' }, // altered, not just the insertion
            { type: 'action_link' },
            { key: 'field_2' },
        ]);
        const result = assertNestedSpliceIsClean(
            before,
            after,
            NESTED_LOCATION,
            1,
            1,
        );
        assert.equal(result.ok, false);
        assert.match(
            (result as { ok: false; message: string }).message,
            /no longer matches what was read from Knack/,
        );
    });

    it('fails when a different block/group/sub-column than the claimed one changed', () => {
        const before = [
            {
                groups: [
                    { columns: [[{ key: 'field_1' }], [{ key: 'field_2' }]] },
                ],
            },
        ];
        // Insertion correctly claimed at sub-column 0, but sub-column 1 (untouched by
        // the claim) was altered too.
        const after = [
            {
                groups: [
                    {
                        columns: [
                            [{ key: 'field_1' }, { type: 'action_link' }],
                            [{ key: 'field_2', label: 'corrupted' }],
                        ],
                    },
                ],
            },
        ];
        const result = assertNestedSpliceIsClean(
            before,
            after,
            { blockIndex: 0, groupIndex: 0, subColumnIndex: 0 },
            1,
            1,
        );
        assert.equal(result.ok, false);
    });
});

describe('knack_add_action_link', () => {
    it('appends an action link to a table and keeps every existing column, verbatim', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        const result = payloadOf(
            await addActionLink.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    actionLinks: JSON.stringify([
                        {
                            link_text: 'Approve',
                            action_rules: [
                                {
                                    link_text: 'Approve',
                                    record_rules: [],
                                    submit_rules: [
                                        { action: 'message', message: 'ok' },
                                    ],
                                },
                            ],
                        },
                    ]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.action, 'add_action_link');
        assert.equal(result.addedCount, 1);
        assert.equal(result.columnCountBefore, 2);
        assert.equal(result.columnCountAfter, 3);

        assert.equal(requests.length, 1);
        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.equal(sentColumns.length, 3);
        // The two original columns, including the existing link column, re-sent unchanged.
        assert.deepEqual(sentColumns.slice(0, 2), TABLE_VIEW.columns);
        assert.equal(sentColumns[2].type, 'action_link');
        assert.equal(sentColumns[2].link_text, 'Approve');
        // Everything else on the view came through the same merge knack_update_view uses.
        assert.equal(sent.name, 'Contacts table');
        assert.deepEqual(sent.source, TABLE_VIEW.source);

        // The response's own diff names exactly what changed: the columns array grew by
        // one, and nothing else. This is the visibility the GAP-Track incident (an
        // unrelated column silently altered outside this tool) was missing.
        assert.deepEqual(result.structuralDiff, [
            { path: '$.columns', before: 'array(2)', after: 'array(3)' },
        ]);
    });

    it('places the new action link with insertAfterFieldKey', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        await addActionLink.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_1',
                viewKey: 'view_1',
                actionLinks: JSON.stringify([{ link_text: 'Approve' }]),
                insertAfterFieldKey: 'field_1',
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.equal(sentColumns[1].type, 'action_link');
        assert.deepEqual(sentColumns[2], TABLE_VIEW.columns[1]);
    });

    it('lets a caller-supplied type override the action_link default', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        await addActionLink.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_1',
                viewKey: 'view_1',
                actionLinks: JSON.stringify([
                    { type: 'custom_link', link_text: 'Approve' },
                ]),
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.equal(sentColumns[2].type, 'custom_link');
    });

    it('refuses conflicting placement and sends nothing', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addActionLink.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    actionLinks: JSON.stringify([{ link_text: 'Approve' }]),
                    insertAfterFieldKey: 'field_1',
                    insertBeforeFieldKey: 'field_1',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'CONFLICTING_PLACEMENT');
        assert.equal(requests.length, 0);
    });

    it('refuses an anchor that names no existing field column', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addActionLink.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    actionLinks: JSON.stringify([{ link_text: 'Approve' }]),
                    insertAfterFieldKey: 'field_99',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'ANCHOR_NOT_FOUND');
        assert.equal(requests.length, 0);
    });

    it('refuses a view type it does not build a nested layout for', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addActionLink.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_2',
                    viewKey: 'view_4',
                    actionLinks: JSON.stringify([{ link_text: 'Approve' }]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'UNSUPPORTED_VIEW_TYPE');
        assert.match(String(result.message), /form/);
        assert.equal(requests.length, 0);
    });

    it('accepts a stored view type with different casing or whitespace', async () => {
        const metadata = makeMetadata();
        const scenes = (
            metadata.application as { scenes: Array<Record<string, unknown>> }
        ).scenes;
        const scene1 = scenes.find((scene) => scene.key === 'scene_1');
        const views = scene1?.views as Array<Record<string, unknown>>;
        const tableView = views.find((view) => view.key === 'view_1');
        if (tableView) tableView.type = ' Table ';

        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_1/views/view_1': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_1' } },
                },
            },
            metadata,
        );

        const result = payloadOf(
            await addActionLink.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    actionLinks: JSON.stringify([{ link_text: 'Approve' }]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(requests.length, 1);
    });

    it('rejects an actionLinks payload that is not a JSON array', async () => {
        const { ctx } = makeCtx();

        await assert.rejects(
            addActionLink.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    actionLinks: JSON.stringify({ link_text: 'Approve' }),
                },
                ctx,
            ),
            /non-empty JSON array/,
        );
    });

    it('rejects an actionLinks entry that is not a JSON object', async () => {
        const { ctx } = makeCtx();

        await assert.rejects(
            addActionLink.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    actionLinks: JSON.stringify(['Approve']),
                },
                ctx,
            ),
            /must be a JSON object/,
        );
    });

    it('previewOnly sends nothing', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addActionLink.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    actionLinks: JSON.stringify([{ link_text: 'Approve' }]),
                    previewOnly: true,
                },
                ctx,
            ),
        );

        assert.equal(result.error, 'PREVIEW_ONLY');
        assert.equal(requests.length, 0);
    });
});

describe('knack_add_action_link on details/list views', () => {
    it('appends an action link into a nested layout, keeping the existing field', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_20': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_20' } },
                },
            },
            metadataWithNestedView(DETAILS_VIEW),
        );

        const result = payloadOf(
            await addActionLink.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_10',
                    viewKey: 'view_20',
                    actionLinks: JSON.stringify([{ link_text: 'Approve' }]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.addedCount, 1);
        // A pre-splice field count, not a count of the action link just added (which
        // carries no `key` for walkNestedFields to see) — spliceColumnItems computes
        // this from nestedFieldLocations before the splice, same as knack_add_view_columns.
        assert.equal(result.columnCountBefore, 1);
        assert.equal(result.columnCountAfter, 2);

        const sent = requests[0].body as Record<string, unknown>;
        const subColumn = nestedSubColumn(sent.columns);
        assert.equal(subColumn.length, 2);
        assert.deepEqual(
            subColumn[0],
            DETAILS_VIEW.columns[0].groups[0].columns[0][0],
        );
        assert.equal(subColumn[1].type, 'action_link');
        assert.equal(sent.name, 'Contact details');
    });

    it('places the new action link next to an anchor field buried in the layout', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_20': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_20' } },
                },
            },
            metadataWithNestedView(DETAILS_VIEW),
        );

        await addActionLink.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_10',
                viewKey: 'view_20',
                actionLinks: JSON.stringify([{ link_text: 'Approve' }]),
                insertBeforeFieldKey: 'field_1',
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        const subColumn = nestedSubColumn(sent.columns);
        assert.equal(subColumn[0].type, 'action_link');
        assert.equal(subColumn[1].key, 'field_1');
    });
});

describe('knack_add_page_link_column', () => {
    it('appends a page link to a table with the default "link" type, keeping every existing column verbatim', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        const result = payloadOf(
            await addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    pageLinks: JSON.stringify([
                        { header: 'Edit', link_text: 'Edit', scene: 'scene_9' },
                    ]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.action, 'add_page_link_column');
        assert.equal(result.addedCount, 1);
        assert.equal(result.columnCountBefore, 2);
        assert.equal(result.columnCountAfter, 3);

        assert.equal(requests.length, 1);
        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.equal(sentColumns.length, 3);
        assert.deepEqual(sentColumns.slice(0, 2), TABLE_VIEW.columns);
        assert.equal(sentColumns[2].type, 'link');
        assert.equal(sentColumns[2].scene, 'scene_9');
        assert.equal(sentColumns[2].link_text, 'Edit');
    });

    it('places the new page link with insertAfterFieldKey', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        await addPageLinkColumn.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_1',
                viewKey: 'view_1',
                pageLinks: JSON.stringify([
                    { link_text: 'Edit', scene: 'scene_9' },
                ]),
                insertAfterFieldKey: 'field_1',
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.equal(sentColumns[1].type, 'link');
        assert.deepEqual(sentColumns[2], TABLE_VIEW.columns[1]);
    });

    it('lets a caller-supplied type override the default', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        await addPageLinkColumn.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_1',
                viewKey: 'view_1',
                pageLinks: JSON.stringify([
                    { type: 'scene_link', link_text: 'Edit', scene: 'scene_9' },
                ]),
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.equal(sentColumns[2].type, 'scene_link');
    });

    it('creates a new page via a well-formed scene specification', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_1': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_1' } },
            },
        });

        const result = payloadOf(
            await addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    pageLinks: JSON.stringify([
                        {
                            header: 'Edit',
                            link_text: 'Edit',
                            scene: {
                                name: 'Edit Zone Rule',
                                parent: 'jobs2',
                                views: [],
                            },
                        },
                    ]),
                },
                ctx,
            ),
        );

        // A specification is not a broken reference, so the guard lets it through —
        // same as knack_update_view does for a hand-built one (view-guard.test.ts).
        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(requests.length, 1);
        const sent = requests[0].body as Record<string, unknown>;
        const sentColumns = sent.columns as Array<Record<string, unknown>>;
        assert.deepEqual(sentColumns[2].scene, {
            name: 'Edit Zone Rule',
            parent: 'jobs2',
            views: [],
        });
    });

    it('refuses a scene specification missing a views array', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    pageLinks: JSON.stringify([
                        {
                            link_text: 'Edit',
                            scene: { name: 'Edit Zone Rule', parent: 'jobs2' },
                        },
                    ]),
                },
                ctx,
            ),
        );

        // This comes from the guard's own collectMalformedScenePageSpecifications,
        // not from a check this tool duplicates — see the tool's doc comment.
        assert.equal(result.ok, false);
        assert.equal(result.error, 'MALFORMED_PAGE_SPECIFICATION');
        assert.match(String(result.message), /no views array/);
        assert.equal(requests.length, 0);
    });

    it('refuses conflicting placement and sends nothing', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    pageLinks: JSON.stringify([
                        { link_text: 'Edit', scene: 'scene_9' },
                    ]),
                    insertAfterFieldKey: 'field_1',
                    insertBeforeFieldKey: 'field_1',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'CONFLICTING_PLACEMENT');
        assert.equal(requests.length, 0);
    });

    it('refuses an anchor that names no existing field column', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    pageLinks: JSON.stringify([
                        { link_text: 'Edit', scene: 'scene_9' },
                    ]),
                    insertAfterFieldKey: 'field_99',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'ANCHOR_NOT_FOUND');
        assert.equal(requests.length, 0);
    });

    it('refuses a view type it does not build a nested layout for', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_2',
                    viewKey: 'view_4',
                    pageLinks: JSON.stringify([
                        { link_text: 'Edit', scene: 'scene_9' },
                    ]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'UNSUPPORTED_VIEW_TYPE');
        assert.match(String(result.message), /form/);
        assert.equal(requests.length, 0);
    });

    it('rejects a pageLinks payload that is not a JSON array', async () => {
        const { ctx } = makeCtx();

        await assert.rejects(
            addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    pageLinks: JSON.stringify({
                        link_text: 'Edit',
                        scene: 'scene_9',
                    }),
                },
                ctx,
            ),
            /non-empty JSON array/,
        );
    });

    it('rejects a pageLinks entry that is not a JSON object', async () => {
        const { ctx } = makeCtx();

        await assert.rejects(
            addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    pageLinks: JSON.stringify(['Edit']),
                },
                ctx,
            ),
            /must be a JSON object/,
        );
    });

    it('previewOnly sends nothing', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    pageLinks: JSON.stringify([
                        { link_text: 'Edit', scene: 'scene_9' },
                    ]),
                    previewOnly: true,
                },
                ctx,
            ),
        );

        assert.equal(result.error, 'PREVIEW_ONLY');
        assert.equal(requests.length, 0);
    });
});

describe('knack_add_page_link_column on details/list views', () => {
    it('appends a page link into a nested layout with the default "scene_link" type, keeping the existing field', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_20': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_20' } },
                },
            },
            metadataWithNestedView(DETAILS_VIEW),
        );

        const result = payloadOf(
            await addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_10',
                    viewKey: 'view_20',
                    pageLinks: JSON.stringify([
                        { link_text: 'Edit', scene: 'scene_9' },
                    ]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.addedCount, 1);
        // Pre-splice field count via spliceColumnItems, same as the action-link case.
        assert.equal(result.columnCountBefore, 1);
        assert.equal(result.columnCountAfter, 2);

        const sent = requests[0].body as Record<string, unknown>;
        const subColumn = nestedSubColumn(sent.columns);
        assert.equal(subColumn.length, 2);
        assert.deepEqual(
            subColumn[0],
            DETAILS_VIEW.columns[0].groups[0].columns[0][0],
        );
        assert.equal(subColumn[1].type, 'scene_link');
        assert.equal(sent.name, 'Contact details');
    });

    it('places the new page link next to an anchor field buried in the layout', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_20': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_20' } },
                },
            },
            metadataWithNestedView(DETAILS_VIEW),
        );

        await addPageLinkColumn.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_10',
                viewKey: 'view_20',
                pageLinks: JSON.stringify([
                    { link_text: 'Edit', scene: 'scene_9' },
                ]),
                insertBeforeFieldKey: 'field_1',
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        const subColumn = nestedSubColumn(sent.columns);
        assert.equal(subColumn[0].type, 'scene_link');
        assert.equal(subColumn[1].key, 'field_1');
    });

    // Live-verified separately (TESTING.md Tier 23, NPS Test App view_1821, 21 September
    // 2026): the nested path had never been exercised with a page-creating specification
    // before that — only with a plain reference (the two tests above). These close the
    // same gap in the unit suite.
    it('creates a new page via a well-formed scene specification, defaulting to scene_link', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_10/views/view_20': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_20' } },
                },
            },
            metadataWithNestedView(DETAILS_VIEW),
        );

        const result = payloadOf(
            await addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_10',
                    viewKey: 'view_20',
                    pageLinks: JSON.stringify([
                        {
                            link_text: 'Edit',
                            scene: {
                                name: 'Edit Zone Rule',
                                parent: 'jobs2',
                                views: [],
                            },
                        },
                    ]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        const sent = requests[0].body as Record<string, unknown>;
        const subColumn = nestedSubColumn(sent.columns);
        assert.equal(subColumn[1].type, 'scene_link');
        assert.deepEqual(subColumn[1].scene, {
            name: 'Edit Zone Rule',
            parent: 'jobs2',
            views: [],
        });
    });

    it('refuses a nested scene specification missing a views array', async () => {
        const { ctx, requests } = makeCtx(
            {},
            metadataWithNestedView(DETAILS_VIEW),
        );

        const result = payloadOf(
            await addPageLinkColumn.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_10',
                    viewKey: 'view_20',
                    pageLinks: JSON.stringify([
                        {
                            link_text: 'Edit',
                            scene: { name: 'Edit Zone Rule', parent: 'jobs2' },
                        },
                    ]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'MALFORMED_PAGE_SPECIFICATION');
        assert.equal(requests.length, 0);
    });
});

describe('knack_add_view_rules', () => {
    /** A form carrying both a submit rule and a record rule already, to prove neither
     * is disturbed by adding to the other. */
    const FORM_WITH_RULES = {
        key: 'view_30',
        name: 'Contact form',
        type: 'form',
        groups: [],
        inputs: [],
        rules: {
            submits: [{ key: 'submit_1', action: 'message', message: 'Saved' }],
            records: [
                {
                    key: '3',
                    criteria: [
                        { field: 'field_1', operator: 'is', value: 'x' },
                    ],
                    values: [{ field: 'field_2', type: 'value', value: 'y' }],
                },
            ],
        },
    };

    function metadataWithFormRules(): RuntimeMetadata {
        const metadata = makeMetadata();
        (
            metadata.application as { scenes: Array<Record<string, unknown>> }
        ).scenes.push({
            key: 'scene_11',
            name: 'Rules test scene',
            slug: 'rules-test',
            views: [FORM_WITH_RULES],
        });
        return metadata;
    }

    it('appends a record rule and keeps the existing submit rule untouched', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_11/views/view_30': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_30' } },
                },
            },
            metadataWithFormRules(),
        );

        const newRule = {
            criteria: [{ field: 'field_3', operator: 'is', value: 'z' }],
            values: [{ field: 'field_4', type: 'value', value: 'w' }],
        };
        const result = payloadOf(
            await addViewRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_11',
                    viewKey: 'view_30',
                    recordRules: JSON.stringify([newRule]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.action, 'add_view_rules');
        assert.equal(result.recordRulesAdded, 1);
        assert.equal(result.recordRuleCountBefore, 1);
        assert.equal(result.recordRuleCountAfter, 2);
        assert.deepEqual(result.recordRuleKeysAdded, ['4']);
        assert.equal('submitRulesAdded' in result, false);

        assert.equal(requests.length, 1);
        const sent = requests[0].body as Record<string, unknown>;
        const rules = sent.rules as Record<string, unknown>;
        assert.deepEqual(rules.submits, FORM_WITH_RULES.rules.submits);
        // The new rule gets the next numeric key, as the Builder would give it.
        assert.deepEqual(rules.records, [
            ...FORM_WITH_RULES.rules.records,
            { key: '4', ...newRule },
        ]);
        // Everything else on the view came through the same merge knack_update_view uses.
        assert.equal(sent.name, 'Contact form');

        // Only rules.records shows up in the diff — rules.submits and the rest of the
        // view are reported unchanged, not just asserted so via the raw body above.
        assert.deepEqual(result.structuralDiff, [
            { path: '$.rules.records', before: 'array(1)', after: 'array(2)' },
        ]);
    });

    it('appends a submit rule and keeps the existing record rule untouched', async () => {
        const { ctx, requests } = makeCtx(
            {
                'PUT /scenes/scene_11/views/view_30': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_30' } },
                },
            },
            metadataWithFormRules(),
        );

        const newRule = { action: 'record_delete' };
        const result = payloadOf(
            await addViewRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_11',
                    viewKey: 'view_30',
                    submitRules: JSON.stringify([newRule]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.submitRulesAdded, 1);
        assert.equal(result.submitRuleCountBefore, 1);
        assert.equal(result.submitRuleCountAfter, 2);
        assert.deepEqual(result.submitRuleKeysAdded, ['submit_2']);
        assert.equal('recordRulesAdded' in result, false);

        const sent = requests[0].body as Record<string, unknown>;
        const rules = sent.rules as Record<string, unknown>;
        assert.deepEqual(rules.records, FORM_WITH_RULES.rules.records);
        assert.deepEqual(rules.submits, [
            ...FORM_WITH_RULES.rules.submits,
            { ...newRule, key: 'submit_2' },
        ]);
    });

    it('refuses a new rule whose key is already used on the view', async () => {
        const { ctx, requests } = makeCtx({}, metadataWithFormRules());
        await assert.rejects(
            addViewRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_11',
                    viewKey: 'view_30',
                    recordRules: JSON.stringify([{ key: '3', criteria: [] }]),
                },
                ctx,
            ),
            /recordRules\[0\]\.key "3" is already used/,
        );
        assert.equal(requests.length, 0);
    });

    it('adds both kinds of rule to a view that starts with neither', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_2/views/view_4': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_4' } },
            },
        });

        const result = payloadOf(
            await addViewRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_2',
                    viewKey: 'view_4',
                    recordRules: JSON.stringify([{ criteria: [], values: [] }]),
                    submitRules: JSON.stringify([{ action: 'message' }]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.recordRuleCountBefore, 0);
        assert.equal(result.submitRuleCountBefore, 0);
        const sent = requests[0].body as Record<string, unknown>;
        const rules = sent.rules as Record<string, unknown>;
        assert.deepEqual(
            (rules.records as Array<{ key: string }>).map((rule) => rule.key),
            ['1'],
        );
        assert.deepEqual(
            (rules.submits as Array<{ key: string }>).map((rule) => rule.key),
            ['submit_0'],
        );
    });

    it('refuses a rule naming a hidden field, in add and in edit, before any request', async () => {
        const metadata = metadataWithFormRules();
        const object = (
            metadata.application as {
                objects: Array<{ fields: Array<Record<string, unknown>> }>;
            }
        ).objects[0];
        object.fields.push({
            key: 'field_9',
            name: 'Secret',
            type: 'short_text',
            meta: { description: '_mcp_hidden' },
        });
        const { ctx, requests } = makeCtx({}, metadata);
        const copying = {
            criteria: [],
            values: [{ field: 'field_2', type: 'record', input: 'field_9' }],
        };
        const added = payloadOf(
            await addViewRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_11',
                    viewKey: 'view_30',
                    recordRules: JSON.stringify([copying]),
                },
                ctx,
            ),
        );
        assert.equal(added.error, 'HIDDEN_FIELD');
        const edited = payloadOf(
            await editViewRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_11',
                    viewKey: 'view_30',
                    ruleSet: 'records',
                    replaceRules: JSON.stringify([{ key: '3', ...copying }]),
                },
                ctx,
            ),
        );
        assert.equal(edited.error, 'HIDDEN_FIELD');
        assert.equal(requests.length, 0);
    });

    it('refuses when neither recordRules nor submitRules is given', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewRules.handler(
                { appKey: 'Demo', sceneKey: 'scene_2', viewKey: 'view_4' },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'NOTHING_TO_ADD');
        assert.equal(requests.length, 0);
    });

    it('rejects a rules payload that is not a JSON array', async () => {
        const { ctx } = makeCtx();

        await assert.rejects(
            addViewRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_2',
                    viewKey: 'view_4',
                    recordRules: JSON.stringify({ criteria: [] }),
                },
                ctx,
            ),
            /non-empty JSON array/,
        );
    });

    it('rejects a rules entry that is not a JSON object', async () => {
        const { ctx } = makeCtx();

        await assert.rejects(
            addViewRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_2',
                    viewKey: 'view_4',
                    submitRules: JSON.stringify(['not-a-rule']),
                },
                ctx,
            ),
            /must be a JSON object/,
        );
    });

    it('refuses a view that cannot be found', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_no_such',
                    recordRules: JSON.stringify([{ criteria: [] }]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'VIEW_NOT_FOUND');
        assert.equal(requests.length, 0);
    });

    it('previewOnly sends nothing', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_2',
                    viewKey: 'view_4',
                    recordRules: JSON.stringify([{ criteria: [] }]),
                    previewOnly: true,
                },
                ctx,
            ),
        );

        assert.equal(result.error, 'PREVIEW_ONLY');
        assert.equal(requests.length, 0);
    });
});

describe('knack_add_view_links', () => {
    it('appends a link and keeps the existing one, verbatim', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_2': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_2' } },
            },
        });

        const newLink = {
            name: 'Docs',
            type: 'url',
            url: 'https://example.com',
        };
        const result = payloadOf(
            await addViewLinks.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_2',
                    links: JSON.stringify([newLink]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.action, 'add_view_links');
        assert.equal(result.addedCount, 1);
        assert.equal(result.linkCountBefore, 1);
        assert.equal(result.linkCountAfter, 2);

        assert.equal(requests.length, 1);
        const sent = requests[0].body as Record<string, unknown>;
        assert.deepEqual(sent.links, [...MENU_VIEW.links, newLink]);
        assert.equal(sent.name, 'Nav');

        assert.deepEqual(result.structuralDiff, [
            { path: '$.links', before: 'array(1)', after: 'array(2)' },
        ]);
    });

    it('inserts at insertAtIndex rather than always appending', async () => {
        const { ctx, requests } = makeCtx({
            'PUT /scenes/scene_1/views/view_2': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_2' } },
            },
        });

        const newLink = { name: 'Home', type: 'scene', scene: 'home' };
        await addViewLinks.handler(
            {
                appKey: 'Demo',
                sceneKey: 'scene_1',
                viewKey: 'view_2',
                links: JSON.stringify([newLink]),
                insertAtIndex: 0,
            },
            ctx,
        );

        const sent = requests[0].body as Record<string, unknown>;
        assert.deepEqual(sent.links, [newLink, ...MENU_VIEW.links]);
    });

    it('refuses an insertAtIndex past the end of the existing links', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewLinks.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_2',
                    links: JSON.stringify([{ name: 'Home' }]),
                    insertAtIndex: 5,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'INDEX_OUT_OF_RANGE');
        assert.equal(requests.length, 0);
    });

    it('rejects a links payload that is not a JSON array', async () => {
        const { ctx } = makeCtx();

        await assert.rejects(
            addViewLinks.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_2',
                    links: JSON.stringify({ name: 'Home' }),
                },
                ctx,
            ),
            /non-empty JSON array/,
        );
    });

    it('rejects a links entry that is not a JSON object', async () => {
        const { ctx } = makeCtx();

        await assert.rejects(
            addViewLinks.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_2',
                    links: JSON.stringify(['Home']),
                },
                ctx,
            ),
            /must be a JSON object/,
        );
    });

    it('refuses a view that cannot be found', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewLinks.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_no_such',
                    links: JSON.stringify([{ name: 'Home' }]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'VIEW_NOT_FOUND');
        assert.equal(requests.length, 0);
    });

    it('previewOnly sends nothing', async () => {
        const { ctx, requests } = makeCtx();

        const result = payloadOf(
            await addViewLinks.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_2',
                    links: JSON.stringify([{ name: 'Home' }]),
                    previewOnly: true,
                },
                ctx,
            ),
        );

        assert.equal(result.error, 'PREVIEW_ONLY');
        assert.equal(requests.length, 0);
    });
});

describe('knack_copy_view', () => {
    /**
     * These two pin the wiring, not the rule. `summariseCopyLinkOwnership` is unit
     * tested against the rule in the incident suite; what is untested without these is
     * that the tool hands it the right thing. The created pages are read from
     * `outcome.body`, whose type is `unknown` at that call site, so a wrong path
     * compiles cleanly and silently reports every copy as sharing.
     */
    it('reports duplicated when the response says a page was created', async () => {
        const { ctx } = makeCtx({
            'POST /scenes/scene_1/copyview': {
                ok: true,
                status: 200,
                body: {
                    view: { key: 'view_11' },
                    changes: {
                        inserts: {
                            scenes: [
                                {
                                    key: 'scene_9',
                                    name: 'Edit contact',
                                    slug: 'edit-contact2',
                                    parent: 'reports',
                                },
                            ],
                            views: ['view_11'],
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

        assert.deepEqual(result.copyLinkOwnership, [
            {
                header: 'Edit',
                childSceneRef: 'edit-contact',
                owned: true,
                onCopy: 'duplicated',
            },
        ]);
        assert.match(
            String(result.copyLinkNote),
            /1 linked page\(s\) were duplicated/,
        );
        assert.match(String(result.copyLinkNote), /new page with a new slug/);
    });

    it('names the new view key up front and reports an empty structuralDiff, not the source view leaf by leaf', async () => {
        // Regression for a live incident (GAP-Track, 2026-09-23): comparing the whole
        // source view against copyview's tiny {action, target_scene_key, ...} control
        // payload produced a "diff" that was really the entire source view, one leaf
        // entry at a time (~120k characters for one call). A copy changes nothing about
        // the source, so there is nothing to diff.
        const { ctx } = makeCtx({
            'POST /scenes/scene_1/copyview': {
                ok: true,
                status: 200,
                body: {
                    view: { key: 'view_11' },
                    changes: { inserts: { views: ['view_11'] } },
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
                    completeViewSchema: true,
                },
                ctx,
            ),
        );

        assert.deepEqual(result.newViewKeys, ['view_11']);
        assert.deepEqual(result.structuralDiff, []);
    });

    it('reports shared when the response created no page, however the link is flagged', async () => {
        // The details and list case: same owned link, same call, and Knack makes no
        // page. Reported from the response, so the flag does not get to overrule it.
        const { ctx } = makeCtx({
            'POST /scenes/scene_1/copyview': {
                ok: true,
                status: 200,
                body: {
                    view: { key: 'view_11' },
                    changes: { inserts: { views: ['view_11'] } },
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

        assert.deepEqual(result.copyLinkOwnership, [
            {
                header: 'Edit',
                childSceneRef: 'edit-contact',
                // Still owned - the flag is absent and that remains a true fact about
                // the link, and the cascade guard still needs it.
                owned: true,
                onCopy: 'shared',
            },
        ]);
        assert.match(
            String(result.copyLinkNote),
            /0 linked page\(s\) were duplicated/,
        );
        // The clause that was false in the wild must not appear when nothing was made.
        assert.doesNotMatch(
            String(result.copyLinkNote),
            /new page with a new slug/,
        );
        assert.match(String(result.copyLinkNote), /linked from two views/);
    });

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
        // One read for the guard, a second for the snapshot's tree — the view itself
        // needed neither, which is what this case is about.
        //
        // The third is the layout repair reading the target page back, added 10
        // September. Knack's copyview endpoint puts the new view's key into every row
        // of the target page's layout, and the only way to know whether it did that
        // here is to look. Counted explicitly rather than loosened to "at least two":
        // a read per mutation is a real cost, and a fourth appearing unnoticed is
        // exactly what this assertion exists to catch.
        assert.deepEqual(runtimeMetadataFetches, ['Demo', 'Demo', 'Demo']);
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
        // What Knack returns for the payload this now sends: the same links, with
        // ownership given up. A copy that came back owning them is a different test.
        const copyAttributes = {
            ...TABLE_VIEW,
            key: 'view_12',
            name: 'Contacts table Copy',
            columns: [
                TABLE_VIEW.columns[0],
                { ...TABLE_VIEW.columns[1], remote: true },
            ],
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
        // The link column is sent with ownership given up: the source still owns
        // edit-contact, and a copy that claimed it too would take the page with it on
        // any later move.
        assert.deepEqual(sent.columns, [
            TABLE_VIEW.columns[0],
            { ...TABLE_VIEW.columns[1], remote: true },
        ]);
        assert.deepEqual(result.sharedPagesReleased, ['edit-contact']);
        assert.equal('sharedPagesStillOwned' in result, false);
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

    it('sharePages true honours a placement anchor against an explicit layout, rather than pinning the copy to the end', async () => {
        const { ctx, requests } = makeCtx({
            'POST /scenes/scene_1/views': {
                ok: true,
                status: 200,
                body: {
                    view: { ...TABLE_VIEW, key: 'view_14' },
                    changes: { inserts: { views: [{ key: 'view_14' }] } },
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
                    existingViewKeys: ['view_1', 'view_2'],
                    insertBeforeViewKey: 'view_2',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        const sent = requests[0].body as {
            pageGroups: Array<{ columns: Array<{ keys: string[] }> }>;
        };
        assert.equal(requests[0].apiPath, '/scenes/scene_1/views');
        assert.deepEqual(
            sent.pageGroups.map((row) => row.columns[0].keys[0]),
            ['view_1', 'new', 'view_2'],
        );
    });

    it('sharePages true refuses an anchor the layout does not render, and both anchors at once, without sending anything', async () => {
        const { ctx, requests } = makeCtx();

        const unknownAnchor = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_1',
                    sharePages: true,
                    completeViewSchema: false,
                    existingViewKeys: ['view_1'],
                    insertAfterViewKey: 'view_999',
                },
                ctx,
            ),
        );
        assert.equal(unknownAnchor.ok, false);
        assert.equal(unknownAnchor.error, 'ANCHOR_NOT_IN_LAYOUT');

        const bothAnchors = payloadOf(
            await copyView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_1',
                    sharePages: true,
                    completeViewSchema: false,
                    insertAfterViewKey: 'view_1',
                    insertBeforeViewKey: 'view_2',
                },
                ctx,
            ),
        );
        assert.equal(bothAnchors.ok, false);
        assert.equal(bothAnchors.error, 'CONFLICTING_PLACEMENT');

        assert.deepEqual(
            requests.filter((request) => request.method === 'POST'),
            [],
        );
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
    it('refuses an anchor the target page does not render, and both anchors at once, without sending anything', async () => {
        const { ctx, requests } = makeCtx();

        const unknownAnchor = payloadOf(
            await moveView.handler(
                {
                    appKey: 'Demo',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_3',
                    viewKey: 'view_3',
                    completeViewSchema: false,
                    insertAfterViewKey: 'view_999',
                },
                ctx,
            ),
        );
        assert.equal(unknownAnchor.ok, false);
        assert.equal(unknownAnchor.error, 'ANCHOR_NOT_IN_LAYOUT');

        const bothAnchors = payloadOf(
            await moveView.handler(
                {
                    appKey: 'Demo',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_3',
                    viewKey: 'view_3',
                    completeViewSchema: false,
                    insertAfterViewKey: 'view_1',
                    insertBeforeViewKey: 'view_2',
                },
                ctx,
            ),
        );
        assert.equal(bothAnchors.ok, false);
        assert.equal(bothAnchors.error, 'CONFLICTING_PLACEMENT');

        // The anchor is checked before the move, so a bad one costs nothing: a view
        // moved and then left unplaced would be worse than one that never moved.
        assert.deepEqual(
            requests.filter((request) => request.method === 'POST'),
            [],
        );
    });

    it('reads the target layout fresh before validating an anchor', async () => {
        // The cached payload is up to five minutes old. An anchor removed inside that
        // window would pass a cached check, let the move go, and then fail the
        // post-move repair — the moved-and-unplaced state the check exists to prevent.
        const app = makeApp({ appFolder: tmpDir });
        const { ctx, runtimeMetadataFetches } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { [app.appKey]: makeMetadata() },
        });
        // Warm the cache, so a second read only happens if it was invalidated.
        await ctx.getRuntimeMetadata(app);
        const warmed = runtimeMetadataFetches.length;

        await moveView.handler(
            {
                appKey: 'Demo',
                sourceSceneKey: 'scene_1',
                targetSceneKey: 'scene_3',
                viewKey: 'view_3',
                completeViewSchema: false,
                insertAfterViewKey: 'view_999',
            },
            ctx,
        );

        assert.ok(
            runtimeMetadataFetches.length > warmed,
            'the anchor preflight read the cache instead of refreshing it',
        );
    });

    it('reports an anchored placement that joined the stack, and one that got its own row', async () => {
        // Both success messages are new, and neither was asserted anywhere: a
        // regression in either would have been silent.
        const layoutOf = async (
            targetGroups: unknown[],
            anchor: string,
        ): Promise<string> => {
            const metadata = makeMetadata();
            const scenes = (
                metadata.application as unknown as {
                    scenes: Array<Record<string, unknown>>;
                }
            ).scenes;
            const target = scenes.find((scene) => scene.key === 'scene_3');
            if (!target) throw new Error('fixture lost scene_3');
            target.groups = targetGroups;
            target.views = [
                ...((target.views as unknown[]) ?? []),
                { key: 'view_3' },
            ];

            const app = makeApp({ appFolder: tmpDir });
            const { ctx } = makeFakeContext({
                apps: [app],
                runtimeMetadata: { [app.appKey]: metadata },
                responses: {
                    'POST /scenes/scene_1/copyview': {
                        ok: true,
                        status: 200,
                        body: { view: { key: 'view_3' } },
                    },
                    'POST /scenes/scene_3/views/sort': {
                        ok: true,
                        status: 200,
                        body: { views: [] },
                    },
                },
            });

            const result = payloadOf(
                await moveView.handler(
                    {
                        appKey: 'Demo',
                        sourceSceneKey: 'scene_1',
                        targetSceneKey: 'scene_3',
                        viewKey: 'view_3',
                        completeViewSchema: false,
                        insertAfterViewKey: anchor,
                    },
                    ctx,
                ),
            );
            assert.equal(result.layoutRepair, 'added', JSON.stringify(result));
            return String(result.layoutNote);
        };

        const joined = await layoutOf(
            [{ columns: [{ keys: ['view_a', 'view_b'], width: 100 }] }],
            'view_a',
        );
        assert.match(joined, /directly after view_a in the column they share/);
        assert.match(joined, /changed no column widths/);

        const ownRow = await layoutOf(
            [{ columns: [{ keys: ['view_a'], width: 100 }] }],
            'view_a',
        );
        assert.match(ownRow, /a row of its own directly after the row/);
        assert.match(ownRow, /view_a is alone in its column/);
    });

    it('describes a refused move as a rebuild, not a destruction', async () => {
        // The guard refuses this move (the view owns edit-contact and no human can be
        // prompted), which is also the only way to see the stakes sentence a caller
        // actually reads. It must not say "destroys".
        const { ctx } = makeCtx();

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
        assert.match(
            String(result.message),
            /rebuilds 1 page under the target page with a new key and slug, and deletes the original/,
        );
        assert.doesNotMatch(String(result.message), /destroys/);
        // One owned page leaves nothing behind, so the aftermath clause stays out.
        assert.doesNotMatch(
            String(result.message),
            /check the source page afterwards/,
        );
        // The refusal still points at the builder as the route.
        assert.match(String(result.message), /Knack builder/);
    });

    it('reports no orphan check for a move whose view owns no pages', async () => {
        const { ctx } = makeCtx({
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
                    completeViewSchema: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        // view_3 is a rich text view with no links: nothing owned, nothing to report.
        assert.equal('orphanCheck' in result, false);
    });

    it('removes the row the moved view left empty on the page it came from', async () => {
        // Knack takes the moved view out of the source page's layout and leaves the
        // row standing. Measured on a live page: the row stayed as
        // {"columns":[{"keys":[],"width":100}]} and nothing looked at it.
        const metadata = makeMetadata();
        const scenes = (
            metadata.application as unknown as {
                scenes: Array<Record<string, unknown>>;
            }
        ).scenes;
        const source = scenes.find((scene) => scene.key === 'scene_1');
        if (!source) throw new Error('fixture lost scene_1');
        source.groups = [
            { columns: [{ keys: ['view_2'], width: 100 }] },
            // Already empty before the move: layout somebody chose, and kept.
            { columns: [{ keys: [], width: 100 }] },
            // The row the move will empty: residue, and removed.
            { columns: [{ keys: ['view_3'], width: 100 }] },
        ];

        const app = makeApp({ appFolder: tmpDir });
        const { ctx, requests } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { [app.appKey]: metadata },
            responses: {
                'POST /scenes/scene_1/copyview': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_3' } },
                },
                'POST /scenes/scene_1/views/sort': {
                    ok: true,
                    status: 200,
                    body: { views: [] },
                },
            },
        });

        const result = payloadOf(
            await moveView.handler(
                {
                    appKey: 'Demo',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_3',
                    viewKey: 'view_3',
                    completeViewSchema: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.sourceLayoutRepair, 'removed');

        const sort = requests.find(
            (request) => request.apiPath === '/scenes/scene_1/views/sort',
        );
        assert.ok(sort, 'the source page layout was never rewritten');
        const sent = sort.body as { pageGroups: unknown[] };
        // view_3's row is gone; the row that was already empty is untouched.
        assert.deepEqual(sent.pageGroups, [
            { columns: [{ keys: ['view_2'], width: 100 }] },
            { columns: [{ keys: [], width: 100 }] },
        ]);
    });

    it('posts nothing to the source page when its layout never rendered the moved view', async () => {
        const metadata = makeMetadata();
        const scenes = (
            metadata.application as unknown as {
                scenes: Array<Record<string, unknown>>;
            }
        ).scenes;
        const source = scenes.find((scene) => scene.key === 'scene_1');
        if (!source) throw new Error('fixture lost scene_1');
        source.groups = [{ columns: [{ keys: ['view_2'], width: 100 }] }];

        const app = makeApp({ appFolder: tmpDir });
        const { ctx, requests } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { [app.appKey]: metadata },
            responses: {
                'POST /scenes/scene_1/copyview': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_3' } },
                },
            },
        });

        const result = payloadOf(
            await moveView.handler(
                {
                    appKey: 'Demo',
                    sourceSceneKey: 'scene_1',
                    targetSceneKey: 'scene_3',
                    viewKey: 'view_3',
                    completeViewSchema: false,
                },
                ctx,
            ),
        );

        assert.equal(result.sourceLayoutRepair, 'not-needed');
        assert.equal(
            requests.some(
                (request) => request.apiPath === '/scenes/scene_1/views/sort',
            ),
            false,
        );
    });

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
     * says. The SDK cancels an overdue elicitation with a SdkError carrying
     * SdkErrorCode.RequestTimeout; everything else that throws is a real failure and
     * stays `supported: false`.
     */
    function contextThatElicits(
        behaviour: (request?: unknown) => Promise<unknown>,
    ) {
        const { ctx } = makeFakeContext();
        ctx.server = {
            server: {
                // form: {} — this server only ever requests form-mode elicitation, so
                // clientCanPromptHuman() checks that specific sub-capability, not just
                // truthiness of the whole elicitation object.
                getClientCapabilities: () => ({ elicitation: { form: {} } }),
                getClientVersion: () => ({ name: 'test', version: '1' }),
                elicitInput: behaviour,
            },
        } as unknown as typeof ctx.server;
        return ctx;
    }

    const input: {
        action: ViewMutationAction;
        sceneKey: string;
        viewKey?: string;
        childPages: ChildPage[];
        externalPages: ClassifiedLinkTarget[];
        transferredPages: ClassifiedLinkTarget[];
        unresolvedLinkCount: number;
    } = {
        action: 'update_view',
        sceneKey: 'scene_1',
        viewKey: 'view_1',
        childPages: [
            {
                sceneKey: 'scene_2',
                sceneName: 'Child',
                sceneSlug: 'child',
                depth: 0,
            },
        ],
        externalPages: [],
        transferredPages: [],
        unresolvedLinkCount: 0,
    };

    it('matches the code the SDK actually sends', () => {
        // The cases below build their error from SdkErrorCode.RequestTimeout, so they
        // check the predicate against the SDK's constant rather than a magic value.
        // That cannot notice the constant changing, which would stop the predicate
        // matching real timeouts — so pin the value once, here. On the v1 SDK a timed
        // out request threw an McpError carrying the JSON-RPC wire code -32001; on the
        // v2 SDK (@modelcontextprotocol/server) it throws a local SdkError instead,
        // whose RequestTimeout code is the string 'REQUEST_TIMEOUT' rather than a wire
        // code at all — the production check in isRequestTimeout reads the constant,
        // not this literal, so it tracked the change automatically.
        assert.equal(SdkErrorCode.RequestTimeout, 'REQUEST_TIMEOUT');
    });

    it('refuses a client that supports only url-mode elicitation, without ever calling it', async () => {
        // elicitInput below is always called with a requestedSchema (form mode), never
        // mode: 'url'. A client advertising only url-mode support must be treated as
        // unable to answer this specific prompt — not passed through to a call that
        // would then fail with a non-timeout CAPABILITY_NOT_SUPPORTED error and get
        // misreported as a generic elicitation failure instead of "cannot be asked".
        let called = false;
        const { ctx } = makeFakeContext();
        ctx.server = {
            server: {
                getClientCapabilities: () => ({ elicitation: { url: {} } }),
                getClientVersion: () => ({ name: 'test', version: '1' }),
                elicitInput: async () => {
                    called = true;
                    return { action: 'decline' };
                },
            },
        } as unknown as typeof ctx.server;

        const result = await askHumanToConfirmPageDeletion(
            ctx,
            makeApp(),
            input,
        );

        assert.deepEqual(result, {
            supported: false,
            reason: 'the client did not advertise the elicitation capability',
        });
        assert.equal(called, false);
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
                    ref: 'protected-page',
                    sceneKey: 'scene_9',
                    sceneName: null,
                    sceneSlug: null,
                    classification: 'transferred',
                    parentSceneKey: 'scene_1',
                    otherReferrers: [
                        { sceneKey: 'scene_7', viewKey: 'view_67' },
                    ],
                    reason: 'another view still links to it',
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
                    ref: 'protected-page',
                    sceneKey: 'scene_9',
                    sceneName: null,
                    sceneSlug: null,
                    classification: 'transferred',
                    parentSceneKey: 'scene_1',
                    otherReferrers: [
                        { sceneKey: 'scene_7', viewKey: 'view_67' },
                        { sceneKey: 'scene_6', viewKey: 'view_68' },
                    ],
                    reason: 'two other views still link to it',
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
            code: SdkErrorCode.RequestTimeout,
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
                code: SdkErrorCode.RequestTimeout,
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
            {
                sceneKey: 'scene_9',
                sceneName: 'Protected page',
                sceneSlug: 'protected-page',
                depth: 0,
            },
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
                getClientCapabilities: () => ({ elicitation: { form: {} } }),
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

describe('knack_edit_view_rules', () => {
    const FORM = {
        key: 'view_30',
        name: 'Contact form',
        type: 'form',
        groups: [],
        inputs: [],
        rules: {
            submits: [
                {
                    key: 'submit_1',
                    action: 'message',
                    message: 'Saved',
                    is_default: true,
                },
                {
                    key: 'submit_2',
                    action: 'redirect',
                    url: 'https://example.com',
                },
            ],
            records: [
                { key: '15', action: 'record', values: [], criteria: [] },
                { key: '16', action: 'record', values: [], criteria: [] },
            ],
            fields: [{ key: '10', actions: [], criteria: [] }],
        },
    };

    function setup() {
        const metadata = makeMetadata();
        (
            metadata.application as {
                objects: Array<{ fields: Array<Record<string, unknown>> }>;
            }
        ).objects[0].fields.push({
            key: 'field_9',
            name: 'Salary',
            type: 'number',
            meta: { description: '_mcp_writeonly' },
        });
        (
            metadata.application as { scenes: Array<Record<string, unknown>> }
        ).scenes.push({
            key: 'scene_11',
            name: 'Rules test scene',
            slug: 'rules-test',
            views: [FORM],
        });
        return makeCtx(
            {
                'PUT /scenes/scene_11/views/view_30': {
                    ok: true,
                    status: 200,
                    body: { view: { key: 'view_30' } },
                },
            },
            metadata,
        );
    }

    const run = (
        ctx: ReturnType<typeof setup>['ctx'],
        args: Record<string, unknown>,
    ) =>
        editViewRules
            .handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_11',
                    viewKey: 'view_30',
                    ...args,
                } as Parameters<typeof editViewRules.handler>[0],
                ctx,
            )
            .then(payloadOf);

    it('removes a record rule and leaves every other rule set untouched', async () => {
        const { ctx, requests } = setup();
        const result = await run(ctx, {
            ruleSet: 'records',
            removeKeys: ['15'],
        });

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.deepEqual(result.removedKeys, ['15']);
        const rules = (requests[0].body as Record<string, unknown>)
            .rules as Record<string, unknown>;
        assert.deepEqual(rules.records, [FORM.rules.records[1]]);
        assert.deepEqual(rules.submits, FORM.rules.submits);
        assert.deepEqual(rules.fields, FORM.rules.fields);
    });

    it('replaces a display rule in place', async () => {
        const { ctx, requests } = setup();
        const replacement = {
            key: '10',
            actions: [{ field: 'field_2', action: 'show-hide', value: '' }],
            criteria: [],
        };
        const result = await run(ctx, {
            ruleSet: 'fields',
            replaceRules: JSON.stringify([replacement]),
        });

        assert.equal(result.ok, true, JSON.stringify(result));
        const rules = (requests[0].body as Record<string, unknown>)
            .rules as Record<string, unknown>;
        assert.deepEqual(rules.fields, [replacement]);
    });

    it('lets a display rule test and target a write-only field, but not a record rule', async () => {
        const { ctx, requests } = setup();
        const display = {
            key: '10',
            actions: [{ field: 'field_9', action: 'hide', value: '' }],
            criteria: [{ field: 'field_9', operator: 'is blank', value: '' }],
        };
        const shown = await run(ctx, {
            ruleSet: 'fields',
            replaceRules: JSON.stringify([display]),
        });
        assert.equal(shown.ok, true, JSON.stringify(shown));

        const record = await run(ctx, {
            ruleSet: 'records',
            replaceRules: JSON.stringify([
                {
                    key: '15',
                    action: 'record',
                    values: [],
                    criteria: [
                        { field: 'field_9', operator: 'is', value: '1' },
                    ],
                },
            ]),
        });
        assert.equal(record.error, 'WRITE_ONLY_FIELD');
        assert.equal(requests.length, 1);
    });

    it('refuses to remove the default submit rule', async () => {
        const { ctx, requests } = setup();
        const result = await run(ctx, {
            ruleSet: 'submits',
            removeKeys: ['submit_1'],
        });

        assert.equal(result.error, 'DEFAULT_SUBMIT_RULE');
        assert.equal(requests.length, 0);
    });

    it('refuses a replacement that drops is_default from the default submit rule', async () => {
        const { ctx, requests } = setup();
        const result = await run(ctx, {
            ruleSet: 'submits',
            replaceRules: JSON.stringify([
                { key: 'submit_1', action: 'message', message: 'New' },
            ]),
        });

        assert.equal(result.error, 'DEFAULT_SUBMIT_RULE');
        assert.equal(requests.length, 0);
    });

    it('refuses a key that is not in the named rule set', async () => {
        const { ctx, requests } = setup();
        const result = await run(ctx, {
            ruleSet: 'records',
            removeKeys: ['submit_2'],
        });

        assert.equal(result.error, 'INVALID_EDIT');
        assert.match(String(result.message), /Stored keys: 15, 16/);
        assert.equal(requests.length, 0);
    });
});

describe('fields the app no longer has', () => {
    const PUT_OK = {
        'PUT /scenes/scene_1/views/view_1': {
            ok: true,
            status: 200,
            body: { view: { key: 'view_1' }, changes: {} },
        },
    };
    const deadColumn = {
        type: 'field',
        field: { key: 'field_77' },
        header: 'Deleted in the builder',
    };

    it('refuses updates that bring back a deleted field, preview included, with nothing sent', async () => {
        const { ctx, requests } = makeCtx(PUT_OK);
        for (const previewOnly of [false, true]) {
            const result = payloadOf(
                await updateView.handler(
                    {
                        appKey: 'Demo',
                        sceneKey: 'scene_1',
                        viewKey: 'view_1',
                        // A columns array built from a copy read before field_77 was deleted.
                        updates: JSON.stringify({
                            columns: [...TABLE_VIEW.columns, deadColumn],
                        }),
                        previewOnly,
                    },
                    ctx,
                ),
            );
            assert.equal(result.error, 'UNKNOWN_FIELD_IN_VIEW');
            assert.deepEqual(result.unknownFieldKeysInUpdates, ['field_77']);
            assert.equal(result.unknownFieldKeysInStoredView, undefined);
            assert.match(String(result.message), /Read the view again/);
        }
        assert.equal(requests.length, 0);
    });

    it('names a deleted field the stored view still carries, and lets the updates drop it', async () => {
        const metadata = makeMetadata();
        const scenes = (
            metadata.application as {
                scenes: Array<{ views: Array<Record<string, unknown>> }>;
            }
        ).scenes;
        scenes[0].views[0] = {
            ...TABLE_VIEW,
            columns: [...TABLE_VIEW.columns, deadColumn],
        };
        const { ctx, requests } = makeCtx(PUT_OK, metadata);

        const renamed = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    updates: JSON.stringify({ name: 'Renamed' }),
                },
                ctx,
            ),
        );
        assert.equal(renamed.error, 'UNKNOWN_FIELD_IN_VIEW');
        assert.deepEqual(renamed.unknownFieldKeysInStoredView, ['field_77']);
        assert.match(String(renamed.message), /without it/);
        assert.equal(requests.length, 0);

        const repaired = payloadOf(
            await updateView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    viewKey: 'view_1',
                    updates: JSON.stringify({ columns: TABLE_VIEW.columns }),
                },
                ctx,
            ),
        );
        assert.equal(repaired.ok, true, JSON.stringify(repaired));
        assert.equal(requests.length, 1);
    });

    it('refuses a new view that names a missing field', async () => {
        const { ctx, requests } = makeCtx({
            'POST /scenes/scene_3/views': {
                ok: true,
                status: 200,
                body: { view: { key: 'view_9' } },
            },
        });
        const result = payloadOf(
            await createView.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_3',
                    payload: JSON.stringify({
                        name: 'New table',
                        type: 'table',
                        source: { object: 'object_1' },
                        columns: [deadColumn],
                    }),
                },
                ctx,
            ),
        );
        assert.equal(result.error, 'UNKNOWN_FIELD_IN_VIEW');
        assert.deepEqual(result.unknownFieldKeysInUpdates, ['field_77']);
        assert.equal(requests.length, 0);
    });
});
