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
import type {
    ChildPage,
    ClassifiedLinkTarget,
    ViewMutationAction,
} from '../lib/view-safety.js';
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
