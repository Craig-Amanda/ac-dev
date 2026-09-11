import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { makeApp, makeFakeContext } from './testing/fake-context.js';
import type { RuntimeMetadata } from './types.js';
import { findOrphansLeftByMove } from './view-mutation.js';

/**
 * Post-move metadata: the moved view links the rebuilt copies, and the originals are
 * in whatever state Knack left them.
 */
function metadataWith(scenes: unknown[]): RuntimeMetadata {
    return {
        application: { name: 'Demo', slug: 'demo', objects: [], scenes },
    } as unknown as RuntimeMetadata;
}

const movedView = (refs: string[]) => ({
    key: 'view_1',
    type: 'details',
    columns: [
        {
            groups: [
                {
                    columns: [
                        refs.map((scene) => ({ type: 'scene_link', scene })),
                    ],
                },
            ],
        },
    ],
});

function ctxFor(scenes: unknown[]) {
    const app = makeApp({});
    const { ctx } = makeFakeContext({
        apps: [app],
        runtimeMetadata: { [app.appKey]: metadataWith(scenes) },
    });
    return { ctx, app };
}

describe('findOrphansLeftByMove', () => {
    it('names an original that survived with nothing linking it', async () => {
        // Knack rebuilt both pages under the target and deleted only the first
        // original. scene_594 is still there, still parented to the source page, and no
        // view links it — the case the whole function exists for.
        const { ctx, app } = ctxFor([
            {
                key: 'scene_9',
                slug: 'target',
                views: [movedView(['edit-client2', 'view-client-details3'])],
            },
            {
                key: 'scene_597',
                slug: 'edit-client2',
                parent: 'target',
                views: [],
            },
            {
                key: 'scene_598',
                slug: 'view-client-details3',
                parent: 'target',
                views: [],
            },
            {
                key: 'scene_594',
                name: 'View Client Details',
                slug: 'view-client-details2',
                parent: 'client-summary',
                views: [],
            },
        ]);

        const result = await findOrphansLeftByMove(ctx, app, [
            'scene_593',
            'scene_594',
        ]);

        assert.equal(result.orphanCheck, 'found');
        // scene_593 was deleted, so it is not an orphan — it is simply gone.
        assert.deepEqual(result.orphansLeftBehind, [
            {
                sceneKey: 'scene_594',
                sceneName: 'View Client Details',
                sceneSlug: 'view-client-details2',
                parentRef: 'client-summary',
            },
        ]);
        assert.match(String(result.orphanNote), /render nowhere/);
    });

    it('reports none when every surviving original is still linked', async () => {
        const { ctx, app } = ctxFor([
            {
                key: 'scene_9',
                slug: 'target',
                views: [movedView(['edit-client'])],
            },
            {
                key: 'scene_593',
                slug: 'edit-client',
                parent: 'client-summary',
                views: [],
            },
        ]);

        const result = await findOrphansLeftByMove(ctx, app, ['scene_593']);
        assert.equal(result.orphanCheck, 'none');
        assert.equal('orphansLeftBehind' in result, false);
    });

    it('resolves a link given as a slug against the page it names', async () => {
        // Links name pages by slug; the owned set is scene keys. A mismatch here would
        // report every still-linked page as orphaned.
        const { ctx, app } = ctxFor([
            {
                key: 'scene_9',
                slug: 'target',
                views: [movedView(['kept-page'])],
            },
            {
                key: 'scene_700',
                slug: 'kept-page',
                parent: 'client-summary',
                views: [],
            },
        ]);

        const result = await findOrphansLeftByMove(ctx, app, ['scene_700']);
        assert.equal(result.orphanCheck, 'none');
    });

    it('does not count a submit-rule redirect as keeping a page alive', async () => {
        // A redirect reaches a page without keeping it: nothing navigates to it. The
        // referrer graph excludes these, and counting one would report a genuinely
        // orphaned page as still reached.
        const { ctx, app } = ctxFor([
            {
                key: 'scene_9',
                slug: 'target',
                views: [
                    {
                        key: 'view_2',
                        type: 'form',
                        rules: {
                            submits: [{ action: 'scene', scene: 'stranded' }],
                        },
                    },
                ],
            },
            {
                key: 'scene_701',
                name: 'Stranded',
                slug: 'stranded',
                parent: 'client-summary',
                views: [],
            },
        ]);

        const result = await findOrphansLeftByMove(ctx, app, ['scene_701']);
        assert.equal(result.orphanCheck, 'found');
        assert.equal(
            (result.orphansLeftBehind as Array<{ sceneKey: string }>)[0]
                .sceneKey,
            'scene_701',
        );
    });

    it('counts a child_page submit rule, which does keep its page', async () => {
        const { ctx, app } = ctxFor([
            {
                key: 'scene_9',
                slug: 'target',
                views: [
                    {
                        key: 'view_2',
                        type: 'form',
                        rules: {
                            submits: [{ action: 'child_page', scene: 'owned' }],
                        },
                    },
                ],
            },
            {
                key: 'scene_702',
                slug: 'owned',
                parent: 'target',
                views: [],
            },
        ]);

        const result = await findOrphansLeftByMove(ctx, app, ['scene_702']);
        assert.equal(result.orphanCheck, 'none');
    });

    it('says nothing at all when the view owned no pages', async () => {
        const { ctx, app } = ctxFor([{ key: 'scene_9', slug: 't', views: [] }]);
        assert.deepEqual(await findOrphansLeftByMove(ctx, app, []), {});
    });

    it('reports unknown rather than clean when the app cannot be read back', async () => {
        const app = makeApp({});
        const { ctx } = makeFakeContext({ apps: [app] });
        ctx.getRuntimeMetadata = async () => null;

        const result = await findOrphansLeftByMove(ctx, app, ['scene_594']);
        assert.equal(result.orphanCheck, 'unknown');
        assert.match(String(result.orphanNote), /scene_594/);
    });
});
