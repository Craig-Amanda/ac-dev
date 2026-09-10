import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    classifyLinkTargets,
    collectChildPageSubmitRefs,
    collectNavigationRefs,
    payloadRetainsSceneRef,
    runGuardedViewMutation,
    type PageDeletionConfirmation,
    type SceneNode,
    type ViewMutationDeps,
} from './view-safety.js';
import {
    collectLayoutViewKeys,
    parseRuntimeScenes,
    unrenderedViewKeys,
} from './metadata.js';
import { buildStarterPageGroups, getSceneViewKeys } from './view-templates.js';
import {
    describeAudienceConsequence,
    ensureMovedViewIsRendered,
    summariseAudienceChanges,
} from '../view-mutation.js';
import { makeApp, makeFakeContext } from '../testing/fake-context.js';
import { buildProfileNameIndex, resolvePageAccess } from './page-access.js';
import type { SceneInfo } from '../types.js';

/**
 * Regression cover for the Noah's Place scene deletion, 10 September 2026.
 *
 * Three defects fired in series. A move landed a view outside the target page's
 * layout, so it existed and rendered nowhere; no tool could read a page's layout,
 * so the omission could not be diagnosed; and a live move used as a diagnostic was
 * cleared by the cascade guard because every page it pointed at had a second
 * referrer.
 *
 * What the cascade actually did, confirmed by the app owner: it **re-created** the
 * linked page trees under the target and removed the originals. No page content was
 * lost — but every page came back under a new key and a new slug, so every reference
 * to the old slug dangled, and the rebuilt subtree hung under a different login page
 * than the table linking into it. The guard's own vocabulary ("destroys N pages")
 * mis-describes this: the harm is re-keying, not destruction, and a page the guard
 * calls `transferred` is one that survives *with a new identity*.
 *
 * The live measurements these tests pin are recorded in TESTING.md Tier 8. They
 * were taken against the Knack MCP Test app on 10 September, reading page layouts
 * from `GET /v1/applications/{appId}` — the payload the front end itself renders
 * from, and the only place a page's `groups` array is visible at all.
 */

describe('incident: the cascade guard cleared the move that caused it', () => {
    /**
     * scene_3 ("Clients") holds view_4 (the live production table), view_219 (Test
     * Clients) and view_1644 (the new copy, whose link columns were copied verbatim
     * from view_4). scene_6 ("Client Details") hangs off scene_3, and all three
     * views link to it.
     */
    const NOAHS_PLACE: SceneNode[] = [
        {
            sceneKey: 'scene_3',
            sceneSlug: 'clients',
            views: [
                { viewKey: 'view_4', childSceneRefs: ['client-details'] },
                { viewKey: 'view_219', childSceneRefs: ['client-details'] },
                { viewKey: 'view_1644', childSceneRefs: ['client-details'] },
            ],
        },
        {
            sceneKey: 'scene_6',
            sceneName: 'Client Details',
            sceneSlug: 'client-details',
            parentRef: 'clients',
            views: [],
        },
    ];

    it('spares the page because two other views link to it, so nothing reaches the prompt', () => {
        const [target] = classifyLinkTargets(
            ['client-details'],
            NOAHS_PLACE,
            'scene_3',
            'view_1644',
        );

        // `transferred` and `external` are both filtered out of the at-risk set, so
        // this classification is what made the forward move look harmless.
        assert.equal(target.classification, 'transferred');
        assert.deepEqual(
            target.otherReferrers.map((entry) => entry.viewKey),
            ['view_4', 'view_219'],
        );
    });

    it('classifies the same page as owned once it has a single referrer', () => {
        // The post-cascade shape: a fresh duplicate under the scratch page, linked
        // only by the view being moved. Identical risk, opposite verdict — which is
        // why the reverse move was refused and the forward one was not.
        const AFTER_CASCADE: SceneNode[] = [
            {
                sceneKey: 'scene_488',
                sceneSlug: 'dev-testing',
                views: [
                    {
                        viewKey: 'view_1644',
                        childSceneRefs: ['client-details3'],
                    },
                ],
            },
            {
                sceneKey: 'scene_900',
                sceneName: 'Client Details',
                sceneSlug: 'client-details3',
                parentRef: 'dev-testing',
                views: [],
            },
        ];

        const [target] = classifyLinkTargets(
            ['client-details3'],
            AFTER_CASCADE,
            'scene_488',
            'view_1644',
        );

        assert.equal(target.classification, 'owned');
    });

    it('spares a shared page however many views link to it', () => {
        // The exemption is anti-correlated with real move risk: the more views share
        // a page, the more confident the guard becomes that moving one is safe —
        // while sharing is what makes Knack's ownership cascade fire hardest.
        const many: SceneNode[] = [
            {
                sceneKey: 'scene_3',
                sceneSlug: 'clients',
                views: Array.from({ length: 9 }, (_unused, index) => ({
                    viewKey: `view_${index + 1}`,
                    childSceneRefs: ['client-details'],
                })),
            },
            {
                sceneKey: 'scene_6',
                sceneSlug: 'client-details',
                parentRef: 'clients',
                views: [],
            },
        ];

        const [target] = classifyLinkTargets(
            ['client-details'],
            many,
            'scene_3',
            'view_1',
        );

        assert.equal(target.classification, 'transferred');
        assert.equal(target.otherReferrers.length, 8);
    });
});

describe('incident: the layout the guard could not read, now readable', () => {
    /**
     * A scene as Knack actually returns it. `views` is what exists on the page;
     * `groups` is what renders. Measured 10 September: with `groups` populated the
     * front end renders only the view keys it names — scene_64 held five views and
     * rendered the three in its layout. With `groups` empty every view renders —
     * scene_62 held two and rendered both.
     */
    const RAW_SCENE = {
        scenes: [
            {
                _id: 'abc',
                key: 'scene_64',
                name: 'New Page 4',
                slug: 'new-page-4',
                views: [
                    { key: 'view_54', name: 'Source Table', type: 'table' },
                    { key: 'view_57', name: 'Probe', type: 'rich_text' },
                    { key: 'view_55', name: 'A4 Probe', type: 'table' },
                ],
                groups: [
                    { columns: [{ keys: ['view_54', 'view_57'], width: 100 }] },
                ],
            },
        ],
    };

    it('reads the layout alongside the views, and names what does not render', () => {
        const [scene] = parseRuntimeScenes(RAW_SCENE);

        // Everything that exists...
        assert.deepEqual(
            scene.views.map((view) => view.viewKey),
            ['view_54', 'view_57', 'view_55'],
        );
        // ...and, separately, everything that renders. The guard used to read only
        // the first and had no way to say the two disagreed.
        assert.deepEqual(scene.layoutViewKeys, ['view_54', 'view_57']);
        assert.deepEqual(unrenderedViewKeys(scene), ['view_55']);
    });

    it('treats an empty layout as "everything renders", not as "nothing does"', () => {
        // Measured 10 September: scene_62 carried `groups: []` and both its views
        // rendered. This is the ordinary state of a page built in the builder — 19 of
        // the test app's 34 pages — so reading it as an empty layout would report
        // most of an app as invisible.
        const [scene] = parseRuntimeScenes({
            scenes: [
                {
                    key: 'scene_62',
                    slug: 'menu-2',
                    views: [{ key: 'view_61' }, { key: 'view_65' }],
                    groups: [],
                },
            ],
        });

        assert.deepEqual(scene.layoutViewKeys, []);
        assert.deepEqual(unrenderedViewKeys(scene), []);
    });

    it('returns null, not empty, when the layout was never read', () => {
        // Absent is not empty. A caller that cannot supply a layout must not be read
        // as "this page renders everything" — that is the wrong way round, and it is
        // how a stranded view would go on being invisible to the tooling.
        assert.equal(
            unrenderedViewKeys({
                sceneKey: 'scene_1',
                sceneName: undefined,
                sceneSlug: undefined,
                parentRef: undefined,
                views: [
                    {
                        viewKey: 'view_1',
                        viewName: undefined,
                        viewType: undefined,
                    },
                ],
            }),
            null,
        );
    });

    it('flattens a multi-column row rather than indexing a fixed shape', () => {
        assert.deepEqual(
            collectLayoutViewKeys([
                { columns: [{ keys: ['view_1', 'view_2'], width: 50 }] },
                { columns: [{ keys: ['view_3'], width: 100 }] },
            ]),
            ['view_1', 'view_2', 'view_3'],
        );
    });

    it('derives layout keys from what exists, not from what renders', () => {
        const scenes = parseRuntimeScenes(RAW_SCENE);

        // getSceneViewKeys feeds buildStarterPageGroups on the copy path. It returns
        // the existence array, so the rebuilt layout is keyed off `views` — the real
        // `groups` never enters the calculation.
        assert.deepEqual(getSceneViewKeys(scenes, 'scene_64'), [
            'view_54',
            'view_57',
            'view_55',
        ]);
    });
});

describe('incident: the fabricated layout cannot express a real one', () => {
    it('emits one full-width row per view and a placeholder for the new one', () => {
        assert.deepEqual(buildStarterPageGroups(['view_1', 'view_2']), [
            { columns: [{ keys: ['view_1'], width: 100 }] },
            { columns: [{ keys: ['view_2'], width: 100 }] },
            { columns: [{ keys: ['new'], width: 100 }] },
        ]);
    });

    it('cannot produce a multi-column row for any input', () => {
        // `pageGroups` replaces a page's layout rather than adding to it, and this is
        // the only shape the copy path can send. So a page whose real layout puts two
        // views side by side is flattened by any copy onto it — structurally, not by
        // accident. There is no argument that would preserve the row.
        for (const keys of [
            [],
            ['view_1'],
            ['view_1', 'view_2'],
            ['view_1', 'view_2', 'view_3', 'view_4'],
        ]) {
            for (const row of buildStarterPageGroups(keys)) {
                assert.equal(row.columns.length, 1);
                assert.equal(row.columns[0].width, 100);
                assert.equal(row.columns[0].keys.length, 1);
            }
        }
    });

    it('strands every view the derived key list omits', () => {
        // The gap that produced the live orphan. A layout built from a key list that
        // is missing a view renders every other view and not that one — and on the
        // default copy path the list is derived silently, with no warning branch.
        const rendered = buildStarterPageGroups(['view_54', 'view_57'])
            .flatMap((row) => row.columns)
            .flatMap((column) => column.keys);

        assert.equal(rendered.includes('view_55'), false);
    });
});

describe('incident: the move that caused it, through the guard', () => {
    /** view_1644 as it stood: view_4's link column, copied verbatim. */
    const VIEW_1644 = {
        key: 'view_1644',
        type: 'table',
        columns: [
            {
                type: 'link',
                header: 'Client',
                scene: 'client-details',
            },
        ],
    };

    const SCENES: SceneNode[] = [
        {
            sceneKey: 'scene_3',
            sceneName: 'Clients',
            sceneSlug: 'clients',
            views: [
                { viewKey: 'view_4', childSceneRefs: ['client-details'] },
                { viewKey: 'view_219', childSceneRefs: ['client-details'] },
                { viewKey: 'view_1644', childSceneRefs: ['client-details'] },
            ],
        },
        {
            sceneKey: 'scene_6',
            sceneName: 'Client Details',
            sceneSlug: 'client-details',
            parentRef: 'clients',
            views: [],
        },
        { sceneKey: 'scene_488', sceneName: 'Dev Testing', views: [] },
    ];

    const makeDeps = (
        confirm?: PageDeletionConfirmation,
    ): {
        deps: ViewMutationDeps;
        writes: string[];
        prompts: Array<{ doomed: string[]; transferred: (string | null)[] }>;
    } => {
        const writes: string[] = [];
        const prompts: Array<{
            doomed: string[];
            transferred: (string | null)[];
        }> = [];

        return {
            writes,
            prompts,
            deps: {
                fetchView: async () => ({
                    ok: true,
                    status: 200,
                    body: { view: VIEW_1644 },
                }),
                listScenes: async () => ({ ok: true, scenes: SCENES }),
                writeSnapshot: async () => ({
                    ok: true,
                    path: '/snapshots/move.json',
                }),
                builderUrlForScene: (sceneKey) =>
                    `https://builder.knack.com/acme/app/pages/${sceneKey}`,
                confirmPageDeletion: async ({
                    childPages,
                    transferredPages,
                }) => {
                    prompts.push({
                        doomed: childPages.map((page) => page.sceneKey),
                        transferred: transferredPages.map(
                            (page) => page.sceneKey,
                        ),
                    });
                    return confirm ?? { supported: false };
                },
            } as ViewMutationDeps,
        };
    };

    const MOVE = {
        action: 'move_view' as const,
        sceneKey: 'scene_3',
        viewKey: 'view_1644',
        targetSceneKey: 'scene_488',
    };

    it('refuses the move on a client that cannot prompt, instead of executing it', async () => {
        const { deps, writes } = makeDeps();

        const result = await runGuardedViewMutation(deps, MOVE, async () => {
            writes.push('WRITE');
            return { sent: true };
        });

        // The live server ran this call with humanConfirmation "not-required" and
        // deleted 18 production pages. The same call must now stop before the
        // transport, on the same client that could not be prompted.
        assert.equal(result.ok, false);
        if (result.ok) return;
        assert.equal(result.code, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        assert.deepEqual(writes, []);
    });

    it('names the shared page as doomed rather than spared', async () => {
        const { deps, prompts, writes } = makeDeps({
            supported: true,
            accepted: true,
            outcome: 'accept',
        });

        const result = await runGuardedViewMutation(deps, MOVE, async () => {
            writes.push('WRITE');
            return { sent: true };
        });

        assert.equal(result.ok, true);
        assert.deepEqual(writes, ['WRITE']);

        // The prompt has to put scene_6 in the doomed list. Previously it appeared in
        // neither list: spared as `transferred`, and then filtered out of the report
        // because the payload was a move. Nobody was asked anything.
        assert.equal(prompts.length, 1);
        assert.deepEqual(prompts[0].doomed, ['scene_6']);
        assert.deepEqual(prompts[0].transferred, []);
        assert.equal(
            result.ok === true && result.humanConfirmation,
            'accepted',
        );
    });

    it('still allows a move of a view carrying no page links', async () => {
        // The fix must not turn every move into a refusal. Measured live on
        // 10 September: view_56, a rich_text view with no links, moved between two
        // pages and back with humanConfirmation "not-required" both ways.
        const { deps, writes } = makeDeps();
        const plain = {
            ...deps,
            fetchView: async () => ({
                ok: true as const,
                status: 200,
                body: { view: { key: 'view_56', type: 'rich_text' } },
            }),
        } as ViewMutationDeps;

        const result = await runGuardedViewMutation(
            plain,
            { ...MOVE, viewKey: 'view_56' },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );

        assert.equal(result.ok, true);
        assert.deepEqual(writes, ['WRITE']);
        assert.equal(
            result.ok === true && result.humanConfirmation,
            'not-required',
        );
    });
});

describe('incident: the login warning that was skipped', () => {
    /**
     * The audience half of the same bypass, and the part the owner actually had to
     * repair by hand.
     *
     * `describeAudienceConsequence` exists to say when a page changing parent becomes
     * reachable by a different set of users. It only runs while a confirmation prompt
     * is being built, and `destroysNothing` is computed from the doomed pages and the
     * unresolved links alone — `transferredPages` is not in that condition. So when
     * every linked page classified `transferred`, the prompt was auto-accepted and
     * this warning never ran.
     *
     * Live consequence, confirmed by the app owner: the client record pages left the
     * `clients-login` chain, which admits five staff roles, and landed on the Dev
     * Testing branch whose nearest login was `scene_183` ("Developer Login") —
     * Developer only. Access narrowed rather than opened: no data was exposed, but
     * Keyworker, Director, Volunteer and Demo lost the client records. The fix was to
     * add a login admitting the right roles to the Dev Testing branch, which is why
     * that page resolves to a five-role login today and did not at the time.
     *
     * The warning is worth firing for a narrowing as much as a widening —
     * `compareAudience` reports any difference in the admitted set, in either
     * direction, and losing access to client records mid-shift is its own incident.
     */
    const scene = (
        sceneKey: string,
        sceneSlug: string,
        parentRef?: string,
        views: SceneInfo['views'] = [],
    ): SceneInfo => ({
        sceneKey,
        sceneName: sceneSlug,
        sceneSlug,
        parentRef,
        views,
    });

    const SCENES: SceneInfo[] = [
        scene('scene_4', 'clients-login', undefined, [
            {
                viewKey: 'view_3',
                viewName: 'Login',
                viewType: 'login',
                allowedProfiles: ['profile_26'],
                limitProfileAccess: true,
            },
        ]),
        scene('scene_3', 'clients', 'clients-login'),
        scene('scene_6', 'client-details', 'clients'),
        // The Dev Testing branch: its nearest login is Developer-only.
        scene('scene_183', 'developer-login', undefined, [
            {
                viewKey: 'view_460',
                viewName: 'Developer Login',
                viewType: 'login',
                allowedProfiles: ['profile_8'],
                limitProfileAccess: true,
            },
        ]),
        scene('scene_488', 'dev-testing', 'developer-login'),
    ];

    it('reads the two sides the move puts either end of, both protected', () => {
        const before = resolvePageAccess('scene_6', SCENES);
        const after = resolvePageAccess('scene_488', SCENES);

        // Neither side is public. The move narrowed the audience from the five staff
        // roles to Developer alone — which is why nothing leaked and why staff still
        // could not reach the client records afterwards.
        assert.equal(before.status, 'protected');
        assert.equal(after.status, 'protected');
        assert.deepEqual(before.roles, ['profile_26']);
        assert.deepEqual(after.roles, ['profile_8']);
    });

    it('warns that the moved pages leave the login that protected them', () => {
        const paragraph = describeAudienceConsequence(
            {
                action: 'move_view',
                sceneKey: 'scene_3',
                viewKey: 'view_1644',
                childPages: [
                    {
                        sceneKey: 'scene_6',
                        sceneName: 'Client Details',
                        sceneSlug: 'client-details',
                        depth: 0,
                    },
                ],
                externalPages: [],
                transferredPages: [],
                unresolvedLinkCount: 0,
            },
            {
                scenes: SCENES,
                profileNames: buildProfileNameIndex([]),
                targetSceneKey: 'scene_488',
            },
        );

        // Fires on a narrowing exactly as it would on a widening.
        assert.match(paragraph, /AUDIENCE CHANGES/);
        assert.match(paragraph, /scene_6/);
    });

    it('says nothing at all when the page is spared, which is how it was missed', () => {
        // The pre-fix shape: `transferred` emptied childPages, `destroysNothing` went
        // true, and no prompt was built — so this paragraph was never even requested.
        // Called directly with the spared shape it still produces only the generic
        // ask, never the AUDIENCE CHANGES headline naming the page.
        const paragraph = describeAudienceConsequence(
            {
                action: 'update_view',
                sceneKey: 'scene_3',
                viewKey: 'view_1644',
                childPages: [],
                externalPages: [],
                transferredPages: [],
                unresolvedLinkCount: 0,
            },
            {
                scenes: SCENES,
                profileNames: buildProfileNameIndex([]),
                targetSceneKey: 'scene_488',
            },
        );

        assert.equal(paragraph, '');
    });
});

describe('incident: the audience change is now reported, not only prompted', () => {
    const scene = (
        sceneKey: string,
        sceneSlug: string,
        parentRef?: string,
        views: SceneInfo['views'] = [],
    ): SceneInfo => ({
        sceneKey,
        sceneName: sceneSlug,
        sceneSlug,
        parentRef,
        views,
    });

    const SCENES: SceneInfo[] = [
        scene('scene_4', 'clients-login', undefined, [
            {
                viewKey: 'view_3',
                viewName: 'Login',
                viewType: 'login',
                allowedProfiles: ['profile_26', 'profile_25'],
                limitProfileAccess: true,
            },
        ]),
        scene('scene_3', 'clients', 'clients-login'),
        scene('scene_6', 'client-details', 'clients'),
        scene('scene_183', 'developer-login', undefined, [
            {
                viewKey: 'view_460',
                viewName: 'Developer Login',
                viewType: 'login',
                allowedProfiles: ['profile_8'],
                limitProfileAccess: true,
            },
        ]),
        scene('scene_488', 'dev-testing', 'developer-login'),
    ];

    const input = {
        action: 'move_view',
        sceneKey: 'scene_3',
        childPageKeys: ['scene_6'],
        transferredPages: [],
    };

    const audience = {
        scenes: SCENES,
        profileNames: buildProfileNameIndex(null),
        targetSceneKey: 'scene_488',
    };

    it('reports the narrowing as a row, with both sides named', () => {
        const [row, ...rest] = summariseAudienceChanges(input, audience);

        assert.deepEqual(rest, []);
        assert.equal(row.sceneKey, 'scene_6');
        assert.equal(row.change, 'changed');
        assert.equal(row.destinationSceneKey, 'scene_488');
        // Two staff roles before, Developer alone after.
        assert.match(row.before, /profile_26|profile_25/);
        assert.match(row.after, /profile_8/);
    });

    it('agrees with the prompt about whether anything changed', () => {
        // The two are deliberately separate functions, so this is the guard against
        // them drifting: the response must never say "unchanged" while the prompt
        // says AUDIENCE CHANGES, or the reverse.
        const rows = summariseAudienceChanges(input, audience);
        const paragraph = describeAudienceConsequence(
            {
                action: 'move_view',
                sceneKey: 'scene_3',
                viewKey: 'view_1644',
                childPages: [
                    {
                        sceneKey: 'scene_6',
                        sceneName: 'Client Details',
                        sceneSlug: 'client-details',
                        depth: 0,
                    },
                ],
                externalPages: [],
                transferredPages: [],
                unresolvedLinkCount: 0,
            },
            audience,
        );

        assert.equal(
            rows.some((row) => row.change !== 'same'),
            /AUDIENCE CHANGES/.test(paragraph),
        );
    });

    it('reports nothing when no page changes parent', () => {
        assert.deepEqual(
            summariseAudienceChanges(
                { ...input, action: 'update_view', childPageKeys: [] },
                audience,
            ),
            [],
        );
    });

    it('reports nothing rather than guessing when the tree cannot be read', () => {
        assert.deepEqual(
            summariseAudienceChanges(input, {
                scenes: null,
                profileNames: buildProfileNameIndex(null),
                targetSceneKey: 'scene_488',
            }),
            [],
        );
    });
});

describe('incident: a moved view is now put into its new page layout', () => {
    /**
     * Defect 1, and the first link in the chain. Measured 10 September on the live
     * app, both halves: a move left `groups` byte-identical so the view rendered
     * nowhere, and appending one full-width row to the stored `groups` made it render
     * while leaving the rest of the layout untouched (TESTING.md Tier 8).
     */
    const app = makeApp({ appKey: 'Demo' });

    const metadata = (groups: unknown, viewKeys: string[]) => ({
        scenes: [
            {
                key: 'scene_64',
                slug: 'new-page-4',
                views: viewKeys.map((key) => ({ key })),
                groups,
            },
        ],
    });

    it('appends a row for the moved view, keeping the layout it found', async () => {
        const stored = [
            { columns: [{ keys: ['view_54', 'view_57'], width: 50 }] },
        ];
        const { ctx, requests } = makeFakeContext({
            apps: [app],
            runtimeMetadata: {
                Demo: metadata(stored, ['view_54', 'view_57', 'view_56']),
            },
            responses: () => ({ ok: true, status: 200, body: {} }),
        });

        const result = await ensureMovedViewIsRendered(
            ctx,
            app,
            'scene_64',
            'view_56',
        );

        assert.equal(result.layoutRepair, 'added');
        assert.equal(requests.length, 1);
        assert.match(requests[0].apiPath, /scene_64\/views\/sort$/);

        const raw = requests[0].body;
        const sent = (
            typeof raw === 'string' ? JSON.parse(raw) : raw
        ) as Record<string, unknown>;
        // The stored two-column row survives verbatim — rebuilding from the flattened
        // key list would have restacked it into two full-width rows.
        assert.deepEqual(sent.pageGroups, [
            ...stored,
            { columns: [{ keys: ['view_56'], width: 100 }] },
        ]);
        assert.deepEqual(sent.order, ['view_54', 'view_57', 'view_56']);
    });

    it('writes exactly what the Knack builder writes', async () => {
        // Measured 10 September, the app owner moving view_56 onto scene_64 in the
        // builder while the layout named only view_54, view_57 and view_58. The
        // builder appended one row and nothing else (TESTING.md Tier 8):
        const BUILDER_APPENDED = {
            columns: [{ keys: ['view_56'], width: 100 }],
        };
        const stored = [
            {
                columns: [
                    { keys: ['view_54', 'view_57', 'view_58'], width: 100 },
                ],
            },
        ];
        const { ctx, requests } = makeFakeContext({
            apps: [app],
            runtimeMetadata: {
                Demo: metadata(stored, [
                    'view_54',
                    'view_57',
                    'view_58',
                    'view_55',
                    'view_56',
                ]),
            },
            responses: () => ({ ok: true, status: 200, body: {} }),
        });

        await ensureMovedViewIsRendered(ctx, app, 'scene_64', 'view_56');

        const raw = requests[0].body;
        const sent = (
            typeof raw === 'string' ? JSON.parse(raw) : raw
        ) as Record<string, unknown>;

        // Byte-for-byte the builder's own answer, arrived at independently.
        assert.deepEqual(sent.pageGroups, [...stored, BUILDER_APPENDED]);
        // And, like the builder, it appends rather than rebuilding: view_55 was
        // stranded on that page before the builder move and stayed stranded after.
        assert.equal(
            JSON.stringify(sent.pageGroups).includes('view_55'),
            false,
        );
    });

    it('writes nothing when the page has no explicit layout', async () => {
        // `groups: []` renders every view, so the moved view is already visible.
        // Writing a layout here would arm the hazard for the next move onto this page.
        const { ctx, requests } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { Demo: metadata([], ['view_54', 'view_56']) },
        });

        const result = await ensureMovedViewIsRendered(
            ctx,
            app,
            'scene_64',
            'view_56',
        );

        assert.equal(result.layoutRepair, 'not-needed');
        assert.deepEqual(requests, []);
    });

    it('writes nothing when the layout already names the view', async () => {
        const { ctx, requests } = makeFakeContext({
            apps: [app],
            runtimeMetadata: {
                Demo: metadata(
                    [{ columns: [{ keys: ['view_56'], width: 100 }] }],
                    ['view_56'],
                ),
            },
        });

        assert.equal(
            (await ensureMovedViewIsRendered(ctx, app, 'scene_64', 'view_56'))
                .layoutRepair,
            'not-needed',
        );
        assert.deepEqual(requests, []);
    });

    it('reports rather than guesses when the page cannot be read back', async () => {
        const { ctx, requests } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { Demo: null },
        });

        const result = await ensureMovedViewIsRendered(
            ctx,
            app,
            'scene_64',
            'view_56',
        );

        assert.equal(result.layoutRepair, 'unknown');
        assert.deepEqual(requests, []);
    });

    it('says the view is invisible when the layout write fails', async () => {
        // The move already happened, so this is reported, never turned into a refusal
        // — but it has to say plainly that the view is on the page and renders nowhere.
        const { ctx } = makeFakeContext({
            apps: [app],
            runtimeMetadata: {
                Demo: metadata(
                    [{ columns: [{ keys: ['view_54'], width: 100 }] }],
                    ['view_54', 'view_56'],
                ),
            },
            responses: () => ({ ok: false, status: 500, body: {} }),
        });

        const result = await ensureMovedViewIsRendered(
            ctx,
            app,
            'scene_64',
            'view_56',
        );

        assert.equal(result.layoutRepair, 'failed');
        assert.match(String(result.layoutNote), /renders nowhere/);
    });
});

describe('incident: preview, so a live mutation is never the way to look', () => {
    /**
     * Defect 3. The move that caused the incident was run to find out what a page
     * looked like, because nothing could answer that question without writing. A
     * preview runs every check and stops one step short of the transport.
     */
    const VIEW_1644 = {
        key: 'view_1644',
        type: 'table',
        columns: [{ type: 'link', header: 'Client', scene: 'client-details' }],
    };

    const SCENES: SceneNode[] = [
        {
            sceneKey: 'scene_3',
            sceneSlug: 'clients',
            views: [
                { viewKey: 'view_4', childSceneRefs: ['client-details'] },
                { viewKey: 'view_1644', childSceneRefs: ['client-details'] },
            ],
        },
        {
            sceneKey: 'scene_6',
            sceneName: 'Client Details',
            sceneSlug: 'client-details',
            parentRef: 'clients',
            views: [],
        },
        { sceneKey: 'scene_488', views: [] },
    ];

    const makeDeps = () => {
        const log = { writes: [] as string[], prompts: 0, snapshots: 0 };
        const deps = {
            fetchView: async () => ({
                ok: true,
                status: 200,
                body: { view: VIEW_1644 },
            }),
            listScenes: async () => ({ ok: true, scenes: SCENES }),
            writeSnapshot: async () => {
                log.snapshots += 1;
                return { ok: true, path: '/snapshots/x.json' };
            },
            builderUrlForScene: (key: string) => `https://builder/${key}`,
            confirmPageDeletion: async () => {
                log.prompts += 1;
                return { supported: true, accepted: true, outcome: 'accept' };
            },
        } as unknown as ViewMutationDeps;
        return { deps, log };
    };

    it('reports the impact, writes nothing, prompts nobody, snapshots nothing', async () => {
        const { deps, log } = makeDeps();

        const result = await runGuardedViewMutation(
            deps,
            {
                action: 'move_view',
                sceneKey: 'scene_3',
                viewKey: 'view_1644',
                previewOnly: true,
            },
            async () => {
                log.writes.push('WRITE');
                return { sent: true };
            },
        );

        assert.equal(result.ok, false);
        if (result.ok) return;
        assert.equal(result.code, 'PREVIEW_ONLY');
        assert.equal(result.details?.preview, true);
        // The page the real move would have taken, named without taking it.
        assert.deepEqual(result.details?.acknowledgedPages, ['scene_6']);
        assert.match(result.message, /nothing was sent to Knack/);

        assert.deepEqual(log.writes, []);
        assert.equal(log.prompts, 0);
        assert.equal(log.snapshots, 0);
    });

    it('answers without a human even on a client that could not be asked', async () => {
        // The condition the incident ran under. A preview has nothing to approve, so
        // it must not come back HUMAN_CONFIRMATION_UNAVAILABLE — that refusal is what
        // would send someone back to probing with a live call.
        const { deps, log } = makeDeps();
        const noPrompt = {
            ...deps,
            confirmPageDeletion: undefined,
        } as unknown as ViewMutationDeps;

        const result = await runGuardedViewMutation(
            noPrompt,
            {
                action: 'move_view',
                sceneKey: 'scene_3',
                viewKey: 'view_1644',
                previewOnly: true,
            },
            async () => {
                log.writes.push('WRITE');
                return { sent: true };
            },
        );

        assert.equal(result.ok === false && result.code, 'PREVIEW_ONLY');
        assert.deepEqual(log.writes, []);
    });

    it('still refuses a malformed payload rather than previewing it', async () => {
        // A refusal reached before the preview point is a real answer, not a bug:
        // the preview has told you the payload is wrong.
        const { deps } = makeDeps();

        const result = await runGuardedViewMutation(
            deps,
            {
                action: 'update_view',
                sceneKey: 'scene_3',
                viewKey: 'view_1644',
                updates: '{}',
                previewOnly: true,
            },
            async () => ({ sent: true }),
        );

        assert.equal(result.ok, false);
        if (result.ok) return;
        assert.equal(result.code, 'EMPTY_UPDATE_PAYLOAD');
    });
});

describe('incident: the second move, and why `external` could not be spared either', () => {
    /**
     * The admissions case, reconstructed from the app owner's own snapshots.
     *
     * 08:06:14 — `admiss-client-details` (scene_9, parent `admissions`) exists, and
     *            view_8 on scene_7 links to it. App in use, nobody reporting anything.
     * 08:06:08 — view_8 was copied to view_1543 **on scene_488**, a scratch page.
     * ~08:07–08:53 — view_1543 moved scene_488 -> scene_7. In that window Knack
     *            deleted 9 pages, including `admiss-client-details` and its subtree,
     *            and created 14 replacements under new slugs.
     * Result — view_8, never touched, left pointing at a page that no longer exists.
     *
     * The copy was harmless: at 08:06:14, six seconds after it, nothing had been
     * duplicated and nothing deleted. **The move did the damage**, exactly as with
     * view_1644 — but through the other exemption. `admiss-client-details` hung off
     * `admissions` (scene_7), not off scene_488, so relative to the view being moved
     * it classified `external`, whose stated reason is that "removing the link
     * removes navigation and leaves the page in place". The page was destroyed.
     *
     * This is why a move spares nothing. Sparing `external` was the call I was least
     * sure of; this incident settles it against.
     */
    const SCENES: SceneNode[] = [
        {
            sceneKey: 'scene_488',
            sceneSlug: 'dev-testing',
            views: [
                {
                    viewKey: 'view_1543',
                    childSceneRefs: ['admiss-client-details'],
                },
            ],
        },
        {
            sceneKey: 'scene_7',
            sceneSlug: 'admissions',
            views: [
                {
                    viewKey: 'view_8',
                    childSceneRefs: ['admiss-client-details'],
                },
            ],
        },
        {
            sceneKey: 'scene_9',
            sceneName: 'Client Details',
            sceneSlug: 'admiss-client-details',
            parentRef: 'admissions',
            views: [],
        },
        {
            sceneKey: 'scene_34',
            sceneName: 'Client Summary',
            sceneSlug: 'client-summary3',
            parentRef: 'admiss-client-details',
            views: [],
        },
    ];

    it('classifies the destroyed page as external, and says it survives', () => {
        const [target] = classifyLinkTargets(
            ['admiss-client-details'],
            SCENES,
            'scene_488',
            'view_1543',
        );

        assert.equal(target.classification, 'external');
        // The reason the guard gave, against what the snapshots show happened.
        assert.match(target.reason, /leaves the page in place/);
    });

    it('refuses the move now, because a move spares nothing', async () => {
        const writes: string[] = [];
        const deps = {
            fetchView: async () => ({
                ok: true,
                status: 200,
                body: {
                    view: {
                        key: 'view_1543',
                        type: 'table',
                        columns: [
                            {
                                type: 'link',
                                header: 'Client',
                                scene: 'admiss-client-details',
                            },
                        ],
                    },
                },
            }),
            listScenes: async () => ({ ok: true, scenes: SCENES }),
            writeSnapshot: async () => ({ ok: true, path: '/s.json' }),
            builderUrlForScene: (key: string) => `https://builder/${key}`,
            confirmPageDeletion: async () => ({ supported: false }),
        } as unknown as ViewMutationDeps;

        const result = await runGuardedViewMutation(
            deps,
            {
                action: 'move_view',
                sceneKey: 'scene_488',
                viewKey: 'view_1543',
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );

        assert.equal(result.ok, false);
        if (result.ok) return;
        assert.equal(result.code, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        // The page and the descendant that went with it.
        assert.deepEqual(
            (result.details?.childPages as Array<{ sceneKey: string }>).map(
                (page) => page.sceneKey,
            ),
            ['scene_9', 'scene_34'],
        );
        assert.deepEqual(writes, []);
    });
});

describe('incident: the child_page submit rule that deleted two pages', () => {
    /**
     * The sixth defect, and the only one found by breaking something rather than by
     * reading. Removing a `child_page` submit rule from view_1789 deleted scene_586
     * and scene_587, taking seven freshly rebuilt views with them. The guard computed
     * nothing at risk and asked nothing; Knack reported the deletion afterwards.
     *
     * The cause was `collectNavigationRefs` reading only `links[]` and `columns[]`,
     * on the stated reasoning that "a rule redirect may carry a `scene`, but it cannot
     * delete or preserve a child page". For `action: "child_page"` that is false.
     */
    const withSubmit = (action: string, scene: unknown) => ({
        key: 'view_1789',
        type: 'form',
        rules: {
            submits: [
                {
                    key: 'submit_1',
                    action,
                    scene,
                    message: 'ok',
                    is_default: true,
                },
            ],
        },
    });

    it('counts a child_page rule as a page reference', () => {
        assert.deepEqual(
            collectChildPageSubmitRefs(
                withSubmit('child_page', 'part-4-mental-health'),
            ),
            ['part-4-mental-health'],
        );
        assert.deepEqual(
            collectNavigationRefs(
                withSubmit('child_page', 'part-4-mental-health'),
            ),
            ['part-4-mental-health'],
        );
    });

    it('ignores a vestigial scene on a message rule', () => {
        // Part 3a carried exactly this: action "message" with a stale
        // scene "assessment-part-4" naming a page deleted hours earlier. It does
        // nothing, and counting it would spare pages that should be put to a human.
        assert.deepEqual(
            collectChildPageSubmitRefs(
                withSubmit('message', 'assessment-part-4'),
            ),
            [],
        );
        assert.deepEqual(
            collectNavigationRefs(withSubmit('message', 'assessment-part-4')),
            [],
        );
    });

    it('ignores a page specification, which creates rather than endangers', () => {
        // The object form is a create request - measured on Part 6a, which built
        // Part 7's page this way. There is no existing page to put at risk.
        assert.deepEqual(
            collectChildPageSubmitRefs(
                withSubmit('child_page', {
                    allowed_profiles: [],
                    name: 'Part 7 - Final Few Questions',
                    parent: 'part-6-legal',
                    object: 'object_69',
                    views: [],
                }),
            ),
            [],
        );
    });

    it('refuses the update that destroyed scene_586, instead of executing it', async () => {
        const writes: string[] = [];
        const SCENES: SceneNode[] = [
            {
                sceneKey: 'scene_585',
                sceneSlug: 'part-3-physical-health',
                views: [
                    {
                        viewKey: 'view_1789',
                        childSceneRefs: ['part-4-mental-health'],
                    },
                ],
            },
            {
                sceneKey: 'scene_586',
                sceneName: 'Part 4 - Mental Health',
                sceneSlug: 'part-4-mental-health',
                parentRef: 'part-3-physical-health',
                views: [],
            },
            {
                sceneKey: 'scene_587',
                sceneName: 'Part 5 - Substances',
                sceneSlug: 'part-5-substances',
                parentRef: 'part-4-mental-health',
                views: [],
            },
        ];

        const deps = {
            fetchView: async () => ({
                ok: true,
                status: 200,
                body: {
                    view: withSubmit('child_page', 'part-4-mental-health'),
                },
            }),
            listScenes: async () => ({ ok: true, scenes: SCENES }),
            writeSnapshot: async () => ({ ok: true, path: '/s.json' }),
            builderUrlForScene: (key: string) => `https://builder/${key}`,
            confirmPageDeletion: async () => ({ supported: false }),
        } as unknown as ViewMutationDeps;

        // The exact shape of the fatal call: replace `rules` with a message-only
        // submit, dropping the child_page rule.
        const result = await runGuardedViewMutation(
            deps,
            {
                action: 'update_view',
                sceneKey: 'scene_585',
                viewKey: 'view_1789',
                updates: JSON.stringify({
                    rules: {
                        submits: [
                            {
                                key: 'submit_1',
                                action: 'message',
                                message: 'ok',
                                is_default: true,
                            },
                        ],
                    },
                }),
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );

        assert.equal(result.ok, false);
        if (result.ok) return;
        assert.equal(result.code, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        // Both pages named, the child and the descendant that went with it.
        assert.deepEqual(
            (result.details?.childPages as Array<{ sceneKey: string }>).map(
                (page) => page.sceneKey,
            ),
            ['scene_586', 'scene_587'],
        );
        assert.deepEqual(writes, []);
    });

    it('leaves an ordinary edit to the same form alone', () => {
        // The fix must not make every form edit destructive-looking. A title change
        // re-sends the rule, so the page keeps its link and nothing is at risk.
        const refs = collectNavigationRefs(
            withSubmit('child_page', 'part-4-mental-health'),
        );
        assert.deepEqual(refs, ['part-4-mental-health']);
        assert.equal(
            payloadRetainsSceneRef(
                {
                    rules: {
                        submits: [
                            {
                                key: 'submit_1',
                                action: 'child_page',
                                scene: 'part-4-mental-health',
                                message: 'ok',
                                is_default: true,
                            },
                        ],
                    },
                },
                'part-4-mental-health',
            ),
            true,
        );
    });
});

describe('live replay on the test app: the chain fixture, 10 September', () => {
    /**
     * The sequence was rebuilt on a disposable app and run against the *old* build to
     * see whether it still broke anything. It did, and it also turned up a condition
     * nobody had stated: **the cascade fires only when the child page has no other
     * referrer.**
     *
     * Two identical three-level chains were built on scene_69, each root form owning a
     * level-2 page through a `child_page` rule, each level-2 form owning a level-3 page
     * the same way. Chain A's level-2 page also had an independent link column pointing
     * at it from view_78; its level-3 page had nothing pointing at it but its parent's
     * rule.
     *
     * Stripping the rule from view_71 (owner of the level-2 page, which had that second
     * referrer) deleted **nothing** - the page and its whole subtree came back
     * byte-identical from the metadata endpoint. Stripping the rule from view_75 (owner
     * of the level-3 page, which had no other referrer) deleted the page and both views
     * on it, with `humanConfirmation: "not-required"` and no prompt.
     *
     * So `transferred` is not a guess: a second referrer really does keep the page,
     * because Knack re-parents it onto that referrer. Which is why the fix kills the
     * exemption for `move_view` only, and leaves it standing for `update_view`. Both
     * halves of that decision are pinned below against the measured outcomes.
     */
    const CHAIN: SceneNode[] = [
        {
            sceneKey: 'scene_69',
            sceneSlug: 'test-create-table-with-child-pages',
            views: [
                // Root form, owns the level-2 page.
                { viewKey: 'view_71', childSceneRefs: ['chain-a-level-2'] },
                // The independent link column into the level-2 page.
                { viewKey: 'view_78', childSceneRefs: ['chain-a-level-2'] },
            ],
        },
        {
            sceneKey: 'scene_85',
            sceneName: 'Chain A Level 2',
            sceneSlug: 'chain-a-level-2',
            parentRef: 'test-create-table-with-child-pages',
            views: [
                // Level-2 form, owns the level-3 page. Nothing else points there.
                { viewKey: 'view_75', childSceneRefs: ['chain-a-level-3'] },
            ],
        },
        {
            sceneKey: 'scene_87',
            sceneName: 'Chain A Level 3',
            sceneSlug: 'chain-a-level-3',
            parentRef: 'chain-a-level-2',
            views: [],
        },
    ];

    /** The rule-stripping patch, the same shape in both runs. */
    const STRIP_RULE = JSON.stringify({
        rules: {
            emails: [],
            fields: [],
            records: [],
            submits: [
                {
                    key: 'submit_1',
                    action: 'message',
                    message: '<p>Form successfully submitted.\n</p>',
                    is_default: true,
                    reload_show: true,
                },
            ],
        },
    });

    const withOwnedChild = (viewKey: string, childSlug: string) => ({
        key: viewKey,
        type: 'form',
        rules: {
            submits: [
                {
                    key: 'submit_1',
                    action: 'child_page',
                    scene: childSlug,
                    message: 'ok',
                    is_default: true,
                },
            ],
        },
    });

    const depsFor = (viewKey: string, childSlug: string) =>
        ({
            fetchView: async () => ({
                ok: true,
                status: 200,
                body: { view: withOwnedChild(viewKey, childSlug) },
            }),
            listScenes: async () => ({ ok: true, scenes: CHAIN }),
            writeSnapshot: async () => ({ ok: true, path: '/s.json' }),
            builderUrlForScene: (key: string) => `https://builder/${key}`,
            confirmPageDeletion: async (): Promise<PageDeletionConfirmation> => ({
                supported: false,
            }),
        }) as unknown as ViewMutationDeps;

    it('lets the level-2 strip through, because a second referrer keeps that page', async () => {
        // Measured: deleted nothing, and scene_85 came back byte-identical.
        const writes: string[] = [];
        const result = await runGuardedViewMutation(
            depsFor('view_71', 'chain-a-level-2'),
            {
                action: 'update_view',
                sceneKey: 'scene_69',
                viewKey: 'view_71',
                updates: STRIP_RULE,
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );

        assert.equal(result.ok, true);
        assert.deepEqual(writes, ['WRITE']);
    });

    it('refuses the level-3 strip, the one that actually destroyed a page', async () => {
        // Measured on the old build: pagesKnackReportsDeleted ["scene_87"], two views
        // gone, humanConfirmation "not-required". The fix must refuse instead.
        const writes: string[] = [];
        const result = await runGuardedViewMutation(
            depsFor('view_75', 'chain-a-level-3'),
            {
                action: 'update_view',
                sceneKey: 'scene_85',
                viewKey: 'view_75',
                updates: STRIP_RULE,
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );

        assert.equal(result.ok, false);
        if (result.ok) return;
        assert.equal(result.code, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        assert.deepEqual(
            (result.details?.childPages as Array<{ sceneKey: string }>).map(
                (page) => page.sceneKey,
            ),
            ['scene_87'],
        );
        assert.deepEqual(writes, []);
    });

    it('names the page in the level-3 refusal by both key and slug', () => {
        // Whoever reads the refusal has to be able to find the page in the builder.
        const [target] = classifyLinkTargets(
            ['chain-a-level-3'],
            CHAIN,
            'scene_85',
            'view_75',
        );
        assert.equal(target.classification, 'owned');
        assert.equal(target.sceneKey, 'scene_87');
        assert.equal(target.sceneSlug, 'chain-a-level-3');
        // Nothing else reaches it, which is what made it deletable.
        assert.deepEqual(target.otherReferrers, []);
    });

    it('classifies the level-2 page as transferred, and names what keeps it', () => {
        const [target] = classifyLinkTargets(
            ['chain-a-level-2'],
            CHAIN,
            'scene_69',
            'view_71',
        );
        assert.equal(target.classification, 'transferred');
        // view_78's link column is the reason the page survived the strip.
        assert.deepEqual(
            target.otherReferrers.map((referrer) => referrer.viewKey),
            ['view_78'],
        );
    });
});
