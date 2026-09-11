import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    classifyLinkTargets,
    collectChildPageSubmitRefs,
    collectLinkTargets,
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
    buildRepairedCopyLayout,
    describeAudienceConsequence,
    describePreviewAudience,
    ensureMovedViewIsRendered,
    insertedViewKeysFromOutcome,
    summariseAudienceChanges,
    summariseCopyLinkOwnership,
    type AudienceRow,
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
                profileNames: buildProfileNameIndex({}),
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
                profileNames: buildProfileNameIndex({}),
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
        // The verb, asserted because omitting it is how this repair shipped with PUT
        // and stayed green. `/views/sort` answers PUT with a 400; the fake context
        // falls back to matching a response by path alone, so the wrong method looked
        // exactly like the right one until the live run. A test that checks the body
        // and the path but not the method is not checking the call.
        assert.equal(requests[0].method, 'POST');

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
        assert.equal(requests[0].method, 'POST');
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
            confirmPageDeletion:
                async (): Promise<PageDeletionConfirmation> => ({
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

describe('incident: copy a view, then move the copy onto the original page', () => {
    /**
     * Measured end-to-end on the test app, both builds, 10 September (Tier 11).
     *
     * `view_3` was the sole referrer to two child pages of its own page, one parenting a
     * chain three deep. A plain copy duplicated the whole subtree; a `sharePages` copy
     * pointed the copy at the originals instead. Moving the second kind onto the
     * original's page is the sequence that broke views in production.
     *
     * On the old build that move executed with no prompt and Knack **deleted** the
     * subtree, rebuilding it under new keys and new slugs beneath the target page -
     * `item-details` became `item-details2`, and the second referrer's link column was
     * left pointing at a slug that no longer existed.
     *
     * The old build had also *told the caller the opposite*: its `pagesMovedToAnotherLink`
     * report named the page and said it was now reached from that second referrer. The
     * page was deleted. So `transferred` on a move was not merely too generous, it
     * produced a false sentence, and the fix has to remove the sentence along with the
     * sparing.
     */
    const SHARED: SceneNode[] = [
        {
            sceneKey: 'scene_61',
            sceneSlug: 'new-page-2',
            views: [
                // The share-copy being moved.
                { viewKey: 'view_90', childSceneRefs: ['item-details'] },
            ],
        },
        {
            sceneKey: 'scene_69',
            sceneSlug: 'test-create-table-with-child-pages',
            views: [
                // The second referrer, which the old build said would keep the page.
                { viewKey: 'view_91', childSceneRefs: ['item-details'] },
            ],
        },
        {
            sceneKey: 'scene_89',
            sceneName: 'Item Details',
            sceneSlug: 'item-details',
            parentRef: 'new-page-2',
            views: [],
        },
        { sceneKey: 'scene_3', sceneSlug: 'items', views: [] },
    ];

    const deps = {
        fetchView: async () => ({
            ok: true,
            status: 200,
            body: {
                view: {
                    key: 'view_90',
                    type: 'table',
                    columns: [
                        {
                            type: 'link',
                            header: 'View Details',
                            scene: 'item-details',
                        },
                    ],
                },
            },
        }),
        listScenes: async () => ({ ok: true, scenes: SHARED }),
        writeSnapshot: async () => ({ ok: true, path: '/s.json' }),
        builderUrlForScene: (key: string) => `https://builder/${key}`,
        confirmPageDeletion: async (): Promise<PageDeletionConfirmation> => ({
            supported: false,
        }),
    } as unknown as ViewMutationDeps;

    it('classifies the shared page transferred, which is why the old build spared it', () => {
        const [target] = classifyLinkTargets(
            ['item-details'],
            SHARED,
            'scene_61',
            'view_90',
        );
        assert.equal(target.classification, 'transferred');
        assert.deepEqual(
            target.otherReferrers.map((referrer) => referrer.viewKey),
            ['view_91'],
        );
    });

    it('refuses the move anyway, and does not call the page a survivor', async () => {
        const writes: string[] = [];
        const result = await runGuardedViewMutation(
            deps,
            {
                action: 'move_view',
                sceneKey: 'scene_61',
                viewKey: 'view_90',
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );

        assert.equal(result.ok, false);
        if (result.ok) return;
        assert.equal(result.code, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        // Named as at risk, which is what the caller needed to be told.
        assert.deepEqual(
            (result.details?.childPages as Array<{ sceneKey: string }>).map(
                (page) => page.sceneKey,
            ),
            ['scene_89'],
        );
        assert.deepEqual(writes, []);
    });

    it('reports no transferred pages for a move, so the false sentence cannot be built', async () => {
        // `pagesMovedToAnotherLink` is rendered from transferredPages. A move now spares
        // nothing, so those pages are doomed, and the doomed filter empties the list.
        // Belt and braces on the wording, not just on the refusal.
        const result = await runGuardedViewMutation(
            deps,
            {
                action: 'move_view',
                sceneKey: 'scene_61',
                viewKey: 'view_90',
            },
            async () => ({ sent: true }),
        );

        assert.equal(result.ok, false);
        if (result.ok) return;
        const transferred = result.details?.transferredPages as
            unknown[] | undefined;
        assert.deepEqual(transferred ?? [], []);
    });

    it('still spares the same page on an update, where Knack really does re-parent it', async () => {
        // The counterpart measurement from Tier 10: dropping a link to a page that has
        // another referrer deleted nothing. Only the move is unconditional.
        const [target] = classifyLinkTargets(
            ['item-details'],
            SHARED,
            'scene_61',
            'view_90',
        );
        assert.equal(target.classification, 'transferred');
        // The update path consults the same classification and lets it through; the
        // move path is the one that ignores it.
        const writes: string[] = [];
        const result = await runGuardedViewMutation(
            deps,
            {
                action: 'update_view',
                sceneKey: 'scene_61',
                viewKey: 'view_90',
                updates: JSON.stringify({ title: 'Renamed, links untouched' }),
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );
        assert.equal(result.ok, true);
        assert.deepEqual(writes, ['WRITE']);
    });
});

describe('incident: a copy renders once per layout row', () => {
    /**
     * The eighth defect, and the one the app owner pointed at: "something in the relink
     * caused the issue". It is not the links - those relink correctly. It is the layout.
     *
     * Measured 10 September on the test app, isolated down to a link-free `rich_text`
     * view copied onto a page with a five-row layout:
     *
     *     before: [[view_71], [view_72], [view_78], [view_84], [view_91]]
     *     after:  [[view_71, view_101], [view_72, view_101], [view_78, view_101],
     *              [view_84, view_101], [view_91, view_101]]
     *
     * One copied view, rendering five times. This server sends no layout on a plain copy
     * - the body is only `{action, target_scene_key, view_key, completeViewSchema}` - so
     * the injection is Knack's `copyview` endpoint and no caller can ask for it not to
     * happen.
     *
     * The mirror image of the move defect. A move writes no layout, so the view renders
     * nowhere; a copy writes it into every row, so it renders everywhere. Both were
     * invisible for as long as nothing read `scene.groups`.
     *
     * Two hypotheses were tested and rejected on the way, both about the *links* rather
     * than the layout: that a child page with more than one referrer gets its link
     * cleared on copy, and that such a page gets shared rather than duplicated. Neither
     * holds - `view_3` was copied with its pages on one referrer and again on two, and
     * both times all four pages duplicated with the links intact.
     */
    const AFTER_COPY = [
        { columns: [{ keys: ['view_71', 'view_101'], width: 100 }] },
        { columns: [{ keys: ['view_72', 'view_101'], width: 100 }] },
        { columns: [{ keys: ['view_78', 'view_101'], width: 100 }] },
        { columns: [{ keys: ['view_84', 'view_101'], width: 100 }] },
        { columns: [{ keys: ['view_91', 'view_101'], width: 100 }] },
    ];

    it('restores the pre-copy layout and appends the copy once', () => {
        assert.deepEqual(buildRepairedCopyLayout(AFTER_COPY, 'view_101'), [
            // Exactly the five rows the page had before the copy...
            { columns: [{ keys: ['view_71'], width: 100 }] },
            { columns: [{ keys: ['view_72'], width: 100 }] },
            { columns: [{ keys: ['view_78'], width: 100 }] },
            { columns: [{ keys: ['view_84'], width: 100 }] },
            { columns: [{ keys: ['view_91'], width: 100 }] },
            // ...plus one full-width row, the same thing a move appends.
            { columns: [{ keys: ['view_101'], width: 100 }] },
        ]);
    });

    it('strips the key from every column of a multi-column row', () => {
        // Knack appends into each column's keys, not just the first, so a two-column
        // row would otherwise keep rendering the copy in its second column.
        const twoColumns = [
            {
                columns: [
                    { keys: ['view_1', 'view_9'], width: 50 },
                    { keys: ['view_2', 'view_9'], width: 50 },
                ],
            },
        ];
        assert.deepEqual(buildRepairedCopyLayout(twoColumns, 'view_9'), [
            {
                columns: [
                    { keys: ['view_1'], width: 50 },
                    { keys: ['view_2'], width: 50 },
                ],
            },
            { columns: [{ keys: ['view_9'], width: 100 }] },
        ]);
    });

    it('keeps a row that empties out rather than dropping it', () => {
        // A row can only empty out if it was already empty before the copy, so dropping
        // it would delete a row someone arranged in order to fix a problem they did not
        // cause. Knack tolerates empty rows - a move measured in this session left one.
        const withEmptyRow = [
            { columns: [{ keys: ['view_1', 'view_9'], width: 100 }] },
            { columns: [{ keys: ['view_9'], width: 100 }] },
        ];
        assert.deepEqual(buildRepairedCopyLayout(withEmptyRow, 'view_9'), [
            { columns: [{ keys: ['view_1'], width: 100 }] },
            { columns: [{ keys: [], width: 100 }] },
            { columns: [{ keys: ['view_9'], width: 100 }] },
        ]);
    });

    it('preserves every other property on rows and columns', () => {
        // The repair rewrites the whole layout, so anything it does not understand it
        // has to carry through untouched.
        const decorated = [
            {
                label: 'Top section',
                columns: [
                    { keys: ['view_1', 'view_9'], width: 100, foo: 'bar' },
                ],
            },
        ];
        assert.deepEqual(buildRepairedCopyLayout(decorated, 'view_9'), [
            {
                label: 'Top section',
                columns: [{ keys: ['view_1'], width: 100, foo: 'bar' }],
            },
            { columns: [{ keys: ['view_9'], width: 100 }] },
        ]);
    });

    it('declines when an occurrence survives a shape it cannot walk', () => {
        // Appending on top of a leftover would leave the view rendering twice, which is
        // the bug being fixed. Declining reports it instead of half-fixing it.
        const odd = [
            { columns: [{ keys: ['view_1', 'view_9'], width: 100 }] },
            { columns: 'not-an-array', nested: { keys: ['view_9'] } },
        ];
        assert.equal(buildRepairedCopyLayout(odd, 'view_9'), null);
    });

    it('leaves a layout alone when the key is not in it', () => {
        const clean = [{ columns: [{ keys: ['view_1'], width: 100 }] }];
        assert.deepEqual(buildRepairedCopyLayout(clean, 'view_9'), [
            { columns: [{ keys: ['view_1'], width: 100 }] },
            { columns: [{ keys: ['view_9'], width: 100 }] },
        ]);
    });

    it('does not mutate the layout it was given', () => {
        const original = JSON.parse(JSON.stringify(AFTER_COPY));
        buildRepairedCopyLayout(AFTER_COPY, 'view_101');
        assert.deepEqual(AFTER_COPY, original);
    });

    it('reads the created view keys out of the response, in order', () => {
        // A copy of a view owning child pages creates a view per duplicated page too.
        // Measured: ['view_85', 'view_86', 'view_87', 'view_88', 'view_89'] for one
        // copy, of which only view_85 landed on the target page.
        assert.deepEqual(
            insertedViewKeysFromOutcome({
                body: {
                    changes: {
                        inserts: {
                            views: ['view_85', 'view_86', 'view_87'],
                        },
                    },
                },
            }),
            ['view_85', 'view_86', 'view_87'],
        );
    });

    it('reports no created views for a refusal, rather than throwing', () => {
        assert.deepEqual(insertedViewKeysFromOutcome({ ok: false }), []);
        assert.deepEqual(insertedViewKeysFromOutcome({ body: {} }), []);
        assert.deepEqual(
            insertedViewKeysFromOutcome({
                body: { changes: { inserts: { views: 'nope' } } },
            }),
            [],
        );
    });
});

describe('incident: the copy moved away, then back, and the trap in between', () => {
    /**
     * The full sequence as the app owner described it, measured on the test app on 10
     * September (Tier 13): a copy that relinks rather than duplicates, moved away, then
     * moved back.
     *
     * Leg 2 - moving the copy off the page - was **allowed** by the old build, whose own
     * response named two surviving referrers for the child page under
     * `pagesMovedToAnotherLink` and then reported it deleted in the same breath. Three
     * pages and three views went, and the link on `view_96` - the original, never named
     * in the call - was left pointing at a slug that no longer existed.
     *
     * Leg 3 - moving it back - was **refused** by the old build, because the forward move
     * had already stripped the page of every referrer but the copy, so it now classified
     * `owned`.
     *
     * That is the trap: the destructive leg passes silently and the tool then refuses the
     * one that would undo it. Which is why the incident felt like it broke on the move
     * back. The wall is there; the damage happened on the way out.
     *
     * Two other referrers did not save the page. Tier 11 measured one failing to; this
     * measured two. On a move the count is irrelevant, so the fix refuses both legs.
     */
    const AWAY: SceneNode[] = [
        {
            sceneKey: 'scene_61',
            sceneSlug: 'new-page-2',
            views: [
                // The original, which nobody asked to change.
                { viewKey: 'view_96', childSceneRefs: ['item-details'] },
                // The relinking copy, sharing the same page.
                { viewKey: 'view_104', childSceneRefs: ['item-details'] },
            ],
        },
        {
            sceneKey: 'scene_69',
            sceneSlug: 'test-create-table-with-child-pages',
            views: [{ viewKey: 'view_91', childSceneRefs: ['item-details'] }],
        },
        {
            sceneKey: 'scene_97',
            sceneName: 'Item Details',
            sceneSlug: 'item-details',
            parentRef: 'new-page-2',
            views: [],
        },
        { sceneKey: 'scene_67', sceneSlug: 'menu-scene-5', views: [] },
    ];

    const depsFor = (scenes: SceneNode[], viewKey: string, ref: string) =>
        ({
            fetchView: async () => ({
                ok: true,
                status: 200,
                body: {
                    view: {
                        key: viewKey,
                        type: 'table',
                        columns: [
                            {
                                type: 'link',
                                header: 'View Details',
                                scene: ref,
                            },
                        ],
                    },
                },
            }),
            listScenes: async () => ({ ok: true, scenes }),
            writeSnapshot: async () => ({ ok: true, path: '/s.json' }),
            builderUrlForScene: (key: string) => `https://builder/${key}`,
            confirmPageDeletion:
                async (): Promise<PageDeletionConfirmation> => ({
                    supported: false,
                }),
        }) as unknown as ViewMutationDeps;

    it('sees two other referrers on the page, which is why the old build allowed leg 2', () => {
        const [target] = classifyLinkTargets(
            ['item-details'],
            AWAY,
            'scene_61',
            'view_104',
        );
        assert.equal(target.classification, 'transferred');
        assert.deepEqual(
            target.otherReferrers.map((referrer) => referrer.viewKey).sort(),
            ['view_91', 'view_96'],
        );
    });

    it('refuses leg 2 anyway, because two referrers did not save the page either', async () => {
        const writes: string[] = [];
        const result = await runGuardedViewMutation(
            depsFor(AWAY, 'view_104', 'item-details'),
            {
                action: 'move_view',
                sceneKey: 'scene_61',
                viewKey: 'view_104',
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
            ['scene_97'],
        );
        assert.deepEqual(writes, []);
    });

    /**
     * Leg 3's shape: the forward move has already rebuilt the subtree under the scratch
     * page, so only the copy links to it. The old build refused this one — and refused it
     * because of the damage the leg it had permitted had done.
     */
    const BACK: SceneNode[] = [
        {
            sceneKey: 'scene_67',
            sceneSlug: 'menu-scene-5',
            views: [{ viewKey: 'view_104', childSceneRefs: ['item-details3'] }],
        },
        {
            sceneKey: 'scene_101',
            sceneName: 'Item Details',
            sceneSlug: 'item-details3',
            parentRef: 'menu-scene-5',
            views: [],
        },
        { sceneKey: 'scene_61', sceneSlug: 'new-page-2', views: [] },
    ];

    it('refuses leg 3 as well, so the two directions agree', async () => {
        const writes: string[] = [];
        const result = await runGuardedViewMutation(
            depsFor(BACK, 'view_104', 'item-details3'),
            {
                action: 'move_view',
                sceneKey: 'scene_67',
                viewKey: 'view_104',
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );

        assert.equal(result.ok, false);
        if (result.ok) return;
        assert.deepEqual(
            (result.details?.childPages as Array<{ sceneKey: string }>).map(
                (page) => page.sceneKey,
            ),
            ['scene_101'],
        );
        assert.deepEqual(writes, []);
    });

    it('classifies leg 3 owned, which is the asymmetry the old build ran on', () => {
        // Same view, same subtree, one leg apart — and the old build's verdict flipped
        // from allow to refuse purely because it had already destroyed the referrers.
        const [away] = classifyLinkTargets(
            ['item-details'],
            AWAY,
            'scene_61',
            'view_104',
        );
        const [back] = classifyLinkTargets(
            ['item-details3'],
            BACK,
            'scene_67',
            'view_104',
        );
        assert.equal(away.classification, 'transferred');
        assert.equal(back.classification, 'owned');
    });
});

describe('the guard, as a property rather than a list of cases', () => {
    /**
     * Answering a fair objection: if the expected value is written by the same hand that
     * wrote the code, what has been proved?
     *
     * For the example-based suites above, the answer is that the *inputs* are recorded
     * payloads and the *expectations* are live measurements taken before the test was
     * written — so they encode observations, not preferences. But that defence has a
     * hole, and the wrong HTTP verb went straight through it: both layout repairs used
     * PUT, every test passed, and nothing executed the call for real.
     *
     * This suite is the part that cannot be tuned to match the code, because it asserts
     * no specific value. It enumerates the whole space of topologies a move can face and
     * claims one thing about all of them: **a move_view never writes.**
     *
     * That claim is now measured on all four classifications, each on the live app
     * against the old build:
     *
     *   owned       (no other referrer)        deleted the page       Tier 11
     *   transferred (one other referrer)       deleted the page       Tier 11
     *   transferred (two other referrers)      deleted the page       Tier 13
     *   external    (parented somewhere else)  deleted the page       Tier 14
     *
     * Four for four. There is no classification under which Knack spares a page on a
     * move, so there is no topology in which allowing one is correct. If a future change
     * reintroduces sparing for any shape of graph, one of these cases fails without
     * anybody having predicted which.
     */
    const TARGET = 'scene_target';

    /** Every distinct shape the classifier can be handed, built combinatorially. */
    function* topologies(): Generator<{ label: string; scenes: SceneNode[] }> {
        const parents: Array<[string, string | undefined]> = [
            ['parent-is-source', 'source-slug'],
            ['parent-is-elsewhere', 'other-slug'],
            ['parent-is-unresolvable', 'no-such-slug'],
            ['parent-is-absent', undefined],
        ];
        for (const [parentLabel, parentRef] of parents) {
            for (const referrers of [0, 1, 2, 5]) {
                const extra = Array.from({ length: referrers }, (_x, i) => ({
                    viewKey: `view_other_${i}`,
                    childSceneRefs: ['child-slug'],
                }));
                yield {
                    label: `${parentLabel}, ${referrers} other referrer(s)`,
                    scenes: [
                        {
                            sceneKey: 'scene_source',
                            sceneSlug: 'source-slug',
                            views: [
                                {
                                    viewKey: 'view_moving',
                                    childSceneRefs: ['child-slug'],
                                },
                                ...extra,
                            ],
                        },
                        {
                            sceneKey: 'scene_child',
                            sceneName: 'Child',
                            sceneSlug: 'child-slug',
                            ...(parentRef ? { parentRef } : {}),
                            views: [],
                        },
                        {
                            sceneKey: 'scene_other',
                            sceneSlug: 'other-slug',
                            views: [],
                        },
                        {
                            sceneKey: TARGET,
                            sceneSlug: 'target-slug',
                            views: [],
                        },
                    ],
                };
            }
        }
    }

    const depsFor = (scenes: SceneNode[]) =>
        ({
            fetchView: async () => ({
                ok: true,
                status: 200,
                body: {
                    view: {
                        key: 'view_moving',
                        type: 'table',
                        columns: [
                            {
                                type: 'link',
                                header: 'Child',
                                scene: 'child-slug',
                            },
                        ],
                    },
                },
            }),
            listScenes: async () => ({ ok: true, scenes }),
            writeSnapshot: async () => ({ ok: true, path: '/s.json' }),
            builderUrlForScene: (key: string) => `https://builder/${key}`,
            confirmPageDeletion:
                async (): Promise<PageDeletionConfirmation> => ({
                    supported: false,
                }),
        }) as unknown as ViewMutationDeps;

    it('never writes on a move, across every topology, without naming one', async () => {
        const allowed: string[] = [];
        let checked = 0;

        for (const { label, scenes } of topologies()) {
            checked += 1;
            const writes: string[] = [];
            const result = await runGuardedViewMutation(
                depsFor(scenes),
                {
                    action: 'move_view',
                    sceneKey: 'scene_source',
                    viewKey: 'view_moving',
                },
                async () => {
                    writes.push('WRITE');
                    return { sent: true };
                },
            );
            if (writes.length > 0 || result.ok) allowed.push(label);
        }

        // 4 parent shapes x 4 referrer counts. Asserted so a generator that silently
        // stops producing cases cannot make this suite pass by testing nothing.
        assert.equal(checked, 16);
        assert.deepEqual(allowed, []);
    });

    it('covers more than one classification, so the property is not vacuous', () => {
        // A property over a space that all collapses to one class proves little. This
        // asserts the space actually spans the classifier's outputs.
        const seen = new Set<string>();
        for (const { scenes } of topologies()) {
            const [target] = classifyLinkTargets(
                ['child-slug'],
                scenes,
                'scene_source',
                'view_moving',
            );
            seen.add(target.classification);
        }
        assert.ok(
            seen.size >= 3,
            `expected several classifications across the space, saw ${[...seen].join(', ')}`,
        );
        // The two the old build spared, both now measured as destructive on a move.
        assert.ok(seen.has('transferred'));
        assert.ok(seen.has('external'));
    });

    it('still allows a move of a view that references no page at all', async () => {
        // The other half of the property, and the answer to "can the MCP move views at
        // all". 84% of the views in the production app carry no page reference, and
        // every one of them still moves. The refusal is scoped to the 16% Knack would
        // rebuild, not to moves in general.
        const writes: string[] = [];
        const deps = {
            fetchView: async () => ({
                ok: true,
                status: 200,
                body: {
                    view: { key: 'view_plain', type: 'table', columns: [] },
                },
            }),
            listScenes: async () => ({
                ok: true,
                scenes: [
                    {
                        sceneKey: 'scene_source',
                        sceneSlug: 'source-slug',
                        views: [{ viewKey: 'view_plain', childSceneRefs: [] }],
                    },
                    { sceneKey: TARGET, sceneSlug: 'target-slug', views: [] },
                ] as SceneNode[],
            }),
            writeSnapshot: async () => ({ ok: true, path: '/s.json' }),
            builderUrlForScene: (key: string) => `https://builder/${key}`,
            confirmPageDeletion:
                async (): Promise<PageDeletionConfirmation> => ({
                    supported: false,
                }),
        } as unknown as ViewMutationDeps;

        const result = await runGuardedViewMutation(
            deps,
            {
                action: 'move_view',
                sceneKey: 'scene_source',
                viewKey: 'view_plain',
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );

        assert.equal(result.ok, true);
        assert.deepEqual(writes, ['WRITE']);
    });
});

describe('incident: `remote` is what decides whether a move destroys a page', () => {
    /**
     * The answer, after four wrong hypotheses. Established 10 September against the
     * live app, with the app owner driving the builder side.
     *
     * The owner's demonstration: two tables on one page, `view_120` and `view_123`,
     * pointing at the **same two child pages**. Moving `view_123` in the builder
     * destroyed nothing - same view key, both pages present with the same slugs and the
     * same parents, all four links resolving, 52 pages and 81 views before and after.
     * Three moves, both directions, all safe.
     *
     * The stored definitions said why. `view_123`'s link columns carry `remote: true`.
     * `view_120`'s, pointing at those same pages, do not.
     *
     *     remote absent  - this view OWNS the page. A move takes it along, which Knack
     *                      implements as delete-and-rebuild under the new parent, with a
     *                      new key and a new slug. Every reference to the old slug then
     *                      dangles, including from views nobody touched.
     *     remote: true   - this view merely LINKS to it. A move leaves it alone:
     *                      `deletes.scenes: []`, `inserts.scenes: []`.
     *
     * Confirmed in the other direction, which is the part that makes it actionable:
     * setting `remote: true` on a link column and then re-running the identical move
     * that had destroyed the page on every previous attempt left the page completely
     * intact - present, same slug, same parent - with the view moved.
     *
     * Four alternatives were tested and none of them mattered: a dedicated
     * `/views/{view}/move` route (404, does not exist), `completeViewSchema` carrying
     * the whole view definition instead of a boolean (still destroyed), the builder's
     * `x-knack-new-builder` header (still destroyed), and the builder's own
     * `/account/{acct}/application/{app}/` base path (404 to a REST key). The builder
     * calls the same endpoint with the same body. The difference was in the view.
     */
    const tableWith = (columns: unknown[]) => ({
        key: 'view_moving',
        type: 'table',
        columns,
    });

    const OWNED_COLUMN = {
        type: 'link',
        header: 'Child',
        scene: 'child-slug',
    };
    const REMOTE_COLUMN = {
        type: 'link',
        header: 'Child',
        scene: 'child-slug',
        remote: true,
    };

    const SCENES: SceneNode[] = [
        {
            sceneKey: 'scene_source',
            sceneSlug: 'source-slug',
            views: [{ viewKey: 'view_moving', childSceneRefs: ['child-slug'] }],
        },
        {
            sceneKey: 'scene_child',
            sceneName: 'Child',
            sceneSlug: 'child-slug',
            parentRef: 'source-slug',
            views: [],
        },
        { sceneKey: 'scene_target', sceneSlug: 'target-slug', views: [] },
    ];

    const move = async (columns: unknown[]) => {
        const writes: string[] = [];
        const deps = {
            fetchView: async () => ({
                ok: true,
                status: 200,
                body: { view: tableWith(columns) },
            }),
            listScenes: async () => ({ ok: true, scenes: SCENES }),
            writeSnapshot: async () => ({ ok: true, path: '/s.json' }),
            builderUrlForScene: (key: string) => `https://builder/${key}`,
            confirmPageDeletion:
                async (): Promise<PageDeletionConfirmation> => ({
                    supported: false,
                }),
        } as unknown as ViewMutationDeps;

        const result = await runGuardedViewMutation(
            deps,
            {
                action: 'move_view',
                sceneKey: 'scene_source',
                viewKey: 'view_moving',
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );
        return { result, writes };
    };

    it('reads the flag off the link column, true, false and absent apart', () => {
        const { linkColumns } = collectLinkTargets(
            tableWith([
                REMOTE_COLUMN,
                OWNED_COLUMN,
                { type: 'link', header: 'Explicit', scene: 'x', remote: false },
            ]),
        );
        assert.deepEqual(
            linkColumns.map((column) => column.remote),
            // Absent is null rather than false: Knack treats them the same, but a
            // person reading a refusal is owed the difference.
            [true, null, false],
        );
    });

    it('allows the move when the only link is remote', async () => {
        // The owner's view_123 case. Nothing is at risk, so nothing is asked.
        const { result, writes } = await move([REMOTE_COLUMN]);
        assert.equal(result.ok, true);
        assert.deepEqual(writes, ['WRITE']);
    });

    it('refuses the move when the link is not remote', async () => {
        // The owner's view_120 case, and every destructive move measured all day.
        const { result, writes } = await move([OWNED_COLUMN]);
        assert.equal(result.ok, false);
        if (result.ok) return;
        assert.equal(result.code, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        assert.deepEqual(
            (result.details?.childPages as Array<{ sceneKey: string }>).map(
                (page) => page.sceneKey,
            ),
            ['scene_child'],
        );
        assert.deepEqual(writes, []);
    });

    it('refuses when remote: false is stated explicitly', async () => {
        const { result, writes } = await move([
            {
                type: 'link',
                header: 'Child',
                scene: 'child-slug',
                remote: false,
            },
        ]);
        assert.equal(result.ok, false);
        assert.deepEqual(writes, []);
    });

    it('refuses when one link is remote and another to the same page is not', async () => {
        // One ownership claim is enough to rebuild the page, so one is enough to refuse.
        const { result, writes } = await move([REMOTE_COLUMN, OWNED_COLUMN]);
        assert.equal(result.ok, false);
        assert.deepEqual(writes, []);
    });

    it('does not spare a page reached by a menu link, remote or not', async () => {
        // Menu links carry no `remote` property at all, so there is no evidence to
        // spare them on, and absence of evidence is not evidence of safety.
        const withMenu = {
            key: 'view_moving',
            type: 'menu',
            columns: [REMOTE_COLUMN],
            links: [{ name: 'Go', scene: 'child-slug' }],
        };
        const writes: string[] = [];
        const deps = {
            fetchView: async () => ({
                ok: true,
                status: 200,
                body: { view: withMenu },
            }),
            listScenes: async () => ({ ok: true, scenes: SCENES }),
            writeSnapshot: async () => ({ ok: true, path: '/s.json' }),
            builderUrlForScene: (key: string) => `https://builder/${key}`,
            confirmPageDeletion:
                async (): Promise<PageDeletionConfirmation> => ({
                    supported: false,
                }),
        } as unknown as ViewMutationDeps;
        const result = await runGuardedViewMutation(
            deps,
            {
                action: 'move_view',
                sceneKey: 'scene_source',
                viewKey: 'view_moving',
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );
        assert.equal(result.ok, false);
        assert.deepEqual(writes, []);
    });

    it('still refuses a non-remote move whatever the page hierarchy looks like', async () => {
        // The property, restated on the axis that actually decides it. Referrer count
        // and parentage were both measured irrelevant to a move; `remote` was measured
        // decisive. So the claim over the whole space is now conditional on the flag,
        // and the flag alone.
        const parents = [
            'source-slug',
            'other-slug',
            'no-such-slug',
            undefined,
        ];
        let checked = 0;
        const wrong: string[] = [];

        for (const parentRef of parents) {
            for (const referrers of [0, 1, 2]) {
                for (const remote of [true, false]) {
                    checked += 1;
                    const scenes: SceneNode[] = [
                        {
                            sceneKey: 'scene_source',
                            sceneSlug: 'source-slug',
                            views: [
                                {
                                    viewKey: 'view_moving',
                                    childSceneRefs: ['child-slug'],
                                },
                                ...Array.from(
                                    { length: referrers },
                                    (_x, i) => ({
                                        viewKey: `view_other_${i}`,
                                        childSceneRefs: ['child-slug'],
                                    }),
                                ),
                            ],
                        },
                        {
                            sceneKey: 'scene_child',
                            sceneSlug: 'child-slug',
                            ...(parentRef ? { parentRef } : {}),
                            views: [],
                        },
                        {
                            sceneKey: 'scene_other',
                            sceneSlug: 'other-slug',
                            views: [],
                        },
                        {
                            sceneKey: 'scene_target',
                            sceneSlug: 'target-slug',
                            views: [],
                        },
                    ];
                    const writes: string[] = [];
                    const deps = {
                        fetchView: async () => ({
                            ok: true,
                            status: 200,
                            body: {
                                view: tableWith([
                                    remote ? REMOTE_COLUMN : OWNED_COLUMN,
                                ]),
                            },
                        }),
                        listScenes: async () => ({ ok: true, scenes }),
                        writeSnapshot: async () => ({
                            ok: true,
                            path: '/s.json',
                        }),
                        builderUrlForScene: (key: string) =>
                            `https://builder/${key}`,
                        confirmPageDeletion:
                            async (): Promise<PageDeletionConfirmation> => ({
                                supported: false,
                            }),
                    } as unknown as ViewMutationDeps;
                    const result = await runGuardedViewMutation(
                        deps,
                        {
                            action: 'move_view',
                            sceneKey: 'scene_source',
                            viewKey: 'view_moving',
                        },
                        async () => {
                            writes.push('WRITE');
                            return { sent: true };
                        },
                    );
                    const label = `parent=${parentRef}, referrers=${referrers}, remote=${remote}`;
                    // Remote links go through; owned links never do, whatever the graph.
                    if (remote && (!result.ok || writes.length === 0)) {
                        wrong.push(`should have allowed: ${label}`);
                    }
                    if (!remote && (result.ok || writes.length > 0)) {
                        wrong.push(`should have refused: ${label}`);
                    }
                }
            }
        }

        assert.equal(checked, 24);
        assert.deepEqual(wrong, []);
    });
});

describe('incident: what a copy does to a linked page is read, not predicted', () => {
    /**
     * Measured 10 September on one table carrying two link columns pointing at
     * **sibling child pages of the same parent** - same position in the tree, differing
     * only in the flag.
     *
     *   owned link (no flag) -> a new page appeared under the copy's target and the copy
     *                           was repointed at it; the original kept the old one
     *   remote: true         -> shared, no page created, both views pointing at the same
     *
     * That was read as "ownership decides", which holds for the table it was measured
     * on and nowhere else. Re-measured 11 September across three view types, the same
     * call each time and the flag absent in every case:
     *
     *   table,   `type: "link"`       -> duplicated; a new scene in changes.inserts
     *   details, `type: "scene_link"` -> shared; no scene created, page gains a referrer
     *   list,    `type: "scene_link"` -> shared; likewise
     *
     * Those three could not tell "the link's node type decides" apart from "the view's
     * type decides": every table carried `link` and every details or list carried
     * `scene_link`, so both read the same. A search view separates them - a fourth view
     * type, links kept in `results.columns[]` rather than `columns`, carrying
     * `type: "link"` - and it **duplicated**, like the table and unlike its fellow
     * non-tables.
     *
     * So the deciding factor is the link's node type, not ownership and not the view. Predicting from the
     * flag told a caller its copy was independent when both views had in fact just been
     * left pointing at one page - and said so in the tool's own voice, down to "the
     * original still points at the old one".
     *
     * `onCopy` now comes from the pages Knack's own response reports creating: the same
     * "measure the response, do not model the vendor" rule the sharePages path already
     * follows with `sharedPagesVerified`. `owned` still reports the flag, which remains
     * a true fact about the link and the right input for the cascade guard.
     *
     * Reported rather than blocked either way. A copy duplicating the pages a view owns
     * is Knack working as intended and usually what the caller wants.
     */
    const CREATED = [
        {
            sceneKey: 'scene_128',
            sceneName: 'AB Child',
            sceneSlug: 'ab-child',
            parentRef: 'main-menu',
        },
    ];
    const NONE: typeof CREATED = [];

    const VIEW = {
        key: 'view_131',
        type: 'table',
        columns: [
            { type: 'field', field: { key: 'field_23' }, header: 'Name' },
            { type: 'link', header: 'OWNED link', scene: 'verify-child2' },
            {
                type: 'link',
                header: 'REMOTE link',
                scene: 'ab-child',
                remote: true,
            },
        ],
    };

    it('separates the pages a copy duplicated from the ones it shared', () => {
        assert.deepEqual(summariseCopyLinkOwnership(VIEW, CREATED), [
            {
                header: 'OWNED link',
                childSceneRef: 'verify-child2',
                owned: true,
                onCopy: 'duplicated',
            },
            {
                header: 'REMOTE link',
                childSceneRef: 'ab-child',
                owned: false,
                onCopy: 'shared',
            },
        ]);
    });

    it('calls an owned link shared when Knack created no page', () => {
        // The details and list case. Ownership is unchanged - the view still claims the
        // page - but nothing was duplicated, so reporting "duplicated" would describe a
        // second page that does not exist and imply an independence the copy lacks.
        const rows = summariseCopyLinkOwnership(VIEW, NONE);
        assert.deepEqual(
            rows.map((row) => [row.owned, row.onCopy]),
            [
                [true, 'shared'],
                [false, 'shared'],
            ],
        );
    });

    it('reads a scene_link buried in a details body', () => {
        // Where details and list views keep their page links: four levels down, as
        // `type: "scene_link"`. The live refusal reported this exact path as
        // `$.columns[0].groups[0].columns[0][1]`.
        const rows = summariseCopyLinkOwnership(
            {
                key: 'view_142',
                type: 'details',
                columns: [
                    {
                        width: 100,
                        groups: [
                            {
                                columns: [
                                    [
                                        { key: 'field_23', type: 'field' },
                                        {
                                            type: 'scene_link',
                                            scene: 'ab-details-child',
                                        },
                                    ],
                                ],
                            },
                        ],
                    },
                ],
            },
            NONE,
        );

        assert.equal(rows.length, 1);
        assert.equal(rows[0].childSceneRef, 'ab-details-child');
        assert.equal(rows[0].owned, true);
        assert.equal(rows[0].onCopy, 'shared');
    });

    it('reads a link nested in a search view results block', () => {
        // A search view keeps its page links in `results.columns[]`; its own `columns`
        // is empty. Measured 11 September, where the live move refusal named this exact
        // node as `$.results.columns[2]`. The walk is generic over the attributes
        // object, which is why a sub-object nobody had measured cost nothing.
        const rows = summariseCopyLinkOwnership(
            {
                key: 'view_150',
                type: 'search',
                columns: [],
                results: {
                    type: 'table',
                    columns: [
                        {
                            type: 'field',
                            field: { key: 'field_23' },
                            header: 'Name',
                        },
                        {
                            type: 'link',
                            header: 'Edit Table 1',
                            scene: 'edit-table-12',
                        },
                    ],
                },
            },
            CREATED,
        );

        assert.equal(rows.length, 1);
        assert.equal(rows[0].childSceneRef, 'edit-table-12');
        assert.equal(rows[0].owned, true);
        assert.equal(rows[0].onCopy, 'duplicated');
    });

    it('counts an absent flag as owned, like Knack does', () => {
        // Absent has to mean owned rather than unknown; it is `onCopy` that no longer
        // follows from it.
        const [row] = summariseCopyLinkOwnership(
            {
                key: 'v',
                type: 'table',
                columns: [{ type: 'link', header: 'X', scene: 'child' }],
            },
            CREATED,
        );
        assert.equal(row.owned, true);
        assert.equal(row.onCopy, 'duplicated');
    });

    it('says nothing for a view with no page links', () => {
        assert.deepEqual(
            summariseCopyLinkOwnership(
                { key: 'v', type: 'table', columns: [] },
                CREATED,
            ),
            [],
        );
        assert.deepEqual(summariseCopyLinkOwnership(null, CREATED), []);
    });

    it('ignores an action link, which is a button and not a page link', () => {
        // Shape taken from a real copyview body on a production app, 11 September, with
        // the keys replaced. An action link carries `link_text`, `link_design_active`
        // and its own `action_rules[].submit_rules[]`, so it looks like navigation from
        // every angle except the one that counts: it has no `scene`. Confirmed against
        // the real payload, which reported no link targets at all.
        assert.deepEqual(
            summariseCopyLinkOwnership(
                {
                    key: 'v',
                    type: 'details',
                    columns: [
                        {
                            groups: [
                                {
                                    columns: [
                                        [
                                            { key: 'field_1', type: 'field' },
                                            {
                                                type: 'action_link',
                                                name: 'Trigger an action',
                                                link_text: 'action',
                                                link_design_active: true,
                                                action_rules: [
                                                    {
                                                        key: '1',
                                                        link_text: 'Mark done',
                                                        record_rules: [
                                                            {
                                                                key: '2',
                                                                action: 'record',
                                                                values: [],
                                                            },
                                                        ],
                                                        submit_rules: [
                                                            {
                                                                action: 'message',
                                                                message: 'done',
                                                                reload_show: false,
                                                            },
                                                        ],
                                                    },
                                                ],
                                            },
                                        ],
                                    ],
                                },
                            ],
                        },
                    ],
                },
                CREATED,
            ),
            [],
        );
    });

    it('ignores a form input that is a link field rather than a page link', () => {
        // A form's Link/URL input is also `type: "link"`, carries a `field` and no
        // `scene`, and points at no page at all.
        assert.deepEqual(
            summariseCopyLinkOwnership(
                {
                    key: 'v',
                    type: 'form',
                    groups: [
                        {
                            columns: [
                                {
                                    inputs: [
                                        {
                                            type: 'link',
                                            field: { key: 'field_30' },
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
                CREATED,
            ),
            [],
        );
    });
});

describe('incident: marking a link remote is itself destructive', () => {
    /**
     * Measured 11 September, closing the row Tier 15 left open - and the answer was not
     * the one the question expected.
     *
     * The plan was to measure what *removing* a remote link does when it is the page's
     * only referrer. It never got that far: marking the link remote deleted the page on
     * its own. A page created by a copy, so with no form rule attached and exactly one
     * link column reaching it, and the single change was setting `remote: true` on that
     * column. Nothing removed, the `scene` still in the body. Knack deleted the page and
     * stripped the link column out of the view as well.
     *
     * Which follows from what the flag means. `remote: true` says "this view does not own
     * the page". With nothing else owning it the page has no owner at all, and Knack
     * resolves that by removing it. Marking a link remote is safe only while somebody
     * else holds it.
     *
     * It also kills the feature this was groundwork for. "Mark the links remote, then
     * move" reads as the obvious way to move a view without disturbing its pages, it is
     * measured to work when another view owns them - and it silently destroys them when
     * none does.
     *
     * Through the tool, before the fix: `ok: true`, no prompt, and
     * `pagesKnackReportsDeleted: ["scene_123"]` in the same response.
     */
    const storedView = (remote: boolean) => ({
        key: 'view_136',
        type: 'table',
        columns: [
            { type: 'field', field: { key: 'field_23' }, header: 'Name' },
            {
                type: 'link',
                header: 'Child',
                scene: 'ab-child2',
                ...(remote ? { remote: true } : {}),
            },
        ],
    });

    /** The page has exactly one referrer: the view being changed. */
    const SOLE: SceneNode[] = [
        {
            sceneKey: 'scene_13',
            sceneSlug: 'view-table-1-details',
            views: [{ viewKey: 'view_136', childSceneRefs: ['ab-child2'] }],
        },
        {
            sceneKey: 'scene_123',
            sceneName: 'AB Child',
            sceneSlug: 'ab-child2',
            parentRef: 'view-table-1-details',
            views: [],
        },
    ];

    /** Same page, but a second view also owns it. */
    const SHARED: SceneNode[] = [
        {
            sceneKey: 'scene_13',
            sceneSlug: 'view-table-1-details',
            views: [
                { viewKey: 'view_136', childSceneRefs: ['ab-child2'] },
                { viewKey: 'view_other', childSceneRefs: ['ab-child2'] },
            ],
        },
        {
            sceneKey: 'scene_123',
            sceneName: 'AB Child',
            sceneSlug: 'ab-child2',
            parentRef: 'view-table-1-details',
            views: [],
        },
    ];

    const depsFor = (scenes: SceneNode[], stored: boolean, writes: string[]) =>
        ({
            fetchView: async () => ({
                ok: true,
                status: 200,
                body: { view: storedView(stored) },
            }),
            listScenes: async () => ({ ok: true, scenes }),
            writeSnapshot: async () => ({ ok: true, path: '/s.json' }),
            builderUrlForScene: (key: string) => `https://builder/${key}`,
            confirmPageDeletion:
                async (): Promise<PageDeletionConfirmation> => ({
                    supported: false,
                }),
            _writes: writes,
        }) as unknown as ViewMutationDeps;

    const run = async (
        scenes: SceneNode[],
        stored: boolean,
        updates: Record<string, unknown>,
    ) => {
        const writes: string[] = [];
        const result = await runGuardedViewMutation(
            depsFor(scenes, stored, writes),
            {
                action: 'update_view',
                sceneKey: 'scene_13',
                viewKey: 'view_136',
                updates: JSON.stringify(updates),
            },
            async () => {
                writes.push('WRITE');
                return { sent: true };
            },
        );
        return { result, writes };
    };

    it('refuses to give up the last claim on a page', async () => {
        // The link is re-sent, not removed. Only `remote` changes.
        const { result, writes } = await run(SOLE, false, {
            columns: storedView(true).columns,
        });
        assert.equal(result.ok, false);
        if (result.ok) return;
        assert.equal(result.code, 'HUMAN_CONFIRMATION_UNAVAILABLE');
        assert.deepEqual(
            (result.details?.childPages as Array<{ sceneKey: string }>).map(
                (page) => page.sceneKey,
            ),
            ['scene_123'],
        );
        assert.deepEqual(writes, []);
    });

    it('allows it when another view still owns the page', async () => {
        // The second owner keeps the page, so renouncing one claim costs nothing. This
        // is the case that makes the check worth having rather than a blanket refusal.
        const { result, writes } = await run(SHARED, false, {
            columns: storedView(true).columns,
        });
        assert.equal(result.ok, true);
        assert.deepEqual(writes, ['WRITE']);
    });

    it('leaves an ordinary edit that re-sends the same links alone', async () => {
        // The check keys off the flag changing, not off the link being present, so a
        // title change is untouched even on a solely-owned page.
        const { result, writes } = await run(SOLE, false, { title: 'Renamed' });
        assert.equal(result.ok, true);
        assert.deepEqual(writes, ['WRITE']);
    });

    it('does not treat remote going the other way as a renunciation', async () => {
        // Claiming ownership of a page is not giving it up. Only false-to-true counts.
        const { result, writes } = await run(SOLE, true, {
            columns: storedView(false).columns,
        });
        assert.equal(result.ok, true);
        assert.deepEqual(writes, ['WRITE']);
    });
});

describe('a preview says what it found about audience, including nothing', () => {
    const row = (
        sceneKey: string,
        change: AudienceRow['change'],
        destinationSceneKey: string | null = 'scene_9',
    ): AudienceRow => ({
        sceneKey,
        change,
        before: 'anyone (no login above it)',
        after: change === 'unknown' ? 'not known here' : 'Manager',
        destinationSceneKey,
    });

    it('emits the key even when no page re-parents', () => {
        // The whole point, and the one deliberate divergence from the executed path.
        // Measured 11 September: a preview of a transfer returned no audience key at
        // all, and the response could not be told apart from one where the question
        // was never asked. An empty array answers it; an absent key does not.
        const result = describePreviewAudience([]);

        assert.deepEqual(result.audienceChanges, []);
        assert.ok('audienceChanges' in result);
        assert.equal(result.audienceWarning, undefined);
    });

    it('stays quiet about pages whose audience is unchanged', () => {
        const result = describePreviewAudience([row('scene_13', 'same')]);

        assert.equal((result.audienceChanges as AudienceRow[]).length, 1);
        assert.equal(result.audienceWarning, undefined);
    });

    it('warns when a page would change who can reach it', () => {
        const result = describePreviewAudience([
            row('scene_13', 'changed'),
            row('scene_14', 'same'),
        ]);

        const warning = result.audienceWarning as string;
        // One page, not both: the unchanged row is reported and not counted.
        assert.match(
            warning,
            /^1 page\(s\) would be reachable by a different set/,
        );
        assert.match(warning, /Nothing has been sent/);
        assert.doesNotMatch(warning, /unknown rather than unchanged/);
    });

    it('counts an unreadable destination apart from a known change', () => {
        // compareAudience answers 'unknown' when either side could not be resolved,
        // and page-access.ts is explicit that this must never collapse into 'same'.
        // Folding it in with 'changed' would lose the opposite way: it would claim to
        // know a new audience it never read.
        const result = describePreviewAudience([
            row('scene_13', 'changed'),
            row('scene_15', 'unknown', null),
        ]);

        const warning = result.audienceWarning as string;
        assert.match(
            warning,
            /^1 page\(s\) would be reachable by a different set/,
        );
        assert.match(
            warning,
            /1 page\(s\) have a destination this server could not resolve/,
        );
        assert.match(warning, /unknown rather than unchanged/);
    });

    it('warns on an unreadable destination even with nothing else changing', () => {
        const result = describePreviewAudience([
            row('scene_15', 'unknown', null),
        ]);

        const warning = result.audienceWarning as string;
        assert.doesNotMatch(warning, /reachable by a different set/);
        assert.match(warning, /could not resolve/);
    });
});
