import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    buildReferrerIndex,
    classifyLinkTargets,
    collectLinkTargets,
    expandChildPages,
    collectPayloadKeys,
    getViewType,
    isMenuView,
    payloadTouchesStructure,
    payloadTouchesLinks,
    resolveViewAttributes,
    sanitiseFileNameComponent,
    type SceneNode,
} from './view-safety.js';

describe('resolveViewAttributes', () => {
    it('reads a bare view object', () => {
        assert.equal(
            getViewType(resolveViewAttributes({ type: 'menu' })),
            'menu',
        );
    });

    it('unwraps the {view: {...}} response shape', () => {
        const attributes = resolveViewAttributes({ view: { type: 'table' } });
        assert.equal(getViewType(attributes), 'table');
    });

    it('unwraps the {view: {attributes: {...}}} runtime-metadata shape', () => {
        const attributes = resolveViewAttributes({
            view: { key: 'view_1', attributes: { type: 'menu' } },
        });
        assert.equal(getViewType(attributes), 'menu');
    });

    it('lowercases the type so casing cannot slip a menu past the check', () => {
        assert.equal(isMenuView(resolveViewAttributes({ type: 'Menu' })), true);
    });

    it('returns null rather than throwing on a non-object', () => {
        assert.equal(resolveViewAttributes(null), null);
        assert.equal(getViewType(null), null);
        assert.equal(isMenuView(null), false);
    });
});

describe('collectLinkTargets', () => {
    it('finds a link column at the top level', () => {
        const targets = collectLinkTargets({
            type: 'table',
            columns: [
                { type: 'field', field: { key: 'field_1' } },
                {
                    type: 'link',
                    header: 'Edit',
                    field: { key: 'field_2' },
                    scene: 'scene_44',
                },
            ],
        });

        assert.equal(targets.linkColumns.length, 1);
        assert.equal(targets.linkColumns[0].header, 'Edit');
        assert.deepEqual(targets.childSceneRefs, ['scene_44']);
    });

    it('finds a link column nested inside groups[].columns[]', () => {
        // Regression: a flat read of `columns` misses this, so the update passes the
        // check and still cascade-deletes scene_77.
        const targets = collectLinkTargets({
            type: 'details',
            groups: [
                {
                    columns: [
                        {
                            columns: [
                                {
                                    type: 'link',
                                    header: 'Open record',
                                    scene: 'scene_77',
                                },
                            ],
                        },
                    ],
                },
            ],
        });

        assert.deepEqual(targets.childSceneRefs, ['scene_77']);
        assert.match(targets.linkColumns[0].sourcePath, /groups\[0\]/);
    });

    it('finds menu links and their scenes', () => {
        const targets = collectLinkTargets({
            type: 'menu',
            links: [
                { name: 'Home', type: 'scene', scene: 'scene_1' },
                { name: 'Reports', type: 'scene', scene: { key: 'scene_2' } },
                { name: 'Docs', type: 'url', url: 'https://example.com' },
            ],
        });

        assert.equal(targets.menuLinks.length, 3);
        assert.deepEqual(targets.childSceneRefs, ['scene_1', 'scene_2']);
    });

    it('dedupes scenes reached by more than one link', () => {
        const targets = collectLinkTargets({
            type: 'table',
            columns: [
                { type: 'link', scene: 'scene_9' },
                { type: 'link', scene: 'scene_9' },
            ],
        });

        assert.deepEqual(targets.childSceneRefs, ['scene_9']);
    });

    it('returns empty results for a view with no links', () => {
        const targets = collectLinkTargets({
            type: 'rich_text',
            content: 'hi',
        });
        assert.deepEqual(targets.childSceneRefs, []);
        assert.equal(targets.linkColumns.length, 0);
    });
});

/**
 * Shapes reported from a live 963-scene production app during review of this branch.
 *
 * Every fixture above this block was hand-written, and hand-written fixtures are why
 * the guard shipped believing it saw details views: they all used `type: "link"`,
 * because that is what the code looked for. Real Knack writes `scene_link` on details
 * and calendar columns, puts slugs where the code expected `scene_N`, and reuses
 * `type: "link"` for form URL inputs that point at no page at all.
 */
describe('real Knack shapes', () => {
    it('finds a details view link column, which Knack types `scene_link`', () => {
        // The fail-open that mattered most: collectLinkTargets matched only
        // `type === "link"`, so on a real details view it returned nothing at all and
        // a layout replacement went to the PUT with no confirmation.
        const targets = collectLinkTargets({
            type: 'details',
            columns: [
                {
                    groups: [
                        {
                            columns: [
                                [
                                    {
                                        type: 'scene_link',
                                        scene: 'roll-details3',
                                        name: 'Roll Details',
                                    },
                                ],
                            ],
                        },
                    ],
                },
            ],
        });

        assert.deepEqual(targets.childSceneRefs, ['roll-details3']);
        assert.equal(targets.linkColumns[0].linkType, 'scene_link');
    });

    it('finds a calendar view link column under details.columns[]', () => {
        const targets = collectLinkTargets({
            type: 'calendar',
            details: {
                columns: [
                    {
                        groups: [
                            {
                                columns: [
                                    [
                                        {
                                            type: 'scene_link',
                                            scene: 'event-details',
                                        },
                                    ],
                                ],
                            },
                        ],
                    },
                ],
            },
        });

        assert.deepEqual(targets.childSceneRefs, ['event-details']);
    });

    it('finds a search view link column under results.columns[]', () => {
        const targets = collectLinkTargets({
            type: 'search',
            results: { columns: [{ type: 'link', scene: 'member-details' }] },
        });

        assert.deepEqual(targets.childSceneRefs, ['member-details']);
    });

    it('ignores a form URL-field input, which is also typed `link`', () => {
        // False positive, not a fail-open — but it made every structural edit to a
        // form carrying a Link/URL field refuse outright on a client that cannot
        // prompt, and told anyone who could that "0 pages" would be destroyed.
        const targets = collectLinkTargets({
            type: 'form',
            groups: [
                {
                    columns: [
                        {
                            inputs: [
                                {
                                    type: 'link',
                                    field: { key: 'field_812' },
                                    label: 'Website',
                                },
                            ],
                        },
                    ],
                },
            ],
        });

        assert.deepEqual(targets.childSceneRefs, []);
        assert.deepEqual(targets.linkColumns, []);
    });

    it('treats an unreadable scene as a link, an absent one as not a link', () => {
        // The two must not collapse together: an unreadable target is a page we
        // cannot name, an absent one is no page at all.
        const unreadable = collectLinkTargets({
            type: 'details',
            columns: [{ type: 'scene_link', scene: { id: 7 } }],
        });
        assert.equal(unreadable.linkColumns.length, 1);
        assert.equal(unreadable.linkColumns[0].childSceneRef, null);

        const absent = collectLinkTargets({
            type: 'table',
            columns: [{ type: 'link', field: { key: 'field_1' } }],
        });
        assert.deepEqual(absent.linkColumns, []);

        const empty = collectLinkTargets({
            type: 'table',
            columns: [{ type: 'link', scene: '' }],
        });
        assert.deepEqual(empty.linkColumns, []);
    });

    it('walks the whole descendant chain when refs and parents are slugs', () => {
        // Knack writes slugs in a link's `scene` and in `scene.parent`, while `key` is
        // the scene_N identifier. Keying the walk on sceneKey meant the seed matched
        // nothing and the parent map matched nothing: a real run reported 3 doomed
        // pages where 5 died. The human confirms, and loses more than they agreed to.
        const slugScenes: SceneNode[] = [
            { sceneKey: 'scene_11', sceneSlug: 'view-rolls-login' },
            {
                sceneKey: 'scene_500',
                sceneName: 'DF Details',
                sceneSlug: 'view-df-details',
                parentRef: 'view-rolls-login',
            },
            {
                sceneKey: 'scene_522',
                sceneName: 'Edit DF',
                sceneSlug: 'edit-df',
                parentRef: 'view-df-details',
            },
            {
                sceneKey: 'scene_405',
                sceneName: 'DF History',
                sceneSlug: 'df-history',
                parentRef: 'edit-df',
            },
            {
                sceneKey: 'scene_1401',
                sceneName: 'Print DF',
                sceneSlug: 'print-df',
                parentRef: 'view-df-details',
            },
            {
                sceneKey: 'scene_1400',
                sceneName: 'Print Options',
                sceneSlug: 'print-options',
                parentRef: 'print-df',
            },
        ];

        const result = expandChildPages(['view-df-details'], slugScenes);

        assert.deepEqual(
            result.pages.map((page) => page.sceneKey),
            ['scene_500', 'scene_522', 'scene_1401', 'scene_405', 'scene_1400'],
        );
        // The directly-linked page resolves to a real name rather than null.
        assert.equal(result.pages[0].sceneName, 'DF Details');
        assert.deepEqual(result.unresolvedRefs, []);
        assert.equal(result.truncated, false);
    });

    it('resolves a reference given as a key even when parents are slugs', () => {
        const mixed: SceneNode[] = [
            { sceneKey: 'scene_500', sceneSlug: 'view-df-details' },
            {
                sceneKey: 'scene_522',
                sceneSlug: 'edit-df',
                parentRef: 'view-df-details',
            },
        ];

        const result = expandChildPages(['scene_500'], mixed);
        assert.deepEqual(
            result.pages.map((page) => page.sceneKey),
            ['scene_500', 'scene_522'],
        );
    });
});

describe('payloadTouchesLinks', () => {
    it('detects a top-level links array', () => {
        assert.equal(payloadTouchesLinks({ links: [] }), true);
    });

    it('detects links nested under attributes', () => {
        assert.equal(
            payloadTouchesLinks({ attributes: { links: [{ name: 'Home' }] } }),
            true,
        );
    });

    it('detects links buried several levels deep', () => {
        assert.equal(
            payloadTouchesLinks({ a: { b: [{ c: { links: [] } }] } }),
            true,
        );
    });

    it('ignores a links property that is not an array', () => {
        assert.equal(payloadTouchesLinks({ links: 'none' }), false);
    });

    it('returns false for an ordinary payload', () => {
        assert.equal(payloadTouchesLinks({ name: 'Contacts' }), false);
    });
});

describe('payloadTouchesStructure', () => {
    it('detects a columns replacement', () => {
        assert.equal(payloadTouchesStructure({ columns: [] }), true);
    });

    it('detects columns nested in groups', () => {
        assert.equal(
            payloadTouchesStructure({ groups: [{ columns: [] }] }),
            true,
        );
    });

    // Regression: the previous check asked "does this replace a `columns` array?", and
    // a details view's layout lives at groups[].columns[]. Clearing `groups` wipes the
    // link columns inside it without a `columns` array appearing anywhere in the
    // payload, so the cascade check never ran.
    it('detects a wholesale groups replacement carrying no columns key', () => {
        assert.equal(payloadTouchesStructure({ groups: [] }), true);
    });

    it('detects a groups write whose entries have no columns', () => {
        assert.equal(
            payloadTouchesStructure({ groups: [{ label: 'x' }] }),
            true,
        );
    });

    it('detects columns sent as something other than an array', () => {
        assert.equal(
            payloadTouchesStructure({ columns: { '0': { type: 'link' } } }),
            true,
        );
    });

    it('treats an unfamiliar layout key as structural', () => {
        assert.equal(payloadTouchesStructure({ rows: [] }), true);
    });

    it('returns false for a scalar-only edit', () => {
        assert.equal(payloadTouchesStructure({ title: 'New title' }), false);
    });

    it('returns false for an empty payload', () => {
        assert.equal(payloadTouchesStructure({}), false);
    });

    it('returns true when a scalar edit is mixed with a structural one', () => {
        assert.equal(
            payloadTouchesStructure({ title: 'New title', groups: [] }),
            true,
        );
    });
});

describe('expandChildPages', () => {
    const scenes: SceneNode[] = [
        { sceneKey: 'scene_1', sceneName: 'Home' },
        { sceneKey: 'scene_10', sceneName: 'Edit', parentRef: 'scene_1' },
        {
            sceneKey: 'scene_11',
            sceneName: 'Edit detail',
            parentRef: 'scene_10',
        },
        {
            sceneKey: 'scene_12',
            sceneName: 'Edit history',
            parentRef: 'scene_11',
        },
        { sceneKey: 'scene_20', sceneName: 'Unrelated' },
    ];

    it('includes descendants, not just the directly linked page', () => {
        const pages = expandChildPages(['scene_10'], scenes).pages;
        assert.deepEqual(
            pages.map((page) => page.sceneKey),
            ['scene_10', 'scene_11', 'scene_12'],
        );
    });

    it('tags each page with its distance from the link', () => {
        const pages = expandChildPages(['scene_10'], scenes).pages;
        assert.deepEqual(
            pages.map((page) => page.depth),
            [0, 1, 2],
        );
    });

    it('carries names through for the refusal message', () => {
        const [first] = expandChildPages(['scene_10'], scenes).pages;
        assert.equal(first.sceneName, 'Edit');
    });

    it('leaves unrelated pages out', () => {
        const pages = expandChildPages(['scene_10'], scenes).pages;
        assert.equal(
            pages.some((page) => page.sceneKey === 'scene_20'),
            false,
        );
    });

    it('reports truncation when the page tree runs deeper than the walk', () => {
        // A chain longer than MAX_WALK_DEPTH: the walk stops, and saying so lets the
        // guard refuse rather than confirm a partial list of doomed pages.
        const deepChain: SceneNode[] = Array.from({ length: 40 }, (_, i) => ({
            sceneKey: `scene_${i}`,
            ...(i > 0 ? { parentRef: `scene_${i - 1}` } : {}),
        }));
        assert.equal(expandChildPages(['scene_0'], deepChain).truncated, true);
    });

    it('does not report truncation for an ordinary tree', () => {
        assert.equal(expandChildPages(['scene_10'], scenes).truncated, false);
    });

    it('survives a parent cycle without hanging', () => {
        const cyclic: SceneNode[] = [
            { sceneKey: 'scene_a', parentRef: 'scene_b' },
            { sceneKey: 'scene_b', parentRef: 'scene_a' },
        ];
        const pages = expandChildPages(['scene_a'], cyclic).pages;
        assert.deepEqual(
            pages.map((page) => page.sceneKey),
            ['scene_a', 'scene_b'],
        );
    });

    it('reports a reference matching no scene as unresolved, not as a page', () => {
        // It used to be emitted as a ChildPage with a null name and slug, which read
        // in the prompt as a real page nobody could identify — and, worse, counted as
        // fully enumerated. A reference naming nothing we hold is missing information,
        // so it belongs in unresolvedRefs where the guard treats it as risk.
        const result = expandChildPages(['scene_999'], scenes);
        assert.deepEqual(result.pages, []);
        assert.deepEqual(result.unresolvedRefs, ['scene_999']);
    });
});

describe('collectPayloadKeys', () => {
    it('lists top-level keys', () => {
        assert.deepEqual(collectPayloadKeys({ title: 'a', name: 'b' }), [
            'name',
            'title',
        ]);
    });

    it('sees through the attributes wrapper', () => {
        assert.ok(
            collectPayloadKeys({
                attributes: { columns: [], title: 'a' },
            }).includes('columns'),
        );
    });

    it('finds a key nested inside an array of objects', () => {
        // A shallow scan reported only "groups" here, which is how a structural
        // payload once read as a flat one to payloadTouchesStructure.
        assert.ok(
            collectPayloadKeys({ groups: [{ columns: [] }] }).includes(
                'columns',
            ),
        );
    });

    it('finds a key buried several levels down', () => {
        assert.ok(
            collectPayloadKeys({
                a: { b: [{ c: { source: {} } }] },
            }).includes('source'),
        );
    });

    it('dedupes a key that appears in several places', () => {
        const keys = collectPayloadKeys({
            groups: [{ columns: [] }, { columns: [] }],
        });
        assert.equal(keys.filter((key) => key === 'columns').length, 1);
    });

    it('returns nothing for a non-object payload', () => {
        assert.deepEqual(collectPayloadKeys('nope'), []);
    });
});

describe('sanitiseFileNameComponent', () => {
    it('strips path traversal out of a caller-supplied key', () => {
        const cleaned = sanitiseFileNameComponent('../../etc/passwd');
        assert.equal(/[./\\]/.test(cleaned), false);
    });

    it('keeps ordinary Knack keys unchanged', () => {
        assert.equal(sanitiseFileNameComponent('view_230'), 'view_230');
        assert.equal(sanitiseFileNameComponent('scene_84'), 'scene_84');
    });

    it('never returns an empty or separator-only name', () => {
        assert.equal(sanitiseFileNameComponent('../..'), 'unnamed');
        assert.equal(sanitiseFileNameComponent(''), 'unnamed');
    });

    it('caps the length so one key cannot dominate the filename', () => {
        assert.ok(sanitiseFileNameComponent('v'.repeat(500)).length <= 64);
    });
});

/** A scene list with no link graph — what every deps implementation supplied before
 *  the referrer rule existed, and what an unreadable metadata payload still gives. */
function withoutLinkGraph(scenes: SceneNode[]): SceneNode[] {
    return scenes.map((scene) => {
        const copy = { ...scene };
        delete copy.views;
        return copy;
    });
}

describe('buildReferrerIndex', () => {
    /**
     * Two views on two different pages both link to scene_95, which hangs off the page
     * holding view_232 — the shape the two-arm test on the live app ran on.
     */
    const SHARED_CHILD: SceneNode[] = [
        {
            sceneKey: 'scene_90',
            sceneSlug: 'dashboard',
            views: [
                { viewKey: 'view_232', childSceneRefs: ['detail', 'summary'] },
            ],
        },
        {
            sceneKey: 'scene_91',
            sceneSlug: 'reports',
            views: [{ viewKey: 'view_233', childSceneRefs: ['detail'] }],
        },
        {
            sceneKey: 'scene_95',
            sceneName: 'Detail',
            sceneSlug: 'detail',
            parentRef: 'dashboard',
            views: [],
        },
        {
            sceneKey: 'scene_96',
            sceneName: 'Summary',
            sceneSlug: 'summary',
            parentRef: 'dashboard',
            views: [],
        },
    ];

    it('returns null when no scene carries a view list', () => {
        // "Not measured" and "nothing links anywhere" have to stay distinguishable.
        // Collapsed into an empty index, every page in the app reads as having no
        // other referrer, and the rule would spare or doom on invented evidence.
        assert.equal(
            buildReferrerIndex([
                { sceneKey: 'scene_1' },
                { sceneKey: 'scene_2', parentRef: 'scene_1' },
            ]),
            null,
        );
    });

    it('counts referrers across the whole app, resolving slugs to keys', () => {
        const index = buildReferrerIndex(SHARED_CHILD);
        assert.ok(index);
        assert.deepEqual(index.get('scene_95'), [
            { sceneKey: 'scene_90', viewKey: 'view_232' },
            { sceneKey: 'scene_91', viewKey: 'view_233' },
        ]);
        assert.deepEqual(index.get('scene_96'), [
            { sceneKey: 'scene_90', viewKey: 'view_232' },
        ]);
    });

    it('counts one view linking twice to a page as one referrer', () => {
        // Counted twice, a sole referrer looks like two and the page it is about to
        // take with it would be reported as surviving.
        const index = buildReferrerIndex([
            {
                sceneKey: 'scene_90',
                sceneSlug: 'dashboard',
                views: [
                    {
                        viewKey: 'view_232',
                        childSceneRefs: ['detail', 'scene_95'],
                    },
                ],
            },
            {
                sceneKey: 'scene_95',
                sceneSlug: 'detail',
                parentRef: 'dashboard',
                views: [],
            },
        ]);
        assert.deepEqual(index?.get('scene_95'), [
            { sceneKey: 'scene_90', viewKey: 'view_232' },
        ]);
    });

    it('ignores a reference matching no page rather than inventing one', () => {
        const index = buildReferrerIndex([
            {
                sceneKey: 'scene_90',
                views: [{ viewKey: 'view_1', childSceneRefs: ['ghost-page'] }],
            },
        ]);
        assert.equal(index?.size, 0);
    });
});

describe('classifyLinkTargets and the last-referrer rule', () => {
    const OWNER = 'scene_90';

    const scenes = (extraReferrer: boolean): SceneNode[] => [
        {
            sceneKey: 'scene_90',
            sceneSlug: 'dashboard',
            views: [{ viewKey: 'view_232', childSceneRefs: ['detail'] }],
        },
        {
            sceneKey: 'scene_91',
            sceneSlug: 'reports',
            views: extraReferrer
                ? [{ viewKey: 'view_233', childSceneRefs: ['detail'] }]
                : [],
        },
        {
            sceneKey: 'scene_95',
            sceneName: 'Detail',
            sceneSlug: 'detail',
            parentRef: 'dashboard',
            views: [],
        },
    ];

    it('calls a page transferred when another view still links to it', () => {
        // Measured on the live app: removing the link column moved the child page
        // under the other referring view. It did not lose its content, its connection
        // or its place in the app — it changed parent, which is what the builder does.
        const [target] = classifyLinkTargets(
            ['detail'],
            scenes(true),
            OWNER,
            'view_232',
        );
        assert.equal(target.classification, 'transferred');
        assert.deepEqual(target.otherReferrers, [
            { sceneKey: 'scene_91', viewKey: 'view_233' },
        ]);
        assert.match(target.reason, /view_233/);
    });

    it('leaves a page owned when this view holds its only link', () => {
        const [target] = classifyLinkTargets(
            ['detail'],
            scenes(false),
            OWNER,
            'view_232',
        );
        assert.equal(target.classification, 'owned');
        assert.deepEqual(target.otherReferrers, []);
    });

    it('leaves a page owned when the scene list carries no link graph at all', () => {
        // Unmeasured must behave exactly as it did before the rule existed. A deps
        // implementation that cannot supply the graph gets the pessimistic answer.
        const [target] = classifyLinkTargets(
            ['detail'],
            withoutLinkGraph(scenes(true)),
            OWNER,
            'view_232',
        );
        assert.equal(target.classification, 'owned');
    });

    it('leaves a page owned when the caller names no mutating view', () => {
        // Without knowing which link is being cut, every referrer looks like somebody
        // else's, and the page would be spared on the strength of its own doomed link.
        const [target] = classifyLinkTargets(['detail'], scenes(true), OWNER);
        assert.equal(target.classification, 'owned');
    });

    it("leaves a page owned when the graph does not list this view's own link", () => {
        // The index must contain the link being cut before it can be believed complete
        // for this page. A graph built from partial metadata otherwise reads as "no
        // other referrers" — indistinguishable from a genuine sole referrer.
        const partial = scenes(true).map((scene) =>
            scene.sceneKey === 'scene_90' ? { ...scene, views: [] } : scene,
        );
        const [target] = classifyLinkTargets(
            ['detail'],
            partial,
            OWNER,
            'view_232',
        );
        assert.equal(target.classification, 'owned');
    });

    it('does not spare a parentless page on referrer count alone', () => {
        // A page with no parent is unknown because the metadata may have lost the
        // pointer — and a link graph read from that same metadata is no sounder than
        // the pointer it lost. Absence of evidence stays absence of evidence.
        const parentless = scenes(true).map((scene) =>
            scene.sceneKey === 'scene_95'
                ? { ...scene, parentRef: undefined }
                : scene,
        );
        const [target] = classifyLinkTargets(
            ['detail'],
            parentless,
            OWNER,
            'view_232',
        );
        assert.equal(target.classification, 'unknown');
    });

    it('keeps a page owned elsewhere external, referrers or not', () => {
        const elsewhere = scenes(true).map((scene) =>
            scene.sceneKey === 'scene_95'
                ? { ...scene, parentRef: 'reports' }
                : scene,
        );
        const [target] = classifyLinkTargets(
            ['detail'],
            elsewhere,
            OWNER,
            'view_232',
        );
        assert.equal(target.classification, 'external');
    });
});
