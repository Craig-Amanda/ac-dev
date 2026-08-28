import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
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
        assert.deepEqual(targets.childSceneKeys, ['scene_44']);
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

        assert.deepEqual(targets.childSceneKeys, ['scene_77']);
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
        assert.deepEqual(targets.childSceneKeys, ['scene_1', 'scene_2']);
    });

    it('dedupes scenes reached by more than one link', () => {
        const targets = collectLinkTargets({
            type: 'table',
            columns: [
                { type: 'link', scene: 'scene_9' },
                { type: 'link', scene: 'scene_9' },
            ],
        });

        assert.deepEqual(targets.childSceneKeys, ['scene_9']);
    });

    it('returns empty results for a view with no links', () => {
        const targets = collectLinkTargets({
            type: 'rich_text',
            content: 'hi',
        });
        assert.deepEqual(targets.childSceneKeys, []);
        assert.equal(targets.linkColumns.length, 0);
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
        { sceneKey: 'scene_10', sceneName: 'Edit', parentSceneKey: 'scene_1' },
        {
            sceneKey: 'scene_11',
            sceneName: 'Edit detail',
            parentSceneKey: 'scene_10',
        },
        {
            sceneKey: 'scene_12',
            sceneName: 'Edit history',
            parentSceneKey: 'scene_11',
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
            ...(i > 0 ? { parentSceneKey: `scene_${i - 1}` } : {}),
        }));
        assert.equal(expandChildPages(['scene_0'], deepChain).truncated, true);
    });

    it('does not report truncation for an ordinary tree', () => {
        assert.equal(expandChildPages(['scene_10'], scenes).truncated, false);
    });

    it('survives a parent cycle without hanging', () => {
        const cyclic: SceneNode[] = [
            { sceneKey: 'scene_a', parentSceneKey: 'scene_b' },
            { sceneKey: 'scene_b', parentSceneKey: 'scene_a' },
        ];
        const pages = expandChildPages(['scene_a'], cyclic).pages;
        assert.deepEqual(
            pages.map((page) => page.sceneKey),
            ['scene_a', 'scene_b'],
        );
    });

    it('still reports a page that is missing from the scene list', () => {
        const pages = expandChildPages(['scene_999'], scenes).pages;
        assert.deepEqual(pages, [
            {
                sceneKey: 'scene_999',
                sceneName: null,
                sceneSlug: null,
                depth: 0,
            },
        ]);
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
