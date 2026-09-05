import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    compactKnackChanges,
    compactToolDescription,
} from './response-shaping.js';

/**
 * The tool catalogue is sent to the model on every turn and a mutation response is
 * read on every write, so both are kept small — but small in a way that keeps what a
 * caller needs. These pin the two rules that do that.
 */
describe('compactToolDescription', () => {
    it('uses the curated summary where one exists', () => {
        const summary = compactToolDescription(
            'knack_update_view',
            'Update an existing Knack view. A very long paragraph follows that the model should not have to read every turn.',
        );
        assert.match(summary, /^Update a view\./);
        assert.match(summary, /goes to the human for confirmation/);
        assert.ok(summary.length <= 160);
    });

    it('keeps a short description whole', () => {
        assert.equal(
            compactToolDescription('knack_x', '  Do one thing.  Well. '),
            'Do one thing. Well.',
        );
    });

    it('falls back to the first sentence, never to a phrase made from the name', () => {
        const summary = compactToolDescription(
            'knack_list_things',
            'List all things in the app with their key and name. ' +
                'Then a second sentence long enough to push the whole description past the cap, so that only the first sentence should survive here.',
        );
        assert.equal(
            summary,
            'List all things in the app with their key and name.',
        );
        assert.doesNotMatch(summary, /^Knack list things/);
    });

    it('cuts a long first sentence rather than dropping it', () => {
        const long = 'A'.repeat(200) + '.';
        const summary = compactToolDescription('knack_y', long);
        assert.equal(summary.length, 160);
        assert.ok(summary.endsWith('…'));
    });
});

describe('compactKnackChanges', () => {
    const view = {
        key: 'view_51',
        type: 'table',
        columns: [{ type: 'field' }],
    };
    const body = {
        view,
        changes: {
            deletes: {
                scenes: [{ key: 'scene_3' }],
                views: [],
                objects: [],
                fields: [],
            },
            inserts: {
                scenes: [
                    {
                        key: 'scene_71',
                        name: 'Detail',
                        slug: 'detail',
                        parent: 'home',
                        views: [],
                        groups: [],
                    },
                ],
                views: [{ scene: { key: 'scene_69' }, view }],
                objects: [],
                fields: [],
            },
            updates: {
                scenes: [{ key: 'scene_69' }],
                views: [],
                objects: [],
                fields: [],
            },
        },
    };

    it('reduces the echoed view and empty arrays to keys and page identities', () => {
        const result = compactKnackChanges(body);
        assert.deepEqual(result, {
            body: {
                view,
                changes: {
                    deletes: { scenes: ['scene_3'] },
                    inserts: {
                        scenes: [
                            {
                                key: 'scene_71',
                                name: 'Detail',
                                slug: 'detail',
                                parent: 'home',
                            },
                        ],
                        views: ['view_51'],
                    },
                    updates: { scenes: ['scene_69'] },
                },
            },
        });
    });

    it('leaves a body with no changes block alone', () => {
        assert.deepEqual(compactKnackChanges({ view }), {});
        assert.deepEqual(compactKnackChanges('not json'), {});
        assert.deepEqual(compactKnackChanges(null), {});
    });

    it('drops a heading that reduces to nothing', () => {
        const result = compactKnackChanges({
            changes: { deletes: { scenes: [], views: [] }, inserts: {} },
        });
        assert.deepEqual(result, { body: { changes: {} } });
    });
});
