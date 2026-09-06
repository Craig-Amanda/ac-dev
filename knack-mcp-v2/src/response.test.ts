import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { compactKnackChanges } from './response.js';

/**
 * The tool catalogue is sent to the model on every turn and a mutation response is
 * read on every write, so both are kept small — but small in a way that keeps what a
 * caller needs. These pin the two rules that do that.
 */
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
