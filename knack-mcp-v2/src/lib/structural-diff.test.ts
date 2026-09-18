import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { computeStructuralDiff, deepEqual } from './structural-diff.js';

describe('deepEqual', () => {
    it('treats identical primitives, arrays and objects as equal', () => {
        assert.equal(deepEqual(1, 1), true);
        assert.equal(deepEqual('a', 'a'), true);
        assert.equal(deepEqual(null, null), true);
        assert.equal(deepEqual([1, { a: 2 }], [1, { a: 2 }]), true);
        assert.equal(deepEqual({ a: 1, b: 2 }, { b: 2, a: 1 }), true);
    });

    it('is false for different types, lengths, key sets or values', () => {
        assert.equal(deepEqual(1, '1'), false);
        assert.equal(deepEqual([1, 2], [1, 2, 3]), false);
        assert.equal(deepEqual({ a: 1 }, { a: 1, b: 2 }), false);
        assert.equal(deepEqual({ a: 1 }, { a: 2 }), false);
        assert.equal(deepEqual(null, {}), false);
        assert.equal(deepEqual([1, 2], { 0: 1, 1: 2 }), false);
    });
});

describe('computeStructuralDiff', () => {
    it('is empty for identical values', () => {
        const value = { a: 1, b: [1, 2, { c: 3 }] };
        assert.deepEqual(computeStructuralDiff(value, value), []);
        assert.deepEqual(
            computeStructuralDiff(value, JSON.parse(JSON.stringify(value))),
            [],
        );
    });

    it('names a single changed leaf by its exact path', () => {
        const before = { columns: [{ scene: 'a' }, { scene: 'b' }] };
        const after = { columns: [{ scene: 'a' }, { scene: 'z' }] };
        assert.deepEqual(computeStructuralDiff(before, after), [
            { path: '$.columns[1].scene', before: 'b', after: 'z' },
        ]);
    });

    it('reports a same-length array with one changed element precisely, not the whole array', () => {
        const before = [1, 2, 3];
        const after = [1, 9, 3];
        assert.deepEqual(computeStructuralDiff(before, after), [
            { path: '$[1]', before: 2, after: 9 },
        ]);
    });

    it('reports an array length change as one entry, not one per trailing index', () => {
        const before = { columns: [{ a: 1 }, { a: 2 }] };
        const after = { columns: [{ a: 1 }, { a: 2 }, { a: 3 }] };
        assert.deepEqual(computeStructuralDiff(before, after), [
            { path: '$.columns', before: 'array(2)', after: 'array(3)' },
        ]);
    });

    it('reports this GAP-Track-shaped case exactly: one column substituted, siblings untouched', () => {
        // Mirrors the real incident: an unrelated "Docs" column gained a new key
        // (link_design), lost one (rules) and changed scene/icon/active — while a
        // sibling column and the view's other top-level keys stayed identical.
        const before = {
            title: 'Cancelled Jobs',
            columns: [
                { link_text: 'Client Portal - View Job Details', scene: 'a' },
                {
                    link_text: 'Docs',
                    scene: 'view-client-documents',
                    icon: { icon: 'fa-folder-open' },
                    rules: [],
                    link_design_active: false,
                },
            ],
        };
        const after = {
            title: 'Cancelled Jobs',
            columns: [
                { link_text: 'Client Portal - View Job Details', scene: 'a' },
                {
                    link_text: 'Docs',
                    scene: 'client-portal-job-docs',
                    icon: { icon: '' },
                    link_design: { format: 'text' },
                    link_design_active: true,
                },
            ],
        };

        const diff = computeStructuralDiff(before, after);
        const paths = diff.map((entry) => entry.path).sort();
        assert.deepEqual(paths, [
            '$.columns[1].icon.icon',
            '$.columns[1].link_design',
            '$.columns[1].link_design_active',
            '$.columns[1].rules',
            '$.columns[1].scene',
        ]);
        // Nothing about the untouched sibling column or the view's title appears at all.
        assert.equal(
            diff.some((entry) => entry.path.startsWith('$.columns[0]')),
            false,
        );
        assert.equal(
            diff.some((entry) => entry.path === '$.title'),
            false,
        );
    });

    it('caps the number of entries for a large genuine rewrite rather than returning thousands', () => {
        const before = Array.from({ length: 500 }, (_, i) => ({ v: i }));
        const after = Array.from({ length: 500 }, (_, i) => ({ v: -i }));
        const diff = computeStructuralDiff(before, after);
        assert.ok(diff.length <= 200);
    });
});
