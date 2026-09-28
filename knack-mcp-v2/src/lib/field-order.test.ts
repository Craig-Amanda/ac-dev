import assert from 'node:assert/strict';
import { test } from 'node:test';

import { equationOrderWarnings, planFieldOrder } from './field-order.js';

const CURRENT = ['field_1', 'field_2', 'field_3', 'field_4', 'field_5'];

test('planFieldOrder moves a group after or before an anchor, keeping its given order', () => {
    assert.deepEqual(
        planFieldOrder(CURRENT, ['field_4', 'field_1'], {
            kind: 'after',
            anchor: 'field_5',
        }).order,
        ['field_2', 'field_3', 'field_5', 'field_4', 'field_1'],
    );
    assert.deepEqual(
        planFieldOrder(CURRENT, ['field_5'], {
            kind: 'before',
            anchor: 'field_1',
        }).order,
        ['field_5', 'field_1', 'field_2', 'field_3', 'field_4'],
    );
});

test('planFieldOrder refuses unknown, repeated and self-anchored keys', () => {
    const { errors } = planFieldOrder(
        CURRENT,
        ['field_9', 'field_2', 'field_2'],
        { kind: 'after', anchor: 'field_2' },
    );
    assert.deepEqual(errors, [
        'field_9 is not a field on this object.',
        'field_2 is named more than once.',
        'field_2 cannot be both moved and the anchor.',
    ]);
});

test('planFieldOrder takes a full order only when it names every field', () => {
    const reversed = [...CURRENT].reverse();
    assert.deepEqual(
        planFieldOrder(CURRENT, reversed, { kind: 'full' }).order,
        reversed,
    );
    assert.equal(
        planFieldOrder(CURRENT, ['field_1'], { kind: 'full' }).errors.length,
        1,
    );
});

// The GAP-Track case: the KPI target read a helper equation placed after it.
const KPI_FIELDS = [
    { key: 'field_790', type: 'date_time' },
    {
        key: 'field_2625',
        type: 'equation',
        format: { equation: '{field_790} * 2' },
    },
    {
        key: 'field_2633',
        type: 'equation',
        format: { equation: '{field_790} + {field_2625}' },
    },
];

test('equationOrderWarnings flags an equation placed before a computed field it reads', () => {
    const warnings = equationOrderWarnings(KPI_FIELDS, [
        'field_790',
        'field_2633',
        'field_2625',
    ]);
    assert.equal(warnings.length, 1);
    assert.match(warnings[0], /field_2633/);
    assert.match(warnings[0], /field_2625/);
});

test('equationOrderWarnings is silent when every computed input comes first', () => {
    assert.deepEqual(
        equationOrderWarnings(KPI_FIELDS, [
            'field_790',
            'field_2625',
            'field_2633',
        ]),
        [],
    );
});

test('equationOrderWarnings ignores connected-record fields and unbraced text', () => {
    // field_2555 connects object_44 to itself, so field_2633 is a key on this table,
    // but `{field_2555.field_2633}` reads it from the linked job, not this save.
    const fields = [
        { key: 'field_2555', type: 'connection' },
        {
            key: 'field_2640',
            type: 'concatenation',
            format: {
                equation:
                    '<span class="field_2633-note">{field_2555.field_2633}</span>',
            },
        },
        ...KPI_FIELDS,
    ];
    assert.deepEqual(
        equationOrderWarnings(fields, [
            'field_2555',
            'field_2640',
            'field_790',
            'field_2625',
            'field_2633',
        ]),
        [],
    );
});
