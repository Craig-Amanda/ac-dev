import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { z } from 'zod';

import type { AppConfig } from '../config.js';
import type { KnackApiResult } from '../http.js';
import type { AnyToolDef } from '../registry.js';
import {
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import { deleteField, updateField } from '../tools/fields.js';
import {
    aggregateRecords,
    createRecords,
    findRecords,
    getRecord,
} from '../tools/records.js';
import { getField, getObject } from '../tools/schema.js';
import type { RuntimeMetadata } from '../types.js';
import {
    REDACTED_VALUE,
    buildFieldExclusions,
    getMcpKeywords,
} from './field-exclusion.js';
import { parseRuntimeSchema } from './metadata.js';

const parseArgs = (tool: AnyToolDef, raw: Record<string, unknown>) =>
    z.object(tool.input).parse(raw);

const STAFF_FIELDS = [
    { key: 'field_1', name: 'Name', type: 'short_text' },
    {
        key: 'field_2',
        name: 'Salary',
        type: 'number',
        meta: { description: 'Pay band _mcp_writeonly' },
    },
    {
        key: 'field_3',
        name: 'NI number',
        type: 'short_text',
        meta: { description: '_mcp_hidden' },
    },
    {
        key: 'field_4',
        name: 'Grade',
        type: 'short_text',
        meta: { description: 'Set by HR _mcp_schemalock' },
    },
    {
        key: 'field_5',
        name: 'Annual salary',
        type: 'equation',
        format: { equation: '{field_2} * 12' },
    },
    {
        key: 'field_6',
        name: 'Team',
        type: 'connection',
        relationship: { object: 'object_2', has: 'one', belongs_to: 'many' },
    },
    {
        key: 'field_7',
        name: 'NI copy',
        type: 'concatenation',
        format: { equation: '{field_3}' },
    },
];

const METADATA: RuntimeMetadata = {
    objects: [
        { key: 'object_1', name: 'Staff', fields: STAFF_FIELDS },
        {
            key: 'object_2',
            name: 'Teams',
            identifier: 'field_8',
            fields: [
                {
                    key: 'field_8',
                    name: 'Team code',
                    type: 'short_text',
                    meta: { description: '_mcp_writeonly' },
                },
                { key: 'field_9', name: 'Budget', type: 'number' },
            ],
        },
        {
            key: 'object_3',
            name: 'Notes',
            fields: [{ key: 'field_10', name: 'Text', type: 'short_text' }],
        },
    ],
};

const STAFF_RECORD = {
    id: 'rec1',
    field_1: 'Ada',
    field_1_raw: 'Ada',
    field_2: '52,000',
    field_2_raw: 52000,
    field_3: 'QQ123456C',
    field_3_raw: 'QQ123456C',
    field_4: 'B',
    field_4_raw: 'B',
    field_6: '<span class="rec2">T-01</span>',
    field_6_raw: [{ id: 'rec2', identifier: 'T-01' }],
};

const ok = (body: unknown): KnackApiResult => ({ ok: true, status: 200, body });

function setup(
    responses: (apiPath: string, init?: RequestInit) => KnackApiResult,
    app: AppConfig = makeApp(),
) {
    const fake = makeFakeContext({
        apps: [app],
        runtimeMetadata: { [app.appKey]: METADATA },
        responses,
    });
    fake.ctx.state.activeAppKey = app.appKey;
    return fake;
}

describe('getMcpKeywords', () => {
    it('matches whole tokens only', () => {
        assert.deepEqual(getMcpKeywords('x _mcp_hidden y'), ['_mcp_hidden']);
        assert.deepEqual(getMcpKeywords('_mcp_hiddenish'), []);
        assert.deepEqual(getMcpKeywords(undefined), []);
    });
});

describe('parseRuntimeSchema derivedFrom', () => {
    it('reads the fields an equation or text formula uses', () => {
        const fields = parseRuntimeSchema(METADATA)!.objects![0].fields!;
        const byKey = new Map(fields.map((field) => [field.key, field]));
        assert.deepEqual(byKey.get('field_5')?.derivedFrom, ['field_2']);
        assert.deepEqual(byKey.get('field_7')?.derivedFrom, ['field_3']);
        assert.equal(byKey.get('field_1')?.derivedFrom, undefined);
    });
});

describe('buildFieldExclusions', () => {
    const schema = parseRuntimeSchema(METADATA);

    it('puts each keyword in its tier', () => {
        const ex = buildFieldExclusions(schema, undefined);
        assert.ok(ex.masked.has('field_2'));
        assert.ok(ex.hidden.has('field_3'));
        assert.ok(ex.schemaLocked.has('field_3'));
        assert.ok(ex.schemaLocked.has('field_4'));
        assert.ok(!ex.readBlocked.has('field_4'));
        assert.ok(ex.readBlocked.has('field_8'));
        assert.deepEqual(
            [...ex.maskedConnections.get('object_1')!],
            ['field_6'],
        );
        assert.ok(ex.objects.has('object_1'));
        assert.ok(!ex.objects.has('object_3'));
    });

    it('applies dataAccess.objectKeywords to every field on the object', () => {
        const ex = buildFieldExclusions(schema, {
            objectKeywords: { object_3: ['_mcp_hidden'] },
        });
        assert.ok(ex.hidden.has('field_10'));
    });

    it('keeps a config-redacted field dropped rather than masked', () => {
        const ex = buildFieldExclusions(schema, {
            redactedFieldKeys: ['field_9'],
        });
        assert.ok(ex.readBlocked.has('field_9'));
        assert.ok(!ex.masked.has('field_9'));
    });

    it('masks a formula over a write-only field', () => {
        const ex = buildFieldExclusions(schema, undefined);
        assert.ok(ex.masked.has('field_5'));
        assert.match(ex.reasons.get('field_5') || '', /field_2/);
    });

    it('hides a formula over a hidden field', () => {
        const ex = buildFieldExclusions(schema, undefined);
        assert.ok(ex.hidden.has('field_7'));
    });

    it('follows a formula that reads another formula', () => {
        const chained = parseRuntimeSchema({
            objects: [
                {
                    key: 'object_1',
                    fields: [
                        ...STAFF_FIELDS,
                        {
                            key: 'field_11',
                            type: 'equation',
                            format: { equation: '{field_5} / 2' },
                        },
                    ],
                },
            ],
        });
        const ex = buildFieldExclusions(chained, undefined);
        assert.ok(ex.masked.has('field_11'));
    });
});

describe('record tools under field exclusions', () => {
    it('knack_get_record masks write-only, drops hidden, masks linked display values', async () => {
        const { ctx } = setup(() => ok(STAFF_RECORD));
        const body = payloadOf(
            await getRecord.handler(
                parseArgs(getRecord, {
                    objectKey: 'object_1',
                    recordId: 'rec1',
                }),
                ctx,
            ),
        ).body as Record<string, unknown>;
        assert.equal(body.field_1, 'Ada');
        assert.equal(body.field_2, REDACTED_VALUE);
        assert.equal(body.field_2_raw, REDACTED_VALUE);
        assert.equal('field_3' in body, false);
        assert.equal('field_3_raw' in body, false);
        assert.equal(body.field_4, 'B');
        assert.equal(body.field_6, REDACTED_VALUE);
        assert.deepEqual(body.field_6_raw, [
            { id: 'rec2', identifier: REDACTED_VALUE },
        ]);
    });

    it('knack_find_records passes an object with no exclusions through untouched', async () => {
        const list = { records: [{ id: 'n1', field_10: 'hi' }] };
        const { ctx } = setup(() => ok(list));
        const payload = payloadOf(
            await findRecords.handler(
                parseArgs(findRecords, { objectKey: 'object_3', q: 'hi' }),
                ctx,
            ),
        );
        assert.deepEqual(payload.body, list);
    });

    it('knack_find_records refuses a filter, a sort or free text that could reveal a write-only value', async () => {
        const { ctx, requests } = setup(() => ok({ records: [] }));
        const filter = {
            match: 'and',
            rules: [{ field: 'field_2', operator: 'higher than', value: 1 }],
        };
        await assert.rejects(
            findRecords.handler(
                parseArgs(findRecords, {
                    objectKey: 'object_1',
                    filters: filter,
                }),
                ctx,
            ),
            /write-only/,
        );
        await assert.rejects(
            findRecords.handler(
                parseArgs(findRecords, {
                    objectKey: 'object_1',
                    sortField: 'field_2',
                }),
                ctx,
            ),
            /write-only/,
        );
        await assert.rejects(
            findRecords.handler(
                parseArgs(findRecords, { objectKey: 'object_1', q: 'Ada' }),
                ctx,
            ),
            /Free-text search is disabled/,
        );
        assert.equal(requests.length, 0);
    });

    it('knack_aggregate_records refuses a sum over a write-only field and masks a linked group-by', async () => {
        const { ctx } = setup(() => ok({ records: [STAFF_RECORD] }));
        await assert.rejects(
            aggregateRecords.handler(
                parseArgs(aggregateRecords, {
                    objectKey: 'object_1',
                    metrics: [{ type: 'sum', fieldKey: 'field_2' }],
                }),
                ctx,
            ),
            /write-only/,
        );
        const text = JSON.stringify(
            payloadOf(
                await aggregateRecords.handler(
                    parseArgs(aggregateRecords, {
                        objectKey: 'object_1',
                        groupByFieldKeys: ['field_6'],
                    }),
                    ctx,
                ),
            ),
        );
        assert.equal(text.includes('T-01'), false);
    });

    it('knack_create_records refuses a hidden field before any request', async () => {
        const { ctx, requests } = setup(() => ok({}));
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [{ field_1: 'Bo', field_3: 'QQ1' }],
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(JSON.stringify(payload.errors), /field_3 is hidden/);
        assert.equal(requests.length, 0);
    });

    it('knack_create_records writes a write-only field but masks it in the echo', async () => {
        const { ctx, requests } = setup(() => ok(STAFF_RECORD));
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [{ field_1: 'Ada', field_2: 52000 }],
                }),
                ctx,
            ),
        );
        assert.deepEqual(requests[0].body, { field_1: 'Ada', field_2: 52000 });
        const text = JSON.stringify(payload.results);
        assert.equal(text.includes('52000'), false);
        assert.equal(text.includes('QQ123456C'), false);
        assert.match(text, /\[redacted\]/);
    });
});

describe('schema tools under field exclusions', () => {
    it('knack_get_object leaves hidden fields out and marks the limited ones', async () => {
        const { ctx } = setup(() => ok({}));
        const payload = payloadOf(
            await getObject.handler(
                parseArgs(getObject, { objectKey: 'object_1' }),
                ctx,
            ),
        );
        const fields = payload.fields as Array<Record<string, unknown>>;
        const byKey = new Map(fields.map((field) => [field.key, field]));
        assert.equal(byKey.has('field_3'), false);
        assert.deepEqual(byKey.get('field_2')?.mcpAccess, ['writeOnly']);
        assert.deepEqual(byKey.get('field_4')?.mcpAccess, ['schemaLocked']);
        assert.equal(byKey.get('field_1')?.mcpAccess, undefined);
    });

    it('knack_get_field reports a hidden field as not found', async () => {
        const { ctx } = setup(() =>
            ok({ object: { key: 'object_1', fields: STAFF_FIELDS } }),
        );
        const payload = payloadOf(
            await getField.handler(
                parseArgs(getField, {
                    objectKey: 'object_1',
                    fieldKey: 'field_3',
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.equal(
            (payload.availableFieldKeys as string[]).includes('field_3'),
            false,
        );
    });
});

describe('field tools under field exclusions', () => {
    it('knack_update_field refuses a schema-locked field before any request', async () => {
        const { ctx, requests } = setup(() => ok({}));
        const payload = payloadOf(
            await updateField.handler(
                parseArgs(updateField, {
                    objectKey: 'object_1',
                    fieldKey: 'field_4',
                    updates: JSON.stringify({ name: 'Band' }),
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(JSON.stringify(payload.errors), /schema-locked/);
        assert.equal(requests.length, 0);
    });

    it('knack_update_field never drops an _mcp_ keyword, even with confirmRemoveKtlKeywords', async () => {
        const { ctx, requests } = setup((apiPath, init) =>
            (init?.method || 'GET') === 'GET'
                ? ok({ object: { key: 'object_1', fields: STAFF_FIELDS } })
                : ok({}),
        );
        const payload = payloadOf(
            await updateField.handler(
                parseArgs(updateField, {
                    objectKey: 'object_1',
                    fieldKey: 'field_2',
                    description: 'Pay band',
                    notedBy: 'Tester',
                    confirmRemoveKtlKeywords: true,
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(JSON.stringify(payload.errors), /_mcp_writeonly/);
        assert.equal(
            requests.some((request) => request.method === 'PUT'),
            false,
        );
    });

    it('knack_update_field refuses when the live description has just gained a lock keyword', async () => {
        const liveFields = STAFF_FIELDS.map((field) =>
            field.key === 'field_1'
                ? { ...field, meta: { description: '_mcp_schemalock' } }
                : field,
        );
        const { ctx, requests } = setup(() =>
            ok({ object: { key: 'object_1', fields: liveFields } }),
        );
        const payload = payloadOf(
            await updateField.handler(
                parseArgs(updateField, {
                    objectKey: 'object_1',
                    fieldKey: 'field_1',
                    description: 'New help text _mcp_schemalock',
                    notedBy: 'Tester',
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.equal(
            requests.some((request) => request.method === 'PUT'),
            false,
        );
    });

    it('knack_delete_field refuses a schema-locked field and deletes an ordinary one', async () => {
        const { ctx, requests } = setup(() => ok({}));
        const locked = payloadOf(
            await deleteField.handler(
                parseArgs(deleteField, {
                    objectKey: 'object_1',
                    fieldKey: 'field_4',
                }),
                ctx,
            ),
        );
        assert.equal(locked.ok, false);
        assert.equal(requests.length, 0);
        await deleteField.handler(
            parseArgs(deleteField, {
                objectKey: 'object_1',
                fieldKey: 'field_1',
            }),
            ctx,
        );
        assert.equal(requests[0]?.method, 'DELETE');
    });
});
