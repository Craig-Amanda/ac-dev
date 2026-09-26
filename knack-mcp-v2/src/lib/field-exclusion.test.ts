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
import {
    createField,
    deleteField,
    duplicateField,
    updateField,
} from '../tools/fields.js';
import { deleteObject, updateObject } from '../tools/objects.js';
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
    collectFieldKeyRefs,
    deprecatedKeywordWarnings,
    expandMcpKeywords,
    getMcpKeywords,
    looseningKeywords,
    readFieldKeyRefs,
    ruleFieldRefusal,
    writeFieldKeyRefs,
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

    it('matches in any case, so a mistyped keyword still protects the field', () => {
        assert.deepEqual(getMcpKeywords('_MCP_Hidden'), ['_mcp_hidden']);
        assert.deepEqual(getMcpKeywords('Pay _Mcp_Writeonly'), [
            '_mcp_writeonly',
        ]);
        assert.deepEqual(getMcpKeywords('_MCP_HIDDENISH'), []);
    });
});

describe('expandMcpKeywords', () => {
    const limits = (...keywords: string[]) =>
        [...expandMcpKeywords(keywords)].sort();

    it('keeps each new keyword as itself', () => {
        assert.deepEqual(limits('_mcp_nodata'), ['_mcp_nodata']);
        assert.deepEqual(limits('_mcp_allowwrite'), ['_mcp_allowwrite']);
        assert.deepEqual(limits('_mcp_schemalock'), ['_mcp_schemalock']);
        assert.deepEqual(limits('_mcp_tablelock'), ['_mcp_tablelock']);
    });

    it('expands the two old names, so existing descriptions keep their protection', () => {
        assert.deepEqual(limits('_mcp_writeonly'), [
            '_mcp_allowwrite',
            '_mcp_nodata',
        ]);
        assert.deepEqual(limits('_mcp_hidden'), [
            '_mcp_nodata',
            '_mcp_schemalock',
        ]);
    });

    it('combines keywords and ignores anything else', () => {
        assert.deepEqual(limits('_mcp_nodata', '_mcp_schemalock', '_sth'), [
            '_mcp_nodata',
            '_mcp_schemalock',
        ]);
        assert.deepEqual(limits(), []);
    });
});

describe('deprecatedKeywordWarnings', () => {
    it('names the replacement for each old keyword, once, in any case', () => {
        const warnings = deprecatedKeywordWarnings([
            '_MCP_Hidden',
            '_mcp_hidden',
            '_mcp_writeonly',
            '_mcp_nodata',
        ]);
        assert.equal(warnings.length, 2);
        assert.match(
            warnings[0],
            /_mcp_hidden is deprecated.*_mcp_nodata _mcp_schemalock/,
        );
        assert.match(
            warnings[1],
            /_mcp_writeonly is deprecated.*_mcp_nodata _mcp_allowwrite/,
        );
        assert.deepEqual(deprecatedKeywordWarnings(['_mcp_nodata']), []);
    });
});

describe('looseningKeywords', () => {
    it('names a keyword that would let the model write a no-data field', () => {
        assert.deepEqual(
            looseningKeywords(
                ['_mcp_nodata'],
                ['_mcp_nodata', '_mcp_allowwrite'],
            ),
            ['_mcp_allowwrite'],
        );
        assert.deepEqual(
            looseningKeywords(
                ['_mcp_hidden'],
                ['_mcp_hidden', '_mcp_writeonly'],
            ),
            ['_mcp_writeonly'],
        );
    });

    it('allows tightening, and adding a keyword to a field that already allows writes', () => {
        assert.deepEqual(looseningKeywords([], ['_mcp_nodata']), []);
        assert.deepEqual(
            looseningKeywords(
                ['_mcp_writeonly'],
                ['_mcp_writeonly', '_mcp_schemalock'],
            ),
            [],
        );
        assert.deepEqual(looseningKeywords([], ['_mcp_allowwrite']), []);
    });
});

describe('conditional rule copies', () => {
    const copying = (source: string) =>
        parseRuntimeSchema({
            objects: [
                {
                    key: 'object_1',
                    fields: [
                        ...STAFF_FIELDS,
                        {
                            key: 'field_12',
                            name: 'Copy',
                            type: 'short_text',
                            conditional: true,
                            rules: [
                                {
                                    key: '1',
                                    criteria: [],
                                    values: [
                                        {
                                            type: 'record',
                                            field: 'field_12',
                                            input: source,
                                        },
                                        {
                                            type: 'value',
                                            field: 'field_12',
                                            value: 'field_1 literal',
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        });
    const copy = (source: string) =>
        copying(source)!.objects![0].fields!.find(
            (field) => field.key === 'field_12',
        )!;

    it('records only what a "record" value copies in', () => {
        assert.deepEqual(copy('field_2').copiedFrom, ['field_2']);
        assert.deepEqual(copy('field_6.field_8').copiedFrom, [
            'field_6',
            'field_8',
        ]);
        assert.equal(
            parseRuntimeSchema(METADATA)!.objects![0].fields![0].copiedFrom,
            undefined,
        );
    });

    it('gives the copy the read tier of its source', () => {
        const masked = buildFieldExclusions(copying('field_2'), undefined);
        assert.ok(masked.masked.has('field_12'));
        assert.match(
            masked.reasons.get('field_12') || '',
            /conditional rule copying field_2/,
        );
        // A copy of a no-data field reads as no-data, but its own writes are not blocked.
        const fromOldHidden = buildFieldExclusions(
            copying('field_3'),
            undefined,
        );
        assert.ok(fromOldHidden.masked.has('field_12'));
        assert.ok(!fromOldHidden.writeBlocked.has('field_12'));
        const plain = buildFieldExclusions(copying('field_1'), undefined);
        assert.ok(!plain.readBlocked.has('field_12'));
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
        // _mcp_writeonly = no data + allow write
        assert.ok(ex.masked.has('field_2'));
        assert.ok(!ex.writeBlocked.has('field_2'));
        assert.ok(!ex.schemaLocked.has('field_2'));
        // _mcp_hidden = no data + schema lock, and visible (masked, not dropped)
        assert.ok(ex.masked.has('field_3'));
        assert.ok(ex.writeBlocked.has('field_3'));
        assert.ok(ex.schemaLocked.has('field_3'));
        // _mcp_schemalock alone: data normal
        assert.ok(ex.schemaLocked.has('field_4'));
        assert.ok(!ex.readBlocked.has('field_4'));
        assert.ok(!ex.writeBlocked.has('field_4'));
        assert.ok(ex.readBlocked.has('field_8'));
        assert.deepEqual(
            [...ex.maskedConnections.get('object_1')!],
            ['field_6'],
        );
        assert.ok(ex.objects.has('object_1'));
        assert.ok(!ex.objects.has('object_3'));
        assert.equal(ex.lockedObjects.size, 0);
    });

    it('blocks writes to a plain _mcp_nodata field', () => {
        const ex = buildFieldExclusions(
            parseRuntimeSchema({
                objects: [
                    {
                        key: 'object_1',
                        fields: [
                            {
                                key: 'field_1',
                                type: 'short_text',
                                meta: { description: '_MCP_NoData' },
                            },
                        ],
                    },
                ],
            }),
            undefined,
        );
        assert.ok(ex.masked.has('field_1'));
        assert.ok(ex.writeBlocked.has('field_1'));
        assert.ok(!ex.schemaLocked.has('field_1'));
        assert.equal(ex.reasons.get('field_1'), '_mcp_nodata');
    });

    it('locks every field of a table when any one carries _mcp_tablelock', () => {
        const ex = buildFieldExclusions(
            parseRuntimeSchema({
                objects: [
                    {
                        key: 'object_1',
                        fields: [
                            { key: 'field_1', type: 'short_text' },
                            {
                                key: 'field_2',
                                type: 'short_text',
                                meta: { description: 'Owner _mcp_tablelock' },
                            },
                        ],
                    },
                    {
                        key: 'object_2',
                        fields: [{ key: 'field_3', type: 'short_text' }],
                    },
                ],
            }),
            undefined,
        );
        assert.deepEqual([...ex.lockedObjects], ['object_1']);
        assert.ok(ex.schemaLocked.has('field_1'));
        assert.ok(ex.schemaLocked.has('field_2'));
        assert.ok(!ex.schemaLocked.has('field_3'));
        // A table lock limits the schema only, not the data.
        assert.ok(!ex.readBlocked.has('field_1'));
        assert.match(ex.lockReasons.get('object_1') || '', /field_2/);
    });

    it('applies dataAccess.objectKeywords to every field on the object', () => {
        const ex = buildFieldExclusions(schema, {
            objectKeywords: { object_3: ['_mcp_hidden'] },
        });
        assert.ok(ex.masked.has('field_10'));
        assert.ok(ex.writeBlocked.has('field_10'));
        assert.ok(ex.schemaLocked.has('field_10'));
        assert.match(
            ex.reasons.get('field_10') || '',
            /dataAccess\.objectKeywords/,
        );
        const locked = buildFieldExclusions(schema, {
            objectKeywords: { object_3: ['_mcp_tablelock'] },
        });
        assert.ok(locked.lockedObjects.has('object_3'));
    });

    it('keeps a config-redacted field dropped rather than masked', () => {
        const ex = buildFieldExclusions(schema, {
            redactedFieldKeys: ['field_9'],
        });
        assert.ok(ex.readBlocked.has('field_9'));
        assert.ok(!ex.masked.has('field_9'));
    });

    it('masks a formula over a no-data field', () => {
        const ex = buildFieldExclusions(schema, undefined);
        assert.ok(ex.masked.has('field_5'));
        assert.match(ex.reasons.get('field_5') || '', /field_2/);
    });

    it('masks a formula over a field that was _mcp_hidden, rather than hiding it', () => {
        const ex = buildFieldExclusions(schema, undefined);
        assert.ok(ex.masked.has('field_7'));
        assert.ok(!ex.schemaLocked.has('field_7'));
        assert.match(ex.reasons.get('field_7') || '', /formula over field_3/);
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
    it('knack_get_record masks every no-data field, old names included, and linked display values', async () => {
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
        // _mcp_hidden no longer drops the field: it reads as no-data.
        assert.equal(body.field_3, REDACTED_VALUE);
        assert.equal(body.field_3_raw, REDACTED_VALUE);
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

    it('knack_find_records refuses a filter, a sort or free text that could reveal a no-data value', async () => {
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
            /no data access/,
        );
        await assert.rejects(
            findRecords.handler(
                parseArgs(findRecords, {
                    objectKey: 'object_1',
                    sortField: 'field_2',
                }),
                ctx,
            ),
            /no data access/,
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

    it('knack_aggregate_records refuses a sum over a no-data field and masks a linked group-by', async () => {
        const { ctx } = setup(() => ok({ records: [STAFF_RECORD] }));
        await assert.rejects(
            aggregateRecords.handler(
                parseArgs(aggregateRecords, {
                    objectKey: 'object_1',
                    metrics: [{ type: 'sum', fieldKey: 'field_2' }],
                }),
                ctx,
            ),
            /no data access/,
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

    it('knack_create_records refuses a no-data field without _mcp_allowwrite before any request', async () => {
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
        assert.match(
            JSON.stringify(payload.errors),
            /field_3 cannot be written through MCP \(_mcp_hidden, no _mcp_allowwrite\)/,
        );
        assert.equal(requests.length, 0);
    });

    it('knack_create_records writes a no-data field with _mcp_allowwrite but masks it in the echo', async () => {
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
    it('knack_get_object lists every field, and marks the limited ones', async () => {
        const { ctx } = setup(() => ok({}));
        const payload = payloadOf(
            await getObject.handler(
                parseArgs(getObject, { objectKey: 'object_1' }),
                ctx,
            ),
        );
        const fields = payload.fields as Array<Record<string, unknown>>;
        const byKey = new Map(fields.map((field) => [field.key, field]));
        assert.deepEqual(byKey.get('field_2')?.mcpAccess, ['noData']);
        assert.deepEqual(byKey.get('field_3')?.mcpAccess, [
            'noData',
            'noWrite',
            'schemaLocked',
        ]);
        assert.deepEqual(byKey.get('field_4')?.mcpAccess, ['schemaLocked']);
        assert.equal(byKey.get('field_1')?.mcpAccess, undefined);
        // The old names are flagged for a person to replace; the new ones are not.
        assert.match(
            JSON.stringify(byKey.get('field_3')?.keywordWarnings),
            /_mcp_hidden is deprecated/,
        );
        assert.match(
            JSON.stringify(byKey.get('field_2')?.keywordWarnings),
            /_mcp_writeonly is deprecated/,
        );
        assert.equal(byKey.get('field_4')?.keywordWarnings, undefined);
    });

    it('flags an old keyword given by app.json objectKeywords', () => {
        const ex = buildFieldExclusions(parseRuntimeSchema(METADATA), {
            objectKeywords: { object_3: ['_mcp_writeonly'] },
        });
        assert.match(
            ex.deprecated.get('field_10')?.[0] || '',
            /_mcp_writeonly is deprecated.*dataAccess\.objectKeywords for object_3/,
        );
    });

    it('knack_create_field warns when a new description uses an old keyword', async () => {
        const { ctx } = setup(() => ok({}));
        const payload = payloadOf(
            await createField.handler(
                parseArgs(createField, {
                    objectKey: 'object_3',
                    name: 'Secret',
                    type: 'short_text',
                    description: '_mcp_hidden',
                    notedBy: 'Tester',
                    dryRun: true,
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, true, JSON.stringify(payload));
        assert.match(
            JSON.stringify(payload.keywordWarnings),
            /_mcp_hidden is deprecated/,
        );
    });

    it('knack_get_field returns the definition of a field that was _mcp_hidden', async () => {
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
        assert.equal(payload.ok, true, JSON.stringify(payload));
        assert.equal(
            (payload.field as Record<string, unknown>).name,
            'NI number',
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
        assert.equal(
            requests.some((request) => request.method === 'DELETE'),
            false,
        );
        await deleteField.handler(
            parseArgs(deleteField, {
                objectKey: 'object_1',
                fieldKey: 'field_1',
            }),
            ctx,
        );
        assert.equal(requests.at(-1)?.method, 'DELETE');
    });

    it('knack_delete_field and knack_delete_object check the lock against the live table', async () => {
        // field_1 has just been locked in the builder; the cache has not seen it yet.
        const liveFields = STAFF_FIELDS.map((field) =>
            field.key === 'field_1'
                ? { ...field, meta: { description: 'Now _mcp_schemalock' } }
                : field,
        );
        const { ctx, requests } = setup((apiPath, init) =>
            (init?.method || 'GET') === 'GET'
                ? ok({ object: { key: 'object_1', fields: liveFields } })
                : ok({}),
        );
        const field = payloadOf(
            await deleteField.handler(
                parseArgs(deleteField, {
                    objectKey: 'object_1',
                    fieldKey: 'field_1',
                }),
                ctx,
            ),
        );
        assert.equal(field.ok, false);
        assert.match(JSON.stringify(field.errors), /_mcp_schemalock/);
        const object = payloadOf(
            await deleteObject.handler(
                parseArgs(deleteObject, {
                    objectKey: 'object_3',
                    confirm: true,
                }),
                ctx,
            ),
        );
        assert.equal(object.ok, false);
        assert.equal(
            requests.some((request) => request.method === 'DELETE'),
            false,
        );
    });

    it('knack_update_field refuses to add _mcp_allowwrite to a no-data field', async () => {
        const liveFields = STAFF_FIELDS.map((field) =>
            field.key === 'field_1'
                ? { ...field, meta: { description: 'Name _mcp_nodata' } }
                : field,
        );
        const { ctx, requests } = setup((apiPath, init) =>
            (init?.method || 'GET') === 'GET'
                ? ok({ object: { key: 'object_1', fields: liveFields } })
                : ok({}),
        );
        const payload = payloadOf(
            await updateField.handler(
                parseArgs(updateField, {
                    objectKey: 'object_1',
                    fieldKey: 'field_1',
                    description: 'Name _mcp_nodata _mcp_allowwrite',
                    notedBy: 'Tester',
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(
            JSON.stringify(payload.errors),
            /would add _mcp_allowwrite/,
        );
        assert.equal(
            requests.some((request) => request.method === 'PUT'),
            false,
        );
    });

    it('knack_update_field will not drop a keyword typed in another case', async () => {
        const liveFields = STAFF_FIELDS.map((field) =>
            field.key === 'field_1'
                ? { ...field, meta: { description: 'Name _MCP_Writeonly' } }
                : field,
        );
        const { ctx, requests } = setup((apiPath, init) =>
            (init?.method || 'GET') === 'GET'
                ? ok({ object: { key: 'object_1', fields: liveFields } })
                : ok({}),
        );
        const payload = payloadOf(
            await updateField.handler(
                parseArgs(updateField, {
                    objectKey: 'object_1',
                    fieldKey: 'field_1',
                    description: 'Name',
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
});

describe('_mcp_tablelock', () => {
    // object_3 in the cache is unlocked; the live table has just been locked.
    const lockedLive = {
        key: 'object_3',
        fields: [
            {
                key: 'field_10',
                name: 'Text',
                type: 'short_text',
                meta: { description: 'Owner _mcp_tablelock' },
            },
        ],
    };
    const lockedMetadata: RuntimeMetadata = {
        objects: [...(METADATA.objects as unknown[]).slice(0, 2), lockedLive],
    };

    it('knack_create_field refuses a new field on a locked table, preview included', async () => {
        const { ctx, requests } = makeFakeContext({
            runtimeMetadata: { Demo: lockedMetadata },
            responses: () => ok({}),
        });
        for (const dryRun of [true, false]) {
            const payload = payloadOf(
                await createField.handler(
                    parseArgs(createField, {
                        appKey: 'Demo',
                        objectKey: 'object_3',
                        name: 'New',
                        type: 'short_text',
                        dryRun,
                    }),
                    ctx,
                ),
            );
            assert.equal(payload.ok, false);
            assert.match(
                JSON.stringify(payload.errors),
                /object_3 is table-locked \(_mcp_tablelock on field_10\)/,
            );
        }
        assert.equal(requests.length, 0);
        // Another table is unaffected.
        const other = payloadOf(
            await createField.handler(
                parseArgs(createField, {
                    appKey: 'Demo',
                    objectKey: 'object_1',
                    name: 'New',
                    type: 'short_text',
                    dryRun: true,
                }),
                ctx,
            ),
        );
        assert.equal(other.ok, true, JSON.stringify(other));
    });

    it('knack_update_object and knack_delete_field refuse a table locked since the cache was read', async () => {
        const { ctx, requests } = setup((apiPath, init) =>
            (init?.method || 'GET') === 'GET'
                ? ok({ object: lockedLive })
                : ok({}),
        );
        const renamed = payloadOf(
            await updateObject.handler(
                parseArgs(updateObject, {
                    objectKey: 'object_3',
                    name: 'Renamed',
                }),
                ctx,
            ),
        );
        assert.equal(renamed.ok, false);
        assert.match(JSON.stringify(renamed.errors), /table-locked/);
        const deleted = payloadOf(
            await deleteField.handler(
                parseArgs(deleteField, {
                    objectKey: 'object_3',
                    fieldKey: 'field_10',
                }),
                ctx,
            ),
        );
        assert.equal(deleted.ok, false);
        assert.match(JSON.stringify(deleted.errors), /table-locked/);
        assert.equal(
            requests.some((request) => request.method !== 'GET'),
            false,
        );
    });
});

describe('knack_duplicate_field keeps the source protection', () => {
    const created = (description?: string) => ({
        field: {
            key: 'field_20',
            name: 'Salary copy',
            type: 'number',
            ...(description ? { meta: { description } } : {}),
        },
    });

    it('checks the copy kept its keywords, and does nothing more when it did', async () => {
        const { ctx, requests } = setup((apiPath, init) =>
            (init?.method || 'GET') === 'GET'
                ? ok({ object: { key: 'object_1', fields: STAFF_FIELDS } })
                : ok(created('Pay band _mcp_writeonly')),
        );
        const payload = payloadOf(
            await duplicateField.handler(
                parseArgs(duplicateField, {
                    objectKey: 'object_1',
                    sourceFieldKey: 'field_2',
                    newName: 'Salary copy',
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, true, JSON.stringify(payload));
        const post = requests.find((request) => request.method === 'POST');
        assert.match(JSON.stringify(post?.body), /_mcp_writeonly/);
        assert.equal(
            requests.some((request) => request.method === 'PUT'),
            false,
        );
    });

    it('puts the keywords back when Knack drops them', async () => {
        const { ctx, requests } = setup((apiPath, init) => {
            const method = init?.method || 'GET';
            if (method === 'GET')
                return ok({
                    object: { key: 'object_1', fields: STAFF_FIELDS },
                });
            if (method === 'POST') return ok(created());
            return ok(created('Pay band _mcp_writeonly'));
        });
        const payload = payloadOf(
            await duplicateField.handler(
                parseArgs(duplicateField, {
                    objectKey: 'object_1',
                    sourceFieldKey: 'field_2',
                    newName: 'Salary copy',
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, true, JSON.stringify(payload));
        const put = requests.find((request) => request.method === 'PUT');
        assert.equal(put?.apiPath, '/objects/object_1/fields/field_20');
        assert.match(JSON.stringify(put?.body), /_mcp_writeonly/);
    });

    it('reports a copy it could not protect', async () => {
        const { ctx } = setup((apiPath, init) => {
            const method = init?.method || 'GET';
            if (method === 'GET')
                return ok({
                    object: { key: 'object_1', fields: STAFF_FIELDS },
                });
            if (method === 'POST') return ok(created());
            return { ok: false, status: 500, body: {} };
        });
        const payload = payloadOf(
            await duplicateField.handler(
                parseArgs(duplicateField, {
                    objectKey: 'object_1',
                    sourceFieldKey: 'field_2',
                    newName: 'Salary copy',
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.equal(payload.error, 'COPY_NOT_PROTECTED');
        assert.equal(payload.createdFieldKey, 'field_20');
        assert.match(String(payload.message), /_mcp_writeonly/);
    });
});

describe('collectFieldKeyRefs and writeFieldKeyRefs', () => {
    it('finds field keys in any string, split across connection paths', () => {
        assert.deepEqual(
            collectFieldKeyRefs({
                criteria: [{ field: 'field_1.field_2', value: 'x' }],
                values: [{ input: 'field_3' }],
                email: { message: 'Hi {field_4}, see {field_5.field_6}' },
                note: 'myfield_7 is not a key',
            }).sort(),
            ['field_1', 'field_2', 'field_3', 'field_4', 'field_5', 'field_6'],
        );
    });

    it('finds only the values[].field write targets, nested rules included', () => {
        assert.deepEqual(
            writeFieldKeyRefs([
                {
                    criteria: [{ field: 'field_1', value: 'x' }],
                    values: [
                        { type: 'record', field: 'field_2', input: 'field_3' },
                        { type: 'value', field: 'field_4.field_5' },
                    ],
                    email: { message: '{field_6}' },
                },
                {
                    action_rules: [
                        { record_rules: [{ values: [{ field: 'field_7' }] }] },
                    ],
                },
            ]).sort(),
            ['field_2', 'field_4', 'field_5', 'field_7'],
        );
    });
});

describe('readFieldKeyRefs and ruleFieldRefusal', () => {
    const exclusions = buildFieldExclusions(
        parseRuntimeSchema(METADATA),
        undefined,
    );

    it('treats values[].field as a write and everything else as a read', () => {
        assert.deepEqual(
            readFieldKeyRefs({
                criteria: [{ field: 'field_1', value: 'x' }],
                values: [
                    { type: 'record', field: 'field_2', input: 'field_9' },
                ],
                email: { message: '{field_10}' },
            }).sort(),
            ['field_1', 'field_10', 'field_9'],
        );
    });

    it('allows writing a no-data field with _mcp_allowwrite and refuses every read of it', () => {
        assert.equal(
            ruleFieldRefusal(
                exclusions,
                [
                    {
                        criteria: [],
                        values: [{ type: 'value', field: 'field_2' }],
                    },
                ],
                'a rule',
            ),
            null,
        );
        for (const rule of [
            { criteria: [{ field: 'field_2', operator: 'is', value: '1' }] },
            {
                values: [
                    { type: 'record', field: 'field_1', input: 'field_2' },
                ],
            },
            { email: { subject: 'Pay', message: 'Salary: {field_2}' } },
            { criteria: [{ field: 'field_6.field_8', operator: 'is' }] },
        ]) {
            assert.equal(
                ruleFieldRefusal(exclusions, [rule], 'a rule')?.error,
                'NO_DATA_FIELD',
                JSON.stringify(rule),
            );
        }
        // A config-redacted field is read-blocked too.
        assert.equal(
            ruleFieldRefusal(
                buildFieldExclusions(parseRuntimeSchema(METADATA), {
                    redactedFieldKeys: ['field_9'],
                }),
                [{ criteria: [{ field: 'field_9', operator: 'is' }] }],
                'a rule',
            )?.error,
            'NO_DATA_FIELD',
        );
    });

    it('lets a display rule test and target any no-data field', () => {
        for (const field of ['field_2', 'field_3']) {
            const display = {
                criteria: [{ field, operator: 'is blank' }],
                actions: [{ field, action: 'hide' }],
            };
            assert.equal(
                ruleFieldRefusal(exclusions, [display], 'a rule', {
                    displayOnly: true,
                }),
                null,
                field,
            );
        }
    });

    it('refuses writing a no-data field without _mcp_allowwrite, reads being refused first', () => {
        const refusal = ruleFieldRefusal(
            exclusions,
            [{ values: [{ type: 'value', field: 'field_3' }] }],
            'a task',
        );
        assert.equal(refusal?.error, 'NO_WRITE_FIELD');
        assert.match(
            refusal?.message || '',
            /field_3 cannot be written through MCP.*so a task cannot write it/,
        );
        assert.equal(
            ruleFieldRefusal(
                exclusions,
                [
                    {
                        criteria: [{ field: 'field_3', operator: 'is' }],
                        values: [{ type: 'value', field: 'field_3' }],
                    },
                ],
                'a rule',
            )?.error,
            'NO_DATA_FIELD',
        );
    });
});
