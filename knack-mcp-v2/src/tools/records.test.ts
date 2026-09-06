import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, it } from 'node:test';

import { z } from 'zod';

import type { AppConfig } from '../config.js';
import type { KnackApiResult } from '../http.js';
import type { AnyToolDef } from '../registry.js';
import {
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import type { RuntimeMetadata } from '../types.js';
import {
    aggregateRecords,
    createRecords,
    deleteRecords,
    findRecords,
    getRecord,
    getRelatedRecords,
    recordTools,
    updateRecords,
    uploadAsset,
    verifyRecordFieldShapes,
} from './records.js';

/** Parse through the tool's own zod shape so defaults apply as they would via MCP. */
const parseArgs = (tool: AnyToolDef, raw: Record<string, unknown>) =>
    z.object(tool.input).parse(raw);

/** A runtime metadata payload in the shape parseRuntimeSchema reads (`objects[].fields[]`). */
const RUNTIME_METADATA: RuntimeMetadata = {
    objects: [
        {
            key: 'object_1',
            name: 'Customers',
            fields: [
                {
                    key: 'field_1',
                    name: 'Name',
                    type: 'short_text',
                    required: true,
                },
                { key: 'field_2', name: 'Amount', type: 'currency' },
                {
                    key: 'field_3',
                    name: 'Orders',
                    type: 'connection',
                    relationship: {
                        object: 'object_2',
                        has: 'many',
                        belongs_to: 'one',
                    },
                },
                { key: 'field_4', name: 'Joined', type: 'date_time' },
                { key: 'field_5', name: 'Secret', type: 'short_text' },
            ],
        },
        {
            key: 'object_2',
            name: 'Orders',
            fields: [
                { key: 'field_10', name: 'Reference', type: 'short_text' },
                {
                    key: 'field_11',
                    name: 'Customer',
                    type: 'connection',
                    relationship: {
                        object: 'object_1',
                        has: 'one',
                        belongs_to: 'many',
                    },
                },
                { key: 'field_12', name: 'Total', type: 'number' },
            ],
        },
        {
            key: 'object_3',
            name: 'Hidden',
            fields: [{ key: 'field_20', type: 'short_text' }],
        },
    ],
};

const CUSTOMER_RECORD = {
    id: 'rec1',
    field_1: 'Ada',
    field_1_raw: 'Ada',
    field_2: '£10.00',
    field_2_raw: 10,
    field_3: '<span>o1</span>',
    field_3_raw: [
        { id: 'o1', identifier: 'Order 1' },
        { id: 'o2', identifier: 'Order 2' },
    ],
    field_4: '',
    field_4_raw: '',
    field_5: 'hush',
    field_5_raw: 'hush',
};

function setup(
    input: {
        app?: Partial<AppConfig>;
        responses?:
            | Record<string, KnackApiResult>
            | ((
                  apiPath: string,
                  init?: RequestInit,
              ) => KnackApiResult | Promise<KnackApiResult>);
    } = {},
) {
    const app = makeApp(input.app);
    const fake = makeFakeContext({
        apps: [app],
        runtimeMetadata: { [app.appKey]: RUNTIME_METADATA },
        responses: input.responses,
    });
    fake.ctx.state.activeAppKey = app.appKey;
    return fake;
}

const ok = (body: unknown): KnackApiResult => ({ ok: true, status: 200, body });

describe('recordTools catalogue', () => {
    it('lists the record tools in order', () => {
        assert.deepEqual(
            recordTools.map((tool) => tool.name),
            [
                'knack_get_record',
                'knack_find_records',
                'knack_get_related_records',
                'knack_aggregate_records',
                'knack_verify_record_field_shapes',
                'knack_create_records',
                'knack_update_records',
                'knack_delete_records',
                'knack_upload_asset',
            ],
        );
        assert.equal(getRecord.access, 'read');
        assert.equal(verifyRecordFieldShapes.access, 'diagnostic');
        assert.equal(createRecords.access, 'write');
        assert.equal(updateRecords.access, 'write');
        assert.equal(deleteRecords.access, 'delete');
        assert.equal(uploadAsset.access, 'write');
    });
});

describe('knack_get_record', () => {
    it('fetches a record and adds the raw-field tip', async () => {
        const { ctx, requests } = setup({
            responses: {
                'GET /objects/object_1/records/rec1': ok(CUSTOMER_RECORD),
            },
        });
        const result = await getRecord.handler(
            parseArgs(getRecord, { objectKey: 'object_1', recordId: 'rec1' }),
            ctx,
        );
        const payload = payloadOf(result);
        assert.equal(payload.ok, true);
        assert.equal(payload.appKey, 'Demo');
        assert.equal(payload.status, 200);
        assert.deepEqual(payload.body, CUSTOMER_RECORD);
        assert.match(String(payload.tip), /field_xxx_raw/);
        assert.deepEqual(requests, [
            {
                apiPath: '/objects/object_1/records/rec1',
                method: 'GET',
                body: null,
            },
        ]);
    });

    it('omits the tip when the fetch fails', async () => {
        const { ctx } = setup({
            responses: {
                'GET /objects/object_1/records/missing': {
                    ok: false,
                    status: 404,
                    body: { error: 'not found' },
                },
            },
        });
        const payload = payloadOf(
            await getRecord.handler(
                parseArgs(getRecord, {
                    objectKey: 'object_1',
                    recordId: 'missing',
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.equal(payload.status, 404);
        assert.equal('tip' in payload, false);
    });

    it('projects the record to the allowed fields under a dataAccess policy', async () => {
        const { ctx } = setup({
            app: {
                dataAccess: {
                    allowedObjectKeys: ['object_1'],
                    allowedFieldKeys: { object_1: ['field_1', 'field_2'] },
                    redactedFieldKeys: ['field_5'],
                },
            },
            responses: {
                'GET /objects/object_1/records/rec1': ok(CUSTOMER_RECORD),
            },
        });
        const payload = payloadOf(
            await getRecord.handler(
                parseArgs(getRecord, {
                    objectKey: 'object_1',
                    recordId: 'rec1',
                }),
                ctx,
            ),
        );
        assert.deepEqual(payload.body, {
            id: 'rec1',
            field_1: 'Ada',
            field_1_raw: 'Ada',
            field_2: '£10.00',
            field_2_raw: 10,
        });
    });

    it('drops redacted fields when no allow-list is set', async () => {
        const { ctx } = setup({
            app: { dataAccess: { redactedFieldKeys: ['field_5'] } },
            responses: {
                'GET /objects/object_1/records/rec1': ok(CUSTOMER_RECORD),
            },
        });
        const body = payloadOf(
            await getRecord.handler(
                parseArgs(getRecord, {
                    objectKey: 'object_1',
                    recordId: 'rec1',
                }),
                ctx,
            ),
        ).body as Record<string, unknown>;
        assert.equal('field_5' in body, false);
        assert.equal('field_5_raw' in body, false);
        assert.equal(body.field_1, 'Ada');
        assert.equal(body.field_3_raw && Array.isArray(body.field_3_raw), true);
    });

    it('refuses objects outside the policy before any request is made', async () => {
        const { ctx, requests } = setup({
            app: { dataAccess: { allowedObjectKeys: ['object_1'] } },
        });
        await assert.rejects(
            getRecord.handler(
                parseArgs(getRecord, {
                    objectKey: 'object_3',
                    recordId: 'rec1',
                }),
                ctx,
            ),
            /Read access to object_3 is not allowed by this app's dataAccess policy\./,
        );
        assert.equal(requests.length, 0);
    });

    it('refuses objects missing from the schema', async () => {
        const { ctx, requests } = setup();
        await assert.rejects(
            getRecord.handler(
                parseArgs(getRecord, {
                    objectKey: 'object_99',
                    recordId: 'rec1',
                }),
                ctx,
            ),
            /Object object_99 was not found in the available schema\./,
        );
        assert.equal(requests.length, 0);
    });
});

describe('knack_find_records', () => {
    const listBody = {
        total_pages: 1,
        current_page: 1,
        total_records: 1,
        records: [CUSTOMER_RECORD],
    };

    it('builds the search query and returns the list with the tip', async () => {
        const { ctx, requests } = setup({ responses: () => ok(listBody) });
        const payload = payloadOf(
            await findRecords.handler(
                parseArgs(findRecords, {
                    objectKey: 'object_1',
                    page: 2,
                    rowsPerPage: 10,
                    sortField: 'field_1',
                    sortOrder: 'desc',
                    filters: {
                        match: 'and',
                        rules: [
                            { field: 'field_1', operator: 'is', value: 'Ada' },
                        ],
                    },
                }),
                ctx,
            ),
        );
        assert.equal(requests.length, 1);
        const url = new URL(`https://x${requests[0].apiPath}`);
        assert.equal(url.pathname, '/objects/object_1/records');
        assert.equal(url.searchParams.get('page'), '2');
        assert.equal(url.searchParams.get('rows_per_page'), '10');
        assert.equal(url.searchParams.get('sort_field'), 'field_1');
        assert.equal(url.searchParams.get('sort_order'), 'desc');
        assert.deepEqual(JSON.parse(url.searchParams.get('filters') || ''), {
            match: 'and',
            rules: [{ field: 'field_1', operator: 'is', value: 'Ada' }],
        });
        assert.equal(payload.ok, true);
        assert.deepEqual(payload.body, listBody);
        assert.match(String(payload.tip), /field_xxx_raw/);
        assert.equal('schema' in payload, false);
        assert.equal('schemaAvailable' in payload, false);
    });

    it('applies the defaults page=1 rows_per_page=25 and passes q through', async () => {
        const { ctx, requests } = setup({ responses: () => ok(listBody) });
        await findRecords.handler(
            parseArgs(findRecords, { objectKey: 'object_1', q: 'ada' }),
            ctx,
        );
        const url = new URL(`https://x${requests[0].apiPath}`);
        assert.equal(url.searchParams.get('page'), '1');
        assert.equal(url.searchParams.get('rows_per_page'), '25');
        assert.equal(url.searchParams.get('q'), 'ada');
        assert.equal(url.searchParams.has('sort_field'), false);
    });

    it('adds the object schema when includeSchema is true', async () => {
        const { ctx } = setup({ responses: () => ok(listBody) });
        const payload = payloadOf(
            await findRecords.handler(
                parseArgs(findRecords, {
                    objectKey: 'object_1',
                    includeSchema: true,
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.deepEqual(payload.body, listBody);
        assert.equal(payload.objectKey, 'object_1');
        assert.equal(payload.objectName, 'Customers');
        assert.equal(payload.schemaSource, 'runtime');
        assert.equal(payload.schemaAvailable, true);
        assert.equal(payload.schemaMessage, null);
        const schema = payload.schema as Record<string, unknown>;
        assert.equal(schema.key, 'object_1');
        assert.equal(schema.name, 'Customers');
        assert.equal(schema.fieldCount, 5);
        const fields = schema.fields as Array<Record<string, unknown>>;
        assert.deepEqual(fields[0], {
            key: 'field_1',
            name: 'Name',
            type: 'short_text',
            required: true,
        });
        assert.deepEqual(Object.keys(fields[2]).sort(), [
            'key',
            'name',
            'type',
        ]);
    });

    it('clamps rowsPerPage to the policy maximum and projects records', async () => {
        const { ctx, requests } = setup({
            app: {
                dataAccess: {
                    allowedFieldKeys: { object_1: ['field_1'] },
                    maxRecordsPerQuery: 5,
                },
            },
            responses: () => ok(listBody),
        });
        const payload = payloadOf(
            await findRecords.handler(
                parseArgs(findRecords, {
                    objectKey: 'object_1',
                    rowsPerPage: 500,
                }),
                ctx,
            ),
        );
        const url = new URL(`https://x${requests[0].apiPath}`);
        assert.equal(url.searchParams.get('rows_per_page'), '5');
        const body = payload.body as { records: unknown[] };
        assert.deepEqual(body.records, [
            { id: 'rec1', field_1: 'Ada', field_1_raw: 'Ada' },
        ]);
    });

    it('refuses free-text search under a dataAccess policy before any request', async () => {
        const { ctx, requests } = setup({
            app: { dataAccess: { redactedFieldKeys: [] } },
        });
        await assert.rejects(
            findRecords.handler(
                parseArgs(findRecords, { objectKey: 'object_1', q: 'ada' }),
                ctx,
            ),
            /Free-text search is disabled for apps with a dataAccess policy/,
        );
        assert.equal(requests.length, 0);
    });

    it('refuses filters and sorts on redacted or disallowed fields', async () => {
        const { ctx, requests } = setup({
            app: {
                dataAccess: {
                    allowedFieldKeys: { object_1: ['field_1', 'field_2'] },
                    redactedFieldKeys: ['field_2'],
                },
            },
        });
        await assert.rejects(
            findRecords.handler(
                parseArgs(findRecords, {
                    objectKey: 'object_1',
                    filters: {
                        rules: [{ field: 'field_2', operator: 'is', value: 1 }],
                    },
                }),
                ctx,
            ),
            /Field field_2 is redacted by this app's dataAccess policy\./,
        );
        await assert.rejects(
            findRecords.handler(
                parseArgs(findRecords, {
                    objectKey: 'object_1',
                    sortField: 'field_4',
                }),
                ctx,
            ),
            /Field field_4 is not allowed by this app's dataAccess policy\./,
        );
        assert.equal(requests.length, 0);
    });

    it('rejects an empty sortField before any request', async () => {
        const { ctx, requests } = setup();
        await assert.rejects(
            findRecords.handler(
                parseArgs(findRecords, {
                    objectKey: 'object_1',
                    sortField: '  ',
                }),
                ctx,
            ),
            /sortField cannot be empty\./,
        );
        assert.equal(requests.length, 0);
    });
});

describe('knack_get_related_records', () => {
    it('follows a forward connection and projects the requested fields', async () => {
        const { ctx, requests } = setup({
            responses: {
                'GET /objects/object_1/records/rec1': ok(CUSTOMER_RECORD),
                'GET /objects/object_2/records/o1': ok({
                    id: 'o1',
                    field_10: 'A-1',
                    field_10_raw: 'A-1',
                    field_12: 5,
                }),
                'GET /objects/object_2/records/o2': ok({
                    id: 'o2',
                    field_10: 'A-2',
                    field_12: 7,
                }),
            },
        });
        const payload = payloadOf(
            await getRelatedRecords.handler(
                parseArgs(getRelatedRecords, {
                    sourceObjectKey: 'object_1',
                    sourceRecordId: 'rec1',
                    direction: 'forward',
                    connectionFieldKey: 'field_3',
                    fieldKeys: ['field_10'],
                }),
                ctx,
            ),
        );
        assert.deepEqual(
            requests.map((request) => request.apiPath),
            [
                '/objects/object_1/records/rec1',
                '/objects/object_2/records/o1',
                '/objects/object_2/records/o2',
            ],
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.direction, 'forward');
        assert.equal(payload.relatedObjectKey, 'object_2');
        assert.deepEqual(payload.source, {
            objectKey: 'object_1',
            recordId: 'rec1',
        });
        assert.equal(payload.returned, 2);
        assert.equal(payload.limit, 25);
        assert.deepEqual(payload.records, [
            { id: 'o1', field_10: 'A-1', field_10_raw: 'A-1' },
            { id: 'o2', field_10: 'A-2' },
        ]);
    });

    it('honours limit on forward lookups', async () => {
        const { ctx, requests } = setup({
            responses: {
                'GET /objects/object_1/records/rec1': ok(CUSTOMER_RECORD),
                'GET /objects/object_2/records/o1': ok({
                    id: 'o1',
                    field_10: 'A-1',
                }),
            },
        });
        const payload = payloadOf(
            await getRelatedRecords.handler(
                parseArgs(getRelatedRecords, {
                    sourceObjectKey: 'object_1',
                    sourceRecordId: 'rec1',
                    direction: 'forward',
                    connectionFieldKey: 'field_3',
                    fieldKeys: ['field_10'],
                    limit: 1,
                }),
                ctx,
            ),
        );
        assert.equal(requests.length, 2);
        assert.equal(payload.returned, 1);
        assert.equal(payload.limit, 1);
    });

    it('queries the related object by connection filter for reverse lookups', async () => {
        const { ctx, requests } = setup({
            responses: () =>
                ok({
                    records: [
                        { id: 'o1', field_10: 'A-1', field_12: 5 },
                        { id: 'o2', field_10: 'A-2', field_12: 7 },
                    ],
                }),
        });
        const payload = payloadOf(
            await getRelatedRecords.handler(
                parseArgs(getRelatedRecords, {
                    sourceObjectKey: 'object_1',
                    sourceRecordId: 'rec1',
                    direction: 'reverse',
                    connectionFieldKey: 'field_11',
                    relatedObjectKey: 'object_2',
                    fieldKeys: ['field_10', 'field_12'],
                    limit: 10,
                    sortField: 'field_12',
                    sortOrder: 'desc',
                }),
                ctx,
            ),
        );
        assert.equal(requests.length, 1);
        const url = new URL(`https://x${requests[0].apiPath}`);
        assert.equal(url.pathname, '/objects/object_2/records');
        assert.equal(url.searchParams.get('rows_per_page'), '10');
        assert.equal(url.searchParams.get('sort_field'), 'field_12');
        assert.equal(url.searchParams.get('sort_order'), 'desc');
        assert.deepEqual(JSON.parse(url.searchParams.get('filters') || ''), {
            match: 'and',
            rules: [{ field: 'field_11', operator: 'is', value: 'rec1' }],
        });
        assert.equal(payload.relatedObjectKey, 'object_2');
        assert.equal(payload.returned, 2);
        assert.deepEqual(payload.records, [
            { id: 'o1', field_10: 'A-1', field_12: 5 },
            { id: 'o2', field_10: 'A-2', field_12: 7 },
        ]);
    });

    it('clamps limit to the policy maximum', async () => {
        const { ctx } = setup({
            app: { dataAccess: { maxRecordsPerQuery: 3 } },
            responses: () => ok({ records: [] }),
        });
        const payload = payloadOf(
            await getRelatedRecords.handler(
                parseArgs(getRelatedRecords, {
                    sourceObjectKey: 'object_1',
                    sourceRecordId: 'rec1',
                    direction: 'reverse',
                    connectionFieldKey: 'field_11',
                    relatedObjectKey: 'object_2',
                    fieldKeys: ['field_10'],
                    limit: 50,
                }),
                ctx,
            ),
        );
        assert.equal(payload.limit, 3);
    });

    it('rejects a forward lookup through a non-connection field', async () => {
        const { ctx, requests } = setup();
        await assert.rejects(
            getRelatedRecords.handler(
                parseArgs(getRelatedRecords, {
                    sourceObjectKey: 'object_1',
                    sourceRecordId: 'rec1',
                    direction: 'forward',
                    connectionFieldKey: 'field_1',
                    fieldKeys: ['field_10'],
                }),
                ctx,
            ),
            /field_1 is not a recognised connection field on object_1\./,
        );
        assert.equal(requests.length, 0);
    });

    it('requires relatedObjectKey and a matching connection for reverse lookups', async () => {
        const { ctx, requests } = setup();
        await assert.rejects(
            getRelatedRecords.handler(
                parseArgs(getRelatedRecords, {
                    sourceObjectKey: 'object_1',
                    sourceRecordId: 'rec1',
                    direction: 'reverse',
                    connectionFieldKey: 'field_11',
                    fieldKeys: ['field_10'],
                }),
                ctx,
            ),
            /relatedObjectKey is required for reverse related-record lookups\./,
        );
        await assert.rejects(
            getRelatedRecords.handler(
                parseArgs(getRelatedRecords, {
                    sourceObjectKey: 'object_1',
                    sourceRecordId: 'rec1',
                    direction: 'reverse',
                    connectionFieldKey: 'field_10',
                    relatedObjectKey: 'object_2',
                    fieldKeys: ['field_10'],
                }),
                ctx,
            ),
            /field_10 must be a connection from object_2 to object_1\./,
        );
        assert.equal(requests.length, 0);
    });

    it('refuses related fields the policy does not allow', async () => {
        const { ctx, requests } = setup({
            app: {
                dataAccess: { allowedFieldKeys: { object_2: ['field_10'] } },
            },
        });
        await assert.rejects(
            getRelatedRecords.handler(
                parseArgs(getRelatedRecords, {
                    sourceObjectKey: 'object_1',
                    sourceRecordId: 'rec1',
                    direction: 'forward',
                    connectionFieldKey: 'field_3',
                    fieldKeys: ['field_12'],
                }),
                ctx,
            ),
            /Field field_12 is not allowed by this app's dataAccess policy\./,
        );
        assert.equal(requests.length, 0);
    });
});

describe('knack_aggregate_records', () => {
    const ORDERS = [
        { id: 'o1', field_10: 'A', field_12: '5', field_11: 'x' },
        { id: 'o2', field_10: 'A', field_12: 7 },
        { id: 'o3', field_10: 'B', field_12: 'n/a' },
    ];

    it('counts and sums grouped records over pages', async () => {
        const { ctx, requests } = setup({
            responses: (apiPath) => {
                const url = new URL(`https://x${apiPath}`);
                const page = Number(url.searchParams.get('page'));
                return ok({
                    records: page === 1 ? ORDERS.slice(0, 2) : ORDERS.slice(2),
                });
            },
        });
        const payload = payloadOf(
            await aggregateRecords.handler(
                parseArgs(aggregateRecords, {
                    objectKey: 'object_2',
                    groupByFieldKeys: ['field_10'],
                    metrics: [
                        { type: 'count' },
                        { type: 'sum', fieldKey: 'field_12' },
                    ],
                    maxRecords: 2,
                }),
                ctx,
            ),
        );
        // rowsPerPage = min(1000, scanLimit) = 2; a full page means more may exist.
        assert.equal(requests.length, 1);
        const url = new URL(`https://x${requests[0].apiPath}`);
        assert.equal(url.searchParams.get('rows_per_page'), '2');
        assert.equal(payload.ok, true);
        assert.equal(payload.scanned, 2);
        assert.equal(payload.capped, true);
        assert.equal(payload.scanLimit, 2);
        assert.deepEqual(payload.fields, ['field_10', 'field_12']);
        assert.match(String(payload.warning), /PARTIAL/);
        assert.deepEqual(payload.groups, [
            {
                dimensions: { field_10: 'A' },
                metrics: { count: 2, 'sum:field_12': 12 },
            },
        ]);
    });

    it('walks every page until a short page, with date buckets', async () => {
        const { ctx, requests } = setup({
            responses: (apiPath) => {
                const url = new URL(`https://x${apiPath}`);
                const page = Number(url.searchParams.get('page'));
                return ok({ records: page === 1 ? ORDERS : [] });
            },
        });
        const payload = payloadOf(
            await aggregateRecords.handler(
                parseArgs(aggregateRecords, {
                    objectKey: 'object_1',
                    dateBucket: { fieldKey: 'field_4', granularity: 'month' },
                    filters: '{"match":"and","rules":[]}',
                }),
                ctx,
            ),
        );
        // Three records on a 1000-row page is a short page, so one request suffices.
        assert.equal(requests.length, 1);
        assert.equal(
            new URL(`https://x${requests[0].apiPath}`).searchParams.get(
                'filters',
            ),
            '{"match":"and","rules":[]}',
        );
        assert.equal(payload.capped, false);
        assert.equal(payload.scanned, 3);
        assert.equal('warning' in payload, false);
        assert.deepEqual(payload.groups, [
            { dimensions: { field_4: 'Unknown' }, metrics: { count: 3 } },
        ]);
    });

    it('reports an upstream failure with its status and body', async () => {
        const { ctx } = setup({
            responses: () => ({
                ok: false,
                status: 500,
                body: { error: 'boom' },
            }),
        });
        const payload = payloadOf(
            await aggregateRecords.handler(
                parseArgs(aggregateRecords, { objectKey: 'object_2' }),
                ctx,
            ),
        );
        assert.deepEqual(payload, {
            ok: false,
            appKey: 'Demo',
            objectKey: 'object_2',
            status: 500,
            body: { error: 'boom' },
        });
    });

    it('requires fieldKey for sum metrics', async () => {
        const { ctx, requests } = setup();
        await assert.rejects(
            aggregateRecords.handler(
                parseArgs(aggregateRecords, {
                    objectKey: 'object_2',
                    metrics: [{ type: 'sum' }],
                }),
                ctx,
            ),
            /A sum metric requires fieldKey\./,
        );
        assert.equal(requests.length, 0);
    });

    it('clamps the scan limit to the policy maximum', async () => {
        const { ctx, requests } = setup({
            app: { dataAccess: { maxRecordsPerQuery: 2 } },
            responses: () => ok({ records: ORDERS.slice(0, 1) }),
        });
        const payload = payloadOf(
            await aggregateRecords.handler(
                parseArgs(aggregateRecords, {
                    objectKey: 'object_2',
                    maxRecords: 500,
                }),
                ctx,
            ),
        );
        assert.equal(payload.scanLimit, 2);
        assert.equal(
            new URL(`https://x${requests[0].apiPath}`).searchParams.get(
                'rows_per_page',
            ),
            '2',
        );
    });
});

describe('knack_verify_record_field_shapes', () => {
    it('compares each populated field against its documented shape', async () => {
        const { ctx } = setup({
            responses: {
                'GET /objects/object_1/records/rec1': ok({
                    ...CUSTOMER_RECORD,
                    field_3_raw: 'not-an-array',
                }),
            },
        });
        const payload = payloadOf(
            await verifyRecordFieldShapes.handler(
                parseArgs(verifyRecordFieldShapes, {
                    objectKey: 'object_1',
                    recordId: 'rec1',
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.objectName, 'Customers');
        assert.equal(payload.schemaSource, 'runtime');
        assert.equal(payload.includeBlankFields, false);
        assert.deepEqual(payload.summary, {
            checkedFieldCount: 4,
            matchCount: 3,
            mismatchCount: 1,
            skippedCount: 1,
            unknownCount: 0,
        });
        const results = payload.results as Array<Record<string, unknown>>;
        assert.deepEqual(
            results.map((entry) => [entry.fieldKey, entry.status]),
            [
                ['field_1', 'match'],
                ['field_2', 'match'],
                ['field_3', 'mismatch'],
                ['field_5', 'match'],
            ],
        );
        const connection = results[2];
        assert.equal(connection.fieldType, 'connection');
        assert.equal(connection.observedRawShape, 'string');
        assert.equal(connection.observedFormattedShape, 'html-string');
        assert.equal(typeof connection.expectedSummary, 'string');
        assert.deepEqual(connection.findings, [
            'Raw value should be an array of connection objects with id and/or identifier.',
        ]);
    });

    it('includes blank fields when asked', async () => {
        const { ctx } = setup({
            responses: {
                'GET /objects/object_1/records/rec1': ok(CUSTOMER_RECORD),
            },
        });
        const payload = payloadOf(
            await verifyRecordFieldShapes.handler(
                parseArgs(verifyRecordFieldShapes, {
                    objectKey: 'object_1',
                    recordId: 'rec1',
                    includeBlankFields: true,
                }),
                ctx,
            ),
        );
        const summary = payload.summary as Record<string, number>;
        assert.equal(summary.checkedFieldCount, 5);
        assert.equal(summary.skippedCount, 1);
        const results = payload.results as Array<Record<string, unknown>>;
        assert.equal(
            results.find((entry) => entry.fieldKey === 'field_4')?.status,
            'skipped',
        );
    });

    it('reports a record that cannot be fetched', async () => {
        const { ctx } = setup({
            responses: {
                'GET /objects/object_1/records/nope': {
                    ok: false,
                    status: 404,
                    body: null,
                },
            },
        });
        const payload = payloadOf(
            await verifyRecordFieldShapes.handler(
                parseArgs(verifyRecordFieldShapes, {
                    objectKey: 'object_1',
                    recordId: 'nope',
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.equal(payload.message, 'Unable to fetch the requested record.');
        assert.deepEqual(payload.recordResponse, {
            ok: false,
            status: 404,
            body: null,
        });
    });

    it('reports an object missing from the schema', async () => {
        const { ctx } = setup({
            responses: {
                'GET /objects/object_99/records/rec1': ok({ id: 'rec1' }),
            },
        });
        const payload = payloadOf(
            await verifyRecordFieldShapes.handler(
                parseArgs(verifyRecordFieldShapes, {
                    objectKey: 'object_99',
                    recordId: 'rec1',
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.equal(payload.schemaSource, 'runtime');
        assert.match(
            String(payload.message),
            /Object was not found in the available schema/,
        );
    });
});

describe('knack_create_records', () => {
    it('creates each record with its own POST and reports per-record results', async () => {
        const { ctx, requests } = setup({
            responses: (_apiPath, init) => {
                const body = JSON.parse(String(init?.body)) as Record<
                    string,
                    unknown
                >;
                return body.field_1 === 'bad'
                    ? { ok: false, status: 400, body: { error: 'invalid' } }
                    : ok({ id: `new-${body.field_1}` });
            },
        });
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [
                        '{"field_1":"Ada"}',
                        '{"field_1":"bad"}',
                        '{"field_1":"Bob"}',
                    ],
                }),
                ctx,
            ),
        );
        assert.deepEqual(requests, [
            {
                apiPath: '/objects/object_1/records',
                method: 'POST',
                body: { field_1: 'Ada' },
            },
            {
                apiPath: '/objects/object_1/records',
                method: 'POST',
                body: { field_1: 'bad' },
            },
            {
                apiPath: '/objects/object_1/records',
                method: 'POST',
                body: { field_1: 'Bob' },
            },
        ]);
        assert.equal(payload.ok, false);
        assert.equal(payload.action, 'batch_create_records');
        assert.equal(payload.requestedCount, 3);
        assert.equal(payload.successCount, 2);
        assert.equal(payload.failureCount, 1);
        assert.deepEqual(payload.results, [
            { index: 0, ok: true, status: 200, body: { id: 'new-Ada' } },
            { index: 1, ok: false, status: 400, body: { error: 'invalid' } },
            { index: 2, ok: true, status: 200, body: { id: 'new-Bob' } },
        ]);
        assert.match(String(payload.note), /not on 5xx/);
    });

    it('treats a single record as a one-element batch', async () => {
        const { ctx, requests } = setup({
            responses: () => ok({ id: 'new-1' }),
        });
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: ['{"field_1":"Ada"}'],
                }),
                ctx,
            ),
        );
        assert.equal(requests.length, 1);
        assert.equal(payload.ok, true);
        assert.equal(payload.successCount, 1);
    });

    it('fails the whole call in preflight when any record is not a JSON object', async () => {
        const { ctx, requests } = setup({ responses: () => ok({ id: 'x' }) });
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: ['{"field_1":"Ada"}', '{not json', '[1,2]'],
                }),
                ctx,
            ),
        );
        assert.equal(requests.length, 0);
        assert.equal(payload.ok, false);
        assert.equal(payload.action, 'batch_create_records_preflight');
        const errors = payload.errors as string[];
        assert.equal(errors.length, 2);
        assert.match(errors[0], /^records\[1\] must be valid JSON: /);
        assert.equal(errors[1], 'records[2] must be a JSON object.');
    });

    it('sends nothing on dryRun', async () => {
        const { ctx, requests } = setup({ responses: () => ok({ id: 'x' }) });
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: ['{"field_1":"Ada"}', '{"field_1":"Bob"}'],
                    dryRun: true,
                }),
                ctx,
            ),
        );
        assert.equal(requests.length, 0);
        assert.deepEqual(payload, {
            ok: true,
            appKey: 'Demo',
            objectKey: 'object_1',
            action: 'batch_create_records_dry_run',
            dryRun: true,
            wouldCreateCount: 2,
            wouldCreate: [{ field_1: 'Ada' }, { field_1: 'Bob' }],
        });
    });

    it('captures a thrown request as a per-record error without aborting the batch', async () => {
        const { ctx } = setup({
            responses: (_apiPath, init) => {
                if (String(init?.body).includes('boom'))
                    throw new Error('network down');
                return ok({ id: 'x' });
            },
        });
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: ['{"field_1":"boom"}', '{"field_1":"ok"}'],
                }),
                ctx,
            ),
        );
        assert.deepEqual(payload.results, [
            { index: 0, ok: false, error: 'network down' },
            { index: 1, ok: true, status: 200, body: { id: 'x' } },
        ]);
        assert.equal(payload.failureCount, 1);
    });
});

describe('knack_update_records', () => {
    it('updates each record with its own PUT and reports per-record results', async () => {
        const { ctx, requests } = setup({
            responses: () => ok({ id: 'updated' }),
        });
        const payload = payloadOf(
            await updateRecords.handler(
                parseArgs(updateRecords, {
                    objectKey: 'object_1',
                    records: [
                        { recordId: 'rec1', data: '{"field_1":"Ada"}' },
                        { recordId: 'rec2', data: '{"field_2":10}' },
                    ],
                }),
                ctx,
            ),
        );
        assert.deepEqual(requests, [
            {
                apiPath: '/objects/object_1/records/rec1',
                method: 'PUT',
                body: { field_1: 'Ada' },
            },
            {
                apiPath: '/objects/object_1/records/rec2',
                method: 'PUT',
                body: { field_2: 10 },
            },
        ]);
        assert.equal(payload.ok, true);
        assert.equal(payload.action, 'batch_update_records');
        assert.equal(payload.requestedCount, 2);
        assert.equal(payload.successCount, 2);
        assert.equal(payload.failureCount, 0);
        assert.deepEqual(payload.results, [
            {
                index: 0,
                recordId: 'rec1',
                ok: true,
                status: 200,
                body: { id: 'updated' },
            },
            {
                index: 1,
                recordId: 'rec2',
                ok: true,
                status: 200,
                body: { id: 'updated' },
            },
        ]);
        assert.match(String(payload.note), /429\/5xx/);
    });

    it('fails the whole call in preflight when any data is invalid JSON', async () => {
        const { ctx, requests } = setup({ responses: () => ok({}) });
        const payload = payloadOf(
            await updateRecords.handler(
                parseArgs(updateRecords, {
                    objectKey: 'object_1',
                    records: [
                        { recordId: 'rec1', data: '{"field_1":"Ada"}' },
                        { recordId: 'rec2', data: 'nope' },
                    ],
                }),
                ctx,
            ),
        );
        assert.equal(requests.length, 0);
        assert.equal(payload.ok, false);
        assert.equal(payload.action, 'batch_update_records_preflight');
        const errors = payload.errors as string[];
        assert.equal(errors.length, 1);
        assert.match(errors[0], /^records\[1\]\.data must be valid JSON: /);
    });

    it('sends nothing on dryRun', async () => {
        const { ctx, requests } = setup({ responses: () => ok({}) });
        const payload = payloadOf(
            await updateRecords.handler(
                parseArgs(updateRecords, {
                    objectKey: 'object_1',
                    records: [{ recordId: 'rec1', data: '{"field_1":"Ada"}' }],
                    dryRun: true,
                }),
                ctx,
            ),
        );
        assert.equal(requests.length, 0);
        assert.deepEqual(payload, {
            ok: true,
            appKey: 'Demo',
            objectKey: 'object_1',
            action: 'batch_update_records_dry_run',
            dryRun: true,
            wouldUpdateCount: 1,
            wouldUpdate: [{ recordId: 'rec1', data: { field_1: 'Ada' } }],
        });
    });

    it('captures a thrown request as a per-record error', async () => {
        const { ctx } = setup({
            responses: (apiPath) => {
                if (apiPath.endsWith('/rec2')) throw new Error('timeout');
                return ok({});
            },
        });
        const payload = payloadOf(
            await updateRecords.handler(
                parseArgs(updateRecords, {
                    objectKey: 'object_1',
                    records: [
                        { recordId: 'rec1', data: '{}' },
                        { recordId: 'rec2', data: '{}' },
                    ],
                }),
                ctx,
            ),
        );
        assert.deepEqual((payload.results as unknown[])[1], {
            index: 1,
            recordId: 'rec2',
            ok: false,
            error: 'timeout',
        });
        assert.equal(payload.ok, false);
    });
});

describe('knack_delete_records', () => {
    it('returns a preview and deletes nothing without confirm', async () => {
        const { ctx, requests } = setup({ responses: () => ok({}) });
        const payload = payloadOf(
            await deleteRecords.handler(
                parseArgs(deleteRecords, {
                    objectKey: 'object_1',
                    recordIds: ['rec1', 'rec2'],
                }),
                ctx,
            ),
        );
        assert.equal(requests.length, 0);
        assert.equal(payload.ok, false);
        assert.equal(payload.action, 'batch_delete_records_preflight');
        assert.equal(payload.wouldDeleteCount, 2);
        assert.deepEqual(payload.wouldDeleteRecordIds, ['rec1', 'rec2']);
        assert.match(
            String(payload.message),
            /permanently delete 2 record\(s\) from object_1.*Pass confirm: true/,
        );
    });

    it('deletes each record with its own DELETE when confirmed', async () => {
        const { ctx, requests } = setup({
            responses: (apiPath) =>
                apiPath.endsWith('/rec2')
                    ? { ok: false, status: 404, body: { error: 'gone' } }
                    : ok({ delete: true }),
        });
        const payload = payloadOf(
            await deleteRecords.handler(
                parseArgs(deleteRecords, {
                    objectKey: 'object_1',
                    recordIds: ['rec1', 'rec2'],
                    confirm: true,
                }),
                ctx,
            ),
        );
        assert.deepEqual(requests, [
            {
                apiPath: '/objects/object_1/records/rec1',
                method: 'DELETE',
                body: null,
            },
            {
                apiPath: '/objects/object_1/records/rec2',
                method: 'DELETE',
                body: null,
            },
        ]);
        assert.equal(payload.ok, false);
        assert.equal(payload.action, 'batch_delete_records');
        assert.equal(payload.requestedCount, 2);
        assert.equal(payload.successCount, 1);
        assert.equal(payload.failureCount, 1);
        assert.deepEqual(payload.results, [
            { recordId: 'rec1', ok: true, status: 200, body: { delete: true } },
            {
                recordId: 'rec2',
                ok: false,
                status: 404,
                body: { error: 'gone' },
            },
        ]);
        assert.match(String(payload.note), /partial failure/);
    });

    it('captures a thrown request as a per-record error', async () => {
        const { ctx } = setup({
            responses: () => {
                throw new Error('refused');
            },
        });
        const payload = payloadOf(
            await deleteRecords.handler(
                parseArgs(deleteRecords, {
                    objectKey: 'object_1',
                    recordIds: ['rec1'],
                    confirm: true,
                }),
                ctx,
            ),
        );
        assert.deepEqual(payload.results, [
            { recordId: 'rec1', ok: false, error: 'refused' },
        ]);
    });
});

describe('knack_upload_asset', () => {
    const originalFetch = globalThis.fetch;
    const tempDirs: string[] = [];

    afterEach(() => {
        globalThis.fetch = originalFetch;
        for (const dir of tempDirs.splice(0))
            fs.rmSync(dir, { recursive: true, force: true });
    });

    const makeTempDir = () => {
        const dir = fs.mkdtempSync(
            path.join(os.tmpdir(), 'knack-mcp-v2-upload-'),
        );
        tempDirs.push(dir);
        return dir;
    };

    it('reports a missing file without calling Knack', async () => {
        let fetched = false;
        globalThis.fetch = (async () => {
            fetched = true;
            return new Response('{}');
        }) as typeof fetch;
        const { ctx } = setup();
        const missing = path.join(makeTempDir(), 'missing.txt');
        const payload = payloadOf(
            await uploadAsset.handler(
                parseArgs(uploadAsset, { filePath: missing }),
                ctx,
            ),
        );
        assert.deepEqual(payload, {
            ok: false,
            status: 0,
            body: { error: 'file_not_found', filePath: missing },
        });
        assert.equal(fetched, false);
    });

    it('reports a directory as not a file', async () => {
        const { ctx } = setup();
        const dir = makeTempDir();
        const payload = payloadOf(
            await uploadAsset.handler(
                parseArgs(uploadAsset, { filePath: dir }),
                ctx,
            ),
        );
        assert.deepEqual(payload, {
            ok: false,
            status: 0,
            body: { error: 'not_a_file', filePath: dir },
        });
    });

    it('posts the file as multipart form data to the asset upload endpoint', async () => {
        const calls: Array<{ url: string; init: RequestInit }> = [];
        globalThis.fetch = (async (
            url: string | URL | Request,
            init?: RequestInit,
        ) => {
            calls.push({ url: String(url), init: init || {} });
            return new Response(
                JSON.stringify({ id: 'asset_1', type: 'image' }),
                {
                    status: 200,
                    headers: { 'content-type': 'application/json' },
                },
            );
        }) as typeof fetch;

        const { ctx } = setup();
        const filePath = path.join(makeTempDir(), 'photo.png');
        fs.writeFileSync(filePath, Buffer.from([1, 2, 3, 4]));

        const payload = payloadOf(
            await uploadAsset.handler(
                parseArgs(uploadAsset, { filePath, assetType: 'image' }),
                ctx,
            ),
        );

        assert.equal(calls.length, 1);
        assert.equal(
            calls[0].url,
            'https://api.knack.com/v1/applications/000000000000000000000000/assets/image/upload',
        );
        assert.equal(calls[0].init.method, 'POST');
        const headers = calls[0].init.headers as Record<string, string>;
        assert.equal(
            headers['X-Knack-Application-Id'],
            '000000000000000000000000',
        );
        assert.equal(headers['X-Knack-REST-API-Key'], 'test-key');
        assert.equal('Content-Type' in headers, false);
        const form = calls[0].init.body as FormData;
        const file = form.get('files') as File;
        assert.equal(file.name, 'photo.png');
        assert.equal(file.size, 4);

        assert.equal(payload.ok, true);
        assert.equal(payload.status, 200);
        assert.equal(payload.action, 'upload_asset');
        assert.equal(payload.appKey, 'Demo');
        assert.equal(payload.filePath, filePath);
        assert.equal(payload.fileName, 'photo.png');
        assert.equal(payload.sizeBytes, 4);
        assert.equal(payload.assetType, 'image');
        assert.deepEqual(payload.body, { id: 'asset_1', type: 'image' });
    });

    it('defaults assetType to file', async () => {
        const calls: string[] = [];
        globalThis.fetch = (async (url: string | URL | Request) => {
            calls.push(String(url));
            return new Response('{}', { status: 200 });
        }) as typeof fetch;
        const { ctx } = setup();
        const filePath = path.join(makeTempDir(), 'doc.txt');
        fs.writeFileSync(filePath, 'hello');
        const payload = payloadOf(
            await uploadAsset.handler(
                parseArgs(uploadAsset, { filePath }),
                ctx,
            ),
        );
        assert.match(calls[0], /\/assets\/file\/upload$/);
        assert.equal(payload.assetType, 'file');
    });
});
