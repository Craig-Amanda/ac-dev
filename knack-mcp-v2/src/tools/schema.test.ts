import assert from 'node:assert/strict';
import { test } from 'node:test';

import { makeCacheEntry } from '../lib/cache.js';
import { parseRuntimeFieldMap, parseRuntimeSchema } from '../lib/metadata.js';
import {
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import { RUNTIME_METADATA } from '../testing/schema-fixture.js';
import {
    checkDuplicateFieldUsage,
    describeFieldShape,
    generateSnapshotStructure,
    getField,
    getObject,
    getObjectConnections,
    listObjects,
    resolve,
    schemaTools,
    validateFieldMapping,
} from './schema.js';

const RAW_OBJECT_1 = {
    key: 'object_1',
    name: 'Customers',
    fields: [
        {
            key: 'field_1',
            _id: 'abc',
            name: 'Name',
            type: 'short_text',
            meta: { description: 'Customer name _ktlHide' },
        },
        {
            key: 'field_2',
            _id: 'def',
            name: 'Total',
            type: 'equation',
            format: { equation: '{field_3} * 2' },
            rules: [],
        },
    ],
};

function setup(overrides: Parameters<typeof makeFakeContext>[0] = {}) {
    const made = makeFakeContext({
        runtimeMetadata: { Demo: RUNTIME_METADATA },
        responses: {
            'GET /objects/object_1': {
                ok: true,
                status: 200,
                body: { object: RAW_OBJECT_1 },
            },
        },
        ...overrides,
    });
    made.ctx.state.activeAppKey = 'Demo';
    return made;
}

test('fixture parses into a schema and a field map', () => {
    const schema = parseRuntimeSchema(RUNTIME_METADATA);
    assert.equal(schema?.objects?.length, 2);
    const company = schema?.objects?.[0].fields?.find(
        (f) => f.key === 'field_4',
    );
    assert.equal(company?.connectedObject, 'object_2');
    assert.equal(company?.allowsMultiple, false);
    const fieldMap = parseRuntimeFieldMap(RUNTIME_METADATA);
    assert.equal(fieldMap?.['object_1.name']?.fieldKey, 'field_1');
    assert.equal(fieldMap?.['object_2.company_name']?.fieldKey, 'field_5');
});

test('schemaTools has unique names and read access throughout', () => {
    const names = schemaTools.map((t) => t.name);
    assert.equal(new Set(names).size, names.length);
    assert.ok(schemaTools.every((t) => t.access === 'read'));
    assert.ok(schemaTools.every((t) => t.description.length <= 120));
});

// ---------------------------------------------------------------- knack_list_objects

test('knack_list_objects lists key, name and field count', async () => {
    const { ctx } = setup();
    const payload = payloadOf(await listObjects.handler({}, ctx));
    assert.equal(payload.ok, true);
    assert.equal(payload.source, 'runtime');
    assert.equal(payload.objectCount, 2);
    assert.deepEqual(payload.objects, [
        { key: 'object_1', name: 'Customers', fieldCount: 5 },
        { key: 'object_2', name: 'Companies', fieldCount: 2 },
    ]);
});

test('knack_list_objects reports a missing schema', async () => {
    const { ctx } = setup({ runtimeMetadata: { Demo: null } });
    const payload = payloadOf(await listObjects.handler({}, ctx));
    assert.equal(payload.ok, false);
    assert.equal(
        payload.message,
        'No schema available from runtime API or schema.json.',
    );
});

// ---------------------------------------------------------------- knack_get_object

test('knack_get_object detail=fields (default) lists fields with builder URLs', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await getObject.handler(
            { objectKey: 'object_1', detail: 'fields' },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.objectKey, 'object_1');
    assert.equal(payload.objectName, 'Customers');
    const fields = payload.fields as Array<Record<string, unknown>>;
    assert.equal(fields.length, 5);
    assert.deepEqual(fields[0], {
        key: 'field_1',
        name: 'Name',
        type: 'short_text',
        required: true,
        description: 'Customer name _ktlHide',
        builderUrl:
            'https://builder.knack.com/acme/demo-app/schema/list/objects/object_1/fields/field_1/settings',
    });
});

test('knack_get_object detail=summary wraps the object with fieldCount', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await getObject.handler(
            { objectKey: 'object_2', detail: 'summary' },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
    const object = payload.object as Record<string, unknown>;
    assert.equal(object.key, 'object_2');
    assert.equal(object.name, 'Companies');
    assert.equal(object.fieldCount, 2);
    assert.equal((object.fields as unknown[]).length, 2);
});

test('knack_get_object detail=types groups fields by type', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await getObject.handler(
            { objectKey: 'object_1', detail: 'types' },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.schemaSource, 'runtime');
    assert.equal(payload.fieldCount, 5);
    assert.deepEqual(payload.typeSummary, [
        { fieldType: 'connection', count: 2 },
        { fieldType: 'equation', count: 1 },
        { fieldType: 'number', count: 1 },
        { fieldType: 'short_text', count: 1 },
    ]);
    assert.deepEqual((payload.fields as unknown[])[0], {
        fieldKey: 'field_1',
        fieldName: 'Name',
        fieldType: 'short_text',
    });
});

test('knack_get_object reports an unknown object with the available keys', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await getObject.handler(
            { objectKey: 'object_9', detail: 'fields' },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.message, 'Object not found in schema: object_9');
    assert.deepEqual(payload.availableObjectKeys, ['object_1', 'object_2']);
});

test('knack_get_object detail=raw fetches the object over REST and inlines the body', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await getObject.handler({ objectKey: 'object_1', detail: 'raw' }, ctx),
    );
    assert.deepEqual(requests, [
        { apiPath: '/objects/object_1', method: 'GET', body: null },
    ]);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'get_raw_object');
    assert.equal(payload.bodyIncluded, true);
    assert.deepEqual(payload.body, { object: RAW_OBJECT_1 });
});

test('knack_get_object detail=rawMetadata returns the unparsed metadata object', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await getObject.handler(
            { objectKey: 'object_2', detail: 'rawMetadata' },
            ctx,
        ),
    );
    assert.equal(requests.length, 0);
    assert.equal(payload.ok, true);
    assert.equal(payload.source, 'runtime');
    assert.equal(payload.rawObjectIncluded, true);
    assert.deepEqual(
        payload.rawObject,
        (RUNTIME_METADATA.objects as unknown[])[1],
    );
});

test('knack_get_object detail=rawMetadata reports an unknown object', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await getObject.handler(
            { objectKey: 'object_9', detail: 'rawMetadata' },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.equal(
        payload.message,
        'Object not found in runtime metadata: object_9',
    );
    assert.deepEqual(payload.availableObjectKeys, ['object_1', 'object_2']);
});

test('knack_get_object raw modes require allowDiagnostics', async () => {
    const { ctx, requests } = setup({
        apps: [makeApp({ allowDiagnostics: false })],
    });
    for (const detail of ['raw', 'rawMetadata'] as const) {
        await assert.rejects(
            getObject.handler({ objectKey: 'object_1', detail }, ctx),
            /does not allow diagnostic tools/,
        );
    }
    assert.equal(requests.length, 0);
    // The schema-backed modes are unaffected by the diagnostic toggle.
    const payload = payloadOf(
        await getObject.handler(
            { objectKey: 'object_1', detail: 'fields' },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
});

test('knack_get_object raw modes are refused in enforced read-only mode', async () => {
    const { ctx } = setup({ options: { readOnly: true } });
    await assert.rejects(
        getObject.handler({ objectKey: 'object_1', detail: 'raw' }, ctx),
        /enforced read-only mode without diagnostic tools/,
    );
});

// ---------------------------------------------------------------- knack_get_field

test('knack_get_field returns the raw field from the object endpoint', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await getField.handler(
            { objectKey: 'object_1', fieldKey: 'field_2' },
            ctx,
        ),
    );
    assert.deepEqual(requests, [
        { apiPath: '/objects/object_1', method: 'GET', body: null },
    ]);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'get_field');
    assert.deepEqual(payload.field, RAW_OBJECT_1.fields[1]);
});

test('knack_get_field lists available keys when the field is missing', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await getField.handler(
            { objectKey: 'object_1', fieldKey: 'field_99' },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.message, 'Field field_99 not found on object_1.');
    assert.deepEqual(payload.availableFieldKeys, ['field_1', 'field_2']);
});

test('knack_get_field surfaces a failed object fetch', async () => {
    const { ctx } = setup({ responses: {} });
    const payload = payloadOf(
        await getField.handler(
            { objectKey: 'object_1', fieldKey: 'field_1' },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.status, 404);
    assert.equal(
        payload.message,
        'Could not fetch object object_1 from the Knack API.',
    );
});

// ---------------------------------------------------------------- knack_resolve

test('knack_resolve resolves a field key to its object, name and type', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await resolve.handler({ identifier: ' field_4 ' }, ctx),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.identifier, 'field_4');
    assert.equal(payload.resolvedBy, 'fieldKey');
    assert.equal(payload.fieldMapSource, null);
    assert.equal(payload.matchCount, 1);
    assert.deepEqual(payload.primary, {
        objectKey: 'object_1',
        objectName: 'Customers',
        fieldKey: 'field_4',
        fieldName: 'Company',
        fieldType: 'connection',
        builderUrl:
            'https://builder.knack.com/acme/demo-app/schema/list/objects/object_1/fields/field_4/settings',
    });
    assert.equal(payload.note, undefined);
});

test('knack_resolve resolves a field key given in the wrong case', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await resolve.handler({ identifier: 'Field_4' }, ctx),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.resolvedFieldKey, 'field_4');
    assert.equal(payload.matchCount, 1);
});

test('knack_resolve resolves an alias through the field map', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await resolve.handler({ identifier: 'object_2.revenue' }, ctx),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.resolvedBy, 'alias');
    assert.equal(payload.resolvedFieldKey, 'field_6');
    assert.equal(payload.fieldMapSource, 'runtime');
    assert.equal(
        (payload.primary as Record<string, unknown>).fieldType,
        'number',
    );
});

test('knack_resolve adds the stale-cache note when the alias came from disk', async () => {
    const { ctx } = setup();
    const fieldMap = parseRuntimeFieldMap(RUNTIME_METADATA);
    assert.ok(fieldMap);
    ctx.caches.fieldMap.set('Demo', makeCacheEntry(fieldMap, 'file'));
    const payload = payloadOf(
        await resolve.handler({ identifier: 'object_1.name' }, ctx),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.fieldMapSource, 'file');
    assert.match(String(payload.note), /on-disk fieldMap.json cache/);
});

test('knack_resolve lists aliases when the identifier is unknown', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await resolve.handler({ identifier: 'object_1.nope' }, ctx),
    );
    assert.equal(payload.ok, false);
    assert.equal(
        payload.message,
        'Identifier not found as alias or field key.',
    );
    assert.ok((payload.availableAliases as string[]).includes('object_1.name'));
});

test('knack_resolve scopes the lookup by objectKey', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await resolve.handler(
            { identifier: 'field_1', objectKey: 'object_2' },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.resolvedFieldKey, 'field_1');
    assert.equal(
        payload.message,
        'Resolved field not found in schema for object object_2: field_1',
    );
});

test('knack_resolve rejects an empty identifier', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await resolve.handler({ identifier: '   ' }, ctx),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.message, 'identifier cannot be empty.');
});

// ---------------------------------------------------------------- knack_get_object_connections

test('knack_get_object_connections names the connected objects', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await getObjectConnections.handler({ objectKey: 'object_1' }, ctx),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.connectionCount, 2);
    assert.deepEqual(payload.connections, [
        {
            fieldKey: 'field_4',
            fieldName: 'Company',
            connectedObjectKey: 'object_2',
            connectedObjectName: 'Companies',
        },
        {
            fieldKey: 'field_7',
            fieldName: 'Tags',
            connectedObjectKey: 'object_2',
            connectedObjectName: 'Companies',
        },
    ]);
    assert.equal(payload.note, null);
});

test('knack_get_object_connections reports an unknown object', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await getObjectConnections.handler({ objectKey: 'object_9' }, ctx),
    );
    assert.equal(payload.ok, false);
    assert.deepEqual(payload.availableObjectKeys, ['object_1', 'object_2']);
});

// ---------------------------------------------------------------- knack_describe_field_shape

test('knack_describe_field_shape describes a known type', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await describeFieldShape.handler({ fieldType: 'connection' }, ctx),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.fieldType, 'connection');
    assert.ok(payload.valueShape);
    assert.ok(payload.definitionShape);
    assert.ok(payload.conditionalRules);
});

test('knack_describe_field_shape lists known types for an unknown one', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await describeFieldShape.handler({ fieldType: 'nonsense' }, ctx),
    );
    assert.equal(payload.ok, false);
    assert.ok((payload.knownTypes as string[]).includes('short_text'));
});

// ---------------------------------------------------------------- knack_validate_field_mapping

test('knack_validate_field_mapping resolves keys and aliases and flags problems', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await validateFieldMapping.handler(
            {
                mappingObject: {
                    name: 'object_1.name',
                    alsoName: 'field_1',
                    amount: 'field_3',
                    ghost: 'field_999',
                    typo: 'object_1.nmae',
                },
            },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.totalMappings, 5);
    assert.equal(payload.validMappings, 3);
    assert.deepEqual(payload.resolvedMapping, {
        name: 'field_1',
        alsoName: 'field_1',
        amount: 'field_3',
    });
    assert.deepEqual(payload.duplicateResolvedFields, [
        { fieldKey: 'field_1', mappingKeys: ['name', 'alsoName'] },
    ]);
    assert.deepEqual(payload.invalidMappings, [
        {
            mappingKey: 'ghost',
            input: 'field_999',
            reason: 'Resolved to field_999, but that field does not exist in schema.',
        },
        {
            mappingKey: 'typo',
            input: 'object_1.nmae',
            reason: 'Not a field key and alias was not found in fieldMap.',
        },
    ]);
});

test('knack_validate_field_mapping is ok when everything resolves', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await validateFieldMapping.handler(
            { mappingObject: { a: 'field_5' } },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.schemaSource, 'runtime');
    assert.equal(payload.fieldMapSource, 'runtime');
});

test('knack_validate_field_mapping resolves a field key given in the wrong case', async () => {
    // The pattern that recognises a direct field key is case-insensitive, but the
    // schema's own keys are always lowercase — normalising here is what makes the
    // match against validFieldKeys succeed instead of reporting a field that does
    // exist as though it did not.
    const { ctx } = setup();
    const payload = payloadOf(
        await validateFieldMapping.handler(
            { mappingObject: { a: 'Field_1' } },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
    assert.deepEqual(payload.resolvedMapping, { a: 'field_1' });
    assert.deepEqual(payload.invalidMappings, []);
});

// ---------------------------------------------------------------- knack_generate_snapshot_structure

test('knack_generate_snapshot_structure builds null templates by key and name', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await generateSnapshotStructure.handler({ objectKey: 'object_2' }, ctx),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.fieldCount, 2);
    assert.deepEqual(payload.snapshotByFieldKey, {
        field_5: null,
        field_6: null,
    });
    assert.deepEqual(payload.snapshotByFieldName, {
        'Company Name': null,
        Revenue: null,
    });
});

test('knack_generate_snapshot_structure reports an unknown object', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await generateSnapshotStructure.handler({ objectKey: 'object_9' }, ctx),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.message, 'Object not found in schema: object_9');
});

// ---------------------------------------------------------------- knack_check_duplicate_field_usage

test('knack_check_duplicate_field_usage reports mapping duplicates and invalid entries', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await checkDuplicateFieldUsage.handler(
            {
                mappingObject: {
                    a: 'field_1',
                    b: 'object_1.name',
                    c: 'field_3',
                    d: 'bogus',
                },
            },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
    assert.equal(payload.mappingProvided, true);
    assert.equal(payload.fieldMapDuplicateCount, 0);
    assert.equal(payload.mappingDuplicateCount, 1);
    assert.deepEqual(payload.mappingDuplicates, [
        { fieldKey: 'field_1', mappingKeys: ['a', 'b'] },
    ]);
    assert.deepEqual(payload.mappingInvalidEntries, [
        {
            mappingKey: 'd',
            input: 'bogus',
            reason: 'Not a field key and alias was not found in fieldMap.',
        },
    ]);
});

test('knack_check_duplicate_field_usage works without a mapping', async () => {
    const { ctx } = setup();
    const payload = payloadOf(await checkDuplicateFieldUsage.handler({}, ctx));
    assert.equal(payload.ok, true);
    assert.equal(payload.mappingProvided, false);
    assert.deepEqual(payload.mappingDuplicates, []);
    assert.deepEqual(payload.mappingInvalidEntries, []);
});

test('knack_check_duplicate_field_usage reports a missing schema', async () => {
    const { ctx } = setup({ runtimeMetadata: { Demo: null } });
    const payload = payloadOf(await checkDuplicateFieldUsage.handler({}, ctx));
    assert.equal(payload.ok, false);
});
