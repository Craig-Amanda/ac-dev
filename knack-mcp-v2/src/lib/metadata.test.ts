import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { parseRuntimeSchema, getViewFieldSettings } from './metadata.js';
import { buildViewGroupField } from './view-templates.js';
import type { RuntimeMetadata } from '../types.js';

describe('parseRuntimeSchema connection cardinality', () => {
    function schemaWithRelationship(relationship: {
        has: string;
        belongs_to: string;
    }) {
        const metadata: RuntimeMetadata = {
            objects: [
                {
                    key: 'object_1',
                    name: 'Customers',
                    fields: [
                        {
                            key: 'field_1',
                            name: 'Company',
                            type: 'connection',
                            relationship: {
                                object: 'object_2',
                                ...relationship,
                            },
                        },
                    ],
                },
            ],
        };
        return parseRuntimeSchema(metadata)?.objects?.[0].fields?.[0];
    }

    it('a many-to-one field (has: one, belongs_to: many) is single-valued', () => {
        // The canonical shape for e.g. a Customer's single Company: many customers
        // belong to that one company, but THIS field — the one on Customer — holds
        // exactly one connected record. `belongs_to` describes the reciprocal field on
        // Company, not this one.
        const field = schemaWithRelationship({
            has: 'one',
            belongs_to: 'many',
        });
        assert.equal(field?.allowsMultiple, false);
    });

    it('a one-to-many field (has: many, belongs_to: one) is multi-valued', () => {
        const field = schemaWithRelationship({
            has: 'many',
            belongs_to: 'one',
        });
        assert.equal(field?.allowsMultiple, true);
    });

    it('a many-to-many field (has: many, belongs_to: many) is multi-valued', () => {
        const field = schemaWithRelationship({
            has: 'many',
            belongs_to: 'many',
        });
        assert.equal(field?.allowsMultiple, true);
    });

    it('a one-to-one field (has: one, belongs_to: one) is single-valued', () => {
        const field = schemaWithRelationship({ has: 'one', belongs_to: 'one' });
        assert.equal(field?.allowsMultiple, false);
    });
});

describe('getViewFieldSettings on a details/list layout', () => {
    it('finds fields nested at columns[].groups[].columns[][], the shape a details or list view actually uses', () => {
        // The exact shape buildViewTemplatePayload builds for `details`/`list`: one
        // outer column, one group, and an ARRAY of field items as the inner "column" —
        // not a container with its own .groups/.columns to recurse into.
        const attributes = {
            type: 'details',
            columns: [
                {
                    width: 100,
                    groups: [
                        {
                            columns: [
                                [
                                    buildViewGroupField({
                                        key: 'field_1',
                                        name: 'Name',
                                    }),
                                    buildViewGroupField({
                                        key: 'field_2',
                                        name: 'Email',
                                    }),
                                ],
                            ],
                        },
                    ],
                },
            ],
        };

        const settings = getViewFieldSettings(attributes);
        assert.equal(settings.configuredFieldCount, 2);
        assert.deepEqual(
            settings.fields.map((field) => field.fieldKey),
            ['field_1', 'field_2'],
        );
        assert.ok(
            settings.fields.every((field) => field.layout === 'view-column'),
        );
    });

    it('still finds table columns, which nest field items one level shallower', () => {
        const attributes = {
            type: 'table',
            columns: [
                { type: 'field', field: { key: 'field_1' }, header: 'Name' },
                { type: 'field', field: { key: 'field_2' }, header: 'Email' },
            ],
        };
        const settings = getViewFieldSettings(attributes);
        assert.deepEqual(
            settings.fields.map((field) => field.fieldKey),
            ['field_1', 'field_2'],
        );
    });
});
