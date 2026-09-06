import type { RuntimeMetadata } from '../types.js';

/**
 * One runtime-metadata payload shaped like Knack's public application endpoint: two
 * objects, a crossable many-to-one connection, a one-to-many connection and an equation
 * field. It parses through parseRuntimeSchema / parseRuntimeFieldMap like production.
 */
export const RUNTIME_METADATA: RuntimeMetadata = {
    application: {
        name: 'Demo App',
        slug: 'demo-app',
        account: { slug: 'acme' },
    },
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
                    meta: { description: 'Customer name _ktlHide' },
                },
                {
                    key: 'field_2',
                    name: 'Total',
                    type: 'equation',
                    format: { equation: '{field_3} * 2' },
                },
                { key: 'field_3', name: 'Amount', type: 'number' },
                {
                    key: 'field_4',
                    name: 'Company',
                    type: 'connection',
                    relationship: {
                        object: 'object_2',
                        has: 'one',
                        belongs_to: 'one',
                    },
                },
                {
                    key: 'field_7',
                    name: 'Tags',
                    type: 'connection',
                    relationship: {
                        object: 'object_2',
                        has: 'many',
                        belongs_to: 'many',
                    },
                },
            ],
        },
        {
            key: 'object_2',
            name: 'Companies',
            fields: [
                { key: 'field_5', name: 'Company Name', type: 'short_text' },
                { key: 'field_6', name: 'Revenue', type: 'number' },
            ],
        },
    ],
    scenes: [],
};
