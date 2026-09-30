import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { buildFieldExclusions } from './field-exclusion.js';
import { readFieldDescription } from './field-description.js';
import { parseRuntimeSchema } from './metadata.js';

describe('readFieldDescription', () => {
    it('trusts meta.description over a top-level copy the builder has left behind', () => {
        assert.equal(
            readFieldDescription({
                description: 'old _mcp_nodata',
                meta: { description: 'new _mcp_nodata _mcp_schemalock' },
            }),
            'new _mcp_nodata _mcp_schemalock',
        );
    });

    it('uses the top-level copy only when there is no meta.description', () => {
        assert.equal(
            readFieldDescription({ description: 'only top' }),
            'only top',
        );
        assert.equal(
            readFieldDescription({ description: 'top', meta: {} }),
            'top',
        );
    });

    it('reads a description a person cleared in the builder as empty', () => {
        assert.equal(
            readFieldDescription({
                description: 'left behind _mcp_schemalock',
                meta: { description: '' },
            }),
            '',
        );
    });

    it('is empty for a field with no description or no field at all', () => {
        assert.equal(readFieldDescription({ key: 'field_1' }), '');
        assert.equal(readFieldDescription(undefined), '');
    });
});

describe('the cached schema and the two copies of a description', () => {
    const schema = (field: Record<string, unknown>) =>
        parseRuntimeSchema({
            objects: [
                {
                    key: 'object_1',
                    name: 'Clients',
                    fields: [
                        {
                            key: 'field_1',
                            name: 'Name',
                            type: 'short_text',
                            ...field,
                        },
                    ],
                },
            ],
        });

    it('applies a keyword added in the builder, which only meta.description carries', () => {
        const ex = buildFieldExclusions(
            schema({
                description: 'Name',
                meta: {
                    description: 'Name <br>_mcp_nodata<br>_mcp_schemalock<br>',
                },
            }),
            undefined,
        );
        assert.ok(ex.masked.has('field_1'));
        assert.ok(ex.schemaLocked.has('field_1'));
    });

    it('stops applying a keyword a person removed, which the top-level copy still shows', () => {
        const ex = buildFieldExclusions(
            schema({
                description: 'Name _mcp_nodata',
                meta: { description: 'Name' },
            }),
            undefined,
        );
        assert.ok(!ex.masked.has('field_1'));
    });
});
