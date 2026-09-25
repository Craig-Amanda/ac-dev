import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { z } from 'zod';

import type { AnyToolDef } from '../registry.js';
import {
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import type { RuntimeMetadata } from '../types.js';
import { editFieldRules } from './fields.js';

const parseArgs = (tool: AnyToolDef, raw: Record<string, unknown>) =>
    z.object(tool.input).parse(raw);

const CONDITIONAL_RULE = {
    key: '1',
    values: [
        {
            type: 'value',
            field: 'field_2',
            value: 'Auto',
            connection_field: null,
        },
    ],
    criteria: [{ field: 'field_1', value: 'x', operator: 'is' }],
};
const VALIDATION_RULE = {
    key: '1',
    value: '',
    message: 'No bad',
    criteria: [{ field: 'field_2', value: 'bad', operator: 'is' }],
};

/** A fake Knack whose field PUT merges at the top level, as measured live. */
function setup(fieldOverrides: Record<string, unknown> = {}) {
    const fields: Array<Record<string, unknown>> = [
        { key: 'field_1', name: 'Name', type: 'short_text' },
        {
            key: 'field_2',
            name: 'Status',
            type: 'short_text',
            conditional: true,
            rules: [CONDITIONAL_RULE],
            validation: [VALIDATION_RULE],
            meta: { description: 'keep me' },
            ...fieldOverrides,
        },
        {
            key: 'field_3',
            name: 'Secret',
            type: 'short_text',
            meta: { description: '_mcp_hidden' },
        },
    ];
    const metadata: RuntimeMetadata = {
        objects: [{ key: 'object_1', name: 'Staff', fields }],
    };
    const app = makeApp();
    const fake = makeFakeContext({
        apps: [app],
        runtimeMetadata: { [app.appKey]: metadata },
        responses: (apiPath, init) => {
            if (apiPath === '/objects/object_1' && !init?.method) {
                return {
                    ok: true,
                    status: 200,
                    body: { object: { key: 'object_1', fields } },
                };
            }
            if (
                apiPath === '/objects/object_1/fields/field_2' &&
                init?.method === 'PUT'
            ) {
                Object.assign(fields[1], JSON.parse(init.body as string));
                return { ok: true, status: 200, body: { field: fields[1] } };
            }
            return { ok: false, status: 404, body: {} };
        },
    });
    fake.ctx.state.activeAppKey = app.appKey;
    return { ...fake, fields };
}

const run = (
    ctx: ReturnType<typeof setup>['ctx'],
    args: Record<string, unknown>,
) =>
    editFieldRules
        .handler(
            parseArgs(editFieldRules, {
                objectKey: 'object_1',
                fieldKey: 'field_2',
                ...args,
            }),
            ctx,
        )
        .then(payloadOf);

describe('knack_edit_field_rules', () => {
    it('is a write', () => {
        assert.equal(editFieldRules.access, 'write');
    });

    it('adds a conditional rule under the next key and sends only that rule set', async () => {
        const { ctx, requests } = setup();
        const result = await run(ctx, {
            ruleSet: 'conditional',
            addRules: JSON.stringify([
                {
                    values: [
                        { type: 'value', field: 'field_2', value: 'Other' },
                    ],
                    criteria: [
                        { field: 'field_1', value: 'y', operator: 'is' },
                    ],
                },
            ]),
        });
        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.verified, true);
        assert.deepEqual(result.addedKeys, ['2']);
        const put = requests.find((request) => request.method === 'PUT');
        const body = put?.body as Record<string, unknown>;
        assert.deepEqual(Object.keys(body).sort(), ['conditional', 'rules']);
        assert.equal(body.conditional, true);
        assert.equal((body.rules as unknown[]).length, 2);
    });

    it('removes the last conditional rule and turns conditional off', async () => {
        const { ctx, requests, fields } = setup();
        const result = await run(ctx, {
            ruleSet: 'conditional',
            removeKeys: ['1'],
        });
        assert.equal(result.verified, true, JSON.stringify(result));
        const body = requests.find((request) => request.method === 'PUT')
            ?.body as Record<string, unknown>;
        assert.deepEqual(body, { rules: [], conditional: false });
        // The measured merge: validation rules and the description are untouched.
        assert.deepEqual(fields[1].validation, [VALIDATION_RULE]);
        assert.deepEqual(fields[1].meta, { description: 'keep me' });
    });

    it('replaces a validation rule in place', async () => {
        const { ctx, requests } = setup();
        const replacement = { ...VALIDATION_RULE, message: 'Still no' };
        const result = await run(ctx, {
            ruleSet: 'validation',
            replaceRules: JSON.stringify([replacement]),
        });
        assert.equal(result.verified, true, JSON.stringify(result));
        const body = requests.find((request) => request.method === 'PUT')
            ?.body as Record<string, unknown>;
        assert.deepEqual(body, { validation: [replacement] });
    });

    it('previewOnly sends nothing', async () => {
        const { ctx, requests } = setup();
        const result = await run(ctx, {
            ruleSet: 'validation',
            removeKeys: ['1'],
            previewOnly: true,
        });
        assert.equal(result.previewOnly, true);
        assert.equal(
            requests.some((request) => request.method === 'PUT'),
            false,
        );
    });

    it('refuses an unknown key, a hidden field, an empty edit and a locked field', async () => {
        const { ctx, requests } = setup();
        assert.equal(
            (await run(ctx, { ruleSet: 'conditional', removeKeys: ['9'] }))
                .error,
            'INVALID_EDIT',
        );
        assert.equal(
            (
                await run(ctx, {
                    ruleSet: 'validation',
                    addRules: JSON.stringify([
                        {
                            criteria: [
                                {
                                    field: 'field_3',
                                    value: 'x',
                                    operator: 'is',
                                },
                            ],
                            message: 'x',
                        },
                    ]),
                })
            ).error,
            'HIDDEN_FIELD',
        );
        // A conditional value copying the hidden field through `input`, and a
        // criterion reaching it across a connection path, are refused as well.
        for (const rule of [
            {
                criteria: [],
                values: [
                    { type: 'record', field: 'field_2', input: 'field_3' },
                ],
            },
            {
                criteria: [
                    { field: 'field_1.field_3', operator: 'is', value: 'x' },
                ],
                values: [{ type: 'value', field: 'field_2', value: 'y' }],
            },
        ]) {
            assert.equal(
                (
                    await run(ctx, {
                        ruleSet: 'conditional',
                        addRules: JSON.stringify([rule]),
                    })
                ).error,
                'HIDDEN_FIELD',
                JSON.stringify(rule),
            );
        }
        assert.equal(
            (await run(ctx, { ruleSet: 'validation' })).error,
            'NOTHING_TO_CHANGE',
        );

        const locked = setup({ meta: { description: '_mcp_schemalock' } });
        const refused = await run(locked.ctx, {
            ruleSet: 'validation',
            removeKeys: ['1'],
        });
        assert.equal(refused.ok, false);
        assert.match(JSON.stringify(refused.errors), /schema-locked/);
        assert.equal(
            [...requests, ...locked.requests].some(
                (request) => request.method === 'PUT',
            ),
            false,
        );
    });
});
