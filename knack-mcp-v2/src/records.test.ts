import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    applyRecordReadPolicy,
    getDefaultPermittedFieldKeys,
    getPermittedReadFields,
} from './records.js';
import type { CachedObject } from './types.js';
import { RUNTIME_METADATA } from './testing/schema-fixture.js';
import { makeApp, makeFakeContext } from './testing/fake-context.js';

describe('getDefaultPermittedFieldKeys', () => {
    it('returns every field when there is no policy', () => {
        const app = makeApp();
        const object = (RUNTIME_METADATA.objects as CachedObject[])[0];
        assert.deepEqual(
            getDefaultPermittedFieldKeys(app, 'object_1', object).sort(),
            ['field_1', 'field_2', 'field_3', 'field_4', 'field_7'].sort(),
        );
    });

    it('drops a redacted field even when allowedFieldKeys also names it', () => {
        // The two lists overlapping is the case a config author would write to mean
        // "allowed in general, except this one" — redaction should win silently, not
        // make the field set invalid.
        const app = makeApp({
            dataAccess: {
                allowedFieldKeys: { object_1: ['field_1', 'field_3'] },
                redactedFieldKeys: ['field_3'],
            },
        });
        const object = (RUNTIME_METADATA.objects as CachedObject[])[0];
        assert.deepEqual(
            getDefaultPermittedFieldKeys(app, 'object_1', object),
            ['field_1'],
        );
    });

    it('drops an allowedFieldKeys entry the schema no longer has', () => {
        const app = makeApp({
            dataAccess: {
                allowedFieldKeys: { object_1: ['field_1', 'field_99'] },
            },
        });
        const object = (RUNTIME_METADATA.objects as CachedObject[])[0];
        assert.deepEqual(
            getDefaultPermittedFieldKeys(app, 'object_1', object),
            ['field_1'],
        );
    });
});

describe('applyRecordReadPolicy', () => {
    it('projects a redacted-and-allowed field out without throwing', async () => {
        const app = makeApp({
            dataAccess: {
                allowedFieldKeys: { object_1: ['field_1', 'field_3'] },
                redactedFieldKeys: ['field_3'],
            },
        });
        const { ctx } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { Demo: RUNTIME_METADATA },
        });
        const result = await applyRecordReadPolicy(ctx, app, 'object_1', {
            ok: true,
            status: 200,
            body: { id: 'rec1', field_1: 'Ada', field_3: 42, field_3_raw: 42 },
        });
        const body = result.body as Record<string, unknown>;
        assert.equal(body.field_1, 'Ada');
        assert.equal('field_3' in body, false);
        assert.equal('field_3_raw' in body, false);
    });

    it('still throws when a caller explicitly requests a redacted field', async () => {
        // The fix scopes to the policy's own default field derivation; a caller-driven
        // request (e.g. a filter or an explicit fieldKeys list) must still refuse.
        const app = makeApp({
            dataAccess: { redactedFieldKeys: ['field_3'] },
        });
        const { ctx } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { Demo: RUNTIME_METADATA },
        });
        await assert.rejects(
            getPermittedReadFields(ctx, app, 'object_1', ['field_3']),
            /field_3 is redacted/,
        );
    });

    it('is a no-op without a dataAccess policy', async () => {
        const app = makeApp();
        const { ctx } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { Demo: RUNTIME_METADATA },
        });
        const result = {
            ok: true,
            status: 200,
            body: { id: 'rec1', field_1: 'Ada' },
        };
        assert.equal(
            await applyRecordReadPolicy(ctx, app, 'object_1', result),
            result,
        );
    });
});
