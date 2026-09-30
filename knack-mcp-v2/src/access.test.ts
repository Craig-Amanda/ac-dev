import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { assertAccess, isAdvertised } from './access.js';
import { makeApp } from './testing/fake-context.js';

describe('assertAccess', () => {
    const open = makeApp();
    const locked = makeApp({
        readonly: true,
        allowDelete: false,
        allowViewMutation: false,
        allowDiagnostics: false,
    });

    it('lets read through on any app', () => {
        assertAccess(locked, 'read', {});
    });

    it('refuses every write on an app that is not readonly:false', () => {
        for (const level of [
            'write',
            'delete',
            'view',
            'view-delete',
        ] as const) {
            assert.throws(() => assertAccess(locked, level, {}), /is readonly/);
        }
    });

    it('needs the specific opt-in beyond writable', () => {
        const writable = makeApp({
            allowDelete: false,
            allowViewMutation: false,
        });
        assertAccess(writable, 'write', {});
        assert.throws(
            () => assertAccess(writable, 'delete', {}),
            /does not allow deletions/,
        );
        assert.throws(
            () => assertAccess(writable, 'view', {}),
            /does not allow view mutations/,
        );
        assert.doesNotThrow(() => assertAccess(makeApp(), 'view-delete', {}));
    });

    it('view-delete needs both view mutation and delete', () => {
        const noDelete = makeApp({ allowDelete: false });
        assert.throws(
            () => assertAccess(noDelete, 'view-delete', {}),
            /does not allow deletions/,
        );
    });

    it('enforced read-only wins over app.json', () => {
        assert.throws(
            () => assertAccess(open, 'write', { readOnly: true }),
            /enforced read-only/,
        );
        assert.throws(
            () => assertAccess(open, 'diagnostic', { readOnly: true }),
            /without diagnostic tools/,
        );
    });

    it('diagnostic is its own opt-in and does not need writable', () => {
        const readOnlyDiag = makeApp({
            readonly: true,
            allowDiagnostics: true,
        });
        assertAccess(readOnlyDiag, 'diagnostic', {});
        assert.throws(
            () => assertAccess(locked, 'diagnostic', {}),
            /does not allow diagnostic/,
        );
    });
});

describe('isAdvertised', () => {
    it('advertises a level when any app opts in, and never in enforced read-only', () => {
        const apps = [
            makeApp({
                readonly: true,
                allowViewMutation: false,
                allowDiagnostics: false,
                allowDelete: false,
            }),
            makeApp({ appKey: 'B' }),
        ];
        assert.equal(isAdvertised('write', apps, {}), true);
        assert.equal(isAdvertised('view', apps, {}), true);
        assert.equal(isAdvertised('diagnostic', apps, {}), true);
        assert.equal(isAdvertised('write', apps, { readOnly: true }), false);
        assert.equal(isAdvertised('read', apps, { readOnly: true }), true);
    });

    it('withholds a level nobody opted into', () => {
        const apps = [
            makeApp({ allowViewMutation: false, allowDiagnostics: false }),
        ];
        assert.equal(isAdvertised('view', apps, {}), false);
        assert.equal(isAdvertised('view-delete', apps, {}), false);
        assert.equal(isAdvertised('diagnostic', apps, {}), false);
        assert.equal(isAdvertised('delete', apps, {}), true);
    });
});

describe('the audit access level', () => {
    it('is its own opt-in, off by default', () => {
        assert.throws(
            () => assertAccess(makeApp(), 'audit', {}),
            /does not allow the exposure audit\. Set "allowAudit": true/,
        );
        assertAccess(makeApp({ allowAudit: true }), 'audit', {});
    });

    it('is advertised only when an app opts in, and in enforced read-only mode too', () => {
        assert.equal(isAdvertised('audit', [makeApp()], {}), false);
        const auditing = [makeApp(), makeApp({ allowAudit: true })];
        assert.equal(isAdvertised('audit', auditing, {}), true);
        assert.equal(isAdvertised('audit', auditing, { readOnly: true }), true);
        assertAccess(makeApp({ allowAudit: true }), 'audit', {
            readOnly: true,
        });
    });
});
