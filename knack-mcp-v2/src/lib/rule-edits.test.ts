import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { applyRuleEdit, readRuleArray } from './rule-edits.js';

const RULES = [
    { key: 'submit_0', action: 'hide_views', view_keys: ['view_1'] },
    { key: 'submit_1', action: 'message', message: 'Hello' },
    { key: 'submit_2', action: 'redirect', url: 'https://example.com' },
];

describe('applyRuleEdit', () => {
    it('removes by key and keeps the others in order', () => {
        const result = applyRuleEdit(RULES, { removeKeys: ['submit_1'] });
        assert.deepEqual(
            result.rules.map((rule) => rule.key),
            ['submit_0', 'submit_2'],
        );
        assert.deepEqual(result.removedKeys, ['submit_1']);
    });

    it('replaces in place, keeping position', () => {
        const replacement = {
            key: 'submit_0',
            action: 'message',
            message: 'Hi',
        };
        const result = applyRuleEdit(RULES, { replaceRules: [replacement] });
        assert.deepEqual(result.rules[0], replacement);
        assert.deepEqual(result.rules.slice(1), RULES.slice(1));
        assert.deepEqual(result.replacedKeys, ['submit_0']);
    });

    it('removes and replaces in one edit', () => {
        const result = applyRuleEdit(RULES, {
            removeKeys: ['submit_2'],
            replaceRules: [
                { key: 'submit_1', action: 'message', message: 'X' },
            ],
        });
        assert.equal(result.rules.length, 2);
        assert.equal(result.rules[1].message, 'X');
    });

    it('can remove every rule', () => {
        const result = applyRuleEdit(RULES, {
            removeKeys: ['submit_0', 'submit_1', 'submit_2'],
        });
        assert.deepEqual(result.rules, []);
    });

    it('refuses an unknown key, naming the stored ones', () => {
        assert.throws(
            () => applyRuleEdit(RULES, { removeKeys: ['submit_9'] }),
            /"submit_9" is not stored\. Stored keys: submit_0, submit_1, submit_2/,
        );
    });

    it('refuses a key both removed and replaced', () => {
        assert.throws(
            () =>
                applyRuleEdit(RULES, {
                    removeKeys: ['submit_0'],
                    replaceRules: [{ key: 'submit_0' }],
                }),
            /named more than once/,
        );
    });

    it('refuses a replacement without a key', () => {
        assert.throws(
            () =>
                applyRuleEdit(RULES, { replaceRules: [{ action: 'message' }] }),
            /must carry the key/,
        );
    });

    it('refuses an empty edit', () => {
        assert.throws(() => applyRuleEdit(RULES, {}), /Pass removeKeys/);
    });
});

describe('readRuleArray', () => {
    it('keeps objects only', () => {
        assert.deepEqual(readRuleArray([{ key: 'a' }, 'x', null]), [
            { key: 'a' },
        ]);
        assert.deepEqual(readRuleArray(undefined), []);
    });
});
