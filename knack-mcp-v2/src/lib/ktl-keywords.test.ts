import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
    applyKtlKeywordEdits,
    isKtlKeywordName,
    parseKtlKeywordCluster,
    serializeKtlKeywordCluster,
} from './ktl-keywords.js';

test('isKtlKeywordName accepts underscore-prefixed identifiers', () => {
    assert.equal(isKtlKeywordName('_notes'), true);
    assert.equal(isKtlKeywordName('_ktlHide'), true);
    assert.equal(isKtlKeywordName('_show_for_2'), true);
});

test('isKtlKeywordName rejects anything not shaped like a keyword', () => {
    assert.equal(isKtlKeywordName('notes'), false);
    assert.equal(isKtlKeywordName('_'), false);
    assert.equal(isKtlKeywordName('_has space'), false);
    assert.equal(isKtlKeywordName('_bad=chars'), false);
    assert.equal(isKtlKeywordName(''), false);
});

test('parseKtlKeywordCluster splits prose from a trailing single keyword', () => {
    assert.deepEqual(parseKtlKeywordCluster('Customer name _ktlHide'), {
        prose: 'Customer name',
        keywords: [{ name: '_ktlHide', raw: '_ktlHide' }],
    });
});

test('parseKtlKeywordCluster bounds a multi-word value to the next keyword start', () => {
    assert.deepEqual(
        parseKtlKeywordCluster(
            'Customer name _notes=Craig on 2026-09-07 _ktlHide',
        ),
        {
            prose: 'Customer name',
            keywords: [
                { name: '_notes', raw: '_notes=Craig on 2026-09-07' },
                { name: '_ktlHide', raw: '_ktlHide' },
            ],
        },
    );
});

test('parseKtlKeywordCluster returns pure prose with no keywords', () => {
    assert.deepEqual(parseKtlKeywordCluster('Just a description'), {
        prose: 'Just a description',
        keywords: [],
    });
});

test('parseKtlKeywordCluster handles a description that is only keywords', () => {
    assert.deepEqual(parseKtlKeywordCluster('_ktlHide _showFor=manager'), {
        prose: '',
        keywords: [
            { name: '_ktlHide', raw: '_ktlHide' },
            { name: '_showFor', raw: '_showFor=manager' },
        ],
    });
});

test('serializeKtlKeywordCluster is the inverse of parseKtlKeywordCluster', () => {
    const original = 'Customer name _notes=Craig on 2026-09-07 _ktlHide';
    const { prose, keywords } = parseKtlKeywordCluster(original);
    assert.equal(serializeKtlKeywordCluster(prose, keywords), original);
});

test('serializeKtlKeywordCluster drops an empty prose cleanly', () => {
    assert.equal(
        serializeKtlKeywordCluster('', [{ name: '_ktlHide', raw: '_ktlHide' }]),
        '_ktlHide',
    );
});

test('applyKtlKeywordEdits appends a brand-new keyword at the end of the cluster', () => {
    const result = applyKtlKeywordEdits('Customer name _ktlHide', {
        _notes: 'Craig on 2026-09-07',
    });
    assert.equal(result, 'Customer name _ktlHide _notes=Craig on 2026-09-07');
});

test('applyKtlKeywordEdits updates an existing keyword in place, keeping siblings', () => {
    const result = applyKtlKeywordEdits(
        'Customer name _ktlHide _notes=Craig on 2026-09-01 _showFor=admin',
        { _notes: 'Craig on 2026-09-07' },
    );
    assert.equal(
        result,
        'Customer name _ktlHide _notes=Craig on 2026-09-07 _showFor=admin',
    );
});

test('applyKtlKeywordEdits adds a bare keyword with a null value', () => {
    const result = applyKtlKeywordEdits('Customer name', { _ktlHide: null });
    assert.equal(result, 'Customer name _ktlHide');
});

test('applyKtlKeywordEdits handles several edits at once, mixing add and update', () => {
    const result = applyKtlKeywordEdits('Customer name _ktlHide', {
        _ktlHide: null,
        _notes: 'Craig on 2026-09-07',
        _showFor: 'admin',
    });
    assert.equal(
        result,
        'Customer name _ktlHide _notes=Craig on 2026-09-07 _showFor=admin',
    );
});

test('applyKtlKeywordEdits on text with no prose still appends correctly', () => {
    const result = applyKtlKeywordEdits('_ktlHide', {
        _notes: 'Craig on 2026-09-07',
    });
    assert.equal(result, '_ktlHide _notes=Craig on 2026-09-07');
});

test('applyKtlKeywordEdits on empty text with one edit produces just that keyword', () => {
    const result = applyKtlKeywordEdits('', { _ktlHide: null });
    assert.equal(result, '_ktlHide');
});
