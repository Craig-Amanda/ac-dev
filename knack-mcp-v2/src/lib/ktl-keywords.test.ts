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
        keywords: [{ name: '_ktlHide', raw: '_ktlHide', separator: ' ' }],
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
                {
                    name: '_notes',
                    raw: '_notes=Craig on 2026-09-07',
                    separator: ' ',
                },
                { name: '_ktlHide', raw: '_ktlHide', separator: ' ' },
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
            // Empty, not a space: this one starts the text, nothing preceded it.
            { name: '_ktlHide', raw: '_ktlHide', separator: '' },
            { name: '_showFor', raw: '_showFor=manager', separator: ' ' },
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

/**
 * Separator preservation. The seventh defect found in the Noah's Place investigation, and
 * the only one that survived to be measured rather than inferred: on 10 September a
 * `keywordEdits` call against a live view came back with a three-keyword description
 * reflowed from three lines onto one. The keywords all persisted correctly; the layout the
 * person had typed did not.
 */

test('parseKtlKeywordCluster records a newline separator', () => {
    assert.deepEqual(parseKtlKeywordCluster('_cls=[probe-a]\n_notes= baseline'), {
        prose: '',
        keywords: [
            { name: '_cls', raw: '_cls=[probe-a]', separator: '' },
            { name: '_notes', raw: '_notes= baseline', separator: '\n' },
        ],
    });
});

test('serializeKtlKeywordCluster round-trips a multi-line cluster unchanged', () => {
    const original =
        'Physical health\n_cls=[sub-heading]\n_style=[margin-bottom: -15px]\n_notes=Craig';
    const { prose, keywords } = parseKtlKeywordCluster(original);
    assert.equal(serializeKtlKeywordCluster(prose, keywords), original);
});

test('applyKtlKeywordEdits keeps the newline in front of an updated keyword', () => {
    // The exact live measurement. Before the fix this returned
    // '_cls=[probe-a] _notes= changed AFTER' - one line, the separator lost.
    const result = applyKtlKeywordEdits('_cls=[probe-a]\n_notes= baseline BEFORE', {
        _notes: ' changed AFTER',
    });
    assert.equal(result, '_cls=[probe-a]\n_notes= changed AFTER');
});

test('applyKtlKeywordEdits appends into a newline cluster on a new line', () => {
    // A new keyword joins the way the cluster already joins itself.
    const result = applyKtlKeywordEdits('Heading\n_cls=[sub-heading]', {
        _notes: 'Craig',
    });
    assert.equal(result, 'Heading\n_cls=[sub-heading]\n_notes=Craig');
});

test('applyKtlKeywordEdits still appends with a space to a space-joined cluster', () => {
    const result = applyKtlKeywordEdits('Heading _cls=[sub-heading]', {
        _notes: 'Craig',
    });
    assert.equal(result, 'Heading _cls=[sub-heading] _notes=Craig');
});

test('applyKtlKeywordEdits does not glue a new keyword onto one starting the text', () => {
    // That keyword's own separator is empty, and inheriting it would produce
    // '_ktlHide_notes=Craig', which KTL reads as a single unknown keyword.
    assert.equal(
        applyKtlKeywordEdits('_ktlHide', { _notes: 'Craig' }),
        '_ktlHide _notes=Craig',
    );
});

test('applyKtlKeywordEdits preserves a mixed cluster keyword by keyword', () => {
    // Nothing normalises the separators it did not touch, in either direction.
    const result = applyKtlKeywordEdits('Prose _a=1\n_b=2 _c=3', { _b: '9' });
    assert.equal(result, 'Prose _a=1\n_b=9 _c=3');
});

test('applyKtlKeywordEdits keeps trailing whitespace inside a value', () => {
    // `raw` is trimmed, so a value's own trailing spaces live in the next keyword's
    // separator. Reserialization has to put them back in the same place.
    const original = 'Heading _notes=Craig   _ktlHide';
    assert.equal(applyKtlKeywordEdits(original, {}), original);
});
