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

/**
 * Keywords behind a line-break tag. Reported by the app owner, then counted: **148** of
 * the 889 view titles and descriptions in the live production app put their keywords
 * behind `<br />`, almost always as `\n<br />`, which is what the builder produces when
 * a person types the cluster on separate lines in a rich-text box.
 *
 * Before this they were invisible to this module. Not misplaced — invisible.
 * parseKtlKeywordCluster returned zero keywords and the whole text as prose, so updating
 * one that was plainly there appended a second copy instead.
 *
 * The safety half was never affected: the keyword-drop guard uses a different pattern
 * whose boundary is any non-word character, so it saw these throughout. Only editing was
 * blind, and only here. Two patterns for one concept, disagreeing quietly.
 *
 * The strings below are real, taken verbatim from the production app.
 */

test('parseKtlKeywordCluster finds keywords behind a break tag', () => {
    // view_4 and view_219, the app's busiest tables, share this exact description.
    const real =
        "Click on a client's name to go to their Client Summary and all client level tabs.\n<br />_vmxw=fit-content\n<br />_hc=IndComplete\n<br />_sth";
    const { prose, keywords } = parseKtlKeywordCluster(real);

    assert.equal(
        prose,
        "Click on a client's name to go to their Client Summary and all client level tabs.",
    );
    assert.deepEqual(keywords, [
        { name: '_vmxw', raw: '_vmxw=fit-content', separator: '\n<br />' },
        { name: '_hc', raw: '_hc=IndComplete', separator: '\n<br />' },
        { name: '_sth', raw: '_sth', separator: '\n<br />' },
    ]);
    assert.equal(serializeKtlKeywordCluster(prose, keywords), real);
});

test('applyKtlKeywordEdits updates a break-tag keyword in place, not twice', () => {
    // The measured failure: this returned the original text with a second `_hc` glued
    // on the end, leaving two conflicting values for one keyword.
    const real =
        "Click on a client's name to go to their Client Summary and all client level tabs.\n<br />_vmxw=fit-content\n<br />_hc=IndComplete\n<br />_sth";
    const result = applyKtlKeywordEdits(real, { _hc: 'Changed' });

    assert.equal(
        result,
        "Click on a client's name to go to their Client Summary and all client level tabs.\n<br />_vmxw=fit-content\n<br />_hc=Changed\n<br />_sth",
    );
    assert.equal((result.match(/_hc/g) ?? []).length, 1);
});

test('parseKtlKeywordCluster handles a cluster that mixes both separators', () => {
    // view_1449: three newline-separated keywords, then one behind a break tag. Neither
    // style is normalised into the other.
    const real =
        '_hsv\n_vmxw=[fit-content]\n_sth\n<br />_obf=[field_2067], [ktlRoles, Developer]';
    const { prose, keywords } = parseKtlKeywordCluster(real);

    assert.equal(prose, '');
    assert.deepEqual(
        keywords.map((entry) => [entry.name, entry.separator]),
        [
            ['_hsv', ''],
            ['_vmxw', '\n'],
            ['_sth', '\n'],
            ['_obf', '\n<br />'],
        ],
    );
    assert.equal(serializeKtlKeywordCluster(prose, keywords), real);
});

test('serializeKtlKeywordCluster keeps a break tag that opens the text', () => {
    // view_705 and 51 others start with one. It is content, not spacing: dropping it
    // deletes a rendered blank line from the top of the view. Leading *whitespace* is
    // still dropped, which is what the next test pins.
    const real = '<br />_hsv=[save, false]\n<br />_vmxw=800';
    const { prose, keywords } = parseKtlKeywordCluster(real);
    assert.equal(serializeKtlKeywordCluster(prose, keywords), real);
});

test('serializeKtlKeywordCluster still drops leading whitespace', () => {
    const { prose, keywords } = parseKtlKeywordCluster('   _ktlHide');
    assert.equal(serializeKtlKeywordCluster(prose, keywords), '_ktlHide');
});

test('applyKtlKeywordEdits appends behind a break tag when the cluster uses them', () => {
    // A new keyword joins the way the cluster already joins itself, whichever style
    // that is.
    assert.equal(
        applyKtlKeywordEdits('Heading\n<br />_hv', { _notes: 'Craig' }),
        'Heading\n<br />_hv\n<br />_notes=Craig',
    );
});

test('parseKtlKeywordCluster accepts every spelling of the tag', () => {
    for (const tag of ['<br>', '<br/>', '<br />', '<BR />', '<br  />']) {
        const { keywords } = parseKtlKeywordCluster(`Heading${tag}_hv`);
        assert.equal(keywords.length, 1, `failed for ${tag}`);
        assert.equal(keywords[0].name, '_hv');
        assert.equal(keywords[0].separator, tag);
    }
});

test('a keyword wrapped in punctuation is still not seen, and that is recorded', () => {
    // The remaining divergence from the drop guard, stated rather than hidden. Its
    // pattern treats any non-word character as a boundary, so it sees `(_notes`; this
    // module sees whitespace and break tags only. No production text in either app hits
    // this, and widening the boundary here would start finding keyword-shaped tokens
    // inside values. Left narrow deliberately.
    const { keywords } = parseKtlKeywordCluster('Heading (_notes=Craig)');
    assert.deepEqual(keywords, []);
});
