import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
    appendKtlNote,
    formatAttribution,
    preserveKtlNote,
    descriptionAsPlainText,
    readDescriptionText,
    stripKtlNoteTag,
    unstampedKtlNote,
} from './field-payload.js';

test('stripKtlNoteTag removes every _notes stamp, not just the first', () => {
    // Two stamps in one description — e.g. left behind by a manual builder edit before
    // this tool existed, or any other way a second one crept in. A non-global strip
    // would only remove the first, leaving a stray old stamp behind.
    const description =
        'Customer name _notes=Craig on 2026-09-01 _ktlHide _notes=Sam on 2026-09-05';
    assert.equal(stripKtlNoteTag(description), 'Customer name _ktlHide');
});

test('appendKtlNote never leaves a second stamp behind when two already exist', () => {
    const description =
        'Customer name _notes=Craig on 2026-09-01 _ktlHide _notes=Sam on 2026-09-05';
    const result = appendKtlNote(description, 'Pat');
    // Exactly one _notes stamp in the result.
    assert.equal((result.match(/_notes=/g) ?? []).length, 1);
    assert.match(
        result,
        /^_notes=\[Customer name \| Pat on \d{2}\/\d{2}\/\d{4}\] _ktlHide$/,
    );
});

// ------------------------------------------------ bracket notes: one note, not two

const WHEN = new Date('2026-09-25T09:00:00Z');
const AMANDA_NOTE =
    '_notes=[Maximum guest age, 0-18, must be above Min Age. Leave blank for X+ products. Feeds Age Display. Added 25/09/26 - AM]';

test('appendKtlNote puts the stamp inside a bracket note instead of adding a second note', () => {
    const result = appendKtlNote(AMANDA_NOTE, 'Amanda', WHEN);
    assert.equal(
        result,
        '_notes=[Maximum guest age, 0-18, must be above Min Age. Leave blank for X+ products. Feeds Age Display. Added 25/09/26 - AM | Amanda on 25/09/2026]',
    );
    assert.equal(result.match(/_notes=/g)?.length, 1);
});

test('appendKtlNote moves words written outside the note inside it, keeping other keywords after', () => {
    assert.equal(
        appendKtlNote(
            'Guest age limit _ktlHide _notes=[Max age]',
            'Amanda',
            WHEN,
        ),
        '_notes=[Guest age limit Max age | Amanda on 25/09/2026] _ktlHide',
    );
});

test('a restamp replaces the attribution inside the brackets rather than adding another', () => {
    const once = appendKtlNote(AMANDA_NOTE, 'Amanda', WHEN);
    const twice = appendKtlNote(
        once,
        'Craig',
        new Date('2026-10-01T09:00:00Z'),
    );
    assert.match(twice, /Added 25\/09\/26 - AM \| Craig on 01\/10\/2026\]$/);
    assert.doesNotMatch(twice, /Amanda on/);
});

test('a bracket note and a stray plain stamp become one note', () => {
    const result = appendKtlNote(
        'Age _notes=[Max age] _notes=Sam on 2026-09-01',
        'Amanda',
        WHEN,
    );
    assert.equal(result, '_notes=[Age Max age | Amanda on 25/09/2026]');
});

test('a plain stamp inside bracket text is not mistaken for a separate note', () => {
    const stored = '_notes=[Max age | Amanda on 2026-09-25]';
    assert.equal(stripKtlNoteTag(stored), '');
});

test('preserveKtlNote writes the new words inside the note with the original attribution', () => {
    const stored = '_notes=[Max age | Amanda on 2026-09-25]';
    // Plain new words replace the old ones.
    assert.equal(
        preserveKtlNote('Guest age, 0-17', stored),
        '_notes=[Guest age, 0-17 | Amanda on 2026-09-25]',
    );
    // Words in a note of their own do the same.
    assert.equal(
        preserveKtlNote('_notes=[Max age, 0-17]', stored),
        '_notes=[Max age, 0-17 | Amanda on 2026-09-25]',
    );
    // Only keywords: the stored words are kept.
    assert.equal(
        preserveKtlNote('_ktlHide', stored),
        '_notes=[Max age | Amanda on 2026-09-25] _ktlHide',
    );
    // A field written before this convention, words outside the note, is migrated.
    assert.equal(
        preserveKtlNote(
            'Age limit',
            'Age _notes=[Max age | Amanda on 2026-09-25]',
        ),
        '_notes=[Age limit | Amanda on 2026-09-25]',
    );
    // A plain stored stamp becomes the attribution of the new words.
    assert.equal(
        preserveKtlNote('Age _notes=[Max age]', 'Age _notes=Sam on 2026-09-01'),
        '_notes=[Age Max age | Sam on 2026-09-01]',
    );
});

test('brackets in the words become parentheses so they cannot close the note early', () => {
    assert.equal(
        appendKtlNote('Range [0-10] inclusive', 'Craig', WHEN),
        '_notes=[Range (0-10) inclusive | Craig on 25/09/2026]',
    );
});

test('readDescriptionText reads the words wherever they sit, without keywords or attribution', () => {
    assert.equal(
        readDescriptionText('_notes=[Holds clients. | Craig on 2026-09-30]'),
        'Holds clients.',
    );
    assert.equal(
        readDescriptionText(
            '_notes=[Holds clients. | Craig on 2026-09-30] _ktlHide',
        ),
        'Holds clients.',
    );
    // Older shape: words outside the note.
    assert.equal(
        readDescriptionText(
            'Holds clients. _ktlHide _notes=[Craig on 2026-09-30]',
        ),
        'Holds clients.',
    );
    assert.equal(readDescriptionText('_notes=[Craig on 2026-09-30]'), '');
    assert.equal(readDescriptionText(''), '');
});

test('a description that is only keywords gets a note with no words', () => {
    assert.equal(
        appendKtlNote('_ktlHide', 'Craig', WHEN),
        '_notes=[Craig on 25/09/2026] _ktlHide',
    );
});

test('a plain stamp from before brackets were the rule comes back in brackets', () => {
    // Carried forward on an ordinary edit, with its attribution kept.
    assert.equal(
        preserveKtlNote(
            'Customer full name',
            'Customer name _notes=Craig on 2026-09-01',
        ),
        '_notes=[Customer full name | Craig on 2026-09-01]',
    );
    // Re-stamped: one bracket note, the old stamp gone.
    assert.equal(
        appendKtlNote('Customer name _notes=Craig on 2026-09-01', 'Sam', WHEN),
        '_notes=[Customer name | Sam on 25/09/2026]',
    );
});

test('a bracket note that is only an attribution is re-stamped, not nested', () => {
    assert.equal(
        appendKtlNote('Name _notes=[Craig on 2026-09-01]', 'Sam', WHEN),
        '_notes=[Name | Sam on 25/09/2026]',
    );
    assert.equal(
        preserveKtlNote('Full name', 'Name _notes=[Craig on 2026-09-01]'),
        '_notes=[Full name | Craig on 2026-09-01]',
    );
});

test('a | inside the words survives a write, an edit and a read', () => {
    const written = appendKtlNote('Yes | No | Maybe', 'Craig', WHEN);
    assert.equal(written, '_notes=[Yes | No | Maybe | Craig on 25/09/2026]');
    // Read back, the words are whole and the attribution is not mistaken for them.
    assert.equal(readDescriptionText(written), 'Yes | No | Maybe');
    // An edit keeps the original attribution, and a | in the new words is fine too.
    assert.equal(
        preserveKtlNote('Open | Closed', written),
        '_notes=[Open | Closed | Craig on 25/09/2026]',
    );
    assert.equal(
        readDescriptionText(preserveKtlNote('Open | Closed', written)),
        'Open | Closed',
    );
    // A restamp replaces only the attribution.
    assert.equal(
        appendKtlNote(written, 'Sam', new Date('2026-10-01T09:00:00Z')),
        '_notes=[Yes | No | Maybe | Sam on 01/10/2026]',
    );
});

// ------------------------------------------------ UK dates and unstamped notes

test('formatAttribution writes the day as dd/mm/yyyy, as it falls in London', () => {
    assert.equal(
        formatAttribution('Amanda', new Date('2026-11-23T12:00:00Z')),
        'Amanda on 23/11/2026',
    );
    // 23:30 UTC on 30 June is already 1 July in London (BST).
    assert.equal(
        formatAttribution('Amanda', new Date('2026-06-30T23:30:00Z')),
        'Amanda on 01/07/2026',
    );
});

test('old ISO stamps and new UK stamps are both read as the attribution', () => {
    for (const stamp of ['Craig on 2026-09-01', 'Craig on 01/09/2026']) {
        // Bracket note with words, bracket note that is only a stamp, plain stamp.
        assert.equal(
            preserveKtlNote('New', `_notes=[Old | ${stamp}]`),
            `_notes=[New | ${stamp}]`,
        );
        assert.equal(
            preserveKtlNote('New', `Name _notes=[${stamp}]`),
            `_notes=[New | ${stamp}]`,
        );
        assert.equal(
            preserveKtlNote('New _ktlHide', `Name _notes=${stamp} _ktlHide`),
            `_notes=[New | ${stamp}] _ktlHide`,
        );
        assert.equal(readDescriptionText(`_notes=[Old | ${stamp}]`), 'Old');
        assert.equal(
            stripKtlNoteTag(`Name _notes=${stamp} _ktlHide`),
            'Name _ktlHide',
        );
        // A restamp replaces either form with a UK one.
        assert.equal(
            appendKtlNote(`_notes=[Old | ${stamp}]`, 'Sam', WHEN),
            '_notes=[Old | Sam on 25/09/2026]',
        );
    }
});

test('unstampedKtlNote writes the words in a note with no stamp, removing any stamp', () => {
    assert.equal(
        unstampedKtlNote('Holds bookings.'),
        '_notes=[Holds bookings.]',
    );
    assert.equal(
        unstampedKtlNote(
            '_notes=[Holds bookings. | Amanda on 23/11/2026] _ktlHide',
        ),
        '_notes=[Holds bookings.] _ktlHide',
    );
    assert.equal(
        unstampedKtlNote('Name _notes=Craig on 2026-09-01 _ktlHide'),
        '_notes=[Name] _ktlHide',
    );
    assert.equal(
        unstampedKtlNote('Range [0-10] _notes=[Amanda on 2026-09-25]'),
        '_notes=[Range (0-10)]',
    );
    // No words: only the other keywords are left, never an empty note.
    assert.equal(
        unstampedKtlNote('_ktlHide _notes=[Craig on 2026-09-01]'),
        '_ktlHide',
    );
    // A later ordinary edit keeps the note unstamped.
    assert.equal(
        preserveKtlNote('New words _ktlHide', '_notes=[Old words] _ktlHide'),
        '_notes=[New words] _ktlHide',
    );
});

test('descriptionAsPlainText turns the Builder HTML into text and leaves plain text alone', () => {
    assert.equal(
        descriptionAsPlainText('<p>Holds clients.</p><p>_mcp_nodata</p>'),
        'Holds clients. _mcp_nodata',
    );
    assert.equal(
        descriptionAsPlainText('Line one<br />_mcp_nodata&nbsp;_ktlHide'),
        'Line one _mcp_nodata _ktlHide',
    );
    assert.equal(
        descriptionAsPlainText('Tom &amp; Jerry &lt;3'),
        'Tom & Jerry <3',
    );
    assert.equal(
        descriptionAsPlainText('_notes=[Yes | No | Craig on 2026-09-30]'),
        '_notes=[Yes | No | Craig on 2026-09-30]',
    );
});
