import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
    appendKtlNote,
    preserveKtlNote,
    extractKtlNoteTag,
    stripKtlNoteTag,
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
        /^Customer name _ktlHide _notes=Pat on \d{4}-\d{2}-\d{2}$/,
    );
});

test('extractKtlNoteTag returns the first stamp when more than one exists', () => {
    const description =
        'Customer name _notes=Craig on 2026-09-01 _ktlHide _notes=Sam on 2026-09-05';
    assert.equal(extractKtlNoteTag(description), '_notes=Craig on 2026-09-01');
});

// ------------------------------------------------ bracket notes: one note, not two

const WHEN = new Date('2026-09-25T09:00:00Z');
const AMANDA_NOTE =
    '_notes=[Maximum guest age, 0-18, must be above Min Age. Leave blank for X+ products. Feeds Age Display. Added 25/09/26 - AM]';

test('appendKtlNote puts the stamp inside a bracket note instead of adding a second note', () => {
    const result = appendKtlNote(AMANDA_NOTE, 'Amanda', WHEN);
    assert.equal(
        result,
        '_notes=[Maximum guest age, 0-18, must be above Min Age. Leave blank for X+ products. Feeds Age Display. Added 25/09/26 - AM | Amanda on 2026-09-25]',
    );
    assert.equal(result.match(/_notes=/g)?.length, 1);
});

test('appendKtlNote keeps text before the bracket note and other keywords', () => {
    assert.equal(
        appendKtlNote(
            'Guest age limit _ktlHide _notes=[Max age]',
            'Amanda',
            WHEN,
        ),
        'Guest age limit _ktlHide _notes=[Max age | Amanda on 2026-09-25]',
    );
});

test('a restamp replaces the attribution inside the brackets rather than adding another', () => {
    const once = appendKtlNote(AMANDA_NOTE, 'Amanda', WHEN);
    const twice = appendKtlNote(
        once,
        'Craig',
        new Date('2026-10-01T09:00:00Z'),
    );
    assert.match(twice, /Added 25\/09\/26 - AM \| Craig on 2026-10-01\]$/);
    assert.doesNotMatch(twice, /Amanda on/);
});

test('a bracket note and a stray plain stamp become one note', () => {
    const result = appendKtlNote(
        'Age _notes=[Max age] _notes=Sam on 2026-09-01',
        'Amanda',
        WHEN,
    );
    assert.equal(result, 'Age _notes=[Max age | Amanda on 2026-09-25]');
});

test('a plain stamp inside bracket text is not mistaken for a separate note', () => {
    const stored = '_notes=[Max age | Amanda on 2026-09-25]';
    assert.equal(stripKtlNoteTag(stored), '');
    assert.equal(extractKtlNoteTag(stored), stored);
});

test('preserveKtlNote keeps edited bracket text with the original attribution', () => {
    const stored = 'Age _notes=[Max age | Amanda on 2026-09-25]';
    assert.equal(
        preserveKtlNote('Age _notes=[Max age, 0-17]', stored),
        'Age _notes=[Max age, 0-17 | Amanda on 2026-09-25]',
    );
    // No note in the new text: the stored note is carried forward whole.
    assert.equal(
        preserveKtlNote('Guest age', stored),
        'Guest age _notes=[Max age | Amanda on 2026-09-25]',
    );
    // A plain stored stamp becomes the attribution of new bracket text.
    assert.equal(
        preserveKtlNote('Age _notes=[Max age]', 'Age _notes=Sam on 2026-09-01'),
        'Age _notes=[Max age | Sam on 2026-09-01]',
    );
});

test('without a bracket note the plain stamp is unchanged', () => {
    assert.equal(
        appendKtlNote('Customer name _ktlHide', 'Craig', WHEN),
        'Customer name _ktlHide _notes=Craig on 2026-09-25',
    );
});
