import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
    appendKtlNote,
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
