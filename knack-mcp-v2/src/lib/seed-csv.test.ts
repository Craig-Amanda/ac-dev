import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { chooseUniqueImportField, escapeCsvCell } from './seed-csv.js';
import type { CachedField } from '../types.js';

describe('chooseUniqueImportField', () => {
    it('picks a short_text field whose header matches the pattern', () => {
        const fields: CachedField[] = [
            { key: 'field_1', name: 'Name', type: 'short_text' },
            { key: 'field_2', name: 'External ID', type: 'short_text' },
        ];
        assert.equal(chooseUniqueImportField(fields)?.key, 'field_2');
    });

    it('does not pick a matching header on a field type that cannot hold a synthetic code', () => {
        // The synthetic value is a short string like "ACME-001". A number field would
        // reject it on import; an email field would fail validation; a boolean can't
        // hold it at all — none of these should be chosen just because the header
        // contains "id" or "email".
        const fields: CachedField[] = [
            { key: 'field_1', name: 'Employee ID', type: 'number' },
            { key: 'field_2', name: 'Email', type: 'email' },
            { key: 'field_3', name: 'Signed Up', type: 'date_time' },
            { key: 'field_4', name: 'Active', type: 'boolean' },
        ];
        assert.equal(chooseUniqueImportField(fields), undefined);
    });

    it('still excludes connection, choice, address and name fields even as short_text-adjacent headers', () => {
        const fields: CachedField[] = [
            { key: 'field_1', name: 'Record Key', type: 'connection' },
            { key: 'field_2', name: 'Code', type: 'multiple_choice' },
        ];
        assert.equal(chooseUniqueImportField(fields), undefined);
    });
});

describe('escapeCsvCell', () => {
    it('passes an ordinary value through unchanged', () => {
        assert.equal(escapeCsvCell('Acme Ltd'), 'Acme Ltd');
    });

    it('quotes a value containing a comma, quote, or newline', () => {
        assert.equal(escapeCsvCell('a,b'), '"a,b"');
        assert.equal(escapeCsvCell('a"b'), '"a""b"');
        assert.equal(escapeCsvCell('a\nb'), '"a\nb"');
    });

    it('quotes a value containing a bare carriage return', () => {
        assert.equal(escapeCsvCell('a\rb'), '"a\rb"');
    });

    it('neutralises a leading carriage return the same as a leading formula trigger', () => {
        assert.equal(escapeCsvCell('\rb'), '"\'\rb"');
    });

    it('neutralises a leading formula trigger with a single quote', () => {
        // A live record identifier pulled in via useExistingConnectionValues is exactly
        // the untrusted value this defends against: opened in a spreadsheet, this
        // formula would fetch an attacker's URL rather than display text.
        assert.equal(
            escapeCsvCell('=HYPERLINK("http://evil","x")'),
            '"\'=HYPERLINK(""http://evil"",""x"")"',
        );
        assert.equal(escapeCsvCell('+1234'), "'+1234");
        assert.equal(escapeCsvCell('-1234'), "'-1234");
        assert.equal(escapeCsvCell('@mention'), "'@mention");
    });

    it('does not touch a value that merely contains one of the trigger characters mid-string', () => {
        assert.equal(escapeCsvCell('email@example.com'), 'email@example.com');
        assert.equal(escapeCsvCell('12-34'), '12-34');
    });
});
