import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    LIST_DESCRIPTION_CHARS,
    readHolderKeywords,
    readObjectDescription,
    shortObjectDescription,
} from './object-description.js';

const ai = (key: string, description?: string) => ({
    key,
    type: 'auto_increment',
    ...(description === undefined ? {} : { description }),
});

describe('readObjectDescription', () => {
    it('reads the auto-increment field, without its _notes stamp', () => {
        const read = readObjectDescription([
            { key: 'field_2', type: 'short_text', description: 'Not this' },
            ai('field_1', 'Holds clients. _notes=[Craig on 2026-09-30]'),
        ]);
        assert.deepEqual(read, {
            fieldKey: 'field_1',
            text: 'Holds clients.',
            autoIncrementKeys: ['field_1'],
        });
    });

    it('reads meta.description as the live API returns it', () => {
        const read = readObjectDescription([
            {
                key: 'field_1',
                type: 'auto_increment',
                meta: { description: 'Live words' },
            },
        ]);
        assert.equal(read.text, 'Live words');
    });

    it('reports no field, and an empty description, distinctly', () => {
        assert.deepEqual(readObjectDescription([]), {
            fieldKey: null,
            text: '',
            autoIncrementKeys: [],
        });
        assert.deepEqual(readObjectDescription(undefined), {
            fieldKey: null,
            text: '',
            autoIncrementKeys: [],
        });
        const empty = readObjectDescription([ai('field_1')]);
        assert.equal(empty.fieldKey, 'field_1');
        assert.equal(empty.text, '');
    });

    it('prefers the auto-increment field that has words when there are several', () => {
        const read = readObjectDescription([
            ai('field_1'),
            ai('field_2', 'Real description'),
            ai('field_3', 'Another'),
        ]);
        assert.equal(read.fieldKey, 'field_2');
        assert.deepEqual(read.autoIncrementKeys, [
            'field_2',
            'field_1',
            'field_3',
        ]);
    });
});

describe('shortObjectDescription', () => {
    it('is undefined without words, flattens whitespace, and marks a cut', () => {
        assert.equal(shortObjectDescription([ai('field_1')]), undefined);
        assert.equal(
            shortObjectDescription([ai('field_1', 'Two\n  lines')]),
            'Two lines',
        );
        const cut = shortObjectDescription([ai('field_1', 'x'.repeat(500))])!;
        assert.equal(cut.length, LIST_DESCRIPTION_CHARS);
        assert.ok(cut.endsWith('…'));
    });
});

describe('readHolderKeywords', () => {
    it('returns the other keywords on the holder, without the note', () => {
        assert.equal(
            readHolderKeywords([
                ai(
                    'field_1',
                    '_notes=[Holds clients. | Craig on 2026-09-30] _ktlHide _mcp_allowwrite',
                ),
            ]),
            '_ktlHide _mcp_allowwrite',
        );
    });

    it('is empty with no keywords, no holder or no fields', () => {
        assert.equal(
            readHolderKeywords([
                ai('field_1', '_notes=[Holds clients. | Craig on 2026-09-30]'),
            ]),
            '',
        );
        assert.equal(readHolderKeywords([]), '');
        assert.equal(readHolderKeywords(undefined), '');
    });
});
