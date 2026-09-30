import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { CachedField } from '../types.js';
import { readDateFieldFormat } from './date-field-defaults.js';
import {
    checkStoredDate,
    describeDateRefusal,
    readDateValue,
} from './date-values.js';
import { parseRuntimeSchema } from './metadata.js';

const DAY_FIRST: CachedField = {
    key: 'field_1',
    name: 'Start',
    type: 'date_time',
    dateFormat: 'dd/mm/yyyy',
    dateHasTime: false,
};
const MONTH_FIRST: CachedField = {
    key: 'field_2',
    name: 'Due',
    type: 'date_time',
    dateFormat: 'mm/dd/yyyy',
    dateHasTime: true,
};

describe('readDateFieldFormat', () => {
    it('reads the order and the time from the field format, per field', () => {
        assert.deepEqual(
            readDateFieldFormat({
                date_format: 'mm/dd/yyyy',
                time_format: 'Ignore Time',
            }),
            { dateFormat: 'mm/dd/yyyy', dateHasTime: false },
        );
        assert.deepEqual(
            readDateFieldFormat({
                date_format: 'dd/mm/yyyy',
                time_format: 'HH MM (military)',
            }),
            { dateFormat: 'dd/mm/yyyy', dateHasTime: true },
        );
    });

    it('gives a time-only field no order and a format-less field nothing', () => {
        assert.deepEqual(
            readDateFieldFormat({
                date_format: 'Ignore Date',
                time_format: 'HH MM (military)',
            }),
            { dateHasTime: true },
        );
        assert.deepEqual(readDateFieldFormat(null), {});
    });

    it('is stored on the cached field by parseRuntimeSchema', () => {
        const schema = parseRuntimeSchema({
            objects: [
                {
                    key: 'object_1',
                    fields: [
                        {
                            key: 'field_1',
                            type: 'date_time',
                            format: {
                                date_format: 'mm/dd/yyyy',
                                time_format: 'Ignore Time',
                            },
                        },
                        { key: 'field_2', type: 'short_text' },
                    ],
                },
            ],
        });
        const [date, text] = schema!.objects![0].fields!;
        assert.equal(date.dateFormat, 'mm/dd/yyyy');
        assert.equal(date.dateHasTime, false);
        assert.equal(text.dateFormat, undefined);
    });
});

describe('readDateValue', () => {
    it('reads the same text in each field’s own order', () => {
        const dayFirst = readDateValue(DAY_FIRST, '03/09/2026');
        const monthFirst = readDateValue(MONTH_FIRST, '03/09/2026');
        assert.equal(dayFirst.understood, '3 September 2026');
        assert.equal(monthFirst.understood, '9 March 2026');
        assert.equal(dayFirst.ambiguous, true);
        assert.equal(monthFirst.ambiguous, true);
    });

    it('does not flag a date that only reads one way, or two equal parts', () => {
        assert.equal(readDateValue(DAY_FIRST, '13/09/2026').ambiguous, false);
        assert.equal(readDateValue(DAY_FIRST, '05/05/2026').ambiguous, false);
    });

    it('refuses a month-first field given a month of 13', () => {
        const reading = readDateValue(MONTH_FIRST, '13/09/2026');
        assert.equal(reading.understood, null);
        assert.match(reading.problems[0], /no month 13/);
        const line = describeDateRefusal(reading, reading.problems[0]);
        assert.match(line, /^DATE_INVALID: field_2 \(Due\) is mm\/dd\/yyyy/);
        assert.match(line, /13\/09\/2026/);
        assert.match(line, /Nothing was sent/);
    });

    it('accepts the same text on a day-first field', () => {
        const reading = readDateValue(DAY_FIRST, '13/09/2026');
        assert.deepEqual(reading.problems, []);
        assert.equal(reading.understood, '13 September 2026');
    });

    it('refuses a day the month does not have, and knows leap years', () => {
        assert.match(
            readDateValue(DAY_FIRST, '31/02/2026').problems[0],
            /no day 31 in February 2026/,
        );
        assert.deepEqual(readDateValue(DAY_FIRST, '29/02/2028').problems, []);
        assert.match(
            readDateValue(DAY_FIRST, '29/02/2026').problems[0],
            /no day 29/,
        );
    });

    it('reads the structured form, with its time', () => {
        const reading = readDateValue(MONTH_FIRST, {
            date: '09/30/2026',
            hours: '10',
            minutes: '5',
            am_pm: 'PM',
        });
        assert.deepEqual(reading.problems, []);
        assert.equal(reading.understood, '30 September 2026, 10:05 pm');
    });

    it('refuses a time that is not on the clock', () => {
        const badHour = readDateValue(MONTH_FIRST, {
            date: '09/30/2026',
            hours: '13',
            minutes: '00',
            am_pm: 'PM',
        });
        assert.match(badHour.problems[0], /no such time as 13:00 pm/);
        const badMinute = readDateValue(DAY_FIRST, {
            date: '30/09/2026',
            hours: '10',
            minutes: '75',
        });
        assert.match(badMinute.problems[0], /no such time as 10:75/);
    });

    it('checks a range the same way, and says where it is wrong', () => {
        const reading = readDateValue(DAY_FIRST, {
            date: '01/09/2026',
            to: { date: '31/09/2026' },
        });
        assert.match(reading.problems[0], /^the end of the range: .*no day 31/);

        const fine = readDateValue(DAY_FIRST, {
            date: '01/09/2026',
            to: { date: '30/09/2026' },
        });
        assert.deepEqual(fine.problems, []);
        assert.equal(fine.understood, '1 September 2026 to 30 September 2026');

        const backwards = readDateValue(DAY_FIRST, {
            date: '30/09/2026',
            to: { date: '01/09/2026' },
        });
        assert.match(backwards.notes.join(' '), /ends before it starts/);
    });

    it('leaves a blank value and a non-slash date unread, with a note', () => {
        assert.equal(readDateValue(DAY_FIRST, '').understood, null);
        const words = readDateValue(DAY_FIRST, '30 Sept 2026');
        assert.deepEqual(words.problems, []);
        assert.equal(words.understood, null);
        assert.match(words.notes[0], /was not checked/);
    });

    it('does not guess when the cached field has no date order', () => {
        const unknown: CachedField = { key: 'field_3', type: 'date_time' };
        const reading = readDateValue(unknown, '03/09/2026');
        assert.equal(reading.understood, null);
        assert.deepEqual(reading.problems, []);
        assert.match(reading.notes[0], /knack_cache/);
        assert.match(
            readDateValue(unknown, '13/13/2026').problems[0],
            /neither order/,
        );
    });

    it('notes that a time is dropped on a field that stores none', () => {
        const reading = readDateValue(DAY_FIRST, '03/09/2026 10:30 am');
        assert.equal(reading.understood, '3 September 2026, 10:30 am');
        assert.match(reading.notes.join(' '), /stores no time/);
    });
});

describe('readDateValue with an ISO date', () => {
    it('rewrites it in the field’s own order, never the other way', () => {
        const dayFirst = readDateValue(DAY_FIRST, '2026-09-03');
        const monthFirst = readDateValue(MONTH_FIRST, '2026-09-03');
        assert.deepEqual(dayFirst.converted, { date: '03/09/2026' });
        assert.deepEqual(monthFirst.converted, { date: '09/03/2026' });
        assert.equal(dayFirst.understood, '3 September 2026');
        assert.equal(monthFirst.understood, '3 September 2026');
        assert.equal(dayFirst.ambiguous, false);
    });

    it('converts a time to 12-hour parts, and notes that an offset is ignored', () => {
        const reading = readDateValue(MONTH_FIRST, '2026-07-01T00:30:00Z');
        assert.deepEqual(reading.converted, {
            date: '07/01/2026',
            hours: '12',
            minutes: '30',
            am_pm: 'AM',
        });
        assert.match(reading.notes.join(' '), /offset Z is ignored/);
        assert.deepEqual(
            readDateValue(MONTH_FIRST, '2026-09-30T14:05').converted,
            { date: '09/30/2026', hours: '02', minutes: '05', am_pm: 'PM' },
        );
    });

    it('converts inside the structured form and a range, keeping the rest', () => {
        const reading = readDateValue(DAY_FIRST, {
            date: '2026-09-01',
            to: { date: '2026-09-05' },
        });
        assert.deepEqual(reading.converted, {
            date: '01/09/2026',
            to: { date: '05/09/2026' },
        });
    });

    it('refuses an ISO date that is not a real day', () => {
        assert.match(
            readDateValue(DAY_FIRST, '2026-02-30').problems[0],
            /no day 30 in February 2026/,
        );
        assert.match(
            readDateValue(DAY_FIRST, '2026-13-01').problems[0],
            /no month 13/,
        );
    });

    it('sends it as written when the field has no cached order', () => {
        const reading = readDateValue(
            { key: 'field_3', type: 'date_time' },
            '2026-09-30',
        );
        assert.equal(reading.converted, undefined);
        assert.match(reading.notes[0], /sent as written/);
    });
});

describe('checkStoredDate', () => {
    const stored = (iso: string, extra: object = {}) => ({
        iso_timestamp: iso,
        // The real instant, a day earlier in UTC in British summer time: never compared.
        proper_iso_timestamp: '2026-09-29T23:00:00.000Z',
        ...extra,
    });

    it('agrees when the stored wall-clock day is the intended one', () => {
        const reading = readDateValue(DAY_FIRST, '30/09/2026');
        assert.equal(
            checkStoredDate(reading, stored('2026-09-30T00:00:00.000Z')),
            null,
        );
    });

    it('warns when the stored day is not the intended one', () => {
        const reading = readDateValue(DAY_FIRST, '03/09/2026');
        const warning = checkStoredDate(
            reading,
            stored('2026-03-09T00:00:00.000Z'),
        );
        assert.match(warning!, /^DATE_MISMATCH: field_1 \(Start\)/);
        assert.match(warning!, /3 September 2026/);
        assert.doesNotMatch(warning!, /2026-03-09/);
    });

    it('compares the time only on a field that stores one', () => {
        const timed = readDateValue(MONTH_FIRST, {
            date: '09/30/2026',
            hours: '2',
            minutes: '05',
            am_pm: 'PM',
        });
        assert.equal(
            checkStoredDate(timed, stored('2026-09-30T14:05:00.000Z')),
            null,
        );
        assert.match(
            checkStoredDate(timed, stored('2026-09-30T12:05:00.000Z'))!,
            /different time/,
        );
    });

    it('warns when a range was sent and no end came back', () => {
        const reading = readDateValue(DAY_FIRST, {
            date: '01/09/2026',
            to: { date: '05/09/2026' },
        });
        assert.match(
            checkStoredDate(reading, stored('2026-09-01T00:00:00.000Z'))!,
            /^DATE_RANGE_NOT_STORED/,
        );
        assert.equal(
            checkStoredDate(
                reading,
                stored('2026-09-01T00:00:00.000Z', {
                    to: { date: '09/05/2026' },
                }),
            ),
            null,
        );
    });

    it('has nothing to say without an intended date or a stored timestamp', () => {
        assert.equal(
            checkStoredDate(readDateValue(DAY_FIRST, '30 Sept 2026'), {
                iso_timestamp: '2026-09-30T00:00:00.000Z',
            }),
            null,
        );
        assert.equal(
            checkStoredDate(readDateValue(DAY_FIRST, '30/09/2026'), {}),
            null,
        );
    });
});
