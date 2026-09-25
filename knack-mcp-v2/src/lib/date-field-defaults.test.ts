import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    buildDateFieldFormat,
    dateFormatForTimeZone,
    readAppTimeZone,
} from './date-field-defaults.js';

describe('dateFormatForTimeZone', () => {
    it('puts the day first outside the US zones', () => {
        assert.equal(dateFormatForTimeZone('London'), 'dd/mm/yyyy');
        assert.equal(dateFormatForTimeZone('Paris'), 'dd/mm/yyyy');
        assert.equal(dateFormatForTimeZone('Sydney'), 'dd/mm/yyyy');
    });

    it('puts the month first in the US zones', () => {
        assert.equal(
            dateFormatForTimeZone('Eastern Time (US & Canada)'),
            'mm/dd/yyyy',
        );
        assert.equal(dateFormatForTimeZone('Hawaii'), 'mm/dd/yyyy');
    });
});

describe('readAppTimeZone', () => {
    it('reads application.settings.timezone', () => {
        assert.equal(
            readAppTimeZone({
                application: { settings: { timezone: 'London' } },
            }),
            'London',
        );
    });

    it('returns null when it is missing', () => {
        assert.equal(readAppTimeZone({ application: {} }), null);
        assert.equal(readAppTimeZone(null), null);
    });
});

describe('buildDateFieldFormat', () => {
    it('defaults to the time zone date order and no time', () => {
        const { format, summary } = buildDateFieldFormat({
            timeZone: 'London',
        });
        assert.deepEqual(format, {
            calendar: false,
            time_type: 'current',
            date_format: 'dd/mm/yyyy',
            time_format: 'Ignore Time',
            default_date: '',
            default_time: '',
            default_type: 'current',
        });
        assert.equal(summary.time, 'none');
        assert.match(summary.note, /time sent with a date is dropped/);
        assert.match(summary.note, /London/);
    });

    it('uses 24-hour time when a time is asked for', () => {
        const { format, summary } = buildDateFieldFormat({
            timeZone: 'London',
            includeTime: true,
        });
        assert.equal(format?.time_format, 'HH MM (military)');
        assert.equal(summary.time, '24-hour');
        assert.doesNotMatch(summary.note, /dropped/);
    });

    it('lets dateFormat override the time zone, and a given format override both', () => {
        assert.equal(
            buildDateFieldFormat({
                timeZone: 'London',
                dateFormat: 'mm/dd/yyyy',
            }).format?.date_format,
            'mm/dd/yyyy',
        );
        const given = buildDateFieldFormat({
            timeZone: 'London',
            dateFormat: 'mm/dd/yyyy',
            given: {
                date_format: 'Ignore Date',
                time_format: 'HH MM (military)',
            },
        });
        assert.equal(given.format?.date_format, 'Ignore Date');
        assert.equal(given.summary.time, 'as given');
        assert.equal(given.summary.source, 'the format you passed');
    });

    it("leaves Knack's default in place when the time zone is unknown and nothing is asked", () => {
        const { format, summary } = buildDateFieldFormat({ timeZone: null });
        assert.equal(format, undefined);
        assert.match(summary.note, /could not be read/);
    });
});
