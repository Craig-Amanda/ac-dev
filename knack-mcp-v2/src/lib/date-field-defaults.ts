/**
 * The format a new date field gets when the caller does not give one.
 *
 * Left to Knack, a date field created through the API gets US dates and no time
 * (`mm/dd/yyyy`, "Ignore Time"). On a UK app that is the wrong way round: measured on
 * 25 September, 187 of NPS Test App's 196 date fields and 46 of NP Place Playground's
 * 49 use `dd/mm/yyyy`, and both apps are set to the "London" time zone. So the date
 * order follows the app's time zone instead.
 *
 * Knack has no time zone per field: every date value is read in the app's time zone
 * (`settings.timezone`), which is why the reply names it.
 *
 * Pure: no I/O.
 */
import { getObjectAtPath } from './metadata.js';
import { getTrimmedString } from './util.js';

export const DATE_FORMATS = ['dd/mm/yyyy', 'mm/dd/yyyy'] as const;
export type DateFormat = (typeof DATE_FORMATS)[number];

/** Knack's 24-hour time format, used when a time is asked for. */
const TIME_24_HOUR = 'HH MM (military)';
const NO_TIME = 'Ignore Time';

/**
 * Knack's time zone names (Rails-style, as stored in `settings.timezone`) for the zones
 * that write dates month first. Every other zone gets day first.
 */
const MONTH_FIRST_TIME_ZONES = new Set([
    'Eastern Time (US & Canada)',
    'Central Time (US & Canada)',
    'Mountain Time (US & Canada)',
    'Pacific Time (US & Canada)',
    'Arizona',
    'Alaska',
    'Hawaii',
    'Indiana (East)',
]);

/** The date order a time zone uses: month first for the US zones, day first otherwise. */
export function dateFormatForTimeZone(timeZone: string): DateFormat {
    return MONTH_FIRST_TIME_ZONES.has(timeZone) ? 'mm/dd/yyyy' : 'dd/mm/yyyy';
}

/** The app's time zone from runtime metadata, or null when it can't be read. */
export function readAppTimeZone(metadata: unknown): string | null {
    return getTrimmedString(
        getObjectAtPath(metadata, 'application', 'settings', 'timezone'),
    );
}

export type DateFieldDefaults = {
    /** The format to send, or undefined to leave Knack's own default. */
    format?: Record<string, unknown>;
    /** What was decided, for the reply. */
    summary: {
        timeZone: string | null;
        dateFormat: string;
        time: 'none' | '24-hour' | 'as given';
        source: string;
        note: string;
    };
};

/**
 * The format for a new date field. `given` (the caller's own format) wins key by key,
 * then `dateFormat`, then the app's time zone; `includeTime` picks 24-hour time.
 *
 * The keys other than the date and time settings are Knack's own defaults, as stored on
 * a date field created through the API with no format (NP Place Playground, 25
 * September), so the field matches one made in the Builder.
 */
export function buildDateFieldFormat(options: {
    timeZone: string | null;
    dateFormat?: DateFormat;
    includeTime?: boolean;
    given?: Record<string, unknown>;
}): DateFieldDefaults {
    const { timeZone, dateFormat, includeTime, given } = options;
    const chosenDateFormat =
        dateFormat ?? (timeZone ? dateFormatForTimeZone(timeZone) : undefined);
    const source = given?.date_format
        ? 'the format you passed'
        : dateFormat
          ? 'dateFormat'
          : timeZone
            ? `the app's time zone (${timeZone})`
            : "Knack's default, because the app's time zone could not be read";

    if (!chosenDateFormat && includeTime === undefined && !given) {
        return {
            summary: {
                timeZone,
                dateFormat: 'mm/dd/yyyy',
                time: 'none',
                source,
                note: "The app's time zone could not be read, so Knack's own default was left in place: US dates (mm/dd/yyyy) and no time. Pass dateFormat to choose.",
            },
        };
    }

    const format: Record<string, unknown> = {
        calendar: false,
        time_type: 'current',
        date_format: chosenDateFormat ?? 'mm/dd/yyyy',
        time_format: includeTime ? TIME_24_HOUR : NO_TIME,
        default_date: '',
        default_time: '',
        default_type: 'current',
        ...given,
    };
    const timeFormat = format.time_format;
    const time =
        given?.time_format !== undefined
            ? 'as given'
            : timeFormat === NO_TIME
              ? 'none'
              : '24-hour';

    const zoneNote = timeZone
        ? `Knack reads every date value in the app's time zone (${timeZone}); a field has no time zone of its own.`
        : "Knack reads every date value in the app's time zone; a field has no time zone of its own.";
    return {
        format,
        summary: {
            timeZone,
            dateFormat: String(format.date_format),
            time,
            source,
            note:
                time === 'none'
                    ? `${zoneNote} This field stores no time, so a time sent with a date is dropped. Pass includeTime: true for 24-hour time.`
                    : zoneNote,
        },
    };
}
