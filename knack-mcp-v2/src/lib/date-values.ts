/**
 * Reading a date value a caller is about to write, in the field's own date order.
 *
 * Knack takes a date field's value as typed and reads it in the field's `date_format`,
 * so `03/09/2026` is 3 September on a day-first field and 9 March on a month-first one,
 * and nothing tells the writer which. The order is per field, never per app, so every
 * check here uses the one field's `CachedField.dateFormat` and guesses nothing else.
 *
 * Pure: no I/O.
 */
import type { CachedField } from '../types.js';
import type { DateFormat } from './date-field-defaults.js';
import { asRecord } from './util.js';

const MONTH_NAMES = [
    'January',
    'February',
    'March',
    'April',
    'May',
    'June',
    'July',
    'August',
    'September',
    'October',
    'November',
    'December',
];

/** What was understood of one date field's value, for the dry run and for refusals. */
export type DateReading = {
    field: string;
    fieldName?: string;
    /** The field's own date order, or null when the cached schema does not have it. */
    format: DateFormat | null;
    input: unknown;
    /** The date in words ("3 September 2026, 10:30 am"), or null when it was not read. */
    understood: string | null;
    /** Both parts are 12 or less and differ, so the other order is also a real date. */
    ambiguous: boolean;
    /** Why the value is refused; a refused value is never sent. */
    problems: string[];
    notes: string[];
    /** The field stores a time (from the cached schema), or undefined when unknown. */
    hasTime?: boolean;
    /** A range was given (`to`), so the stored value should have an end too. */
    hasRange: boolean;
    /** The wall-clock date (and time) intended, as ISO text, for the check after a write. */
    intended?: IntendedDate;
    /** The value to send in place of an ISO input: the same date in the field's own format. */
    converted?: unknown;
};

type IntendedDate = { date: string; time?: string };

type SentPart = {
    date: string;
    hours?: string;
    minutes?: string;
    am_pm?: string;
};

type Part = {
    intended?: IntendedDate;
    /** An ISO input rewritten in the field's own date order. */
    send?: SentPart;
    words: string | null;
    ambiguous: boolean;
    problems: string[];
    notes: string[];
    /** Minutes since the epoch, for ordering a range; null when it could not be read. */
    at: number | null;
};

const SLASH_DATE = /^\s*(\d{1,2})\/(\d{1,2})\/(\d{4})\b/;
/** YYYY-MM-DD, optionally with a time and an offset (which Knack ignores). */
const ISO_DATE =
    /^\s*(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?\s*(Z|[+-]\d{2}:?\d{2})?)?\s*$/i;
const TRAILING_TIME = /(\d{1,2}):(\d{2})\s*(am|pm)?\s*$/i;

function daysInMonth(month: number, year: number): number {
    return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

const two = (n: number) => String(n).padStart(2, '0');

function clock(hours: number, minutes: number, amPm: string | null): string {
    return `${hours}:${two(minutes)}${amPm ? ` ${amPm.toLowerCase()}` : ''}`;
}

/** Whether day/month/year is a real date; if not, says why in `problems`. */
function isRealDate(
    text: string,
    date: { day: number; month: number; year: number },
    suffix: string,
    problems: string[],
): boolean {
    const { day, month, year } = date;
    if (month < 1 || month > 12) {
        problems.push(`"${text}" has no month ${month}${suffix}`);
        return false;
    }
    if (day < 1 || day > daysInMonth(month, year)) {
        problems.push(
            `"${text}" has no day ${day} in ${MONTH_NAMES[month - 1]} ${year}${suffix}`,
        );
        return false;
    }
    return true;
}

/** Hours and minutes from the structured form's strings or numbers, or null if absent. */
function readTime(
    hours: unknown,
    minutes: unknown,
    amPm: unknown,
    problems: string[],
    describe: string,
): { hours: number; minutes: number; amPm: string | null } | null {
    const present = (value: unknown) =>
        value !== undefined && value !== null && value !== '';
    if (!present(hours) && !present(minutes)) return null;
    const h = Number(present(hours) ? hours : 0);
    const m = Number(present(minutes) ? minutes : 0);
    const half =
        typeof amPm === 'string' && /^(am|pm)$/i.test(amPm.trim())
            ? amPm.trim().toUpperCase()
            : null;
    if (!Number.isInteger(h) || !Number.isInteger(m)) {
        problems.push(`${describe} has a time that is not whole numbers`);
        return null;
    }
    const validHours = half ? h >= 1 && h <= 12 : h >= 0 && h <= 23;
    if (!validHours || m < 0 || m > 59) {
        problems.push(
            `${describe} has no such time as ${clock(h, m, half)}${half ? '' : ' (24-hour clock)'}`,
        );
        return null;
    }
    return { hours: h, minutes: m, amPm: half };
}

function readPart(
    fieldKey: string,
    dateFormat: DateFormat | null,
    value: unknown,
): Part {
    const part: Part = {
        words: null,
        ambiguous: false,
        problems: [],
        notes: [],
        at: null,
    };
    let dateText: string | null;
    let time: ReturnType<typeof readTime> = null;

    const record = asRecord(value);
    if (typeof value === 'string') {
        dateText = value;
        const match = TRAILING_TIME.exec(value);
        if (match && SLASH_DATE.test(value)) {
            time = readTime(
                match[1],
                match[2],
                match[3] ?? null,
                part.problems,
                `${fieldKey}: "${value}"`,
            );
        }
    } else if (record) {
        dateText = typeof record.date === 'string' ? record.date : null;
        time = readTime(
            record.hours,
            record.minutes,
            record.am_pm,
            part.problems,
            `${fieldKey}: ${JSON.stringify(value)}`,
        );
    } else {
        part.notes.push(
            'The value is not text or a { date, hours, minutes, am_pm } object, so it was not read.',
        );
        return part;
    }

    let date: { day: number; month: number; year: number } | null = null;
    let fromIso = false;
    const iso = dateText ? ISO_DATE.exec(dateText) : null;
    const match = dateText && !iso ? SLASH_DATE.exec(dateText) : null;
    if (iso) {
        const given = {
            year: Number(iso[1]),
            month: Number(iso[2]),
            day: Number(iso[3]),
        };
        if (isRealDate(iso[0].trim(), given, '', part.problems)) {
            date = given;
            fromIso = true;
            if (!time && iso[4] !== undefined) {
                time = readTime(
                    iso[4],
                    iso[5],
                    null,
                    part.problems,
                    `${fieldKey}: "${dateText}"`,
                );
                if (iso[6]) {
                    part.notes.push(
                        `The offset ${iso[6]} is ignored: Knack reads the time as written, in the app's time zone.`,
                    );
                }
            }
        }
    } else if (match) {
        const first = Number(match[1]);
        const second = Number(match[2]);
        const year = Number(match[3]);
        if (dateFormat) {
            const monthFirst = dateFormat === 'mm/dd/yyyy';
            const given = {
                year,
                month: monthFirst ? first : second,
                day: monthFirst ? second : first,
            };
            if (
                isRealDate(
                    match[0].trim(),
                    given,
                    `; this field reads ${dateFormat}`,
                    part.problems,
                )
            ) {
                date = given;
                part.ambiguous =
                    first <= 12 && second <= 12 && first !== second;
            }
        } else if (first > 12 && second > 12) {
            part.problems.push(
                `"${match[0].trim()}" is a real date in neither order`,
            );
        } else {
            part.notes.push(
                "The field's date order is not in the cached schema, so the date could not be read; refresh with knack_cache (refresh: true).",
            );
        }
    } else if (dateText) {
        part.notes.push(
            `"${dateText}" is not a dd/mm/yyyy, mm/dd/yyyy or YYYY-MM-DD date, so it was not checked.`,
        );
    }

    if (date) {
        const day = `${date.day} ${MONTH_NAMES[date.month - 1]} ${date.year}`;
        part.words = time
            ? `${day}, ${clock(time.hours, time.minutes, time.amPm)}`
            : day;
        const hours = !time
            ? 0
            : time.amPm
              ? (time.hours % 12) + (time.amPm === 'PM' ? 12 : 0)
              : time.hours;
        part.at =
            Date.UTC(
                date.year,
                date.month - 1,
                date.day,
                hours,
                time?.minutes ?? 0,
            ) / 60000;
        part.intended = {
            date: `${date.year}-${two(date.month)}-${two(date.day)}`,
            ...(time ? { time: `${two(hours)}:${two(time.minutes)}` } : {}),
        };
        if (fromIso && dateFormat) {
            const dd = two(date.day);
            const mm = two(date.month);
            part.send = {
                date:
                    dateFormat === 'dd/mm/yyyy'
                        ? `${dd}/${mm}/${date.year}`
                        : `${mm}/${dd}/${date.year}`,
                ...(time
                    ? {
                          hours: two(hours % 12 || 12),
                          minutes: two(time.minutes),
                          am_pm: hours >= 12 ? 'PM' : 'AM',
                      }
                    : {}),
            };
        } else if (fromIso) {
            part.notes.push(
                "The field's date order is not in the cached schema, so the ISO date was sent as written; refresh with knack_cache (refresh: true).",
            );
        }
    } else if (time && !dateText) {
        part.words = clock(time.hours, time.minutes, time.amPm);
    }
    return part;
}

/**
 * Read one date field's value in that field's own date order.
 *
 * Refuses (in `problems`) a date the order cannot hold, such as 13/09/2026 on a
 * month-first field or 31/02/2026, and a time that is not on the clock. Flags
 * (`ambiguous`) a date that reads as a real date in both orders. Accepts the structured
 * `{ date, hours, minutes, am_pm }` form and a range in `to`, read the same way.
 * An ISO date (YYYY-MM-DD, or with a time) is checked as a real date and rewritten in the
 * field's own order in `converted`; its offset is ignored, because Knack reads the time as
 * written. Anything else is left unread with a note.
 */
export function readDateValue(field: CachedField, value: unknown): DateReading {
    const format = field.dateFormat ?? null;
    const reading: DateReading = {
        field: field.key,
        ...(field.name ? { fieldName: field.name } : {}),
        format,
        input: value,
        understood: null,
        ambiguous: false,
        problems: [],
        notes: [],
        hasTime: field.dateHasTime,
        hasRange: false,
    };
    if (value === null || value === undefined || value === '') return reading;

    const start = readPart(field.key, format, value);
    const to = asRecord(value)?.to;
    const end =
        to !== undefined && to !== null && to !== ''
            ? readPart(field.key, format, to)
            : null;

    reading.ambiguous = start.ambiguous || Boolean(end?.ambiguous);
    reading.problems.push(
        ...start.problems,
        ...(end?.problems.map(
            (problem) => `the end of the range: ${problem}`,
        ) ?? []),
    );
    reading.notes.push(...start.notes, ...(end?.notes ?? []));
    reading.understood = end
        ? start.words && end.words
            ? `${start.words} to ${end.words}`
            : null
        : start.words;
    reading.intended = start.intended;
    reading.hasRange = end !== null;
    if (start.send || end?.send) {
        const record = asRecord(value);
        const endRecord = asRecord(to);
        reading.converted = record
            ? {
                  ...record,
                  ...(start.send ?? {}),
                  ...(end?.send ? { to: { ...endRecord, ...end.send } } : {}),
              }
            : start.send;
    }
    if (start.at !== null && end?.at != null && end.at < start.at) {
        reading.notes.push('The range ends before it starts.');
    }
    if (field.dateHasTime === false && reading.understood?.includes(':')) {
        reading.notes.push(
            'This field stores no time, so the time is dropped.',
        );
    }
    return reading;
}

/** The refusal line for one date problem: a code, the field, its format, the value. */
export function describeDateRefusal(
    reading: DateReading,
    problem: string,
): string {
    const name = reading.fieldName ? ` (${reading.fieldName})` : '';
    const format = reading.format ?? 'unknown order';
    const value =
        typeof reading.input === 'string'
            ? reading.input
            : JSON.stringify(reading.input);
    return `DATE_INVALID: ${reading.field}${name} is ${format}; ${problem}. Value sent: ${value}. Nothing was sent.`;
}

/**
 * Compare what Knack stored with what was meant, from the `field_N_raw` of its response.
 * `iso_timestamp` is the wall-clock time as written (not shifted to UTC: the real
 * instant is in `proper_iso_timestamp`, which is one hour earlier in British summer
 * time and must not be compared). Returns a warning, or null when they agree or there
 * is nothing to compare. The stored value is left out of the message on purpose.
 */
export function checkStoredDate(
    reading: DateReading,
    raw: unknown,
): string | null {
    const intended = reading.intended;
    if (!intended) return null;
    const stored = asRecord(raw);
    const iso = stored?.iso_timestamp;
    if (typeof iso !== 'string') return null;
    const name = reading.fieldName ? ` (${reading.fieldName})` : '';
    const sent = reading.understood ?? JSON.stringify(reading.input);
    const mismatch = (what: string) =>
        `DATE_MISMATCH: ${reading.field}${name} was sent as ${sent}, but Knack's response holds a different ${what}. Read the record to check.`;
    if (iso.slice(0, 10) !== intended.date) return mismatch('day');
    if (
        intended.time &&
        reading.hasTime !== false &&
        iso.slice(11, 16) !== intended.time
    ) {
        return mismatch('time');
    }
    if (reading.hasRange && !stored?.to) {
        return `DATE_RANGE_NOT_STORED: ${reading.field}${name} was sent as ${sent}, but Knack's response has no end date; only the start was stored. Read the record to check.`;
    }
    return null;
}
