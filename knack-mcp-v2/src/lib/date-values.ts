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
    format: string | null;
    input: unknown;
    /** The date in words ("3 September 2026, 10:30 am"), or null when it was not read. */
    understood: string | null;
    /** Both parts are 12 or less and differ, so the other order is also a real date. */
    ambiguous: boolean;
    /** Why the value is refused; a refused value is never sent. */
    problems: string[];
    notes: string[];
};

type Part = {
    words: string | null;
    ambiguous: boolean;
    problems: string[];
    notes: string[];
    /** Minutes since the epoch, for ordering a range; null when it could not be read. */
    at: number | null;
};

const SLASH_DATE = /^\s*(\d{1,2})\/(\d{1,2})\/(\d{4})\b/;
const TRAILING_TIME = /(\d{1,2}):(\d{2})\s*(am|pm)?\s*$/i;

function daysInMonth(month: number, year: number): number {
    return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

function clock(hours: number, minutes: number, amPm: string | null): string {
    return `${hours}:${String(minutes).padStart(2, '0')}${amPm ? ` ${amPm.toLowerCase()}` : ''}`;
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
    dateFormat: string | null,
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
    const match = dateText ? SLASH_DATE.exec(dateText) : null;
    if (match) {
        const first = Number(match[1]);
        const second = Number(match[2]);
        const year = Number(match[3]);
        const monthFirst = dateFormat === 'mm/dd/yyyy';
        const dayFirst = dateFormat === 'dd/mm/yyyy';
        if (dateFormat && !monthFirst && !dayFirst) {
            part.notes.push(`Date order ${dateFormat} is not checked.`);
        } else if (monthFirst || dayFirst) {
            const month = monthFirst ? first : second;
            const day = monthFirst ? second : first;
            if (month < 1 || month > 12) {
                part.problems.push(
                    `"${match[0].trim()}" has no month ${month}; this field reads ${dateFormat}`,
                );
            } else if (day < 1 || day > daysInMonth(month, year)) {
                part.problems.push(
                    `"${match[0].trim()}" has no day ${day} in ${MONTH_NAMES[month - 1]} ${year}; this field reads ${dateFormat}`,
                );
            } else {
                date = { day, month, year };
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
            `"${dateText}" is not a dd/mm/yyyy or mm/dd/yyyy date, so its order was not checked.`,
        );
    }

    if (date) {
        const day = `${date.day} ${MONTH_NAMES[date.month - 1]} ${date.year}`;
        part.words = time
            ? `${day}, ${clock(time.hours, time.minutes, time.amPm)}`
            : day;
        const hour24 = time
            ? (time.hours % 12) + (time.amPm === 'PM' ? 12 : 0)
            : 0;
        const hours = time && !time.amPm ? time.hours : hour24;
        part.at =
            Date.UTC(
                date.year,
                date.month - 1,
                date.day,
                hours,
                time?.minutes ?? 0,
            ) / 60000;
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
 * Anything that is not a slash date (ISO text, say) is left unread with a note.
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
