import { asRecord } from './util.js';
import { getStringFromUnknown, truncateText } from './field-references.js';
import { CONNECTION_DISPLAY_VALUE_PRIORITY } from './seed-csv.js';

export type ShapeValidationStatus =
    'match' | 'mismatch' | 'skipped' | 'unknown';

export type ShapeValidationResult = {
    status: ShapeValidationStatus;
    observedFormattedShape: string;
    observedRawShape: string;
    findings: string[];
};

export function isBlankKnackValue(value: unknown): boolean {
    return (
        value === null ||
        value === undefined ||
        value === '' ||
        (Array.isArray(value) && value.length === 0)
    );
}

export function isHtmlLikeString(value: string): boolean {
    return /<[^>]+>/.test(value);
}

export function getObservedShape(value: unknown): string {
    if (value === null) return 'null';
    if (value === undefined) return 'undefined';
    if (Array.isArray(value)) {
        if (!value.length) return 'array(empty)';
        const firstNonBlank = value.find((entry) => !isBlankKnackValue(entry));
        if (firstNonBlank === undefined) return 'array(empty-like)';
        return `array(${getObservedShape(firstNonBlank)})`;
    }
    if (typeof value === 'string') {
        return isHtmlLikeString(value) ? 'html-string' : 'string';
    }
    if (typeof value === 'number' || typeof value === 'boolean') {
        return typeof value;
    }

    const rec = asRecord(value);
    if (rec) {
        const keys = Object.keys(rec).slice(0, 6);
        return `object(${keys.join(', ')})`;
    }

    return typeof value;
}

export function getValuePreview(value: unknown): unknown {
    if (typeof value === 'string') {
        return truncateText(value, 160);
    }
    if (Array.isArray(value)) {
        return value.slice(0, 2);
    }

    const rec = asRecord(value);
    if (rec) {
        return Object.fromEntries(Object.entries(rec).slice(0, 8));
    }

    return value;
}

export function rawHasKeys(value: unknown, keys: string[]): boolean {
    const rec = asRecord(value);
    return Boolean(rec) && keys.some((key) => key in rec!);
}

export function rawIsConnectionArray(value: unknown): boolean {
    if (!Array.isArray(value)) return false;
    return value.every((entry) => {
        const rec = asRecord(entry);
        if (!rec) return false;
        return typeof rec.id === 'string' || typeof rec.identifier === 'string';
    });
}

export function rawIsStringArray(value: unknown): boolean {
    return (
        Array.isArray(value) &&
        value.every((entry) => typeof entry === 'string')
    );
}

export function extractRecordList(body: unknown): Record<string, unknown>[] {
    if (Array.isArray(body)) {
        return body
            .map((entry) => asRecord(entry))
            .filter((entry): entry is Record<string, unknown> =>
                Boolean(entry),
            );
    }

    const rec = asRecord(body);
    const records = rec?.records;
    if (!Array.isArray(records)) return [];
    return records
        .map((entry) => asRecord(entry))
        .filter((entry): entry is Record<string, unknown> => Boolean(entry));
}

export function extractConnectionDisplayValues(body: unknown): string[] {
    const values: string[] = [];
    const seen = new Set<string>();

    for (const record of extractRecordList(body)) {
        const value = CONNECTION_DISPLAY_VALUE_PRIORITY.map((key) =>
            getStringFromUnknown(record[key]),
        ).find((candidate): candidate is string => Boolean(candidate));
        if (!value) continue;
        const dedupeKey = value.toLowerCase();
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);
        values.push(value);
    }

    return values;
}

export function validateFieldShape(
    fieldType: string,
    formatted: unknown,
    raw: unknown,
): ShapeValidationResult {
    const observedFormattedShape = getObservedShape(formatted);
    const observedRawShape = getObservedShape(raw);

    if (isBlankKnackValue(formatted) && isBlankKnackValue(raw)) {
        return {
            status: 'skipped',
            observedFormattedShape,
            observedRawShape,
            findings: [],
        };
    }

    const findings: string[] = [];
    const addFinding = (condition: boolean, message: string) => {
        if (!condition) findings.push(message);
    };

    switch (fieldType.toLowerCase()) {
        case 'short_text':
        case 'paragraph_text':
        case 'concatenation':
        case 'rich_text':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                typeof raw === 'string',
                'Raw value should be a string.',
            );
            break;
        case 'email':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string or HTML anchor.',
            );
            addFinding(
                rawHasKeys(raw, ['email']),
                'Raw value should be an object containing an email key.',
            );
            break;
        case 'phone':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string or HTML anchor.',
            );
            addFinding(
                rawHasKeys(raw, ['number', 'full', 'formatted']),
                'Raw value should be a phone object with number/full/formatted keys.',
            );
            break;
        case 'number':
            addFinding(
                typeof formatted === 'string' || typeof formatted === 'number',
                'Formatted value should be a string or number.',
            );
            addFinding(
                typeof raw === 'number' || typeof raw === 'string',
                'Raw value should be a number or numeric string.',
            );
            break;
        case 'currency':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                typeof raw === 'number' || typeof raw === 'string',
                'Raw value should be a number or numeric string.',
            );
            break;
        case 'auto_increment':
        case 'rating':
        case 'sum':
        case 'count':
        case 'average':
        case 'min':
        case 'max':
            addFinding(
                typeof formatted === 'string' || typeof formatted === 'number',
                'Formatted value should be numeric-like.',
            );
            addFinding(
                typeof raw === 'number',
                'Raw value should be a number.',
            );
            break;
        case 'boolean':
        case 'yes_no':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a display string such as Yes/No.',
            );
            addFinding(
                typeof raw === 'boolean',
                'Raw value should be a boolean.',
            );
            break;
        case 'equation':
            addFinding(
                typeof formatted === 'string' || typeof formatted === 'number',
                'Formatted value should be a string or number.',
            );
            addFinding(
                typeof raw === 'number' ||
                    typeof raw === 'string' ||
                    asRecord(raw) !== null,
                'Raw value should be a number, string, or structured date-like object.',
            );
            break;
        case 'name':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, [
                    'first',
                    'last',
                    'full',
                    'title',
                    'middle',
                    'suffix',
                ]),
                'Raw value should be an object containing name parts.',
            );
            break;
        case 'address':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string, often with HTML line breaks.',
            );
            addFinding(
                rawHasKeys(raw, ['street', 'city', 'zip', 'full']),
                'Raw value should be an object containing address components.',
            );
            break;
        case 'date_time':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, [
                    'date',
                    'timestamp',
                    'unix_timestamp',
                    'iso_timestamp',
                    'to',
                ]),
                'Raw value should be a structured date/time object.',
            );
            break;
        case 'timer':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, ['times', 'hours', 'minutes', 'seconds']),
                'Raw value should be a timer object containing time segments or totals.',
            );
            break;
        case 'multiple_choice':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a display string.',
            );
            addFinding(
                typeof raw === 'string' || Array.isArray(raw),
                'Raw value should be a string or an array of strings.',
            );
            if (Array.isArray(raw)) {
                addFinding(
                    rawIsStringArray(raw),
                    'Raw multiple choice arrays should contain strings.',
                );
            }
            break;
        case 'connection':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string, usually HTML.',
            );
            addFinding(
                rawIsConnectionArray(raw),
                'Raw value should be an array of connection objects with id and/or identifier.',
            );
            break;
        case 'file':
        case 'image':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, [
                    'id',
                    'filename',
                    'url',
                    'thumb_url',
                    'mime_type',
                ]),
                'Raw value should be an attachment object with file metadata.',
            );
            break;
        case 'signature':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, [
                    'svg',
                    'base30',
                    'base64',
                    'url',
                    'thumb_url',
                    'timestamp',
                    'date',
                ]),
                'Raw value should be a signature object with stroke/image metadata.',
            );
            break;
        case 'link':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string, often HTML.',
            );
            addFinding(
                rawHasKeys(raw, ['url', 'label']),
                'Raw value should be an object containing url/label.',
            );
            break;
        case 'user_roles':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a display string.',
            );
            addFinding(
                rawIsStringArray(raw),
                'Raw value should be an array of role name strings.',
            );
            break;
        case 'password':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, ['validation']),
                'Raw value should be an object containing password validation metadata.',
            );
            break;
        default:
            return {
                status: 'unknown',
                observedFormattedShape,
                observedRawShape,
                findings: [
                    `No automated verifier is defined for field type ${fieldType}.`,
                ],
            };
    }

    return {
        status: findings.length ? 'mismatch' : 'match',
        observedFormattedShape,
        observedRawShape,
        findings,
    };
}
