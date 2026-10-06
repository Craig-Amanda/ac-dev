/**
 * Pure helpers for a record's change history (the Builder's record-history feed): each
 * entry is a full snapshot of the record after one save, newest first, with the user,
 * time and the page or API call that made the save.
 */
import { asRecord } from './util.js';

const FIELD_KEY = /^field_\d+$/;

/** Field keys an entry carries a value for (formatted keys only, not their `_raw` twins). */
export function historyFieldKeys(entry: Record<string, unknown>): string[] {
    return Object.keys(entry).filter((key) => FIELD_KEY.test(key));
}

/** The raw value of a field when the entry has one, otherwise its formatted value. */
export function historyValue(
    entry: Record<string, unknown>,
    fieldKey: string,
): unknown {
    const rawKey = `${fieldKey}_raw`;
    return rawKey in entry ? entry[rawKey] : entry[fieldKey];
}

export function isBlankHistoryValue(value: unknown): boolean {
    return (
        value === undefined ||
        value === null ||
        value === '' ||
        (Array.isArray(value) && value.length === 0)
    );
}

/**
 * Fields of `entry` whose value differs from `older` (a missing older entry counts as
 * empty, so a record's first save lists every field it set). Only keys in `fieldKeys`
 * are compared. Blank and absent values are the same value.
 */
export function changedHistoryFields(
    entry: Record<string, unknown>,
    older: Record<string, unknown> | undefined,
    fieldKeys: string[],
): string[] {
    return fieldKeys.filter((key) => {
        const now = historyValue(entry, key);
        const before = older ? historyValue(older, key) : undefined;
        if (isBlankHistoryValue(now) && isBlankHistoryValue(before))
            return false;
        return JSON.stringify(now) !== JSON.stringify(before);
    });
}

/** Where a save came from: the app page or API call, read from the entry's `origin`. */
export function describeHistoryOrigin(entry: Record<string, unknown>): {
    source?: string;
    method?: string;
    scene?: string;
    view?: string;
} {
    const origin = asRecord(entry.origin);
    if (!origin) return {};
    const url = typeof origin.url === 'string' ? origin.url : '';
    const scene = /scenes\/(scene_\d+)/.exec(url)?.[1];
    const view = /views\/(view_\d+)/.exec(url)?.[1];
    return {
        ...(typeof origin.source === 'string' ? { source: origin.source } : {}),
        ...(typeof origin.method === 'string' ? { method: origin.method } : {}),
        ...(scene ? { scene } : {}),
        ...(view ? { view } : {}),
    };
}
