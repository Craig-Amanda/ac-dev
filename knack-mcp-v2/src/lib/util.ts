import path from 'node:path';
import fs from 'node:fs';

export function isEnabledEnv(
    value: string | undefined,
    defaultValue: boolean,
): boolean {
    if (!value) return defaultValue;
    const normalised = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalised)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalised)) return false;
    return defaultValue;
}

export function getPositiveIntEnv(
    value: string | undefined,
    fallback: number,
): number {
    if (!value) return fallback;
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    return Math.trunc(parsed);
}

export function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Run `worker` over `items` with at most `concurrency` in flight at once. Results are
 * returned in the same order as `items` regardless of completion order. Used by batch
 * record tools to overlap several Knack API calls instead of running fully sequentially.
 */
export async function runWithConcurrency<T, R>(
    items: T[],
    concurrency: number,
    worker: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
    const results: R[] = new Array(items.length);
    let nextIndex = 0;

    async function runNext(): Promise<void> {
        while (nextIndex < items.length) {
            const currentIndex = nextIndex;
            nextIndex += 1;
            results[currentIndex] = await worker(
                items[currentIndex],
                currentIndex,
            );
        }
    }

    const workerCount = Math.max(1, Math.min(concurrency, items.length));
    await Promise.all(Array.from({ length: workerCount }, () => runNext()));

    return results;
}

export function normalisePath(p: string): string {
    // Normalise for Windows/Mac comparisons
    return path.resolve(p).replaceAll('\\', '/').toLowerCase();
}

export function normaliseAppIdentity(value: string): string {
    return value
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '');
}

export function readJsonFile<T>(filePath: string): T | null {
    try {
        const raw = fs.readFileSync(filePath, 'utf8');
        return JSON.parse(raw) as T;
    } catch {
        return null;
    }
}

export function writeJsonFile(
    filePath: string,
    data: unknown,
): { ok: true } | { ok: false; error: string } {
    try {
        fs.mkdirSync(path.dirname(filePath), { recursive: true });
        fs.writeFileSync(
            filePath,
            `${JSON.stringify(data, null, 2)}\n`,
            'utf8',
        );
        return { ok: true };
    } catch (error) {
        return {
            ok: false,
            error: error instanceof Error ? error.message : String(error),
        };
    }
}

export function asRecord(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value))
        return null;
    return value as Record<string, unknown>;
}

/**
 * Recursively merge plain-object properties (e.g. format, relationship) so a dry-run preview
 * of a partial update — {format: {precision: "2"}} — keeps sibling keys instead of replacing
 * the whole nested object, matching how a caller reads "merged" intuitively.
 *
 * @param base Current value (e.g. the live field definition).
 * @param updates Partial value to layer on top.
 * @returns A new object with updates applied, merging nested plain objects recursively.
 */
export function deepMergeRecords(
    base: Record<string, unknown>,
    updates: Record<string, unknown>,
): Record<string, unknown> {
    const merged: Record<string, unknown> = { ...base };
    for (const [key, value] of Object.entries(updates)) {
        const baseRecord = asRecord(merged[key]);
        const updateRecord = asRecord(value);
        merged[key] =
            baseRecord && updateRecord
                ? deepMergeRecords(baseRecord, updateRecord)
                : value;
    }
    return merged;
}

export function getTrimmedString(value: unknown): string | null {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
}

export function parseJsonInput<T>(label: string, text: string): T {
    const trimmed = text.trim();
    if (!trimmed) {
        throw new Error(`${label} cannot be empty.`);
    }
    return JSON.parse(trimmed) as T;
}

export function cloneJsonValue<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
}

export function fileExists(filePath: string): boolean {
    try {
        fs.accessSync(filePath, fs.constants.F_OK);
        return true;
    } catch {
        return false;
    }
}
