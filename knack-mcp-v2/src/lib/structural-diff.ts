import { asRecord } from './util.js';

/** One leaf-level difference between two JSON-like values, located by path. */
export interface StructuralDiffEntry {
    path: string;
    before: unknown;
    after: unknown;
}

/**
 * Caps how many entries `computeStructuralDiff` collects, so a caller who genuinely
 * replaces a large structure (or a whole view) gets a usably-sized response instead of a
 * multi-thousand-entry wall. The entries collected are always the first found in a
 * depth-first walk, not a sample — enough to show the shape of a difference even when
 * truncated.
 */
const MAX_DIFF_ENTRIES = 200;

/**
 * True if two JSON-like values are equal by value — same primitives, same array
 * elements in the same order, same object keys mapping to equal values, key order
 * ignored. Reference equality (`===`) is checked first as a fast path; everything below
 * it is structural.
 */
export function deepEqual(a: unknown, b: unknown): boolean {
    if (a === b) return true;
    if (a === null || b === null) return false;
    if (typeof a !== typeof b) return false;
    if (typeof a !== 'object') return false;

    const aArr = Array.isArray(a);
    const bArr = Array.isArray(b);
    if (aArr !== bArr) return false;

    if (aArr && bArr) {
        if (a.length !== b.length) return false;
        return a.every((item, index) => deepEqual(item, b[index]));
    }

    const recA = asRecord(a);
    const recB = asRecord(b);
    if (!recA || !recB) return false;
    const keysA = Object.keys(recA);
    const keysB = Object.keys(recB);
    if (keysA.length !== keysB.length) return false;
    return keysA.every(
        (key) => key in recB && deepEqual(recA[key], recB[key]),
    );
}

/**
 * Every leaf-level difference between two JSON-like values, by path (e.g. `$.columns[5].scene`).
 *
 * Arrays are compared positionally — the same convention Knack's own view JSON uses
 * (columns, rules, links carry no stable id) — so `path[5]` differing means exactly
 * that: whatever sits at position 5 changed, not that some element moved elsewhere. A
 * length mismatch is reported once, at the array's own path, rather than walked
 * index-by-index: past the shorter array's length every index would otherwise show as a
 * spurious "added"/"removed" pair, drowning out whatever actually changed among the
 * elements both arrays share.
 *
 * Exists so a mutation's effect on a view can be stated precisely — these paths, and no
 * others, differ from the live definition — rather than left for a reader to work out by
 * eye from two large JSON blobs. Built after a live incident (`GAP-Track`, `view_3255`,
 * 2026-09-18) where a hand-built `columns` patch silently altered a column the caller
 * never meant to touch (a "Docs" link column's `scene` and styling, copied from an
 * unrelated column elsewhere on the same view), and nothing in the response said so —
 * the guard's merge is a top-level spread (`buildEffectiveUpdateBody`), so it has no way
 * to know a caller-supplied replacement value differs from the live one anywhere but the
 * places the caller already declared. This is that visibility, computed once and
 * attached to every view-mutation response so an unintended change is named, not missed.
 */
export function computeStructuralDiff(
    before: unknown,
    after: unknown,
    path = '$',
): StructuralDiffEntry[] {
    const out: StructuralDiffEntry[] = [];
    diffInto(before, after, path, out);
    return out;
}

function diffInto(
    before: unknown,
    after: unknown,
    path: string,
    out: StructuralDiffEntry[],
): void {
    if (out.length >= MAX_DIFF_ENTRIES) return;
    if (deepEqual(before, after)) return;

    const beforeArr = Array.isArray(before);
    const afterArr = Array.isArray(after);
    if (beforeArr && afterArr) {
        if (before.length !== after.length) {
            out.push({
                path,
                before: `array(${before.length})`,
                after: `array(${after.length})`,
            });
            return;
        }
        for (let i = 0; i < after.length; i++) {
            if (out.length >= MAX_DIFF_ENTRIES) return;
            diffInto(before[i], after[i], `${path}[${i}]`, out);
        }
        return;
    }

    const beforeRec = asRecord(before);
    const afterRec = asRecord(after);
    if (beforeRec && afterRec) {
        const keys = new Set([
            ...Object.keys(beforeRec),
            ...Object.keys(afterRec),
        ]);
        for (const key of keys) {
            if (out.length >= MAX_DIFF_ENTRIES) return;
            diffInto(beforeRec[key], afterRec[key], `${path}.${key}`, out);
        }
        return;
    }

    out.push({ path, before, after });
}
