/**
 * Field order on an object. Knack's sort route (`POST /objects/{key}/fields/sort`, captured
 * from the builder) takes the whole order, system fields included, so a move is always
 * turned into a complete list here before anything is sent.
 *
 * The order is not only cosmetic: Knack evaluates equation and text formula fields in field
 * order within one save, so a formula placed before a computed field it reads sees that
 * field's previous value (GAP-Track object_44, field_2633 and field_2620-2625, 28
 * September).
 */
import { asRecord } from './util.js';

export type FieldPlacement =
    | { kind: 'full' }
    | { kind: 'after'; anchor: string }
    | { kind: 'before'; anchor: string };

/**
 * The complete new order, or the errors that stop it. `moving` keeps the order the caller
 * gave; with `full` it must name every current field exactly once.
 */
export function planFieldOrder(
    current: string[],
    moving: string[],
    placement: FieldPlacement,
): { order: string[]; errors: string[] } {
    const errors: string[] = [];
    const known = new Set(current);
    const seen = new Set<string>();
    for (const key of moving) {
        if (seen.has(key)) errors.push(`${key} is named more than once.`);
        seen.add(key);
        if (!known.has(key))
            errors.push(`${key} is not a field on this object.`);
    }

    if (placement.kind === 'full') {
        const missing = current.filter((key) => !seen.has(key));
        if (missing.length)
            errors.push(
                `A full order must name every field; missing: ${missing.join(', ')}. To move only some fields, pass after or before.`,
            );
        return { order: errors.length ? [] : [...moving], errors };
    }

    const { anchor } = placement;
    if (!known.has(anchor))
        errors.push(`${anchor} is not a field on this object.`);
    if (seen.has(anchor))
        errors.push(`${anchor} cannot be both moved and the anchor.`);
    if (errors.length) return { order: [], errors };

    const rest = current.filter((key) => !seen.has(key));
    const at = rest.indexOf(anchor) + (placement.kind === 'after' ? 1 : 0);
    return {
        order: [...rest.slice(0, at), ...moving, ...rest.slice(at)],
        errors,
    };
}

/** The field keys of a sort response (`{ fields: [...] }`), in order, or undefined. */
export function readSortedFieldKeys(body: unknown): string[] | undefined {
    const fields = asRecord(body)?.fields;
    if (!Array.isArray(fields)) return undefined;
    return fields
        .map((field) => asRecord(field)?.key)
        .filter((key): key is string => typeof key === 'string');
}

/** Field types whose value Knack computes on save from other fields. */
const COMPUTED_TYPES = new Set(['equation', 'concatenation']);

/**
 * One warning per computed field that, in `order`, sits before a computed field on the
 * same object that its formula reads — the case where a save computes it from a stale
 * value.
 */
export function equationOrderWarnings(
    fields: Array<Record<string, unknown>>,
    order: string[],
): string[] {
    const position = new Map(order.map((key, index) => [key, index]));
    const computed = new Map(
        fields
            .filter((field) => COMPUTED_TYPES.has(String(field.type)))
            .map((field) => [
                String(field.key),
                String(asRecord(field.format)?.equation ?? ''),
            ]),
    );
    const warnings: string[] = [];
    for (const [key, equation] of computed) {
        // Only `{field_N}` on this object: `{field_A.field_B}` reads a connected
        // record, which is not recomputed by this save. A self-reference is skipped.
        const at = position.get(key) ?? -1;
        const later = [
            ...new Set(
                [...equation.matchAll(/\{(field_\d+)\}/g)].map(
                    (match) => match[1],
                ),
            ),
        ].filter(
            (input) =>
                input !== key &&
                computed.has(input) &&
                (position.get(input) ?? -1) > at,
        );
        if (!later.length) continue;
        const lowest = later.reduce((a, b) =>
            (position.get(b) ?? -1) > (position.get(a) ?? -1) ? b : a,
        );
        warnings.push(
            `${key} reads ${later.join(', ')}, which ${later.length === 1 ? 'comes' : 'come'} after it; Knack evaluates equations in field order, so ${key} will use the previous value. Move ${key} after ${lowest}.`,
        );
    }
    return warnings;
}
