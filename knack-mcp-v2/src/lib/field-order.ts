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
import { getDerivedFromFieldKeys } from './metadata.js';
import { asRecord } from './util.js';

export type FieldPlacement =
    { kind: 'full' } | { kind: 'after' | 'before'; anchor: string };

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
    } else {
        if (!known.has(placement.anchor))
            errors.push(`${placement.anchor} is not a field on this object.`);
        if (seen.has(placement.anchor))
            errors.push(
                `${placement.anchor} cannot be both moved and the anchor.`,
            );
    }
    if (errors.length) return { order: [], errors };
    if (placement.kind === 'full') return { order: moving, errors };

    const rest = current.filter((key) => !seen.has(key));
    const at =
        rest.indexOf(placement.anchor) + (placement.kind === 'after' ? 1 : 0);
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

/** Whether a field of `type` is computed on save from other fields. */
export function isComputedFieldType(type: unknown): boolean {
    return COMPUTED_TYPES.has(String(type));
}

/**
 * One warning per computed field that, in `order`, sits before a computed field on the
 * same object that its formula reads — the case where a save computes it from a stale
 * value. With `involving`, only the warnings that name that field, either as the formula
 * or as one of the inputs it reads too early.
 */
export function equationOrderWarnings(
    fields: Array<Record<string, unknown>>,
    order: string[],
    involving?: string,
): string[] {
    const position = new Map(order.map((key, index) => [key, index]));
    const at = (key: string) => position.get(key) ?? -1;
    const computed = fields.filter((field) => isComputedFieldType(field.type));
    const computedKeys = new Set(computed.map((field) => String(field.key)));
    const warnings: string[] = [];
    for (const field of computed) {
        const key = String(field.key);
        const format = asRecord(field.format);
        const equation = String(format?.equation ?? '');
        // Only a plain `{field_N}` computed on this object counts. The field in
        // `{field_A.field_B}` is read from a connected record, which this save does not
        // recompute — even through a connection back to this object, where field_B is a
        // key on this table too. The field's own key is already dropped.
        const later = (getDerivedFromFieldKeys(key, format) ?? [])
            .filter(
                (input) =>
                    computedKeys.has(input) &&
                    equation.includes(`{${input}}`) &&
                    at(input) > at(key),
            )
            .sort((a, b) => at(a) - at(b));
        if (!later.length) continue;
        if (involving && key !== involving && !later.includes(involving))
            continue;
        warnings.push(
            `${key} reads ${later.join(', ')}, which ${later.length === 1 ? 'comes' : 'come'} after it; Knack evaluates equations in field order, so ${key} will use the previous value. Move ${key} after ${later.at(-1)}.`,
        );
    }
    return warnings;
}
