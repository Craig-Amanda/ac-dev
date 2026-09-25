/**
 * Field exclusion: KTL-style `_mcp_*` keywords that limit what the model can see or change.
 *
 * A keyword goes in a field's description in the builder, where only a person can add or
 * remove it (the field tools refuse to drop one). Knack objects have no description, so
 * an object-wide keyword lives in app.json as `dataAccess.objectKeywords` instead.
 *
 * - `_mcp_writeonly`: values read back as "[redacted]"; writes still go through.
 * - `_mcp_schemalock`: records behave normally; the field definition cannot be edited,
 *   duplicated or deleted through MCP.
 * - `_mcp_hidden`: the field is left out of every record and schema read, and cannot be
 *   written, filtered on or edited.
 *
 * A formula field (equation, text formula, sum/min/max/average over a connection) that
 * reads a redacted field would reproduce its value, so it inherits the strictest read
 * tier of anything it reads. So does a field whose conditional rule copies a redacted
 * field's value into it. A count field whose filters test a redacted field is left
 * alone: it reads no values, only how many records match.
 */
import type { AppConfig } from '../config.js';
import type { KnackContext } from '../context.js';
import type { CachedObject, CachedSchema } from '../types.js';
import { containsKtlKeywordToken } from './field-references.js';
import { asRecord } from './util.js';

const MCP_WRITEONLY = '_mcp_writeonly';
const MCP_SCHEMALOCK = '_mcp_schemalock';
const MCP_HIDDEN = '_mcp_hidden';
export const MCP_KEYWORDS = [
    MCP_WRITEONLY,
    MCP_SCHEMALOCK,
    MCP_HIDDEN,
] as const;
export type McpKeyword = (typeof MCP_KEYWORDS)[number];

/** What a masked value reads as, formatted and `_raw` alike. */
export const REDACTED_VALUE = '[redacted]';

export type FieldExclusions = {
    /** Never returned as a value: config-redacted, write-only and hidden fields, plus formulas over them. */
    readBlocked: Set<string>;
    /** The part of readBlocked that is returned as "[redacted]" rather than left out. */
    masked: Set<string>;
    /** Left out of schema and record reads entirely, and refused on writes. */
    hidden: Set<string>;
    /** Definition cannot be changed through MCP: schema-locked and hidden fields. */
    schemaLocked: Set<string>;
    /** Per object, connection fields whose linked records' display values are masked. */
    maskedConnections: Map<string, Set<string>>;
    /** The keyword or config entry that put each field in its tier, for error messages. */
    reasons: Map<string, string>;
    /** Objects with a read-blocked field or a masked connection: their record reads are projected. */
    objects: Set<string>;
};

/**
 * The `_mcp_*` keywords a description carries, as whole tokens in any case: a person who
 * types `_MCP_Hidden` in the builder means the field to be hidden, and a case-sensitive
 * match would leave it readable without anyone noticing.
 */
export function getMcpKeywords(text: string | undefined): McpKeyword[] {
    if (!text) return [];
    const lower = text.toLowerCase();
    return MCP_KEYWORDS.filter((keyword) =>
        containsKtlKeywordToken(lower, keyword),
    );
}

export function buildFieldExclusions(
    schema: CachedSchema | null,
    dataAccess: AppConfig['dataAccess'],
): FieldExclusions {
    const exclusions: FieldExclusions = {
        readBlocked: new Set(),
        masked: new Set(),
        hidden: new Set(),
        schemaLocked: new Set(),
        maskedConnections: new Map(),
        reasons: new Map(),
        objects: new Set(),
    };
    const objects = schema?.objects || [];

    const markHidden = (key: string, reason: string) => {
        exclusions.hidden.add(key);
        exclusions.masked.delete(key);
        exclusions.readBlocked.add(key);
        exclusions.schemaLocked.add(key);
        exclusions.reasons.set(key, reason);
    };
    const markWriteOnly = (key: string, reason: string) => {
        if (exclusions.readBlocked.has(key)) return;
        exclusions.readBlocked.add(key);
        exclusions.masked.add(key);
        exclusions.reasons.set(key, reason);
    };

    for (const key of dataAccess?.redactedFieldKeys || []) {
        exclusions.readBlocked.add(key);
        exclusions.reasons.set(key, 'dataAccess.redactedFieldKeys');
    }

    for (const object of objects) {
        const objectKeywords = new Set(
            dataAccess?.objectKeywords?.[object.key] || [],
        );
        for (const field of object.fields || []) {
            const keywords = new Set<string>([
                ...objectKeywords,
                ...getMcpKeywords(field.description),
            ]);
            const source = (keyword: string) =>
                objectKeywords.has(keyword)
                    ? `${keyword} on ${object.key} (dataAccess.objectKeywords)`
                    : keyword;
            if (keywords.has(MCP_HIDDEN)) {
                markHidden(field.key, source(MCP_HIDDEN));
                continue;
            }
            if (keywords.has(MCP_SCHEMALOCK)) {
                exclusions.schemaLocked.add(field.key);
                exclusions.reasons.set(field.key, source(MCP_SCHEMALOCK));
            }
            if (keywords.has(MCP_WRITEONLY))
                markWriteOnly(field.key, source(MCP_WRITEONLY));
        }
    }

    inheritDerivedTiers(objects, exclusions, markHidden, markWriteOnly);

    const objectByKey = new Map(objects.map((object) => [object.key, object]));
    for (const object of objects) {
        for (const field of object.fields || []) {
            if (!field.connectedObject) continue;
            const target = objectByKey.get(field.connectedObject);
            if (!target?.identifier) continue;
            if (!exclusions.readBlocked.has(target.identifier)) continue;
            const set =
                exclusions.maskedConnections.get(object.key) || new Set();
            set.add(field.key);
            exclusions.maskedConnections.set(object.key, set);
        }
        if (
            exclusions.maskedConnections.has(object.key) ||
            (object.fields || []).some((field) =>
                exclusions.readBlocked.has(field.key),
            )
        )
            exclusions.objects.add(object.key);
    }

    return exclusions;
}

/**
 * Give every formula field the strictest read tier of the fields it reads (`derivedFrom`),
 * and every field with a copying conditional rule that of the fields it copies
 * (`copiedFrom`), repeating until nothing changes, since a formula can read another
 * formula or a copy.
 *
 * - Reads a hidden field → markHidden(formula, reason).
 * - Reads any other read-blocked field (write-only, or dataAccess.redactedFieldKeys) →
 *   markWriteOnly(formula, reason), so it reads back as "[redacted]".
 * - Only the read tier is inherited: a formula over a schema-locked field is not locked.
 * - `reason` should name the source field, e.g. `formula over field_12 (_mcp_writeonly)`,
 *   because it ends up in refusal messages.
 */
function inheritDerivedTiers(
    objects: CachedObject[],
    exclusions: FieldExclusions,
    markHidden: (key: string, reason: string) => void,
    markWriteOnly: (key: string, reason: string) => void,
): void {
    const derived = objects
        .flatMap((object) => object.fields || [])
        .filter(
            (field) => field.derivedFrom?.length || field.copiedFrom?.length,
        );

    let changed = true;
    while (changed) {
        changed = false;
        for (const field of derived) {
            if (exclusions.hidden.has(field.key)) continue;
            const sources = [
                ...(field.derivedFrom || []),
                ...(field.copiedFrom || []),
            ];
            const sourceReason = (key: string) =>
                `${field.derivedFrom?.includes(key) ? 'formula over' : 'conditional rule copying'} ${key} (${exclusions.reasons.get(key) || 'field exclusion'})`;
            const hiddenSource = sources.find((key) =>
                exclusions.hidden.has(key),
            );
            if (hiddenSource) {
                markHidden(field.key, sourceReason(hiddenSource));
                changed = true;
                continue;
            }
            if (exclusions.readBlocked.has(field.key)) continue;
            const blockedSource = sources.find((key) =>
                exclusions.readBlocked.has(key),
            );
            if (blockedSource) {
                markWriteOnly(field.key, sourceReason(blockedSource));
                changed = true;
            }
        }
    }
}

/** A schema with the hidden fields removed, for every read that describes the app. */
export function withoutHiddenFields(
    schema: CachedSchema,
    exclusions: FieldExclusions,
): CachedSchema {
    if (!exclusions.hidden.size) return schema;
    return {
        ...schema,
        objects: (schema.objects || []).map((object) => ({
            ...object,
            fields: (object.fields || []).filter(
                (field) => !exclusions.hidden.has(field.key),
            ),
        })),
    };
}

/**
 * A raw object definition (REST `object` or a runtime-metadata object) with its hidden
 * fields removed, for the diagnostic reads that return Knack's own payload.
 */
export function withoutHiddenRawFields(
    rawObject: unknown,
    exclusions: FieldExclusions,
): unknown {
    const object = asRecord(rawObject);
    if (!object || !Array.isArray(object.fields) || !exclusions.hidden.size)
        return rawObject;
    return {
        ...object,
        fields: object.fields.filter((field) => {
            const key = asRecord(field)?.key;
            return !(typeof key === 'string' && exclusions.hidden.has(key));
        }),
    };
}

/**
 * The limits on a visible field, for schema reads to show beside it: `writeOnly` (its
 * value reads as "[redacted]"), `redacted` (left out of records) and `schemaLocked`.
 * Undefined when nothing limits it.
 */
export function getFieldAccessLimits(
    exclusions: FieldExclusions,
    fieldKey: string,
): string[] | undefined {
    const limits: string[] = [];
    if (exclusions.masked.has(fieldKey)) limits.push('writeOnly');
    else if (exclusions.readBlocked.has(fieldKey)) limits.push('redacted');
    if (exclusions.schemaLocked.has(fieldKey)) limits.push('schemaLocked');
    return limits.length ? limits : undefined;
}

/**
 * Why the definition of `fieldKey` (or, with no field key, of any field on the object)
 * cannot be changed through MCP; empty when nothing locks it. Reads the cached policy,
 * which also covers `dataAccess.objectKeywords`, plus the live field list when the
 * caller has already fetched it: a keyword a person has just added in the builder may
 * not be in the cache yet. It never fetches on its own, so a guarded tool makes the
 * same requests it always did.
 */
export async function getSchemaLockReasons(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    fieldKey?: string,
    liveFields?: unknown,
): Promise<string[]> {
    const exclusions = await ctx.getFieldExclusions(app);
    const { schema } = await ctx.getFullSchema(app);
    const cachedKeys = fieldKey
        ? [fieldKey]
        : (
              schema?.objects?.find((entry) => entry.key === objectKey)
                  ?.fields || []
          ).map((field) => field.key);
    const reasons = new Map<string, string>();
    for (const key of cachedKeys) {
        if (exclusions.schemaLocked.has(key))
            reasons.set(key, describeExclusion(exclusions, key));
    }

    for (const entry of Array.isArray(liveFields) ? liveFields : []) {
        const field = asRecord(entry);
        const key = typeof field?.key === 'string' ? field.key : null;
        if (!key || reasons.has(key) || (fieldKey && key !== fieldKey))
            continue;
        const description =
            typeof field?.description === 'string'
                ? field.description
                : asRecord(field?.meta)?.description;
        const locking = getMcpKeywords(
            typeof description === 'string' ? description : undefined,
        ).filter((keyword) => keyword !== MCP_WRITEONLY);
        if (locking.length)
            reasons.set(key, `${key} carries ${locking.join(', ')}`);
    }
    return [...reasons.values()];
}

/**
 * Every field key a rule, task action or other JSON value names: each `field_N` token in
 * any string inside it, so `{field_12}` in an email message and both halves of a
 * `field_1.field_2` connection path count, as well as `field`, `input` and `value_field`.
 * Any string, not only the known keys: the field-exclusion checks must not depend on
 * knowing every property Knack puts a field key under.
 */
export function collectFieldKeyRefs(value: unknown): string[] {
    const keys = new Set<string>();
    const walk = (entry: unknown) => {
        if (typeof entry === 'string') {
            for (const key of entry.match(/\bfield_\d+\b/g) || [])
                keys.add(key);
        } else if (Array.isArray(entry)) {
            entry.forEach(walk);
        } else {
            const record = asRecord(entry);
            if (record) Object.values(record).forEach(walk);
        }
    };
    walk(value);
    return [...keys];
}

/** The `_mcp_hidden` fields a JSON value names (see collectFieldKeyRefs). */
export function hiddenFieldRefs(
    exclusions: FieldExclusions,
    value: unknown,
): string[] {
    return collectFieldKeyRefs(value).filter((key) =>
        exclusions.hidden.has(key),
    );
}

/**
 * The field keys a rule or task action reads: every `field_N` token in it except a
 * `values[].field`, which is where a record or conditional rule writes. A criterion, a
 * value copied through `values[].input`, a `{field_N}` in an email or message, and any
 * key Knack might add all read the field.
 */
export function readFieldKeyRefs(value: unknown): string[] {
    const keys = new Set<string>();
    const walk = (entry: unknown) => {
        if (typeof entry === 'string') {
            for (const key of entry.match(/\bfield_\d+\b/g) || [])
                keys.add(key);
        } else if (Array.isArray(entry)) {
            entry.forEach(walk);
        } else {
            const record = asRecord(entry);
            if (!record) return;
            for (const [key, child] of Object.entries(record)) {
                if (key !== 'values' || !Array.isArray(child)) {
                    walk(child);
                    continue;
                }
                for (const item of child) {
                    const target = asRecord(item);
                    if (!target) walk(item);
                    else
                        for (const [itemKey, itemValue] of Object.entries(
                            target,
                        ))
                            if (itemKey !== 'field') walk(itemValue);
                }
            }
        }
    };
    walk(value);
    return [...keys];
}

/**
 * Why a rule or task action cannot be stored, or null. Two refusals:
 * - HIDDEN_FIELD: it names an `_mcp_hidden` field anywhere.
 * - WRITE_ONLY_FIELD: it reads a write-only or redacted field (see readFieldKeyRefs).
 *   Writing one through `values[].field` is allowed, but a criterion is a per-record
 *   equality probe, and an `input` copy or a `{field_N}` in an email would send the
 *   value somewhere the model can read.
 *
 * @param what What would carry the field, for the message ("a rule", "a task").
 */
export function ruleFieldRefusal(
    exclusions: FieldExclusions,
    value: unknown,
    what: string,
): { error: 'HIDDEN_FIELD' | 'WRITE_ONLY_FIELD'; message: string } | null {
    const describe = (keys: string[]) =>
        keys.map((key) => describeExclusion(exclusions, key)).join('; ');
    const hidden = hiddenFieldRefs(exclusions, value);
    if (hidden.length) {
        return {
            error: 'HIDDEN_FIELD',
            message: `${describe(hidden)}, so ${what} cannot use ${hidden.length === 1 ? 'it' : 'them'}. Nothing was sent.`,
        };
    }
    const readBlocked = readFieldKeyRefs(value).filter((key) =>
        exclusions.readBlocked.has(key),
    );
    if (readBlocked.length) {
        return {
            error: 'WRITE_ONLY_FIELD',
            message: `${describe(readBlocked)}, so ${what} cannot read ${readBlocked.length === 1 ? 'it' : 'them'}: not in criteria, not copied through values[].input, not quoted as {field_N} in a message or email. ${what[0].toUpperCase()}${what.slice(1)} may still write ${readBlocked.length === 1 ? 'it' : 'them'} as a values[].field target. Nothing was sent.`,
        };
    }
    return null;
}

/** "field_12 is write-only (_mcp_writeonly)" and the like, for refusals. */
export function describeExclusion(
    exclusions: FieldExclusions,
    fieldKey: string,
): string {
    const reason = exclusions.reasons.get(fieldKey) || 'field exclusion';
    if (exclusions.hidden.has(fieldKey))
        return `${fieldKey} is hidden from MCP (${reason})`;
    if (exclusions.masked.has(fieldKey))
        return `${fieldKey} is write-only for MCP (${reason})`;
    if (exclusions.readBlocked.has(fieldKey))
        return `${fieldKey} is redacted (${reason})`;
    return `${fieldKey} is schema-locked (${reason})`;
}
