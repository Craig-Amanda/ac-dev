/**
 * Field exclusion: KTL-style `_mcp_*` keywords that limit what the model can read or change.
 *
 * A keyword goes in a field's description in the builder, where only a person can add or
 * remove it (the field tools refuse to drop one, or to add one that loosens a limit).
 * Every field stays visible in the schema, so the model can still be asked to work with
 * it; the keywords only limit its data and its definition.
 *
 * - `_mcp_nodata`: the model never sees a value. Values read back as "[redacted]",
 *   filtering, sorting, aggregating and downloading by it are refused, and so is writing
 *   it. Schema tools still work on it.
 * - `_mcp_allowwrite`: with `_mcp_nodata`, the model may write values it cannot read back.
 * - `_mcp_schemalock`: data reads and writes are normal; the definition cannot be edited,
 *   duplicated or deleted through MCP, nor the table holding it deleted.
 * - `_mcp_tablelock`: on any field of a table, locks the whole table's schema: no field
 *   added, edited, duplicated or deleted, and the table itself not edited or deleted.
 *
 * Two older names are kept as aliases so descriptions already in the builder keep their
 * protection: `_mcp_writeonly` is `_mcp_nodata` + `_mcp_allowwrite`, and `_mcp_hidden` is
 * `_mcp_nodata` + `_mcp_schemalock` (visible now; its data no less protected). Knack
 * objects have no description, so app.json's `dataAccess.objectKeywords` can also apply
 * keywords to every field of a table.
 *
 * A formula field (equation, text formula, sum/min/max/average over a connection) that
 * reads a no-data field would reproduce its value, so it gets no-data reads too. So does a
 * field whose conditional rule copies a no-data field's value into it. A count field
 * whose filters test one is left alone: it reads no values, only how many records match.
 */
import type { AppConfig } from '../config.js';
import type { KnackContext } from '../context.js';
import type { CachedObject, CachedSchema } from '../types.js';
import { containsKtlKeywordToken } from './field-references.js';
import { asRecord } from './util.js';

const MCP_NODATA = '_mcp_nodata';
const MCP_ALLOWWRITE = '_mcp_allowwrite';
const MCP_SCHEMALOCK = '_mcp_schemalock';
const MCP_TABLELOCK = '_mcp_tablelock';
const MCP_WRITEONLY = '_mcp_writeonly';
const MCP_HIDDEN = '_mcp_hidden';
export const MCP_KEYWORDS = [
    MCP_NODATA,
    MCP_ALLOWWRITE,
    MCP_SCHEMALOCK,
    MCP_TABLELOCK,
    MCP_WRITEONLY,
    MCP_HIDDEN,
] as const;
export type McpKeyword = (typeof MCP_KEYWORDS)[number];
/** What a keyword means once its aliases are expanded. */
export type McpLimit =
    | typeof MCP_NODATA
    | typeof MCP_ALLOWWRITE
    | typeof MCP_SCHEMALOCK
    | typeof MCP_TABLELOCK;

/** The limits each written keyword stands for; the two old names are aliases. */
const KEYWORD_LIMITS: Record<string, readonly McpLimit[]> = {
    [MCP_NODATA]: [MCP_NODATA],
    [MCP_ALLOWWRITE]: [MCP_ALLOWWRITE],
    [MCP_SCHEMALOCK]: [MCP_SCHEMALOCK],
    [MCP_TABLELOCK]: [MCP_TABLELOCK],
    [MCP_WRITEONLY]: [MCP_NODATA, MCP_ALLOWWRITE],
    [MCP_HIDDEN]: [MCP_NODATA, MCP_SCHEMALOCK],
};

/** The old keyword names, with what to write instead. They still work. */
const DEPRECATED_KEYWORDS: Record<string, string> = {
    [MCP_WRITEONLY]: `${MCP_NODATA} ${MCP_ALLOWWRITE}`,
    [MCP_HIDDEN]: `${MCP_NODATA} ${MCP_SCHEMALOCK}`,
};

/**
 * One warning per deprecated keyword among `keywords`, naming its replacement: the old
 * names keep their protection, but a person should move to the new ones.
 */
export function deprecatedKeywordWarnings(
    keywords: Iterable<string>,
): string[] {
    return [...new Set([...keywords].map((keyword) => keyword.toLowerCase()))]
        .filter((keyword) => keyword in DEPRECATED_KEYWORDS)
        .map(
            (keyword) =>
                `${keyword} is deprecated: it still works, as ${DEPRECATED_KEYWORDS[keyword]}. Replace it with ${DEPRECATED_KEYWORDS[keyword]} in the Knack builder.`,
        );
}

/** What a masked value reads as, formatted and `_raw` alike. */
export const REDACTED_VALUE = '[redacted]';

export type FieldExclusions = {
    /** Never returned as a value: config-redacted and no-data fields, plus formulas over them. */
    readBlocked: Set<string>;
    /** The part of readBlocked that is returned as "[redacted]" rather than left out. */
    masked: Set<string>;
    /** Values the model may not write: no-data fields without `_mcp_allowwrite`. */
    writeBlocked: Set<string>;
    /** Definition cannot be changed through MCP: schema-locked fields and every field of a locked table. */
    schemaLocked: Set<string>;
    /** Tables whose schema is locked (`_mcp_tablelock`): no field added, the table not edited. */
    lockedObjects: Set<string>;
    /** Per object, connection fields whose linked records' display values are masked. */
    maskedConnections: Map<string, Set<string>>;
    /** The keyword or config entry behind each field's data limits, for error messages. */
    reasons: Map<string, string>;
    /** The keyword behind each schema lock, by field key and by locked object key. */
    lockReasons: Map<string, string>;
    /** Per field, a warning for each deprecated keyword it carries or its table's config gives it. */
    deprecated: Map<string, string[]>;
    /** Objects with a read-blocked field or a masked connection: their record reads are projected. */
    objects: Set<string>;
};

/**
 * The `_mcp_*` keywords a description carries, as whole tokens in any case: a person who
 * types `_MCP_NoData` in the builder means the field to be protected, and a
 * case-sensitive match would leave it readable without anyone noticing.
 */
export function getMcpKeywords(text: string | undefined): McpKeyword[] {
    if (!text) return [];
    const lower = text.toLowerCase();
    return MCP_KEYWORDS.filter((keyword) =>
        containsKtlKeywordToken(lower, keyword),
    );
}

/**
 * The limits a set of written keywords stands for, with the two old names expanded:
 * `_mcp_writeonly` is `_mcp_nodata` + `_mcp_allowwrite`, and `_mcp_hidden` is
 * `_mcp_nodata` + `_mcp_schemalock`.
 */
export function expandMcpKeywords(keywords: Iterable<string>): Set<McpLimit> {
    const limits = new Set<McpLimit>();
    for (const keyword of keywords) {
        // Lower-cased here too: dataAccess.objectKeywords comes straight from app.json.
        for (const limit of KEYWORD_LIMITS[keyword.toLowerCase()] || [])
            limits.add(limit);
    }
    return limits;
}

/**
 * Keywords that would loosen a limit already on a field if the model added them: allowing
 * writes to a no-data field. Only a person in the builder may do that.
 */
export function looseningKeywords(
    currentKeywords: Iterable<string>,
    nextKeywords: Iterable<string>,
): McpKeyword[] {
    const current = expandMcpKeywords(currentKeywords);
    const next = expandMcpKeywords(nextKeywords);
    const loosens =
        current.has(MCP_NODATA) &&
        !current.has(MCP_ALLOWWRITE) &&
        next.has(MCP_ALLOWWRITE);
    if (!loosens) return [];
    const already = new Set(currentKeywords);
    return [...new Set(nextKeywords)].filter(
        (keyword): keyword is McpKeyword =>
            !already.has(keyword) &&
            expandMcpKeywords([keyword]).has(MCP_ALLOWWRITE),
    );
}

export function buildFieldExclusions(
    schema: CachedSchema | null,
    dataAccess: AppConfig['dataAccess'],
): FieldExclusions {
    const exclusions: FieldExclusions = {
        readBlocked: new Set(),
        masked: new Set(),
        writeBlocked: new Set(),
        schemaLocked: new Set(),
        lockedObjects: new Set(),
        maskedConnections: new Map(),
        reasons: new Map(),
        lockReasons: new Map(),
        deprecated: new Map(),
        objects: new Set(),
    };
    const objects = schema?.objects || [];

    const markNoData = (key: string, reason: string) => {
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
        const configured = dataAccess?.objectKeywords?.[object.key] || [];
        const fieldKeywords = new Map(
            (object.fields || []).map((field) => [
                field.key,
                getMcpKeywords(field.description),
            ]),
        );
        // Which written keyword, and where, produced a limit: for refusal messages.
        const source = (limit: McpLimit, written: string[]) => {
            const fromConfig = configured.filter((keyword) =>
                expandMcpKeywords([keyword]).has(limit),
            );
            const fromField = written.filter((keyword) =>
                expandMcpKeywords([keyword]).has(limit),
            );
            return fromField.length
                ? fromField.join(', ')
                : `${fromConfig.join(', ')} on ${object.key} (dataAccess.objectKeywords)`;
        };

        const tableLockField = (object.fields || []).find((field) =>
            expandMcpKeywords(fieldKeywords.get(field.key) || []).has(
                MCP_TABLELOCK,
            ),
        );
        if (
            tableLockField ||
            expandMcpKeywords(configured).has(MCP_TABLELOCK)
        ) {
            exclusions.lockedObjects.add(object.key);
            exclusions.lockReasons.set(
                object.key,
                tableLockField
                    ? `${MCP_TABLELOCK} on ${tableLockField.key}`
                    : `${MCP_TABLELOCK} on ${object.key} (dataAccess.objectKeywords)`,
            );
        }

        for (const field of object.fields || []) {
            const written = fieldKeywords.get(field.key) || [];
            const limits = expandMcpKeywords([...configured, ...written]);
            const warnings = [
                ...deprecatedKeywordWarnings(written),
                ...deprecatedKeywordWarnings(configured).map(
                    (warning) =>
                        `${warning.replace(' in the Knack builder.', '')} in app.json's dataAccess.objectKeywords for ${object.key}.`,
                ),
            ];
            if (warnings.length) exclusions.deprecated.set(field.key, warnings);
            if (limits.has(MCP_NODATA)) {
                markNoData(field.key, source(MCP_NODATA, written));
                if (!limits.has(MCP_ALLOWWRITE))
                    exclusions.writeBlocked.add(field.key);
            }
            if (limits.has(MCP_SCHEMALOCK)) {
                exclusions.schemaLocked.add(field.key);
                exclusions.lockReasons.set(
                    field.key,
                    source(MCP_SCHEMALOCK, written),
                );
            } else if (exclusions.lockedObjects.has(object.key)) {
                exclusions.schemaLocked.add(field.key);
                exclusions.lockReasons.set(
                    field.key,
                    exclusions.lockReasons.get(object.key)!,
                );
            }
        }
    }

    inheritDerivedTiers(objects, exclusions, markNoData);

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
 * Give every formula field no-data reads when it reads a read-blocked field
 * (`derivedFrom`), and every field with a copying conditional rule the same when it copies
 * one (`copiedFrom`), repeating until nothing changes, since a formula can read another
 * formula or a copy.
 *
 * - Only reads are inherited: a formula over a no-data field reads back as "[redacted]",
 *   but it is not write-blocked or schema-locked.
 * - `reason` names the source field, e.g. `formula over field_12 (_mcp_nodata)`, because
 *   it ends up in refusal messages.
 */
function inheritDerivedTiers(
    objects: CachedObject[],
    exclusions: FieldExclusions,
    markNoData: (key: string, reason: string) => void,
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
            if (exclusions.readBlocked.has(field.key)) continue;
            const sources = [
                ...(field.derivedFrom || []),
                ...(field.copiedFrom || []),
            ];
            const blockedSource = sources.find((key) =>
                exclusions.readBlocked.has(key),
            );
            if (blockedSource) {
                markNoData(
                    field.key,
                    `${field.derivedFrom?.includes(blockedSource) ? 'formula over' : 'conditional rule copying'} ${blockedSource} (${exclusions.reasons.get(blockedSource) || 'field exclusion'})`,
                );
                changed = true;
            }
        }
    }
}

/**
 * The limits on a field, for schema reads to show beside it: `noData` (its value reads as
 * "[redacted]"), `redacted` (left out of records by dataAccess), `noWrite` (the model may
 * not write it) and `schemaLocked`. Undefined when nothing limits it.
 */
export function getFieldAccessLimits(
    exclusions: FieldExclusions,
    fieldKey: string,
): string[] | undefined {
    const limits: string[] = [];
    if (exclusions.masked.has(fieldKey)) limits.push('noData');
    else if (exclusions.readBlocked.has(fieldKey)) limits.push('redacted');
    if (exclusions.writeBlocked.has(fieldKey)) limits.push('noWrite');
    if (exclusions.schemaLocked.has(fieldKey)) limits.push('schemaLocked');
    return limits.length ? limits : undefined;
}

/** The `_mcp_*` keywords on a raw field definition (REST or runtime metadata). */
function rawFieldKeywords(rawField: unknown): McpKeyword[] {
    const field = asRecord(rawField);
    const description =
        typeof field?.description === 'string'
            ? field.description
            : asRecord(field?.meta)?.description;
    return getMcpKeywords(
        typeof description === 'string' ? description : undefined,
    );
}

/**
 * Why the definition of `fieldKey` (or, with no field key, of any field on the object, or
 * the table itself) cannot be changed through MCP; empty when nothing locks it. A table
 * lock counts either way. Reads the cached policy, which also covers
 * `dataAccess.objectKeywords`, plus the live field list when the caller has already
 * fetched it: a keyword a person has just added in the builder may not be in the cache
 * yet. It never fetches on its own, so a guarded tool makes the same requests it always
 * did.
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
    const reasons = new Map<string, string>();
    const tableLock = (reason: string) => {
        if (!reasons.has(objectKey))
            reasons.set(objectKey, `${objectKey} is table-locked (${reason})`);
    };

    if (exclusions.lockedObjects.has(objectKey))
        tableLock(exclusions.lockReasons.get(objectKey) || MCP_TABLELOCK);
    const cachedKeys = fieldKey
        ? [fieldKey]
        : (
              schema?.objects?.find((entry) => entry.key === objectKey)
                  ?.fields || []
          ).map((field) => field.key);
    for (const key of cachedKeys) {
        if (
            exclusions.schemaLocked.has(key) &&
            !exclusions.lockedObjects.has(objectKey)
        )
            reasons.set(key, describeSchemaLock(exclusions, key));
    }

    for (const entry of Array.isArray(liveFields) ? liveFields : []) {
        const key = asRecord(entry)?.key;
        if (typeof key !== 'string') continue;
        const limits = expandMcpKeywords(rawFieldKeywords(entry));
        if (limits.has(MCP_TABLELOCK)) tableLock(`${MCP_TABLELOCK} on ${key}`);
        if (
            limits.has(MCP_SCHEMALOCK) &&
            !reasons.has(key) &&
            (!fieldKey || key === fieldKey)
        ) {
            const locking = rawFieldKeywords(entry).filter((keyword) =>
                expandMcpKeywords([keyword]).has(MCP_SCHEMALOCK),
            );
            reasons.set(key, `${key} carries ${locking.join(', ')}`);
        }
    }
    // A table lock is the whole story: no need to list each field it covers.
    if (reasons.has(objectKey)) return [reasons.get(objectKey)!];
    return [...reasons.values()];
}

/**
 * Why a new field cannot be added to `objectKey`, or null: the table is locked. Like
 * getSchemaLockReasons, reads the cache plus any live field list the caller has.
 */
export async function getTableLockReason(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    liveFields?: unknown,
): Promise<string | null> {
    const exclusions = await ctx.getFieldExclusions(app);
    if (exclusions.lockedObjects.has(objectKey))
        return `${objectKey} is table-locked (${exclusions.lockReasons.get(objectKey) || MCP_TABLELOCK})`;
    for (const entry of Array.isArray(liveFields) ? liveFields : []) {
        const key = asRecord(entry)?.key;
        if (
            typeof key === 'string' &&
            expandMcpKeywords(rawFieldKeywords(entry)).has(MCP_TABLELOCK)
        )
            return `${objectKey} is table-locked (${MCP_TABLELOCK} on ${key})`;
    }
    return null;
}

/**
 * Whether `property` of `record` holds a field key Knack keeps but never reads. The
 * builder shows neither, so a person cannot remove one, and a stale one breaks nothing:
 *
 * - A criterion's `value_field` on `value_type: "custom"`, which compares with the typed
 *   `value` (Spot, field_175: six rules on field_174 each carry `value_field: field_41`,
 *   a field long deleted). Any other `value_type`, or none, still counts.
 * - A field's `connectionMatchField`, the import wizard's record of which field on the
 *   connected object a CSV column was matched on (Spot, field_1384: `field_1377`,
 *   deleted since). It sits beside `connectionObjectKey` and `connectionNoMatchRule`; the
 *   connection itself is `relationship`.
 * - A record rule's `connection` on `action: "record"` (Update this record), which
 *   changes the form's own record; only "connection" and "insert" follow the path. Knack
 *   keeps the last one chosen: 219 of Spot's 232 record rules carry one (view_1174:
 *   `object_2.field_487`, deleted since).
 * - A rule value's `input` on `type: "value"` (to a custom value), which writes the
 *   typed `value`; only `type: "record"` copies from `input` (view_1175: field_922 set to
 *   a blank custom value, still carrying `input: field_487`).
 *
 * Only for the missing-field checks: the no-data and no-write checks count all of these,
 * since a person can switch the Builder choice back and the stored key would be read.
 */
export function isDormantFieldRef(
    record: Record<string, unknown>,
    property: string,
): boolean {
    switch (property) {
        case 'connectionMatchField':
            return true;
        case 'value_field':
            return record.value_type === 'custom';
        case 'connection':
            return record.action === 'record';
        case 'input':
            return record.type === 'value';
        default:
            return false;
    }
}

/**
 * Every field key a rule, task action or other JSON value names: each `field_N` token in
 * any string inside it, so `{field_12}` in an email message and both halves of a
 * `field_1.field_2` connection path count, as well as `field`, `input` and `value_field`.
 * Any string, not only the known keys: the field-exclusion checks must not depend on
 * knowing every property Knack puts a field key under.
 *
 * `skipDormantRefs` leaves out field keys Knack never reads (see isDormantFieldRef),
 * for the missing-field checks: a stale one breaks nothing, and counting it would
 * refuse every save of the view.
 */
export function collectFieldKeyRefs(
    value: unknown,
    options: { skipDormantRefs?: boolean } = {},
): string[] {
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
            for (const [property, child] of Object.entries(record)) {
                if (
                    options.skipDormantRefs &&
                    isDormantFieldRef(record, property)
                )
                    continue;
                walk(child);
            }
        }
    };
    walk(value);
    return [...keys];
}

/**
 * The field keys a rule or task action writes: each `values[].field`, including both
 * halves of a `field_1.field_2` connection path. The counterpart of readFieldKeyRefs.
 */
export function writeFieldKeyRefs(value: unknown): string[] {
    const keys = new Set<string>();
    const walk = (entry: unknown) => {
        if (Array.isArray(entry)) {
            entry.forEach(walk);
            return;
        }
        const record = asRecord(entry);
        if (!record) return;
        for (const [key, child] of Object.entries(record)) {
            if (key === 'values' && Array.isArray(child)) {
                for (const item of child) {
                    const target = asRecord(item)?.field;
                    if (typeof target === 'string')
                        for (const match of target.match(/\bfield_\d+\b/g) ||
                            [])
                            keys.add(match);
                }
            }
            walk(child);
        }
    };
    walk(value);
    return [...keys];
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
 * - NO_DATA_FIELD: it reads a no-data or redacted field (see readFieldKeyRefs). A
 *   criterion is a per-record equality probe, and an `input` copy or a `{field_N}` in an
 *   email would send the value somewhere the model can read.
 * - NO_WRITE_FIELD: it writes a no-data field through `values[].field` (see
 *   writeFieldKeyRefs) that has no `_mcp_allowwrite`.
 *
 * A display rule (`displayOnly`) only shows, hides or relabels inputs and details for a
 * person in the live app: nothing is stored and MCP never reads the rendered page, so it
 * may test or target a no-data field.
 *
 * @param what What would carry the field, for the message ("a rule", "a task").
 */
export function ruleFieldRefusal(
    exclusions: FieldExclusions,
    value: unknown,
    what: string,
    options: { displayOnly?: boolean } = {},
): { error: 'NO_DATA_FIELD' | 'NO_WRITE_FIELD'; message: string } | null {
    if (options.displayOnly) return null;
    const describe = (keys: string[]) =>
        keys.map((key) => describeExclusion(exclusions, key)).join('; ');
    const them = (keys: string[]) => (keys.length === 1 ? 'it' : 'them');
    const readBlocked = readFieldKeyRefs(value).filter((key) =>
        exclusions.readBlocked.has(key),
    );
    if (readBlocked.length) {
        return {
            error: 'NO_DATA_FIELD',
            message: `${describe(readBlocked)}, so ${what} cannot read ${them(readBlocked)}: not in criteria, not copied through values[].input, not quoted as {field_N} in a message or email. Nothing was sent.`,
        };
    }
    const writeBlocked = writeFieldKeyRefs(value).filter((key) =>
        exclusions.writeBlocked.has(key),
    );
    if (writeBlocked.length) {
        return {
            error: 'NO_WRITE_FIELD',
            message: `${writeBlocked.map((key) => describeWriteBlock(exclusions, key)).join('; ')}, so ${what} cannot write ${them(writeBlocked)} either. Nothing was sent.`,
        };
    }
    return null;
}

/** "field_12 has no data access for MCP (_mcp_nodata)" and the like, for refusals. */
export function describeExclusion(
    exclusions: FieldExclusions,
    fieldKey: string,
): string {
    const reason = exclusions.reasons.get(fieldKey) || 'field exclusion';
    if (exclusions.masked.has(fieldKey))
        return `${fieldKey} has no data access for MCP (${reason})`;
    if (exclusions.readBlocked.has(fieldKey))
        return `${fieldKey} is redacted (${reason})`;
    return describeSchemaLock(exclusions, fieldKey);
}

/** "field_12 cannot be written through MCP (_mcp_nodata, no _mcp_allowwrite)". */
export function describeWriteBlock(
    exclusions: FieldExclusions,
    fieldKey: string,
): string {
    const reason = exclusions.reasons.get(fieldKey) || 'field exclusion';
    return `${fieldKey} cannot be written through MCP (${reason}, no ${MCP_ALLOWWRITE})`;
}

/** "field_12 is schema-locked (_mcp_schemalock)", for refusals. */
export function describeSchemaLock(
    exclusions: FieldExclusions,
    fieldKey: string,
): string {
    const reason = exclusions.lockReasons.get(fieldKey) || 'field exclusion';
    return `${fieldKey} is schema-locked (${reason})`;
}
