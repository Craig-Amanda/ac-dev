/**
 * Field mutation tools: create, update, delete and duplicate fields through the Knack
 * Builder API. Every write is preflighted locally (payload contract, equation tokens,
 * KTL keyword protection) before a request is made, and dryRun returns the preflight
 * result without persisting anything.
 */
import { z } from 'zod';

import type { AppConfig } from '../config.js';
import type { KnackContext } from '../context.js';
import {
    DATE_FORMATS,
    buildDateFieldFormat,
    readAppTimeZone,
} from '../lib/date-field-defaults.js';
import {
    deprecatedKeywordWarnings,
    expandMcpKeywords,
    getMcpKeywords,
    getSchemaLockReasons,
    getTableLockReason,
    looseningKeywords,
    ruleFieldRefusal,
} from '../lib/field-exclusion.js';
import {
    NESTED_MERGE_UNCERTAINTY_NOTE,
    SCHEMA_CACHE_STALE_NOTE,
    appendKtlNote,
    findFieldInFieldWriteResponse,
    normalizeFieldDescriptionForWrite,
    parseJsonObjectInput,
    preserveKtlNote,
    validateEquationTokens,
    validateFieldPayload,
} from '../lib/field-payload.js';
import {
    containsKtlKeywordToken,
    extractKtlKeywordsFromText,
} from '../lib/field-references.js';
import {
    asRecord,
    deepMergeRecords,
    parseJsonObjectArray,
    readWireObjectEntity,
} from '../lib/util.js';
import {
    applyRuleEdit,
    assignNumericRuleKeys,
    readRuleArray,
} from '../lib/rule-edits.js';
import { deepEqual } from '../lib/structural-diff.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { getInlineDetail, makeTextResponse, toolReplies } from '../response.js';

const UNCHECKED_EQUATION_WARNING =
    'Could not validate equation tokens: no schema is available (neither runtime API nor schema.json) for this app, so this write is going out unchecked.';

const NOTED_BY_DESCRIPTION_CREATE =
    'Human who instructed this field to be created with a description; required (non-empty) whenever description is set to non-empty text — stamped as a trailing _notes=[<name> on <date>] KTL keyword recording who added it (inside an existing _notes=[...] note, if the description has one).';

const NOTED_BY_DESCRIPTION_UPDATE =
    'Human who instructed this description change. Required (non-empty) only when the field has no _notes stamp yet (first note being added) or when restampNote is true. Otherwise the existing _notes=[... <name> on <date>] attribution is preserved untouched — it records who added the note, not who last edited it.';

/**
 * Validate the {...} tokens of an equation against the cached schema. Errors block the
 * write; warnings ride along on the response.
 */
async function checkEquation(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    equation: string,
): Promise<{ errors: string[]; warnings: string[] }> {
    const { schema } = await ctx.getSchema(app);
    if (!schema) return { errors: [], warnings: [UNCHECKED_EQUATION_WARNING] };
    return validateEquationTokens(schema, objectKey, equation);
}

/**
 * A refusal when the field (or, for a whole-object change, any field on the object) is
 * schema-locked or hidden; null when the change may go ahead.
 */
export async function refuseSchemaLockedField(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    fieldKey: string | undefined,
    action: string,
    liveFields?: unknown,
) {
    const reasons = await getSchemaLockReasons(
        ctx,
        app,
        objectKey,
        fieldKey,
        liveFields,
    );
    if (!reasons.length) return null;
    return makeTextResponse({
        ok: false,
        appKey: app.appKey,
        objectKey,
        ...(fieldKey ? { fieldKey } : {}),
        action: `${action}_preflight`,
        errors: reasons.map(
            (reason) =>
                `${reason}, so its definition cannot be changed through MCP. A person can change it, or remove the keyword, in the Knack builder.`,
        ),
    });
}

/** A raw field's description, top-level or under `meta`, or ''. */
function rawDescription(field: Record<string, unknown> | undefined): string {
    if (!field) return '';
    if (typeof field.description === 'string') return field.description;
    const meta = asRecord(field.meta)?.description;
    return typeof meta === 'string' ? meta : '';
}

/**
 * Check that a duplicated field kept every `_mcp_*` keyword its source had, and write
 * the source's description back once if not. `ok: false` means the copy exists but may
 * be less protected than its source, which a person must fix.
 */
async function ensureCopyKeepsKeywords(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    sourceField: Record<string, unknown>,
    createdField: Record<string, unknown> | undefined,
): Promise<{ ok: boolean; message: string; fieldKey?: string }> {
    const wanted = getMcpKeywords(rawDescription(sourceField));
    if (!wanted.length || !expandMcpKeywords(wanted).size)
        return { ok: true, message: '' };
    const fieldKey =
        typeof createdField?.key === 'string' ? createdField.key : undefined;
    const missing = (field: Record<string, unknown> | undefined) => {
        const kept = getMcpKeywords(rawDescription(field));
        return wanted.filter((keyword) => !kept.includes(keyword));
    };
    if (!fieldKey) {
        return {
            ok: false,
            message: `The copy was created, but it could not be found in Knack's response to check that it kept ${wanted.join(', ')}. Find it with knack_get_object on ${objectKey} and check its description in the Knack builder before any data goes into it.`,
        };
    }
    if (!missing(createdField).length)
        return { ok: true, message: '', fieldKey };

    const description = rawDescription(sourceField);
    const repair = await ctx.request(
        app,
        `/objects/${objectKey}/fields/${fieldKey}`,
        {
            method: 'PUT',
            body: JSON.stringify({ description, meta: { description } }),
        },
    );
    const repaired = repair.ok
        ? findFieldInFieldWriteResponse(repair.body, objectKey, { fieldKey })
        : undefined;
    const stillMissing = missing(repaired);
    if (repair.ok && repaired && !stillMissing.length)
        return { ok: true, message: '', fieldKey };
    return {
        ok: false,
        fieldKey,
        message: `The copy ${fieldKey} was created without ${stillMissing.join(', ')} from its source, and putting ${stillMissing.length === 1 ? 'it' : 'them'} back failed. Add ${stillMissing.join(', ')} to ${fieldKey}'s description in the Knack builder before any data goes into it.`,
    };
}

/** The field list of a raw `GET /objects/{key}` response body. */
function readObjectFields(
    body: unknown,
): Array<Record<string, unknown>> | undefined {
    const fields = readWireObjectEntity(body)?.fields;
    if (!Array.isArray(fields)) return undefined;
    return fields
        .map((entry) => asRecord(entry))
        .filter((entry): entry is Record<string, unknown> => Boolean(entry));
}

export const createField = defineTool({
    name: 'knack_create_field',
    description:
        'Create a field on an object; dryRun validates and previews the definition without creating it.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        name: z.string(),
        type: z
            .string()
            .describe('e.g. short_text, number, connection, equation'),
        required: z.boolean().default(false),
        unique: z.boolean().default(false),
        format: z.string().optional().describe('Format object as JSON'),
        relationship: z
            .string()
            .optional()
            .describe('Relationship object as JSON (connections)'),
        description: z
            .string()
            .optional()
            .describe('Help text, stored as meta.description'),
        notedBy: z.string().optional().describe(NOTED_BY_DESCRIPTION_CREATE),
        dateFormat: z
            .enum(DATE_FORMATS)
            .optional()
            .describe("date_time: default follows the app's time zone"),
        includeTime: z
            .boolean()
            .optional()
            .describe('date_time: store a 24-hour time (default: no time)'),
        dryRun: z.boolean().default(false),
    },
    handler: async (
        {
            appKey,
            objectKey,
            name,
            type,
            required,
            unique,
            format,
            relationship,
            description,
            notedBy,
            dateFormat,
            includeTime,
            dryRun,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);

        const payload: Record<string, unknown> = {
            name,
            type,
            required,
            unique,
        };

        const validationErrors: string[] = [];
        let equationWarnings: string[] = [];

        if (description !== undefined) {
            const trimmed = description.trim();
            if (trimmed && !notedBy?.trim()) {
                validationErrors.push(
                    'notedBy is required when setting a non-empty description — it attributes the trailing _notes KTL keyword (who + when).',
                );
            } else {
                // Normalize whitespace-only input to '' rather than sending invisible
                // characters through as an apparently blank description.
                payload.description = trimmed
                    ? appendKtlNote(trimmed, notedBy!.trim())
                    : trimmed;
                normalizeFieldDescriptionForWrite(payload);
            }
        }
        if (format) {
            const parsed = parseJsonObjectInput(format, 'format');
            validationErrors.push(...parsed.errors);
            if (parsed.payload) {
                payload.format = parsed.payload;
                const equation = parsed.payload.equation;
                if (typeof equation === 'string' && equation.trim()) {
                    const check = await checkEquation(
                        ctx,
                        app,
                        objectKey,
                        equation,
                    );
                    validationErrors.push(...check.errors);
                    equationWarnings = check.warnings;
                }
            }
        }
        if (relationship) {
            const parsed = parseJsonObjectInput(relationship, 'relationship');
            validationErrors.push(...parsed.errors);
            if (parsed.payload) payload.relationship = parsed.payload;
        }

        // A date field left to Knack gets US dates and no time; this one follows the
        // app's time zone instead (see lib/date-field-defaults.ts).
        let dateField: Record<string, unknown> | undefined;
        if (type === 'date_time') {
            const defaults = buildDateFieldFormat({
                timeZone: readAppTimeZone(await ctx.getRuntimeMetadata(app)),
                dateFormat,
                includeTime,
                given: asRecord(payload.format) ?? undefined,
            });
            if (defaults.format) payload.format = defaults.format;
            dateField = defaults.summary;
        } else if (dateFormat !== undefined || includeTime !== undefined) {
            validationErrors.push(
                `dateFormat and includeTime only apply to a date_time field, not ${type}.`,
            );
        }
        validationErrors.push(...validateFieldPayload(payload, true));
        // An old keyword name still works; this only asks for the new one.
        const deprecation = deprecatedKeywordWarnings(
            getMcpKeywords(
                typeof payload.description === 'string'
                    ? payload.description
                    : undefined,
            ),
        );
        const keywordWarnings = deprecation.length
            ? { keywordWarnings: deprecation }
            : {};
        const tableLock = await getTableLockReason(ctx, app, objectKey);
        if (tableLock) {
            validationErrors.push(
                `${tableLock}, so no field can be added to it through MCP. A person can add it, or remove the keyword, in the Knack builder.`,
            );
        }

        if (validationErrors.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'create_field_preflight',
                errors: validationErrors,
            });
        }

        if (dryRun) {
            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                objectKey,
                action: 'create_field_dry_run',
                dryRun: true,
                wouldCreate: payload,
                ...keywordWarnings,
                ...(dateField ? { dateField } : {}),
                ...(equationWarnings.length ? { equationWarnings } : {}),
            });
        }

        const result = await ctx.request(app, `/objects/${objectKey}/fields`, {
            method: 'POST',
            body: JSON.stringify(payload),
        });

        if (result.ok) {
            const bodyDetail = getInlineDetail(result.body);
            if (!bodyDetail.included) {
                // Knack returns the full application schema for connection-field writes
                // (creating a connection also updates the cross-object relationship
                // graph) — project it down to the created field.
                const createdField = findFieldInFieldWriteResponse(
                    result.body,
                    objectKey,
                    {
                        name,
                        type,
                    },
                );
                return makeTextResponse({
                    appKey: app.appKey,
                    objectKey,
                    action: 'create_field',
                    ok: true,
                    status: result.status,
                    ...keywordWarnings,
                    ...(dateField ? { dateField } : {}),
                    ...(equationWarnings.length ? { equationWarnings } : {}),
                    ...(createdField ? { field: createdField } : {}),
                    bodySizeBytes: bodyDetail.sizeBytes,
                    bodySummary: bodyDetail.summary,
                    note: createdField
                        ? "Knack's response for this write included the full application schema (expected for connection fields, since they update the cross-object relationship graph) — projected down to the created field above plus a structural summary. Call knack_get_field for the full raw field definition if needed."
                        : `Knack's response for this write included the full application schema. Could not unambiguously identify the created field in it (e.g. another field named "${name}" of type ${type} may already exist on ${objectKey} — Knack field names aren't unique — or the response may not have included this object at all) — call knack_get_object on ${objectKey} to find the new field's key.`,
                    cacheNote: SCHEMA_CACHE_STALE_NOTE,
                });
            }
        }

        return makeTextResponse({
            appKey: app.appKey,
            objectKey,
            action: 'create_field',
            ...(result.ok ? keywordWarnings : {}),
            ...(dateField && result.ok ? { dateField } : {}),
            ...(equationWarnings.length ? { equationWarnings } : {}),
            ...result,
            ...(result.ok ? { cacheNote: SCHEMA_CACHE_STALE_NOTE } : {}),
        });
    },
});

export const updateField = defineTool({
    name: 'knack_update_field',
    description:
        'Update properties of an existing field; dryRun previews the merged definition without persisting.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        fieldKey: z.string(),
        updates: z
            .string()
            .optional()
            .describe(
                'Partial field definition as JSON (name, format, rules ...)',
            ),
        description: z
            .string()
            .optional()
            .describe('Help text (meta.description); empty string clears it'),
        notedBy: z.string().optional().describe(NOTED_BY_DESCRIPTION_UPDATE),
        restampNote: z
            .boolean()
            .default(false)
            .describe(
                'Re-attribute the _notes stamp to notedBy/now, replacing who added it. Only set this when the instructor explicitly asked to update the note attribution — an ordinary description edit preserves the original stamp.',
            ),
        confirmRemoveKtlKeywords: z
            .boolean()
            .default(false)
            .describe('Allow dropping KTL keyword tokens from the description'),
        dryRun: z.boolean().default(false),
    },
    handler: async (
        {
            appKey,
            objectKey,
            fieldKey,
            updates,
            description,
            notedBy,
            restampNote,
            confirmRemoveKtlKeywords,
            dryRun,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);

        const locked = await refuseSchemaLockedField(
            ctx,
            app,
            objectKey,
            fieldKey,
            'update_field',
        );
        if (locked) return locked;

        if (!updates && description === undefined) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                fieldKey,
                action: 'update_field_preflight',
                errors: [
                    'Provide updates and/or description — nothing to update.',
                ],
            });
        }

        const parsed = updates
            ? parseJsonObjectInput(updates, 'updates')
            : {
                  payload: {} as Record<string, unknown>,
                  errors: [] as string[],
              };
        if (description !== undefined && parsed.payload) {
            // Plain top-level assignment (no HTML wrapping) so this stays consistent with
            // knack_create_field, and so it actually takes precedence over a raw
            // "description" key already in `updates` — the note-stamping logic below acts
            // on whichever value wins here.
            parsed.payload = { ...parsed.payload, description };
        }

        if (parsed.payload) {
            // A non-string description (e.g. `{"description": null}` sent through raw
            // `updates` JSON) must be refused rather than silently coerced to '' below —
            // that would clear the description when the caller likely made a mistake, not
            // asked for a clear. An explicit "" is still the way to clear it.
            if (
                Object.hasOwn(parsed.payload, 'description') &&
                typeof parsed.payload.description !== 'string'
            ) {
                parsed.errors.push(
                    'description in updates must be a string — use "" to clear it, or omit the key to leave it unchanged.',
                );
            }
            const metaRecord = asRecord(parsed.payload.meta);
            if (
                metaRecord &&
                Object.hasOwn(metaRecord, 'description') &&
                typeof metaRecord.description !== 'string'
            ) {
                parsed.errors.push(
                    'meta.description in updates must be a string — use "" to clear it, or omit the key to leave it unchanged.',
                );
            }
        }

        const validationErrors = [
            ...parsed.errors,
            ...(parsed.payload
                ? validateFieldPayload(parsed.payload, false)
                : []),
        ];

        let equationWarnings: string[] = [];
        const equation = parsed.payload?.format
            ? asRecord(parsed.payload.format)?.equation
            : undefined;
        if (typeof equation === 'string' && equation.trim()) {
            const check = await checkEquation(ctx, app, objectKey, equation);
            validationErrors.push(...check.errors);
            equationWarnings = check.warnings;
        }

        if (validationErrors.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                fieldKey,
                action: 'update_field_preflight',
                errors: validationErrors,
            });
        }

        const descriptionKeyPresent = Boolean(
            parsed.payload &&
            (Object.hasOwn(parsed.payload, 'description') ||
                typeof asRecord(parsed.payload.meta)?.description === 'string'),
        );

        let currentField: Record<string, unknown> | undefined;
        let currentFieldFetchOk = true;
        let currentFieldFetchStatus = 0;

        if (dryRun || descriptionKeyPresent) {
            const objResult = await ctx.request(app, `/objects/${objectKey}`);
            currentField = readObjectFields(objResult.body)?.find(
                (entry) => entry.key === fieldKey,
            );
            currentFieldFetchOk = objResult.ok && Boolean(currentField);
            currentFieldFetchStatus = objResult.status;
            // The live description may carry a lock keyword the cache has not seen yet.
            const lockedLive = await refuseSchemaLockedField(
                ctx,
                app,
                objectKey,
                fieldKey,
                'update_field',
                currentField ? [currentField] : undefined,
            );
            if (lockedLive) return lockedLive;
        }

        const ktlKeywordWarnings: string[] = [];
        if (descriptionKeyPresent && parsed.payload) {
            const rawNewDescription =
                (typeof parsed.payload.description === 'string'
                    ? parsed.payload.description
                    : typeof asRecord(parsed.payload.meta)?.description ===
                        'string'
                      ? (asRecord(parsed.payload.meta)!.description as string)
                      : '') || '';
            const trimmedNewDescription = rawNewDescription.trim();
            if (!trimmedNewDescription) {
                // Normalize whitespace-only (or already-empty) input to '' rather than
                // sending invisible characters through as an apparently blank description.
                parsed.payload.description = '';
            }

            if (currentField) {
                const currentFieldMeta = asRecord(currentField.meta);
                const currentDescription =
                    (typeof currentField.description === 'string'
                        ? currentField.description
                        : typeof currentFieldMeta?.description === 'string'
                          ? currentFieldMeta.description
                          : '') || '';

                if (trimmedNewDescription) {
                    // _notes records who *added* the note, not who last touched the field —
                    // an ordinary content edit carries the existing stamp forward untouched.
                    // Only a first-ever note, or an explicit restampNote ask, re-attributes it.
                    const hasExistingNote = containsKtlKeywordToken(
                        currentDescription,
                        '_notes',
                    );
                    if (hasExistingNote && !restampNote) {
                        parsed.payload.description = preserveKtlNote(
                            trimmedNewDescription,
                            currentDescription,
                        );
                    } else if (!notedBy?.trim()) {
                        return makeTextResponse({
                            ok: false,
                            appKey: app.appKey,
                            objectKey,
                            fieldKey,
                            action: 'update_field_preflight',
                            errors: [
                                hasExistingNote
                                    ? 'notedBy is required to restamp the _notes KTL keyword — set restampNote: true only when the instructor explicitly asked to re-attribute it.'
                                    : 'notedBy is required when adding the first _notes KTL keyword to this field — it attributes who added it and when.',
                            ],
                        });
                    } else {
                        parsed.payload.description = appendKtlNote(
                            trimmedNewDescription,
                            notedBy.trim(),
                        );
                    }
                }

                const newDescriptionForDropCheck =
                    (typeof parsed.payload.description === 'string'
                        ? parsed.payload.description
                        : '') || '';
                const currentKeywords = [
                    ...new Set(
                        extractKtlKeywordsFromText(currentDescription).map(
                            (hit) => hit.keyword,
                        ),
                    ),
                ];
                const droppedKeywords = currentKeywords.filter(
                    (keyword) =>
                        !containsKtlKeywordToken(
                            newDescriptionForDropCheck,
                            keyword,
                        ),
                );
                // By getMcpKeywords, not the KTL list: it matches `_MCP_NoData` too.
                const keptMcpKeywords = getMcpKeywords(
                    newDescriptionForDropCheck,
                );
                const droppedMcpKeywords = getMcpKeywords(
                    currentDescription,
                ).filter((keyword) => !keptMcpKeywords.includes(keyword));
                if (droppedMcpKeywords.length) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        fieldKey,
                        action: 'update_field_preflight',
                        errors: [
                            `This update would drop ${droppedMcpKeywords.join(', ')} from the field description. MCP field-exclusion keywords can only be removed by a person in the Knack builder, even with confirmRemoveKtlKeywords.`,
                        ],
                        currentDescription,
                        droppedKtlKeywords: droppedKeywords,
                    });
                }
                // Adding a keyword is fine when it tightens a limit, but one that lets the
                // model write a no-data field is a person's decision.
                const loosening = looseningKeywords(
                    getMcpKeywords(currentDescription),
                    keptMcpKeywords,
                );
                if (loosening.length) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        fieldKey,
                        action: 'update_field_preflight',
                        errors: [
                            `This update would add ${loosening.join(', ')}, which lets MCP write a field whose data it may not see. Only a person can add it, in the Knack builder.`,
                        ],
                        currentDescription,
                    });
                }
                if (droppedKeywords.length && !confirmRemoveKtlKeywords) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        fieldKey,
                        action: 'update_field_preflight',
                        errors: [
                            `This update would drop existing KTL keyword(s) from the field description: ${droppedKeywords.join(', ')}. Keep them in the new description, or pass confirmRemoveKtlKeywords: true only after explicitly confirming the removal with the user.`,
                        ],
                        currentDescription,
                        droppedKtlKeywords: droppedKeywords,
                    });
                }
            } else {
                // The live description could not be read, but the cached one still shows
                // its _mcp_* keywords: a description leaving one out would drop it, and
                // one adding _mcp_allowwrite would loosen it, which only a person may do.
                const { schema } = await ctx.getFullSchema(app);
                const cachedKeywords = getMcpKeywords(
                    schema?.objects
                        ?.find((entry) => entry.key === objectKey)
                        ?.fields?.find((entry) => entry.key === fieldKey)
                        ?.description,
                );
                const nextKeywords = getMcpKeywords(trimmedNewDescription);
                const dropped = cachedKeywords.filter(
                    (keyword) => !nextKeywords.includes(keyword),
                );
                const loosening = looseningKeywords(
                    cachedKeywords,
                    nextKeywords,
                );
                if (dropped.length || loosening.length) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        fieldKey,
                        action: 'update_field_preflight',
                        errors: [
                            dropped.length
                                ? `This update would drop ${dropped.join(', ')} from the field description (the current field could not be fetched, but the cached schema shows it). MCP field-exclusion keywords can only be removed by a person in the Knack builder.`
                                : `This update would add ${loosening.join(', ')}, which lets MCP write a field whose data it may not see (the current field could not be fetched, but the cached schema shows it). Only a person can add it, in the Knack builder.`,
                        ],
                    });
                }
                if (trimmedNewDescription) {
                    if (!notedBy?.trim()) {
                        return makeTextResponse({
                            ok: false,
                            appKey: app.appKey,
                            objectKey,
                            fieldKey,
                            action: 'update_field_preflight',
                            errors: [
                                'notedBy is required when setting a non-empty description and the current field could not be fetched to check for an existing _notes stamp.',
                            ],
                        });
                    }
                    parsed.payload.description = appendKtlNote(
                        trimmedNewDescription,
                        notedBy.trim(),
                    );
                }
                ktlKeywordWarnings.push(
                    'Could not fetch the current field to check for KTL keywords in its existing description, so this description change is going out without that safety check.',
                );
            }
            // An old keyword name still works, and the model may not remove one, so this
            // only asks a person to move it to the new names.
            ktlKeywordWarnings.push(
                ...deprecatedKeywordWarnings(
                    getMcpKeywords(
                        typeof parsed.payload.description === 'string'
                            ? parsed.payload.description
                            : undefined,
                    ),
                ),
            );
        }

        if (parsed.payload) {
            // Covers description set via the dedicated parameter above, and via a raw
            // {"description": "..."} key inside `updates` JSON.
            normalizeFieldDescriptionForWrite(parsed.payload);
        }

        if (dryRun) {
            if (!currentFieldFetchOk || !currentField) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    objectKey,
                    fieldKey,
                    action: 'update_field_dry_run',
                    message: `Could not fetch current definition for ${fieldKey} on ${objectKey}.`,
                    status: currentFieldFetchStatus,
                });
            }
            const existing = currentField;
            const changedKeys = parsed.payload
                ? Object.keys(parsed.payload)
                : [];
            const mergedPreview = parsed.payload
                ? deepMergeRecords(existing, parsed.payload)
                : existing;
            const resolveCurrentValue = (key: string): unknown => {
                // Knack's raw field payload sometimes nests description under
                // meta.description rather than the top-level key; fall back to that so
                // the diff doesn't show a false "from: undefined".
                if (
                    key === 'description' &&
                    existing.description === undefined
                ) {
                    const meta = asRecord(existing.meta);
                    if (typeof meta?.description === 'string')
                        return meta.description;
                }
                return existing[key];
            };
            const changes: Record<string, { from: unknown; to: unknown }> = {};
            for (const key of changedKeys) {
                changes[key] = {
                    from: resolveCurrentValue(key),
                    to: mergedPreview[key],
                };
            }
            const touchesNestedPreview = changedKeys.some(
                (key) => key === 'format' || key === 'relationship',
            );

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                objectKey,
                fieldKey,
                action: 'update_field_dry_run',
                dryRun: true,
                currentField: existing,
                changes,
                ...(equationWarnings.length ? { equationWarnings } : {}),
                ...(ktlKeywordWarnings.length ? { ktlKeywordWarnings } : {}),
                ...(touchesNestedPreview
                    ? { mergeNote: NESTED_MERGE_UNCERTAINTY_NOTE }
                    : {}),
            });
        }

        const result = await ctx.request(
            app,
            `/objects/${objectKey}/fields/${fieldKey}`,
            {
                method: 'PUT',
                body: JSON.stringify(parsed.payload),
            },
        );
        const payloadTouchesNested = Boolean(
            parsed.payload &&
            (Object.hasOwn(parsed.payload, 'format') ||
                Object.hasOwn(parsed.payload, 'relationship')),
        );

        if (result.ok) {
            const bodyDetail = getInlineDetail(result.body);
            if (!bodyDetail.included) {
                // Same connection-field bloat as knack_create_field: Knack's response
                // can include the full application schema — project it down to the
                // updated field.
                const updatedField = findFieldInFieldWriteResponse(
                    result.body,
                    objectKey,
                    {
                        fieldKey,
                    },
                );
                return makeTextResponse({
                    appKey: app.appKey,
                    objectKey,
                    fieldKey,
                    action: 'update_field',
                    ok: true,
                    status: result.status,
                    ...(equationWarnings.length ? { equationWarnings } : {}),
                    ...(ktlKeywordWarnings.length
                        ? { ktlKeywordWarnings }
                        : {}),
                    ...(updatedField ? { field: updatedField } : {}),
                    bodySizeBytes: bodyDetail.sizeBytes,
                    bodySummary: bodyDetail.summary,
                    note: updatedField
                        ? "Knack's response for this write included the full application schema (expected for connection fields, since they update the cross-object relationship graph) — projected down to the updated field above plus a structural summary. Call knack_get_field for the full raw field definition if needed."
                        : `Knack's response for this write included the full application schema. Could not locate ${fieldKey} in it — call knack_get_field to fetch the updated field's definition directly.`,
                    cacheNote: SCHEMA_CACHE_STALE_NOTE,
                    ...(payloadTouchesNested
                        ? { mergeNote: NESTED_MERGE_UNCERTAINTY_NOTE }
                        : {}),
                });
            }
        }

        return makeTextResponse({
            appKey: app.appKey,
            objectKey,
            fieldKey,
            action: 'update_field',
            ...(equationWarnings.length ? { equationWarnings } : {}),
            ...(ktlKeywordWarnings.length ? { ktlKeywordWarnings } : {}),
            ...result,
            ...(result.ok ? { cacheNote: SCHEMA_CACHE_STALE_NOTE } : {}),
            ...(result.ok && payloadTouchesNested
                ? { mergeNote: NESTED_MERGE_UNCERTAINTY_NOTE }
                : {}),
        });
    },
});

export const deleteField = defineTool({
    name: 'knack_delete_field',
    description: 'Permanently delete a field from an object.',
    access: 'delete',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        fieldKey: z.string(),
    },
    handler: async ({ appKey, objectKey, fieldKey }, ctx) => {
        const app = ctx.getApp(appKey);
        // Read live first: a lock keyword a person has just added in the builder may not
        // be in the cache yet, and this is the change that cannot be undone.
        const objResult = await ctx.request(app, `/objects/${objectKey}`);
        const locked = await refuseSchemaLockedField(
            ctx,
            app,
            objectKey,
            fieldKey,
            'delete_field',
            readObjectFields(objResult.body),
        );
        if (locked) return locked;
        const result = await ctx.request(
            app,
            `/objects/${objectKey}/fields/${fieldKey}`,
            {
                method: 'DELETE',
            },
        );
        return makeTextResponse({
            appKey: app.appKey,
            objectKey,
            fieldKey,
            action: 'delete_field',
            ...result,
            ...(result.ok ? { cacheNote: SCHEMA_CACHE_STALE_NOTE } : {}),
        });
    },
});

export const duplicateField = defineTool({
    name: 'knack_duplicate_field',
    description:
        'Create a copy of an existing field under a new name on the same object.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        sourceFieldKey: z.string(),
        newName: z.string(),
    },
    handler: async ({ appKey, objectKey, sourceFieldKey, newName }, ctx) => {
        const app = ctx.getApp(appKey);

        // Fetch the object to get the source field definition.
        const objResult = await ctx.request(app, `/objects/${objectKey}`);
        const fields = readObjectFields(objResult.body);
        if (!fields) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: 'Could not fetch object fields.',
            });
        }
        // A copy would carry the source's value rules and keywords to a field nothing
        // locks, so a locked source cannot be duplicated.
        const locked = await refuseSchemaLockedField(
            ctx,
            app,
            objectKey,
            sourceFieldKey,
            'duplicate_field',
            fields,
        );
        if (locked) return locked;

        const sourceField = fields.find((f) => f.key === sourceFieldKey);
        if (!sourceField) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: `Source field ${sourceFieldKey} not found on ${objectKey}.`,
            });
        }

        // Clone and strip identifiers.
        const newField: Record<string, unknown> = { ...sourceField };
        delete newField.key;
        delete newField._id;
        newField.name = newName;
        // The source field may only have a top-level `description` (e.g. it predates
        // normalizeFieldDescriptionForWrite), which wouldn't reliably persist on this new
        // POST either — mirror it into meta.description too.
        normalizeFieldDescriptionForWrite(newField);

        const result = await ctx.request(app, `/objects/${objectKey}/fields`, {
            method: 'POST',
            body: JSON.stringify(newField),
        });

        // The copy starts empty, but it must be as protected as its source from the
        // start, or data written to it later could be read. Knack is sent the source's
        // description; this checks it kept the _mcp_* keywords, and puts them back once
        // if it did not.
        const protection = result.ok
            ? await ensureCopyKeepsKeywords(
                  ctx,
                  app,
                  objectKey,
                  sourceField,
                  findFieldInFieldWriteResponse(result.body, objectKey, {
                      name: newName,
                      type: String(sourceField.type ?? ''),
                  }),
              )
            : null;
        if (protection && !protection.ok) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'duplicate_field',
                sourceFieldKey,
                newName,
                error: 'COPY_NOT_PROTECTED',
                message: protection.message,
                ...(protection.fieldKey
                    ? { createdFieldKey: protection.fieldKey }
                    : {}),
                cacheNote: SCHEMA_CACHE_STALE_NOTE,
            });
        }

        if (result.ok) {
            const bodyDetail = getInlineDetail(result.body);
            if (!bodyDetail.included) {
                // Same connection-field bloat as knack_create_field: Knack returns the
                // full application schema, not just the field.
                const duplicatedField = findFieldInFieldWriteResponse(
                    result.body,
                    objectKey,
                    {
                        name: newName,
                        type: String(sourceField.type ?? ''),
                    },
                );
                return makeTextResponse({
                    appKey: app.appKey,
                    objectKey,
                    action: 'duplicate_field',
                    sourceFieldKey,
                    newName,
                    ok: true,
                    status: result.status,
                    ...(duplicatedField ? { field: duplicatedField } : {}),
                    bodySizeBytes: bodyDetail.sizeBytes,
                    bodySummary: bodyDetail.summary,
                    note: duplicatedField
                        ? "Knack's response for this write included the full application schema (expected for connection fields, since they update the cross-object relationship graph) — projected down to the duplicated field above plus a structural summary. Call knack_get_field for the full raw field definition if needed."
                        : `Knack's response for this write included the full application schema. Could not unambiguously identify the duplicated field in it (e.g. another field named "${newName}" of the same type may already exist on ${objectKey} — Knack field names aren't unique — or the response may not have included this object at all) — call knack_get_object on ${objectKey} to find the new field's key.`,
                    cacheNote: SCHEMA_CACHE_STALE_NOTE,
                });
            }
        }

        return makeTextResponse({
            appKey: app.appKey,
            objectKey,
            action: 'duplicate_field',
            sourceFieldKey,
            newName,
            ...result,
            ...(result.ok ? { cacheNote: SCHEMA_CACHE_STALE_NOTE } : {}),
        });
    },
});

/** A field's two rule sets as Knack stores them, and the property each lives under. */
const FIELD_RULE_SETS = {
    conditional: 'rules',
    validation: 'validation',
} as const;

/**
 * Add, replace or remove a field's conditional rules (which set its value) or
 * validation rules (which reject input), by key.
 *
 * `PUT /objects/:key/fields/:field` merges at the top level: measured on NP Place
 * Playground on 25 September, a body of `{rules, conditional}` alone set the conditional
 * rules and left the name, description (with its `_notes` stamp) and validation rules
 * as they were, and `{validation}` alone did the same the other way. So this sends only
 * the one rule set, with `conditional` kept true exactly while conditional rules exist.
 * New rules get the next free numeric key, the scheme Knack uses on field rules.
 */
export const editFieldRules = defineTool({
    name: 'knack_edit_field_rules',
    description:
        "Add, replace or remove a field's conditional or validation rules by key; sends only that rule set and reads it back.",
    access: 'write',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        fieldKey: z.string(),
        ruleSet: z
            .enum(['conditional', 'validation'])
            .describe(
                "conditional = rules that set this field's value; validation = rules that reject input with a message",
            ),
        addRules: z
            .string()
            .optional()
            .describe(
                'JSON array of rules to append, e.g. conditional [{"criteria":[{"field":"field_1","operator":"is","value":"x"}],"values":[{"type":"value","field":"field_2","value":"y"}]}] or validation [{"criteria":[{"field":"field_2","operator":"is blank"}],"message":"Required"}]',
            ),
        replaceRules: z
            .string()
            .optional()
            .describe(
                'JSON array of whole rules, each carrying the key of the stored rule it replaces',
            ),
        removeKeys: z
            .array(z.string())
            .optional()
            .describe('Keys of rules to remove, e.g. ["2"]'),
        previewOnly: z
            .boolean()
            .optional()
            .describe('Return the edited rules without sending them'),
    },
    handler: async (
        {
            appKey,
            objectKey,
            fieldKey,
            ruleSet,
            addRules,
            replaceRules,
            removeKeys,
            previewOnly,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);
        const { respond, refuse } = toolReplies(
            app.appKey,
            'edit_field_rules',
            {
                objectKey,
                fieldKey,
                ruleSet,
            },
        );

        let added: Array<Record<string, unknown>>;
        let replacements: Array<Record<string, unknown>> | undefined;
        try {
            added = addRules
                ? parseJsonObjectArray('addRules', addRules, 'rule')
                : [];
            replacements = replaceRules
                ? parseJsonObjectArray('replaceRules', replaceRules, 'rule')
                : undefined;
        } catch (error) {
            return refuse('INVALID_RULES', (error as Error).message);
        }
        if (!added.length && !replacements?.length && !removeKeys?.length) {
            return refuse(
                'NOTHING_TO_CHANGE',
                'Pass addRules, replaceRules and/or removeKeys. Nothing was sent.',
            );
        }

        const objResult = await ctx.request(app, `/objects/${objectKey}`);
        const liveFields = readObjectFields(objResult.body);
        const field = liveFields?.find((entry) => entry.key === fieldKey);
        if (!field) {
            return refuse(
                'FIELD_NOT_FOUND',
                `${fieldKey} was not found on ${objectKey}. Nothing was sent.`,
            );
        }
        const locked = await refuseSchemaLockedField(
            ctx,
            app,
            objectKey,
            fieldKey,
            'edit_field_rules',
            liveFields,
        );
        if (locked) return locked;

        // A rule reading a no-data field (a criterion, a value copied through `input`, a
        // connection path) would copy or probe its value, and one writing a no-data field
        // without _mcp_allowwrite would change data the model may not touch.
        const refusal = ruleFieldRefusal(
            await ctx.getFieldExclusions(app),
            [...added, ...(replacements ?? [])],
            'a rule',
        );
        if (refusal) return refuse(refusal.error, refusal.message);

        const property = FIELD_RULE_SETS[ruleSet];
        const existing = readRuleArray(field[property]);
        let rules: Array<Record<string, unknown>>;
        let removedKeys: string[] = [];
        let replacedKeys: string[] = [];
        try {
            rules = existing;
            if (replacements?.length || removeKeys?.length) {
                const edited = applyRuleEdit(
                    existing,
                    { removeKeys, replaceRules: replacements },
                    `${ruleSet} rule`,
                );
                rules = edited.rules;
                removedKeys = edited.removedKeys;
                replacedKeys = edited.replacedKeys;
            }
            const withKeys = assignNumericRuleKeys(rules, added, 'addRules');
            rules = [...rules, ...withKeys];
            added = withKeys;
        } catch (error) {
            return refuse('INVALID_EDIT', (error as Error).message);
        }

        const body: Record<string, unknown> =
            ruleSet === 'conditional'
                ? { rules, conditional: rules.length > 0 }
                : { validation: rules };
        const summary = {
            ruleCountBefore: existing.length,
            ruleCountAfter: rules.length,
            addedKeys: added.map((rule) => rule.key),
            replacedKeys,
            removedKeys,
        };

        if (previewOnly) {
            return respond({
                ok: true,
                previewOnly: true,
                ...summary,
                wouldSend: body,
                rulesBefore: existing,
            });
        }

        const result = await ctx.request(
            app,
            `/objects/${objectKey}/fields/${fieldKey}`,
            { method: 'PUT', body: JSON.stringify(body) },
        );
        if (!result.ok) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message:
                    'Knack refused the change. The rules below are what the field had before, unchanged.',
                rulesBefore: existing,
            });
        }

        const after = await ctx.request(app, `/objects/${objectKey}`);
        const stored = readObjectFields(after.body)?.find(
            (entry) => entry.key === fieldKey,
        );
        const verified =
            Boolean(stored) &&
            deepEqual(stored?.[property] ?? [], rules) &&
            (ruleSet !== 'conditional' ||
                stored?.conditional === rules.length > 0);

        return respond({
            ok: true,
            status: result.status,
            ...summary,
            verified,
            ...(verified
                ? {}
                : {
                      warning:
                          'The rules read back from Knack differ from what was sent. Check the field in the Builder; rulesBefore restores the previous state.',
                  }),
            rulesBefore: existing,
            cacheNote: SCHEMA_CACHE_STALE_NOTE,
        });
    },
});

export const fieldTools: AnyToolDef[] = [
    createField,
    updateField,
    editFieldRules,
    deleteField,
    duplicateField,
];
