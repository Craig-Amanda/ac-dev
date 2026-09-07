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
import { asRecord, deepMergeRecords } from '../lib/util.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { getInlineDetail, makeTextResponse } from '../response.js';

const UNCHECKED_EQUATION_WARNING =
    'Could not validate equation tokens: no schema is available (neither runtime API nor schema.json) for this app, so this write is going out unchecked.';

const NOTED_BY_DESCRIPTION_CREATE =
    'Human who instructed this field to be created with a description; required (non-empty) whenever description is set to non-empty text — stamped as a trailing _notes=<name> on <date> KTL keyword recording who added it.';

const NOTED_BY_DESCRIPTION_UPDATE =
    'Human who instructed this description change. Required (non-empty) only when the field has no _notes stamp yet (first note being added) or when restampNote is true. Otherwise the existing _notes=<name> on <date> stamp is preserved untouched — it records who added the note, not who last edited it.';

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

/** The field list of a raw `GET /objects/{key}` response body. */
function readObjectFields(
    body: unknown,
): Array<Record<string, unknown>> | undefined {
    const fields = asRecord(asRecord(body)?.object)?.fields;
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
                payload.description = trimmed
                    ? appendKtlNote(trimmed, notedBy!.trim())
                    : description;
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
        validationErrors.push(...validateFieldPayload(payload, true));

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

export const fieldTools: AnyToolDef[] = [
    createField,
    updateField,
    deleteField,
    duplicateField,
];
