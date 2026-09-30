/**
 * Object (table) mutation tools: create, rename/reconfigure and delete.
 *
 * The endpoints and payload shapes here were captured from Knack's Builder UI network
 * traffic, not from the documented public REST API reference (which covers fields and
 * records, but not objects themselves). They authenticate the same way every other
 * request in this server does — app id + REST API key — confirmed live against a real
 * app before this file was written.
 */
import { z } from 'zod';

import {
    SCHEMA_CACHE_STALE_NOTE,
    appendKtlNote,
    descriptionAsPlainText,
    preserveKtlNote,
    readDescriptionText,
} from '../lib/field-payload.js';
import { getTableLockReason } from '../lib/field-exclusion.js';
import {
    AUTO_INCREMENT_FIELD_NAME,
    readHolderKeywords,
    readHolderRawDescription,
    readObjectDescription,
} from '../lib/object-description.js';
import { deepEqual } from '../lib/structural-diff.js';
import { asRecord, readWireObjectEntity } from '../lib/util.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { createField, refuseSchemaLockedField, updateField } from './fields.js';
import {
    getInlineDetail,
    makeTextResponse,
    type ToolResult,
} from '../response.js';
import type { AppConfig } from '../config.js';
import type { KnackContext } from '../context.js';
import type { KnackApiResult } from '../http.js';

/**
 * What the update does and why, returned with every write. Measured on NP Place
 * Playground on 25 September; see knack_update_object for what Knack accepts without
 * complaint.
 */
const OBJECT_MERGE_NOTE =
    'Measured on 25 September: PUT /objects/:key merges at the top level (a body with only identifier, or only sort, left the rest as it was). This call still sends name, identifier and sort together, with only what you passed changed, and reads them back.';

/**
 * Shape a create/update object response: project the write down to the touched object
 * when Knack's body is too large to inline (as with a connection field write, this can
 * carry the whole application schema), otherwise pass the raw result through as-is.
 */
/**
 * A create or delete answers with the whole application schema, tens of kilobytes that
 * the caller never needs and pays for in tokens. Above this size the body is left out.
 */
const OBJECT_BODY_INLINE_MAX_BYTES = 8192;

function respondToObjectMutation(
    app: AppConfig,
    action: string,
    result: KnackApiResult,
    extra: Record<string, unknown> = {},
): ToolResult {
    if (result.ok) {
        const bodyDetail = getInlineDetail(result.body);
        if (
            !bodyDetail.included ||
            bodyDetail.sizeBytes > OBJECT_BODY_INLINE_MAX_BYTES
        ) {
            const object = readWireObjectEntity(result.body);
            return makeTextResponse({
                appKey: app.appKey,
                action,
                ok: true,
                status: result.status,
                ...(object ? { object } : {}),
                bodySizeBytes: bodyDetail.sizeBytes,
                bodySummary: bodyDetail.summary,
                cacheNote: SCHEMA_CACHE_STALE_NOTE,
                ...extra,
            });
        }
    }

    return makeTextResponse({
        appKey: app.appKey,
        action,
        ...result,
        ...(result.ok ? { cacheNote: SCHEMA_CACHE_STALE_NOTE, ...extra } : {}),
    });
}

/** What happened to an object's description, returned beside the object's own result. */
type DescriptionOutcome = {
    ok: boolean;
    fieldKey?: string;
    /** True when the object had no auto-increment field and one was added. */
    addedAutoIncrementField?: boolean;
    verified?: boolean;
    error?: string;
};

const NO_AUTO_INCREMENT_HINT =
    'Add one with knack_create_field (type auto_increment), then set the description again.';

function payloadOfResult(result: ToolResult): Record<string, unknown> {
    try {
        return JSON.parse(result.content[0].text) as Record<string, unknown>;
    } catch {
        return {};
    }
}

function firstError(payload: Record<string, unknown>): string {
    if (Array.isArray(payload.errors) && payload.errors.length) {
        return payload.errors.join(' ');
    }
    if (typeof payload.message === 'string') return payload.message;
    return `Knack answered ${String(payload.status ?? 'with an error')}.`;
}

async function readLiveFields(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
): Promise<unknown> {
    const result = await ctx.request(app, `/objects/${objectKey}`);
    return result.ok ? readWireObjectEntity(result.body)?.fields : undefined;
}

/**
 * What a description change would do, for a dry run: the field, the words before and
 * after, any other keywords on the field that are kept, and the exact string that would be
 * stored (when it can be worked out without a `notedBy`).
 */
function previewObjectDescription(
    fields: unknown,
    description: string,
    notedBy: string | undefined,
) {
    const held = readObjectDescription(fields);
    const keywords = readHolderKeywords(fields);
    const raw = descriptionAsPlainText(readHolderRawDescription(fields));
    const body = [description, keywords].filter(Boolean).join(' ');
    const hasNote = /_notes=/i.test(raw);
    const wouldStore = hasNote
        ? preserveKtlNote(body, raw)
        : notedBy?.trim()
          ? appendKtlNote(body, notedBy.trim())
          : undefined;
    return {
        onField: held.fieldKey,
        from: held.text,
        to: description,
        ...(keywords ? { keywordsKept: keywords } : {}),
        ...(wouldStore
            ? { wouldStore }
            : {
                  notedByNeeded:
                      'This field has no _notes yet, so notedBy is needed to stamp it.',
              }),
    };
}

const normaliseWords = (text: string) => text.replace(/\s+/g, ' ').trim();

/**
 * Write an object's description onto its auto-increment field, stamped with who and when,
 * and read it back. With `createIfMissing`, an object with no such field gets one first
 * (a new object must never be left without a place for its description); otherwise a
 * missing field is reported and nothing is added.
 *
 * Goes through knack_create_field and knack_update_field, so every guard they apply
 * (locks, keyword protection, the stamp) applies here too.
 */
async function writeObjectDescription(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    description: string,
    notedBy: string | undefined,
    options: { createIfMissing: boolean; knownFields?: unknown },
): Promise<DescriptionOutcome> {
    let fields = options.knownFields;
    let fieldKey = readObjectDescription(fields).fieldKey;
    if (!fieldKey) {
        // The create response may not list fields at all, so look at the live object.
        fields = await readLiveFields(ctx, app, objectKey);
        fieldKey = readObjectDescription(fields).fieldKey;
    }

    let addedAutoIncrementField = false;
    if (!fieldKey) {
        if (!options.createIfMissing) {
            return {
                ok: false,
                error: `${objectKey} has no auto-increment field to hold its description. ${NO_AUTO_INCREMENT_HINT}`,
            };
        }
        const created = payloadOfResult(
            await createField.handler(
                {
                    appKey: app.appKey,
                    objectKey,
                    name: AUTO_INCREMENT_FIELD_NAME,
                    type: 'auto_increment',
                    required: false,
                    unique: false,
                    description,
                    notedBy,
                    dryRun: false,
                },
                ctx,
            ),
        );
        if (created.ok === false) {
            return {
                ok: false,
                error: `Adding the auto-increment field failed: ${firstError(created)}`,
            };
        }
        addedAutoIncrementField = true;
        fieldKey = readObjectDescription(
            await readLiveFields(ctx, app, objectKey),
        ).fieldKey;
        if (!fieldKey) {
            return {
                ok: false,
                addedAutoIncrementField,
                error: `An auto-increment field was added to ${objectKey} but could not be found afterwards. Check the table in the Builder.`,
            };
        }
    } else {
        const updated = payloadOfResult(
            await updateField.handler(
                {
                    appKey: app.appKey,
                    objectKey,
                    fieldKey,
                    // Keywords on the field stay: leaving them out would read as
                    // removing them, which the field guards refuse.
                    description: [description, readHolderKeywords(fields)]
                        .filter(Boolean)
                        .join(' '),
                    notedBy,
                    restampNote: false,
                    confirmRemoveKtlKeywords: false,
                    dryRun: false,
                },
                ctx,
            ),
        );
        if (updated.ok === false) {
            return { ok: false, fieldKey, error: firstError(updated) };
        }
    }

    const stored = readObjectDescription(
        await readLiveFields(ctx, app, objectKey),
    ).text;
    // What was written, as the stored form reads back (brackets in the words become
    // parentheses), so the comparison is like for like.
    const expected = readDescriptionText(appendKtlNote(description, 'x'));
    const verified = normaliseWords(stored).includes(normaliseWords(expected));
    return {
        ok: true,
        fieldKey,
        ...(addedAutoIncrementField ? { addedAutoIncrementField } : {}),
        verified,
        ...(verified
            ? {}
            : {
                  error: `Read back from Knack, ${fieldKey} does not carry the description that was sent. Check the field in the Builder.`,
              }),
    };
}

export const createObject = defineTool({
    name: 'knack_create_object',
    description:
        'Create a table with a description, held on its auto-increment field; dryRun previews it. Add fields afterwards with knack_create_field.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        name: z.string(),
        description: z
            .string()
            .describe('What the table holds, for an AI reading the schema'),
        notedBy: z
            .string()
            .describe('Human who asked for this table; stamped as _notes'),
        userTable: z
            .boolean()
            .default(false)
            .describe(
                "Knack's `user` flag — marks this as a user/account table rather than a plain data table.",
            ),
        isBookableResource: z.boolean().default(false),
        template: z.string().default(''),
        dryRun: z.boolean().default(false),
    },
    handler: async (
        {
            appKey,
            name,
            description,
            notedBy,
            userTable,
            isBookableResource,
            template,
            dryRun,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);

        const preflightErrors = [
            ...(name.trim() ? [] : ['name must be a non-empty string.']),
            ...(description.trim()
                ? []
                : [
                      'description must say what the table holds: every table carries one, on its auto-increment field.',
                  ]),
            ...(notedBy.trim()
                ? []
                : [
                      'notedBy is required: the description is stamped with who asked for it and when.',
                  ]),
        ];
        if (preflightErrors.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'create_object_preflight',
                errors: preflightErrors,
            });
        }

        const payload: Record<string, unknown> = {
            name,
            user: userTable,
            isBookableResource,
            fields: [],
            template,
        };

        if (dryRun) {
            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                action: 'create_object_dry_run',
                dryRun: true,
                wouldCreate: payload,
                wouldWriteDescription: {
                    onField:
                        'the auto-increment field (added if Knack does not create one)',
                    description: description.trim(),
                    notedBy: notedBy.trim(),
                },
            });
        }

        const result = await ctx.request(app, '/objects', {
            method: 'POST',
            body: JSON.stringify(payload),
        });
        if (!result.ok)
            return respondToObjectMutation(app, 'create_object', result);

        const created = readWireObjectEntity(result.body);
        if (typeof created?.key !== 'string') {
            return respondToObjectMutation(app, 'create_object', result, {
                objectDescription: {
                    ok: false,
                    error: "The table was created, but its key could not be found in Knack's response, so its description was not written. Find the table with knack_list_objects, then set the description with knack_update_object.",
                },
            });
        }

        const objectDescription = await writeObjectDescription(
            ctx,
            app,
            created.key,
            description.trim(),
            notedBy.trim(),
            { createIfMissing: true, knownFields: created.fields },
        );
        return respondToObjectMutation(app, 'create_object', result, {
            objectKey: created.key,
            objectDescription,
            ...(objectDescription.ok && objectDescription.verified
                ? {}
                : {
                      warning: `${created.key} was created but its description was not written cleanly: ${objectDescription.error ?? 'see objectDescription'}. Nothing was rolled back; fix it with knack_update_object.`,
                  }),
        });
    },
});

export const updateObject = defineTool({
    name: 'knack_update_object',
    description:
        'Rename a table, change its display field or default sort, and/or set its description (on its auto-increment field); dryRun previews.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        name: z.string().optional(),
        identifier: z
            .string()
            .optional()
            .describe(
                "Field key to use as this object's display field (e.g. field_23).",
            ),
        sortField: z
            .string()
            .optional()
            .describe('Field key for the default sort.'),
        sortOrder: z.enum(['asc', 'desc']).optional(),
        description: z
            .string()
            .optional()
            .describe('What the table holds; replaces the current text'),
        notedBy: z
            .string()
            .optional()
            .describe('Needed only if the description has no _notes stamp yet'),
        dryRun: z.boolean().default(false),
    },
    handler: async (
        {
            appKey,
            objectKey,
            name,
            identifier,
            sortField,
            sortOrder,
            description,
            notedBy,
            dryRun,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);

        const objectChange =
            name !== undefined ||
            identifier !== undefined ||
            sortField !== undefined ||
            sortOrder !== undefined;
        if (!objectChange && description === undefined) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'update_object_preflight',
                errors: [
                    'Provide at least one of name, identifier, sortField, sortOrder or description — nothing to update.',
                ],
            });
        }
        if (description !== undefined && !description.trim()) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'update_object_preflight',
                errors: [
                    'description must not be empty: every table carries one. Nothing was sent.',
                ],
            });
        }

        const objResult = await ctx.request(app, `/objects/${objectKey}`);
        const current = readWireObjectEntity(objResult.body);
        if (!objResult.ok || !current) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'update_object_preflight',
                message: `Could not fetch current definition for ${objectKey}.`,
                status: objResult.status,
            });
        }

        // A table lock covers the table's own settings too. Checked against the live
        // fields as well as the cache: a person may have just added it in the builder.
        const tableLock = await getTableLockReason(
            ctx,
            app,
            objectKey,
            current.fields,
        );
        if (tableLock) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'update_object_preflight',
                errors: [
                    `${tableLock}, so its schema cannot be changed through MCP. A person can change it, or remove the keyword, in the Knack builder.`,
                ],
            });
        }

        // Knack stores whatever it is sent here: measured on NP Place Playground on 25
        // September, a PUT answered 200 for an identifier belonging to another object
        // and for a sort on a field that does not exist, leaving the table's display
        // values and default sort pointing at nothing. So both are checked against the
        // object's own fields first.
        const ownFields = new Set(
            (Array.isArray(current.fields) ? current.fields : [])
                .map((field) => asRecord(field)?.key)
                .filter((key): key is string => typeof key === 'string'),
        );
        const notOwn = [identifier, sortField].filter(
            (key): key is string => key !== undefined && !ownFields.has(key),
        );
        if (notOwn.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'update_object_preflight',
                errors: [
                    `${notOwn.join(', ')} ${notOwn.length === 1 ? 'is not a field' : 'are not fields'} on ${objectKey}. Knack would store it anyway and the table would point at nothing. Nothing was sent.`,
                ],
            });
        }

        // A table with no auto-increment field cannot hold a description, and only new
        // tables get one added. Refused before anything is sent, so a rename asked for in
        // the same call is not left half done.
        const held = readObjectDescription(current.fields);
        if (description !== undefined && !held.fieldKey) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'update_object_preflight',
                errors: [
                    `${objectKey} has no auto-increment field to hold its description. ${NO_AUTO_INCREMENT_HINT} Nothing was sent.`,
                ],
            });
        }

        // Only the description changes: no PUT to the object at all.
        if (!objectChange) {
            if (dryRun) {
                return makeTextResponse({
                    ok: true,
                    appKey: app.appKey,
                    objectKey,
                    action: 'update_object_dry_run',
                    dryRun: true,
                    wouldWriteDescription: previewObjectDescription(
                        current.fields,
                        description!.trim(),
                        notedBy,
                    ),
                });
            }
            const objectDescription = await writeObjectDescription(
                ctx,
                app,
                objectKey,
                description!.trim(),
                notedBy?.trim(),
                { createIfMissing: false, knownFields: current.fields },
            );
            return makeTextResponse({
                ok:
                    objectDescription.ok &&
                    objectDescription.verified !== false,
                appKey: app.appKey,
                objectKey,
                action: 'update_object',
                objectDescription,
                cacheNote: SCHEMA_CACHE_STALE_NOTE,
            });
        }

        const currentSort = asRecord(current.sort);
        if (sortOrder !== undefined && !sortField && !currentSort?.field) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'update_object_preflight',
                errors: [
                    `${objectKey} has no default sort field yet, so sortOrder needs sortField. Nothing was sent.`,
                ],
            });
        }
        // A table with no default sort and none asked for gets no sort key at all:
        // sending {} would read back as "no sort" and look like a failed write.
        const nextSortField = sortField ?? currentSort?.field;
        const payload: Record<string, unknown> = {
            name: name ?? current.name,
            identifier: identifier ?? current.identifier,
            ...(nextSortField
                ? {
                      sort: {
                          field: nextSortField,
                          order: sortOrder ?? currentSort?.order ?? 'asc',
                      },
                  }
                : {}),
        };

        if (dryRun) {
            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                objectKey,
                action: 'update_object_dry_run',
                dryRun: true,
                currentObject: {
                    name: current.name,
                    identifier: current.identifier,
                    sort: current.sort,
                },
                wouldUpdate: payload,
                ...(description !== undefined
                    ? {
                          wouldWriteDescription: previewObjectDescription(
                              current.fields,
                              description.trim(),
                              notedBy,
                          ),
                      }
                    : {}),
                mergeNote: OBJECT_MERGE_NOTE,
            });
        }

        const result = await ctx.request(app, `/objects/${objectKey}`, {
            method: 'PUT',
            body: JSON.stringify(payload),
        });
        if (!result.ok) {
            return respondToObjectMutation(app, 'update_object', result, {
                objectKey,
            });
        }

        // Read back: the name, display field and default sort are what was sent.
        const after = readWireObjectEntity(
            (await ctx.request(app, `/objects/${objectKey}`)).body,
        );
        const stored = {
            name: after?.name,
            identifier: after?.identifier,
            sort: after?.sort,
        };
        const verified =
            stored.name === payload.name &&
            stored.identifier === payload.identifier &&
            (payload.sort === undefined ||
                deepEqual(stored.sort, payload.sort));

        const objectDescription =
            description !== undefined
                ? await writeObjectDescription(
                      ctx,
                      app,
                      objectKey,
                      description.trim(),
                      notedBy?.trim(),
                      { createIfMissing: false, knownFields: current.fields },
                  )
                : undefined;

        return respondToObjectMutation(app, 'update_object', result, {
            objectKey,
            verified,
            stored,
            ...(objectDescription ? { objectDescription } : {}),
            ...(verified
                ? {}
                : {
                      warning:
                          'Read back from Knack, the name, display field or sort differs from what was sent. Check the table in the Builder.',
                  }),
            mergeNote: OBJECT_MERGE_NOTE,
        });
    },
});

export const deleteObject = defineTool({
    name: 'knack_delete_object',
    description:
        'Permanently delete a table and all of its fields and records; previews unless confirm is true.',
    access: 'delete',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        confirm: z
            .boolean()
            .optional()
            .default(false)
            .describe(
                'Must be true to delete; otherwise a preview is returned.',
            ),
    },
    handler: async ({ appKey, objectKey, confirm }, ctx) => {
        const app = ctx.getApp(appKey);
        // Deleting the table deletes its fields, so a locked field locks the table. The
        // live fields are checked as well as the cache: a lock keyword a person has just
        // added in the builder may not be in the cache yet.
        const objResult = await ctx.request(app, `/objects/${objectKey}`);
        const current = readWireObjectEntity(objResult.body);
        const locked = await refuseSchemaLockedField(
            ctx,
            app,
            objectKey,
            undefined,
            'delete_object',
            current?.fields,
        );
        if (locked) return locked;

        if (!confirm) {
            const fieldCount = Array.isArray(current?.fields)
                ? current.fields.length
                : undefined;

            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'delete_object_preflight',
                message: current
                    ? `This would permanently delete the table "${current.name}" (${objectKey})${
                          fieldCount !== undefined
                              ? `, its ${fieldCount} field(s),`
                              : ''
                      } and every record in it. This cannot be undone. Pass confirm: true only after explicitly confirming this with the user.`
                    : `This would permanently delete ${objectKey}, all of its fields and every record in it. This cannot be undone. Pass confirm: true only after explicitly confirming this with the user. (Could not fetch the current definition to name it here — status ${objResult.status}.)`,
                ...(current
                    ? {
                          wouldDeleteName: current.name,
                          wouldDeleteFieldCount: fieldCount,
                      }
                    : {}),
            });
        }

        const result = await ctx.request(app, `/objects/${objectKey}`, {
            method: 'DELETE',
        });

        const deleted = getInlineDetail(result.body);
        const slim =
            result.ok && deleted.sizeBytes > OBJECT_BODY_INLINE_MAX_BYTES;
        return makeTextResponse({
            appKey: app.appKey,
            objectKey,
            action: 'delete_object',
            ...(slim
                ? {
                      ok: result.ok,
                      status: result.status,
                      bodySizeBytes: deleted.sizeBytes,
                      bodySummary: deleted.summary,
                  }
                : result),
            ...(result.ok ? { cacheNote: SCHEMA_CACHE_STALE_NOTE } : {}),
        });
    },
});

export const objectTools: AnyToolDef[] = [
    createObject,
    updateObject,
    deleteObject,
];
