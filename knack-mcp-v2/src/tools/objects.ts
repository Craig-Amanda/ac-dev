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

import { SCHEMA_CACHE_STALE_NOTE } from '../lib/field-payload.js';
import { asRecord, readWireObjectEntity } from '../lib/util.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import {
    getInlineDetail,
    makeTextResponse,
    type ToolResult,
} from '../response.js';
import type { AppConfig } from '../config.js';
import type { KnackApiResult } from '../http.js';

/**
 * Knack's merge behaviour for `PUT /objects/:key` is unverified — the Builder UI's own
 * table-settings form always resubmits name, identifier and sort together even when only
 * one changed, which is the safest assumption to copy rather than risk a partial PUT
 * silently clearing the other two.
 */
const OBJECT_MERGE_UNCERTAINTY_NOTE =
    "Knack's merge behaviour for /objects/:key PUT is unverified — this call always resends the object's current name, identifier and sort together (with only the fields you passed changed), mirroring what the Builder UI itself sends, so an unspecified field cannot be silently cleared. Not yet independently confirmed against a live app.";

/**
 * Shape a create/update object response: project the write down to the touched object
 * when Knack's body is too large to inline (as with a connection field write, this can
 * carry the whole application schema), otherwise pass the raw result through as-is.
 */
function respondToObjectMutation(
    app: AppConfig,
    action: string,
    result: KnackApiResult,
    extra: Record<string, unknown> = {},
): ToolResult {
    if (result.ok) {
        const bodyDetail = getInlineDetail(result.body);
        if (!bodyDetail.included) {
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

export const createObject = defineTool({
    name: 'knack_create_object',
    description:
        'Create a table (object) with no custom fields yet; dryRun previews the definition without creating it. Add fields afterwards with knack_create_field.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        name: z.string(),
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
        { appKey, name, userTable, isBookableResource, template, dryRun },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);

        if (!name.trim()) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'create_object_preflight',
                errors: ['name must be a non-empty string.'],
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
            });
        }

        const result = await ctx.request(app, '/objects', {
            method: 'POST',
            body: JSON.stringify(payload),
        });

        return respondToObjectMutation(app, 'create_object', result);
    },
});

export const updateObject = defineTool({
    name: 'knack_update_object',
    description:
        'Rename a table and/or change its display field (identifier) or default sort; dryRun previews the merged definition without persisting.',
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
        dryRun: z.boolean().default(false),
    },
    handler: async (
        { appKey, objectKey, name, identifier, sortField, sortOrder, dryRun },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);

        if (
            name === undefined &&
            identifier === undefined &&
            sortField === undefined &&
            sortOrder === undefined
        ) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'update_object_preflight',
                errors: [
                    'Provide at least one of name, identifier, sortField or sortOrder — nothing to update.',
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

        const currentSort = asRecord(current.sort);
        const payload: Record<string, unknown> = {
            name: name ?? current.name,
            identifier: identifier ?? current.identifier,
            sort: {
                field: sortField ?? currentSort?.field,
                order: sortOrder ?? currentSort?.order,
            },
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
                mergeNote: OBJECT_MERGE_UNCERTAINTY_NOTE,
            });
        }

        const result = await ctx.request(app, `/objects/${objectKey}`, {
            method: 'PUT',
            body: JSON.stringify(payload),
        });

        return respondToObjectMutation(app, 'update_object', result, {
            objectKey,
            mergeNote: OBJECT_MERGE_UNCERTAINTY_NOTE,
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

        if (!confirm) {
            const objResult = await ctx.request(app, `/objects/${objectKey}`);
            const current = readWireObjectEntity(objResult.body);
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

        return makeTextResponse({
            appKey: app.appKey,
            objectKey,
            action: 'delete_object',
            ...result,
            ...(result.ok ? { cacheNote: SCHEMA_CACHE_STALE_NOTE } : {}),
            note: 'DELETE /objects/{objectKey} is inferred from the create/rename endpoints already confirmed live — the method and response shape have not been independently verified. Confirm against a real request before merging.',
        });
    },
});

export const objectTools: AnyToolDef[] = [
    createObject,
    updateObject,
    deleteObject,
];
