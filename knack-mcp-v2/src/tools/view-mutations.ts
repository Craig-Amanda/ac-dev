/**
 * View mutation tools. Every one of them runs through runViewMutationTool, which owns the
 * guard: fresh metadata, the cascade check, the human confirmation, the snapshot and the
 * response shape. Nothing here re-implements any of that; each tool only names the
 * action and performs the Knack request the guard lets through.
 */
import { z } from 'zod';

/**
 * `field-payload.js`'s own `FIELD_KEY_PATTERN` is case-insensitive so callers matching a
 * possibly-mistyped-case key against the (always lower-case) generated field map still
 * resolve — but a real Knack field key is only ever lower-case, so that same leniency
 * would let a value like "FIELD_5" through the guards in this file, which exist
 * specifically to reject anything that is not a real field key: `fieldKeys` (below) and
 * `columnConnections`' keys and values both feed straight into a lower-case field-key
 * lookup or a payload sent to Knack, so both are checked against this pattern instead.
 */
const FIELD_KEY_PATTERN_CASE_SENSITIVE = /^field_\d+$/;
import {
    findRawViewInMetadata,
    parseRuntimeScenes,
    readSceneGroups,
} from '../lib/metadata.js';
import { asRecord, parseJsonInput } from '../lib/util.js';
import {
    collectLinkTargets,
    getViewType,
    planSharedPageCopy,
    resolveViewAttributes,
    readChangedScenes,
    verifySharedPageCopy,
} from '../lib/view-safety.js';
import {
    buildPageGroupsPreservingLayout,
    buildStarterLayoutRows,
    buildTemplateFieldDescriptors,
    buildViewFieldColumn,
    buildViewGroupField,
    type NewViewPlacement,
    placeNewViewInLayout,
    placeViewInLayout,
    describeLayoutKeyGap,
    getSceneViewKeys,
} from '../lib/view-templates.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { makeTextResponse } from '../response.js';
import {
    ensureCopiedViewRendersOnce,
    ensureMovedViewIsRendered,
    ensureMovedViewLeavesNoResidue,
    findOrphansLeftByMove,
    insertedViewKeysFromOutcome,
    runViewMutationTool,
    summariseCopyLinkOwnership,
} from '../view-mutation.js';

/**
 * What a plain copy did to the pages its links pointed at.
 *
 * Both halves are stated because they have opposite consequences and a caller acts on
 * the difference: a duplicated page makes the copy independent, a shared one does not.
 * The counts come from the response, so this describes the copy that happened.
 */
function describeCopyLinkOutcome(
    rows: ReturnType<typeof summariseCopyLinkOwnership>,
): string {
    const duplicated = rows.filter((row) => row.onCopy === 'duplicated').length;
    const shared = rows.length - duplicated;
    return [
        `${duplicated} linked page(s) were duplicated and ${shared} shared, read from the pages Knack reported creating rather than predicted from the link flags.`,
        duplicated > 0
            ? 'A duplicated page is a new page with a new slug; the copy points at it and the original still points at the old one.'
            : null,
        shared > 0
            ? 'A shared page is now linked from two views: an edit to it shows in both, and removing one of those links re-parents it rather than deleting it.'
            : null,
    ]
        .filter((line): line is string => line !== null)
        .join(' ');
}

/** Shared wording so all three destructive tools describe the flag identically. */
const PREVIEW_DESCRIPTION =
    'Work out what this would do and return it without doing it: no prompt, no snapshot, nothing sent to Knack.';

export const createView = defineTool({
    name: 'knack_create_view',
    description: 'Create a view on a scene from a full view definition.',
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        payload: z
            .string()
            .describe('Full view definition as JSON, with pageGroups'),
    },
    handler: async ({ appKey, sceneKey, payload }, ctx) => {
        const app = ctx.getApp(appKey);
        // Resolved before the guard runs any I/O: a missing key must refuse here, not
        // after a human has already been prompted or a snapshot written.
        ctx.getApiKey(app.appKey);

        return makeTextResponse(
            await runViewMutationTool(
                ctx,
                app,
                { action: 'create_view', sceneKey, updates: payload },
                () =>
                    ctx.request(app, `/scenes/${sceneKey}/views`, {
                        method: 'POST',
                        body: payload,
                    }),
            ),
        );
    },
});

export const updateViewOrder = defineTool({
    name: 'knack_update_view_order',
    description:
        'Update the order and page-group layout of the views on a scene.',
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        order: z
            .union([z.string(), z.array(z.string())])
            .describe(
                'View keys in the desired order, as an array or its JSON',
            ),
        pageGroups: z
            .union([z.string(), z.array(z.unknown())])
            .optional()
            .describe(
                'Page groups layout, as an array or its JSON: rows shaped [{ "columns": [{ "keys": ["view_1"], "width": 100 }] }] — it replaces the page\'s whole layout, and a view no row names renders nowhere. Omitted: one full-width row per view, in order',
            ),
    },
    handler: async ({ appKey, sceneKey, order, pageGroups }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        // Both inputs used to be JSON strings only, and `pageGroups` was required. A
        // caller sending the array itself, or leaving the layout alone, failed MCP input
        // validation before this handler ran — a schema error that looked like a
        // refusal and was not one (6 September). Knack's sort route needs both, so a
        // missing layout is derived from the order: one row per view.
        const orderKeys =
            typeof order === 'string'
                ? parseJsonInput<unknown[]>('order', order)
                : order;
        // Checked after parsing, whichever form arrived: an empty list, or an entry
        // that is not a view key, would otherwise reach Knack as a sort request naming
        // no views, with a derived layout just as empty.
        if (
            !Array.isArray(orderKeys) ||
            orderKeys.length === 0 ||
            orderKeys.some(
                (key) => typeof key !== 'string' || key.trim() === '',
            )
        ) {
            throw new Error(
                'order must be a non-empty array of view keys (as an array or its JSON).',
            );
        }
        const layout =
            pageGroups === undefined
                ? orderKeys.map((viewKey) => ({
                      columns: [{ keys: [viewKey], width: 100 }],
                  }))
                : typeof pageGroups === 'string'
                  ? parseJsonInput<unknown[]>('pageGroups', pageGroups)
                  : pageGroups;
        const body = JSON.stringify({ order: orderKeys, pageGroups: layout });

        return makeTextResponse(
            await runViewMutationTool(
                ctx,
                app,
                {
                    action: 'update_view_order',
                    sceneKey,
                    // Passed so the payload gets the same depth and links inspection as
                    // any other caller-supplied JSON, rather than reaching the API
                    // unexamined.
                    updates: body,
                },
                () =>
                    ctx.request(app, `/scenes/${sceneKey}/views/sort`, {
                        method: 'POST',
                        body,
                    }),
            ),
        );
    },
});

export const updateView = defineTool({
    name: 'knack_update_view',
    description:
        'Update a view: the changes are merged into its live definition and sent whole.',
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        viewKey: z.string(),
        updates: z
            .string()
            .optional()
            .describe(
                'JSON of the top-level properties to replace; omit if only using keywordEdits',
            ),
        keywordEdits: z
            .string()
            .optional()
            .describe(
                'JSON: {"title"?: {"_keyword": "value or null"}, "description"?: {...}} — adds each keyword at the end of the trailing KTL keyword cluster if new, or updates it in place (siblings untouched) if it already exists',
            ),
        confirmRemoveKtlKeywords: z
            .boolean()
            .default(false)
            .describe(
                'Allow a title/description change to drop an existing KTL keyword token',
            ),
        confirmDestructive: z
            .boolean()
            .optional()
            .describe('Removed; any value is refused'),
        previewOnly: z.boolean().optional().describe(PREVIEW_DESCRIPTION),
    },
    handler: async (
        {
            appKey,
            sceneKey,
            viewKey,
            updates,
            keywordEdits,
            confirmRemoveKtlKeywords,
            confirmDestructive,
            previewOnly,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        return makeTextResponse(
            await runViewMutationTool(
                ctx,
                app,
                {
                    action: 'update_view',
                    sceneKey,
                    viewKey,
                    updates: updates ?? '{}',
                    keywordEdits,
                    confirmRemoveKtlKeywords,
                    confirmDestructive,
                    previewOnly,
                },
                async ({ outgoingBody }) => {
                    // The guard merged this from the live definition and the caller's
                    // patch, and every decision it made — which pages die, whether a
                    // human had to agree — was made against this exact object.
                    // Rebuilding it here would put two reasoners on one payload.
                    const completeBody = outgoingBody;

                    return ctx.request(
                        app,
                        `/scenes/${sceneKey}/views/${viewKey}`,
                        {
                            method: 'PUT',
                            body: completeBody
                                ? JSON.stringify(completeBody)
                                : updates,
                        },
                    );
                },
            ),
        );
    },
});

/** A table column's own field key, or null for a column with none (a link, an action). */
function columnFieldKey(column: unknown): string | null {
    const key = asRecord(asRecord(column)?.field)?.key;
    return typeof key === 'string' ? key : null;
}

/** View types whose fields live in the nested layout `walkNestedFields` understands. */
const NESTED_COLUMN_VIEW_TYPES = new Set(['details', 'list']);

/** One field item's exact position inside a details/list view's nested `columns`. */
type NestedFieldLocation = {
    blockIndex: number;
    groupIndex: number;
    subColumnIndex: number;
    itemIndex: number;
    key: string;
};

/**
 * Every field item in a details/list view's `columns[].groups[].columns[][]`, with its
 * exact position — width-block, group, sub-column and index within it. Built once and
 * read for both duplicate detection and anchor placement, so the two can never
 * disagree about where a key lives.
 *
 * Measured shape (buildViewGroupField's doc comment): each width-block carries
 * `groups`, each group carries `columns` (sub-columns), and each sub-column is a bare
 * array of field items — not wrapped in an object. An item that is a link, a
 * divider or anything else this tool does not recognise as a field is skipped rather
 * than guessed at: only items exposing a string `key` are collected.
 */
function walkNestedFields(columns: unknown[]): NestedFieldLocation[] {
    const locations: NestedFieldLocation[] = [];
    columns.forEach((block, blockIndex) => {
        const groups = asRecord(block)?.groups;
        if (!Array.isArray(groups)) return;
        groups.forEach((group, groupIndex) => {
            const subColumns = asRecord(group)?.columns;
            if (!Array.isArray(subColumns)) return;
            subColumns.forEach((subColumn, subColumnIndex) => {
                if (!Array.isArray(subColumn)) return;
                subColumn.forEach((item, itemIndex) => {
                    const key = asRecord(item)?.key;
                    if (typeof key === 'string') {
                        locations.push({
                            blockIndex,
                            groupIndex,
                            subColumnIndex,
                            itemIndex,
                            key,
                        });
                    }
                });
            });
        });
    });
    return locations;
}

/**
 * Where new field items land with no anchor: the end of the last field list in the
 * last group of the last width-block — the same "append at the end" default a table
 * column gets, translated into the nested shape.
 *
 * Declines rather than invents a structure for a view whose `columns` is empty, or
 * whose last block or group carries none to append into. Those are shapes this tool
 * has not measured, and guessing at one risks writing a layout Knack does not expect
 * from a caller who never saw it.
 */
function findNestedAppendLocation(columns: unknown[]):
    | {
          ok: true;
          blockIndex: number;
          groupIndex: number;
          subColumnIndex: number;
          insertIndex: number;
      }
    | { ok: false; code: string; message: string } {
    if (columns.length === 0) {
        return {
            ok: false,
            code: 'EMPTY_LAYOUT',
            message:
                'This view has an empty columns array, so there is no width-block to append into.',
        };
    }
    const blockIndex = columns.length - 1;
    const groups = asRecord(columns[blockIndex])?.groups;
    if (!Array.isArray(groups) || groups.length === 0) {
        return {
            ok: false,
            code: 'NO_GROUPS',
            message:
                "This view's last width-block carries no groups to append into.",
        };
    }
    const groupIndex = groups.length - 1;
    const subColumns = asRecord(groups[groupIndex])?.columns;
    if (!Array.isArray(subColumns) || subColumns.length === 0) {
        return {
            ok: false,
            code: 'NO_SUBCOLUMNS',
            message:
                "This view's last group carries no columns to append into.",
        };
    }
    const subColumnIndex = subColumns.length - 1;
    const subColumn = subColumns[subColumnIndex];
    if (!Array.isArray(subColumn)) {
        return {
            ok: false,
            code: 'UNEXPECTED_SHAPE',
            message:
                "This view's last column is not a field list this tool recognises, so it will not guess at appending to it.",
        };
    }
    return {
        ok: true,
        blockIndex,
        groupIndex,
        subColumnIndex,
        insertIndex: subColumn.length,
    };
}

/**
 * Splice new items into one field list inside `columns[].groups[].columns[][]`,
 * rebuilding every level above it (width-block, group, sub-column) so nothing else in
 * the layout — other sub-columns, other groups, other width-blocks — is touched.
 */
function spliceNestedFields(
    columns: unknown[],
    location: {
        blockIndex: number;
        groupIndex: number;
        subColumnIndex: number;
    },
    insertIndex: number,
    newItems: Record<string, unknown>[],
): unknown[] {
    return columns.map((block, blockIndex) => {
        if (blockIndex !== location.blockIndex) return block;
        const blockRecord = asRecord(block) as Record<string, unknown>;
        const groups = blockRecord.groups as unknown[];
        return {
            ...blockRecord,
            groups: groups.map((group, groupIndex) => {
                if (groupIndex !== location.groupIndex) return group;
                const groupRecord = asRecord(group) as Record<string, unknown>;
                const subColumns = groupRecord.columns as unknown[];
                return {
                    ...groupRecord,
                    columns: subColumns.map((subColumn, subColumnIndex) => {
                        if (subColumnIndex !== location.subColumnIndex)
                            return subColumn;
                        const items = subColumn as unknown[];
                        return [
                            ...items.slice(0, insertIndex),
                            ...newItems,
                            ...items.slice(insertIndex),
                        ];
                    }),
                };
            }),
        };
    });
}

/**
 * Append new fields to a table, details or list view without the caller ever handling
 * its raw `columns`.
 *
 * Knack's view PUT replaces the whole view, and knack_update_view's guard only merges
 * top level: a patch's `columns` replaces the array wholesale rather than adding to it
 * (see buildEffectiveUpdateBody in lib/view-safety.ts). So adding to a table with 61
 * columns needs a request carrying all 64 - column 1 through 61 unchanged, byte for
 * byte, plus the 3 new ones. Building that by hand means reading the existing 61 back
 * in their exact raw shape first, which only knack_get_view's diagnostic-gated
 * `attributes` mode returns; the ungated `fields` mode hands back a summary (key,
 * type, label, rules, defaults) that is not reconstructable into Knack's raw column
 * objects without risking the loss of whatever the summary does not carry.
 *
 * This tool removes the need for that read: it takes the existing `columns` off the
 * same fresh metadata fetch runViewMutationTool's guard already makes for any update,
 * appends the caller's new fields to it, and sends that. No raw JSON reaches the
 * caller and allowDiagnostics plays no part.
 *
 * A table's `columns` is a flat array of column objects; a details or list view nests
 * its fields several levels down (columns[].groups[].columns[][]) — a run of
 * width-blocks, each with groups, each group with sub-columns, each sub-column a bare
 * array of field items. Both shapes are handled; a new field is appended to the end of
 * the last sub-column in the last group of the last width-block by default, or spliced
 * next to an anchor field when one is given. A form's fields live under
 * groups[].columns[].inputs instead — a third shape this tool does not build, so a
 * form view is refused rather than risk silently mishandling a shape not measured
 * here; likewise search, whose raw column shape nothing in this codebase has measured
 * yet.
 */
export const addViewColumns = defineTool({
    name: 'knack_add_view_columns',
    description:
        "Append new fields to a table, details or list view's existing columns; reads the live columns itself so nothing else on the view is touched.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        viewKey: z.string(),
        fieldKeys: z
            .array(z.string().regex(FIELD_KEY_PATTERN_CASE_SENSITIVE))
            .min(1)
            .describe(
                'Field keys to add as new columns, in order, e.g. ["field_10"] — keys, not field names: a column naming something that is not a field is stored and shows nothing',
            ),
        columnConnections: z
            .string()
            .optional()
            .describe(
                'JSON { "field_10": "field_3" }: new field key to connection field',
            ),
        insertAfterFieldKey: z
            .string()
            .optional()
            .describe(
                'Place the new columns directly after this existing column',
            ),
        insertBeforeFieldKey: z
            .string()
            .optional()
            .describe(
                'As insertAfterFieldKey, but before. Default: append at the end',
            ),
        previewOnly: z.boolean().optional().describe(PREVIEW_DESCRIPTION),
    },
    handler: async (
        {
            appKey,
            sceneKey,
            viewKey,
            fieldKeys,
            columnConnections,
            insertAfterFieldKey,
            insertBeforeFieldKey,
            previewOnly,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        // Every refusal below shares this shape; naming it once keeps the branching
        // that follows (table vs. details/list) from drifting into two different
        // refusal formats for the same error.
        const refuse = (error: string, message: string) =>
            makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'add_view_columns',
                sceneKey,
                viewKey,
                error,
                message,
            });

        if (insertAfterFieldKey && insertBeforeFieldKey) {
            return refuse(
                'CONFLICTING_PLACEMENT',
                'insertAfterFieldKey and insertBeforeFieldKey both name a position for the new column(s). Pass one, or neither to add them at the end. Nothing was sent.',
            );
        }

        const seenFieldKeys = new Set<string>();
        const repeatedFieldKeys = fieldKeys.filter((key) =>
            seenFieldKeys.has(key) ? true : (seenFieldKeys.add(key), false),
        );
        if (repeatedFieldKeys.length > 0) {
            return refuse(
                'DUPLICATE_FIELD_KEY',
                `fieldKeys repeats ${[...new Set(repeatedFieldKeys)].join(', ')} — each would add a second, identical column. List each field key once. Nothing was sent.`,
            );
        }

        let parsedColumnConnections: Record<string, string> | undefined;
        if (columnConnections) {
            const raw = parseJsonInput<unknown>(
                'columnConnections',
                columnConnections,
            );
            if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
                throw new Error(
                    'columnConnections must be a JSON object mapping a field key to a connection field key, e.g. { "field_10": "field_3" }.',
                );
            }
            for (const [fieldKey, connection] of Object.entries(raw)) {
                // This key is looked up later as `parsedColumnConnections[field.key]` —
                // a plain-object property lookup against the always-lower-case key from
                // fieldKeys — so it must pass the same case-sensitive check as fieldKeys.
                if (
                    typeof connection !== 'string' ||
                    !FIELD_KEY_PATTERN_CASE_SENSITIVE.test(connection)
                ) {
                    throw new Error(
                        `columnConnections["${fieldKey}"] must be a connection field key like "field_3", not ${JSON.stringify(connection)}.`,
                    );
                }
                if (!FIELD_KEY_PATTERN_CASE_SENSITIVE.test(fieldKey)) {
                    throw new Error(
                        `columnConnections key "${fieldKey}" must be a field key like "field_10" — it names the new column the connection applies to.`,
                    );
                }
            }
            parsedColumnConnections = raw as Record<string, string>;
        }

        // Read fresh: this becomes both the source of the existing columns below and,
        // passed through to runViewMutationTool, what the guard merges the patch into
        // — one read, one instant, so the columns appended to match the columns that
        // guard judges.
        ctx.caches.runtimeMetadata.delete(app.appKey);
        const metadata = await ctx.getRuntimeMetadata(app);
        if (!metadata) {
            return refuse(
                'COULD_NOT_VERIFY_VIEW',
                'Runtime metadata could not be fetched from Knack, so the view could not be read. Nothing was sent.',
            );
        }

        const rawView = findRawViewInMetadata(metadata, sceneKey, viewKey);
        const attributes = rawView ? resolveViewAttributes(rawView) : null;
        if (!attributes) {
            return refuse(
                'VIEW_NOT_FOUND',
                `${viewKey} was not found in ${sceneKey} in this app's metadata. Nothing was sent.`,
            );
        }

        const viewType = getViewType(attributes);
        const isTable = viewType === 'table';
        const isNested =
            viewType !== null && NESTED_COLUMN_VIEW_TYPES.has(viewType);
        if (!isTable && !isNested) {
            return refuse(
                'UNSUPPORTED_VIEW_TYPE',
                `knack_add_view_columns only supports table, details and list views. ${viewKey} is a "${viewType ?? 'unknown'}" view. A form's fields live under groups[].columns[].inputs instead, and search's raw column shape is unmeasured here — both are different shapes this tool does not build. Use knack_update_view with a hand-built patch for those. Nothing was sent.`,
            );
        }

        const existingColumns = Array.isArray(attributes.columns)
            ? attributes.columns
            : [];
        const nestedFieldLocations = isNested
            ? walkNestedFields(existingColumns)
            : [];
        const existingFieldKeys = new Set(
            isTable
                ? existingColumns
                      .map((column) => columnFieldKey(column))
                      .filter((key): key is string => key !== null)
                : nestedFieldLocations.map((location) => location.key),
        );
        const duplicates = fieldKeys.filter((key) =>
            existingFieldKeys.has(key),
        );
        if (duplicates.length > 0) {
            return refuse(
                'FIELD_ALREADY_A_COLUMN',
                `${duplicates.join(', ')} already have a column on this view. knack_add_view_columns only adds new columns — remove the already-present key(s) from fieldKeys, or use knack_update_view to change an existing one. Nothing was sent.`,
            );
        }

        const objectKey = asRecord(attributes.source)?.object;
        const { schema } = await ctx.getSchema(app);
        const sourceObject =
            typeof objectKey === 'string'
                ? schema?.objects?.find((object) => object.key === objectKey)
                : undefined;
        const allObjectFields = sourceObject?.fields || [];

        const fieldDescriptors = buildTemplateFieldDescriptors(
            fieldKeys,
            allObjectFields,
            fieldKeys.length,
        );
        const namedFromSchema = fieldDescriptors.filter(
            (field) => field.name !== field.key,
        ).length;

        const newItems = fieldDescriptors.map((field) => {
            const descriptor = parsedColumnConnections?.[field.key]
                ? {
                      ...field,
                      connectionKey: parsedColumnConnections[field.key],
                  }
                : field;
            return isTable
                ? buildViewFieldColumn(descriptor)
                : buildViewGroupField(descriptor);
        });

        const anchorKey = insertAfterFieldKey ?? insertBeforeFieldKey;
        let finalColumns: unknown[];
        let columnCountBefore: number;
        let columnCountAfter: number;

        if (isTable) {
            let anchorIndex: number | null = null;
            if (anchorKey) {
                anchorIndex = existingColumns.findIndex(
                    (column) => columnFieldKey(column) === anchorKey,
                );
                if (anchorIndex === -1) {
                    return refuse(
                        'ANCHOR_NOT_FOUND',
                        `${anchorKey} is not an existing column on ${viewKey}, so the new column(s) cannot be placed ${insertAfterFieldKey ? 'after' : 'before'} it. Nothing was sent.`,
                    );
                }
            }

            finalColumns =
                anchorIndex === null
                    ? [...existingColumns, ...newItems]
                    : insertAfterFieldKey
                      ? [
                            ...existingColumns.slice(0, anchorIndex + 1),
                            ...newItems,
                            ...existingColumns.slice(anchorIndex + 1),
                        ]
                      : [
                            ...existingColumns.slice(0, anchorIndex),
                            ...newItems,
                            ...existingColumns.slice(anchorIndex),
                        ];
            columnCountBefore = existingColumns.length;
            columnCountAfter = finalColumns.length;
        } else {
            let location: {
                blockIndex: number;
                groupIndex: number;
                subColumnIndex: number;
            };
            let insertIndex: number;

            if (anchorKey) {
                const found = nestedFieldLocations.find(
                    (candidate) => candidate.key === anchorKey,
                );
                if (!found) {
                    return refuse(
                        'ANCHOR_NOT_FOUND',
                        `${anchorKey} is not an existing column on ${viewKey}, so the new column(s) cannot be placed ${insertAfterFieldKey ? 'after' : 'before'} it. Nothing was sent.`,
                    );
                }
                location = found;
                insertIndex = insertAfterFieldKey
                    ? found.itemIndex + 1
                    : found.itemIndex;
            } else {
                const appendAt = findNestedAppendLocation(existingColumns);
                if (!appendAt.ok) {
                    return refuse(
                        appendAt.code,
                        `${appendAt.message} Nothing was sent.`,
                    );
                }
                location = appendAt;
                insertIndex = appendAt.insertIndex;
            }

            finalColumns = spliceNestedFields(
                existingColumns,
                location,
                insertIndex,
                newItems,
            );
            columnCountBefore = nestedFieldLocations.length;
            columnCountAfter = columnCountBefore + newItems.length;
        }

        const updates = JSON.stringify({ columns: finalColumns });

        const outcome = await runViewMutationTool(
            ctx,
            app,
            {
                action: 'update_view',
                sceneKey,
                viewKey,
                updates,
                previewOnly,
            },
            async ({ outgoingBody }) =>
                ctx.request(app, `/scenes/${sceneKey}/views/${viewKey}`, {
                    method: 'PUT',
                    body: outgoingBody ? JSON.stringify(outgoingBody) : updates,
                }),
            // Already fetched fresh above; avoids re-fetching the whole application
            // payload a second time for the guard's own preflight.
            { metadata },
        );

        return makeTextResponse({
            ...outcome,
            // The guard reports its own request as `update_view` — this tool's
            // identity wins, as it does for copyView's sharePages path.
            action: 'add_view_columns',
            addedFieldKeys: fieldDescriptors.map((field) => field.key),
            columnCountBefore,
            columnCountAfter,
            ...(allObjectFields.length === 0
                ? {
                      note: `No schema fields were available for ${objectKey ?? 'this object'}, so the new column header(s) fall back to the field key.`,
                  }
                : namedFromSchema < fieldDescriptors.length
                  ? {
                        note: `${fieldDescriptors.length - namedFromSchema} of ${fieldDescriptors.length} new field(s) were not found in the object's schema, so those column headers fall back to the field key.`,
                    }
                  : {}),
        });
    },
});

/**
 * Append action-link column(s) to a table, details or list view's existing columns.
 *
 * An action link is not a field — it carries `link_text` and its own `action_rules[]`
 * (each with `record_rules`/`submit_rules`), not a `field` key — so knack_add_view_columns
 * cannot place one: it only builds field descriptors from a schema lookup. Hand-building
 * the patch with knack_update_view hits the same clobbering problem knack_add_view_columns
 * was built to avoid: the guard merges `columns` only at the top level, so a patch touching
 * it replaces the whole array, and getting the exact existing shape first meant
 * knack_get_view's diagnostic-gated `attributes` mode — the one thing allowViewMutation
 * alone was supposed to be enough for.
 *
 * This tool takes the caller's action-link object(s) as JSON — Knack's shape for one is
 * the caller's to build (from an existing action link, or knack_get_view_payload_template),
 * not this tool's to guess — and does only what knack_add_view_columns already does for
 * fields: read the live `columns` off the same fresh metadata fetch the mutation guard
 * makes, splice the new item(s) in without touching anything else, and send the merged
 * result through the normal guarded update path. No raw JSON needs to reach the caller,
 * so allowViewMutation alone is enough.
 *
 * Reuses knack_add_view_columns's walker/splice helpers for the nested details/list shape
 * (columns[].groups[].columns[][]) — table's `columns` is flat. An anchor names an
 * existing *field* column (action links carry no key of their own to anchor by); a form's
 * action links live under groups[].columns[].inputs instead, a shape this tool does not
 * build, so a form view is refused rather than guessed at.
 */
export const addActionLink = defineTool({
    name: 'knack_add_action_link',
    description:
        "Append action-link column(s) to a table, details or list view's existing columns; reads the live columns itself so nothing else on the view is touched.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        viewKey: z.string(),
        actionLinks: z
            .string()
            .describe(
                'JSON array of action-link column objects to add, e.g. [{"link_text":"Approve","action_rules":[{"link_text":"Approve","record_rules":[],"submit_rules":[{"action":"message","message":"Approved"}]}]}] — "type":"action_link" is set automatically if omitted',
            ),
        insertAfterFieldKey: z
            .string()
            .optional()
            .describe(
                'Place the new action link(s) directly after this existing field column',
            ),
        insertBeforeFieldKey: z
            .string()
            .optional()
            .describe(
                'As insertAfterFieldKey, but before. Default: append at the end',
            ),
        previewOnly: z.boolean().optional().describe(PREVIEW_DESCRIPTION),
    },
    handler: async (
        {
            appKey,
            sceneKey,
            viewKey,
            actionLinks,
            insertAfterFieldKey,
            insertBeforeFieldKey,
            previewOnly,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        const refuse = (error: string, message: string) =>
            makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'add_action_link',
                sceneKey,
                viewKey,
                error,
                message,
            });

        if (insertAfterFieldKey && insertBeforeFieldKey) {
            return refuse(
                'CONFLICTING_PLACEMENT',
                'insertAfterFieldKey and insertBeforeFieldKey both name a position for the new action link(s). Pass one, or neither to add them at the end. Nothing was sent.',
            );
        }

        const parsedActionLinks = parseJsonInput<unknown>(
            'actionLinks',
            actionLinks,
        );
        if (
            !Array.isArray(parsedActionLinks) ||
            parsedActionLinks.length === 0
        ) {
            throw new Error(
                'actionLinks must be a non-empty JSON array of action-link objects.',
            );
        }
        const newItems = parsedActionLinks.map((entry, index) => {
            const record = asRecord(entry);
            if (!record) {
                throw new Error(
                    `actionLinks[${index}] must be a JSON object, not ${JSON.stringify(entry)}.`,
                );
            }
            return { type: 'action_link', ...record };
        });

        // Read fresh: the source of the existing columns below, and — passed through to
        // runViewMutationTool — what the guard merges the patch into, so the columns
        // spliced into match the columns that guard judges.
        ctx.caches.runtimeMetadata.delete(app.appKey);
        const metadata = await ctx.getRuntimeMetadata(app);
        if (!metadata) {
            return refuse(
                'COULD_NOT_VERIFY_VIEW',
                'Runtime metadata could not be fetched from Knack, so the view could not be read. Nothing was sent.',
            );
        }

        const rawView = findRawViewInMetadata(metadata, sceneKey, viewKey);
        const attributes = rawView ? resolveViewAttributes(rawView) : null;
        if (!attributes) {
            return refuse(
                'VIEW_NOT_FOUND',
                `${viewKey} was not found in ${sceneKey} in this app's metadata. Nothing was sent.`,
            );
        }

        const viewType = getViewType(attributes);
        const isTable = viewType === 'table';
        const isNested =
            viewType !== null && NESTED_COLUMN_VIEW_TYPES.has(viewType);
        if (!isTable && !isNested) {
            return refuse(
                'UNSUPPORTED_VIEW_TYPE',
                `knack_add_action_link only supports table, details and list views. ${viewKey} is a "${viewType ?? 'unknown'}" view. A form's action links live under groups[].columns[].inputs instead — a different shape this tool does not build. Use knack_update_view with a hand-built patch for those. Nothing was sent.`,
            );
        }

        const existingColumns = Array.isArray(attributes.columns)
            ? attributes.columns
            : [];

        const anchorKey = insertAfterFieldKey ?? insertBeforeFieldKey;
        let finalColumns: unknown[];
        let columnCountBefore: number | undefined;
        let columnCountAfter: number | undefined;

        if (isTable) {
            let anchorIndex: number | null = null;
            if (anchorKey) {
                anchorIndex = existingColumns.findIndex(
                    (column) => columnFieldKey(column) === anchorKey,
                );
                if (anchorIndex === -1) {
                    return refuse(
                        'ANCHOR_NOT_FOUND',
                        `${anchorKey} is not an existing field column on ${viewKey}, so the new action link(s) cannot be placed ${insertAfterFieldKey ? 'after' : 'before'} it. Nothing was sent.`,
                    );
                }
            }

            finalColumns =
                anchorIndex === null
                    ? [...existingColumns, ...newItems]
                    : insertAfterFieldKey
                      ? [
                            ...existingColumns.slice(0, anchorIndex + 1),
                            ...newItems,
                            ...existingColumns.slice(anchorIndex + 1),
                        ]
                      : [
                            ...existingColumns.slice(0, anchorIndex),
                            ...newItems,
                            ...existingColumns.slice(anchorIndex),
                        ];
            columnCountBefore = existingColumns.length;
            columnCountAfter = finalColumns.length;
        } else {
            const nestedFieldLocations = walkNestedFields(existingColumns);
            let location: {
                blockIndex: number;
                groupIndex: number;
                subColumnIndex: number;
            };
            let insertIndex: number;

            if (anchorKey) {
                const found = nestedFieldLocations.find(
                    (candidate) => candidate.key === anchorKey,
                );
                if (!found) {
                    return refuse(
                        'ANCHOR_NOT_FOUND',
                        `${anchorKey} is not an existing field column on ${viewKey}, so the new action link(s) cannot be placed ${insertAfterFieldKey ? 'after' : 'before'} it. Nothing was sent.`,
                    );
                }
                location = found;
                insertIndex = insertAfterFieldKey
                    ? found.itemIndex + 1
                    : found.itemIndex;
            } else {
                const appendAt = findNestedAppendLocation(existingColumns);
                if (!appendAt.ok) {
                    return refuse(
                        appendAt.code,
                        `${appendAt.message} Nothing was sent.`,
                    );
                }
                location = appendAt;
                insertIndex = appendAt.insertIndex;
            }

            finalColumns = spliceNestedFields(
                existingColumns,
                location,
                insertIndex,
                newItems,
            );
            // Field locations, not action links (which carry no `key` walkNestedFields
            // can count) — reporting a before/after here would claim a fact about the
            // items just added rather than the ones that were already there.
        }

        const updates = JSON.stringify({ columns: finalColumns });

        const outcome = await runViewMutationTool(
            ctx,
            app,
            {
                action: 'update_view',
                sceneKey,
                viewKey,
                updates,
                previewOnly,
            },
            async ({ outgoingBody }) =>
                ctx.request(app, `/scenes/${sceneKey}/views/${viewKey}`, {
                    method: 'PUT',
                    body: outgoingBody ? JSON.stringify(outgoingBody) : updates,
                }),
            // Already fetched fresh above; avoids re-fetching the whole application
            // payload a second time for the guard's own preflight.
            { metadata },
        );

        return makeTextResponse({
            ...outcome,
            // The guard reports its own request as `update_view` — this tool's identity
            // wins, as it does for knack_add_view_columns.
            action: 'add_action_link',
            addedCount: newItems.length,
            ...(columnCountBefore !== undefined
                ? { columnCountBefore, columnCountAfter }
                : {}),
        });
    },
});

/**
 * Append new record and/or submit rules to a view's `rules` object without disturbing
 * anything else stored there.
 *
 * knack_update_view's guard merges a patch into the live view definition only at the top
 * level (buildEffectiveUpdateBody in lib/view-safety.ts), so a patch touching `rules`
 * replaces the whole object. A form keeps both its record rules (`rules.records`) and its
 * submit rules (`rules.submits`) there; adding one record rule without first reading the
 * exact existing `rules` verbatim would silently delete the other array and every rule
 * already configured — which meant knack_get_view's diagnostic-gated `attributes` mode,
 * the one thing allowViewMutation alone was supposed to be enough for.
 *
 * This tool reads the live `rules` off the same fresh metadata fetch the mutation guard
 * already makes, appends the caller's new rule(s) to whichever array they named, and
 * leaves every other key of `rules` — and the rest of the view — untouched. The rule
 * objects themselves are supplied by the caller as JSON rather than built here: their
 * internal criteria/values shape is Knack's to define, and this tool's only job is not to
 * clobber what already exists. Not exclusive to forms — `rules` is a generic top-level
 * key, so this works on any view type that carries one.
 */
export const addViewRules = defineTool({
    name: 'knack_add_view_rules',
    description:
        "Append record and/or submit rules to a view's existing rules; reads the live rules itself so nothing else on the view is touched.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        viewKey: z.string(),
        recordRules: z
            .string()
            .optional()
            .describe(
                'JSON array of record rule objects to append to rules.records, e.g. [{"criteria":[{"field":"field_1","operator":"is","value":"x"}],"values":[{"field":"field_2","type":"value","value":"y"}]}]',
            ),
        submitRules: z
            .string()
            .optional()
            .describe(
                'JSON array of submit rule objects to append to rules.submits, e.g. [{"criteria":[],"action":"message","message":"Saved"}]',
            ),
        previewOnly: z.boolean().optional().describe(PREVIEW_DESCRIPTION),
    },
    handler: async (
        { appKey, sceneKey, viewKey, recordRules, submitRules, previewOnly },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        const refuse = (error: string, message: string) =>
            makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'add_view_rules',
                sceneKey,
                viewKey,
                error,
                message,
            });

        if (!recordRules && !submitRules) {
            return refuse(
                'NOTHING_TO_ADD',
                'Pass recordRules and/or submitRules. Nothing was sent.',
            );
        }

        const parseRuleArray = (
            label: string,
            json: string,
        ): Record<string, unknown>[] => {
            const parsed = parseJsonInput<unknown>(label, json);
            if (!Array.isArray(parsed) || parsed.length === 0) {
                throw new Error(
                    `${label} must be a non-empty JSON array of rule objects.`,
                );
            }
            return parsed.map((entry, index) => {
                const record = asRecord(entry);
                if (!record) {
                    throw new Error(
                        `${label}[${index}] must be a JSON object, not ${JSON.stringify(entry)}.`,
                    );
                }
                return record;
            });
        };

        const parsedRecordRules = recordRules
            ? parseRuleArray('recordRules', recordRules)
            : undefined;
        const parsedSubmitRules = submitRules
            ? parseRuleArray('submitRules', submitRules)
            : undefined;

        // Read fresh, for the same reason knack_add_view_columns and knack_add_action_link
        // do: this becomes both the source of the existing rules below and, passed through
        // to runViewMutationTool, what the guard merges the patch into.
        ctx.caches.runtimeMetadata.delete(app.appKey);
        const metadata = await ctx.getRuntimeMetadata(app);
        if (!metadata) {
            return refuse(
                'COULD_NOT_VERIFY_VIEW',
                'Runtime metadata could not be fetched from Knack, so the view could not be read. Nothing was sent.',
            );
        }

        const rawView = findRawViewInMetadata(metadata, sceneKey, viewKey);
        const attributes = rawView ? resolveViewAttributes(rawView) : null;
        if (!attributes) {
            return refuse(
                'VIEW_NOT_FOUND',
                `${viewKey} was not found in ${sceneKey} in this app's metadata. Nothing was sent.`,
            );
        }

        const existingRules = asRecord(attributes.rules) ?? {};
        const existingRecordRules = Array.isArray(existingRules.records)
            ? existingRules.records
            : [];
        const existingSubmitRules = Array.isArray(existingRules.submits)
            ? existingRules.submits
            : [];

        const mergedRules: Record<string, unknown> = { ...existingRules };
        if (parsedRecordRules) {
            mergedRules.records = [
                ...existingRecordRules,
                ...parsedRecordRules,
            ];
        }
        if (parsedSubmitRules) {
            mergedRules.submits = [
                ...existingSubmitRules,
                ...parsedSubmitRules,
            ];
        }

        const updates = JSON.stringify({ rules: mergedRules });

        const outcome = await runViewMutationTool(
            ctx,
            app,
            {
                action: 'update_view',
                sceneKey,
                viewKey,
                updates,
                previewOnly,
            },
            async ({ outgoingBody }) =>
                ctx.request(app, `/scenes/${sceneKey}/views/${viewKey}`, {
                    method: 'PUT',
                    body: outgoingBody ? JSON.stringify(outgoingBody) : updates,
                }),
            { metadata },
        );

        return makeTextResponse({
            ...outcome,
            action: 'add_view_rules',
            ...(parsedRecordRules
                ? {
                      recordRulesAdded: parsedRecordRules.length,
                      recordRuleCountBefore: existingRecordRules.length,
                      recordRuleCountAfter:
                          existingRecordRules.length + parsedRecordRules.length,
                  }
                : {}),
            ...(parsedSubmitRules
                ? {
                      submitRulesAdded: parsedSubmitRules.length,
                      submitRuleCountBefore: existingSubmitRules.length,
                      submitRuleCountAfter:
                          existingSubmitRules.length + parsedSubmitRules.length,
                  }
                : {}),
        });
    },
});

/**
 * Append new entries to a view's top-level `links[]` — a menu view's nav entries, or the
 * extra link buttons Knack allows on other view types — without disturbing the ones
 * already there.
 *
 * The same clobbering problem as `rules` and `columns`: knack_update_view's guard merges
 * a patch into the live view definition only at the top level, so a patch touching
 * `links` replaces the whole array. `updateView.handler`'s own test for a menu view has
 * always had to spread `...MENU_VIEW.links` by hand to add one entry without dropping
 * the rest — which only works because the test fixture is known verbatim; a real menu
 * with entries this server has never seen needed knack_get_view's diagnostic-gated
 * `attributes` mode to get that same certainty, the one thing allowViewMutation alone
 * was supposed to be enough for.
 *
 * This tool reads the live `links` off the same fresh metadata fetch the mutation guard
 * already makes, appends the caller's new entries to it, and sends the merged result
 * through the normal guarded update path — the same shape this file already uses for
 * `columns` and `rules`. The link objects themselves are supplied by the caller as JSON
 * rather than built here, same reasoning as knack_add_action_link: their shape is
 * Knack's to define, not this tool's to guess.
 */
export const addViewLinks = defineTool({
    name: 'knack_add_view_links',
    description:
        "Append new entries to a view's existing top-level links (a menu view's nav entries, or another view type's link buttons); reads the live links itself so nothing else on the view is touched.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        viewKey: z.string(),
        links: z
            .string()
            .describe(
                'JSON array of link objects to append, e.g. [{"name":"Reports","type":"scene","scene":"reports"}]',
            ),
        insertAtIndex: z
            .number()
            .int()
            .min(0)
            .optional()
            .describe(
                'Insert at this 0-based position among the existing links. Default: append at the end',
            ),
        previewOnly: z.boolean().optional().describe(PREVIEW_DESCRIPTION),
    },
    handler: async (
        { appKey, sceneKey, viewKey, links, insertAtIndex, previewOnly },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        const refuse = (error: string, message: string) =>
            makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'add_view_links',
                sceneKey,
                viewKey,
                error,
                message,
            });

        const parsedLinks = parseJsonInput<unknown>('links', links);
        if (!Array.isArray(parsedLinks) || parsedLinks.length === 0) {
            throw new Error(
                'links must be a non-empty JSON array of link objects.',
            );
        }
        const newItems = parsedLinks.map((entry, index) => {
            const record = asRecord(entry);
            if (!record) {
                throw new Error(
                    `links[${index}] must be a JSON object, not ${JSON.stringify(entry)}.`,
                );
            }
            return record;
        });

        // Read fresh, for the same reason the other add_* tools in this file do: this
        // becomes both the source of the existing links below and, passed through to
        // runViewMutationTool, what the guard merges the patch into.
        ctx.caches.runtimeMetadata.delete(app.appKey);
        const metadata = await ctx.getRuntimeMetadata(app);
        if (!metadata) {
            return refuse(
                'COULD_NOT_VERIFY_VIEW',
                'Runtime metadata could not be fetched from Knack, so the view could not be read. Nothing was sent.',
            );
        }

        const rawView = findRawViewInMetadata(metadata, sceneKey, viewKey);
        const attributes = rawView ? resolveViewAttributes(rawView) : null;
        if (!attributes) {
            return refuse(
                'VIEW_NOT_FOUND',
                `${viewKey} was not found in ${sceneKey} in this app's metadata. Nothing was sent.`,
            );
        }

        const existingLinks = Array.isArray(attributes.links)
            ? attributes.links
            : [];
        if (
            insertAtIndex !== undefined &&
            insertAtIndex > existingLinks.length
        ) {
            return refuse(
                'INDEX_OUT_OF_RANGE',
                `insertAtIndex ${insertAtIndex} is past the end of the existing ${existingLinks.length} link(s). Omit it to append at the end. Nothing was sent.`,
            );
        }

        const finalLinks =
            insertAtIndex === undefined
                ? [...existingLinks, ...newItems]
                : [
                      ...existingLinks.slice(0, insertAtIndex),
                      ...newItems,
                      ...existingLinks.slice(insertAtIndex),
                  ];

        const updates = JSON.stringify({ links: finalLinks });

        const outcome = await runViewMutationTool(
            ctx,
            app,
            {
                action: 'update_view',
                sceneKey,
                viewKey,
                updates,
                previewOnly,
            },
            async ({ outgoingBody }) =>
                ctx.request(app, `/scenes/${sceneKey}/views/${viewKey}`, {
                    method: 'PUT',
                    body: outgoingBody ? JSON.stringify(outgoingBody) : updates,
                }),
            { metadata },
        );

        return makeTextResponse({
            ...outcome,
            action: 'add_view_links',
            addedCount: newItems.length,
            linkCountBefore: existingLinks.length,
            linkCountAfter: existingLinks.length + newItems.length,
        });
    },
});

/**
 * One tool for the two legacy copies. `sharePages: false` is Knack's own copyview
 * endpoint (legacy knack_copy_view), which duplicates a table's owned child pages.
 * `sharePages: true` creates the copy from the source's definition (legacy
 * knack_copy_view_sharing_pages) so its link columns keep pointing at the original
 * pages, and checks Knack's response for exactly that.
 */
export const copyView = defineTool({
    name: 'knack_copy_view',
    description:
        "Copy a view to another scene, via Knack's copy or sharing its child pages.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        viewKey: z.string(),
        targetSceneKey: z.string(),
        sourceSceneKey: z
            .string()
            .optional()
            .describe(
                'Scene owning the view; derived only when sharePages is true',
            ),
        sharePages: z
            .boolean()
            .default(false)
            .describe(
                'Create from the source definition so link columns keep their pages',
            ),
        completeViewSchema: z
            .boolean()
            .default(false)
            .describe('Knack copyView flag; plain copy only'),
        name: z
            .string()
            .optional()
            .describe('sharePages only; defaults to "<name> Copy"'),
        title: z
            .string()
            .optional()
            .describe('sharePages only; source title kept if omitted'),
        existingViewKeys: z
            .array(z.string())
            .optional()
            .describe(
                'sharePages only: views already on the target page, in order. Omit it and the layout the page already has is kept',
            ),
        insertAfterViewKey: z
            .string()
            .optional()
            .describe(
                'sharePages only: place the copy directly after this view. It joins the stack when this view shares its column, otherwise it gets a row of its own next to that row. Default: the end of the page',
            ),
        insertBeforeViewKey: z
            .string()
            .optional()
            .describe('sharePages only: as insertAfterViewKey, but before'),
    },
    handler: async (args, ctx) => {
        const app = ctx.getApp(args.appKey);
        ctx.getApiKey(app.appKey);
        const { viewKey, targetSceneKey, sourceSceneKey } = args;

        if (!args.sharePages) {
            if (!sourceSceneKey) {
                throw new Error(
                    'sourceSceneKey is required when sharePages is false.',
                );
            }

            // The guard resolves the source view's live definition and hands it to
            // the perform callback. Captured here rather than read off the outcome,
            // which does not carry it — a cast made that compile and would have made
            // the ownership report silently empty at runtime.
            let sourceAttributes: Record<string, unknown> | null = null;
            const outcome = await runViewMutationTool(
                ctx,
                app,
                { action: 'copy_view', sceneKey: sourceSceneKey, viewKey },
                ({ currentAttributes }) => {
                    sourceAttributes = currentAttributes;
                    return ctx.request(
                        app,
                        `/scenes/${sourceSceneKey}/copyview`,
                        {
                            method: 'POST',
                            body: JSON.stringify({
                                action: 'copy',
                                target_scene_key: targetSceneKey,
                                view_key: viewKey,
                                completeViewSchema: args.completeViewSchema,
                            }),
                        },
                    );
                },
            );

            // Which linked pages this copy duplicated and which it shared. The link
            // set comes from the source definition; which of the two happened comes
            // from the pages Knack's own response reported creating. Predicting it
            // from the `remote` flag was wrong for every `type: "scene_link"` column,
            // which Knack shares rather than duplicates.
            const linkOwnership = summariseCopyLinkOwnership(
                sourceAttributes,
                readChangedScenes(outcome.body, 'inserts'),
            );

            // Only after the copy actually landed, and only for this plain path.
            // Knack's copyview endpoint adds the new key to every row of the target
            // page's layout, so without this the copy renders once per row. The
            // sharePages path below builds its own layout and never goes near it.
            const layout =
                outcome.ok === true
                    ? await ensureCopiedViewRendersOnce(
                          ctx,
                          app,
                          targetSceneKey,
                          insertedViewKeysFromOutcome(outcome),
                      )
                    : {};

            return makeTextResponse({
                // `sceneKey` is what the guard reports, but this tool has always named
                // its two scenes explicitly. Keep both so a caller written against the
                // old response shape still finds sourceSceneKey.
                sourceSceneKey,
                targetSceneKey,
                ...outcome,
                ...layout,
                ...(linkOwnership.length > 0
                    ? {
                          copyLinkOwnership: linkOwnership,
                          copyLinkNote: describeCopyLinkOutcome(linkOwnership),
                      }
                    : {}),
            });
        }

        const sourceViewKey = viewKey;
        const {
            name,
            title,
            existingViewKeys,
            insertAfterViewKey,
            insertBeforeViewKey,
        } = args;

        if (insertAfterViewKey && insertBeforeViewKey) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'copy_view_sharing_pages',
                sourceViewKey: viewKey,
                error: 'CONFLICTING_PLACEMENT',
                message:
                    'insertAfterViewKey and insertBeforeViewKey both name a position for the copy. Pass one, or neither to add it at the end. Nothing was sent.',
            });
        }
        const placement: NewViewPlacement = insertAfterViewKey
            ? { at: 'after', viewKey: insertAfterViewKey }
            : insertBeforeViewKey
              ? { at: 'before', viewKey: insertBeforeViewKey }
              : { at: 'end' };

        // Read the source fresh. The payload posted is its stored definition, and a
        // definition up to five minutes old is not the one being copied.
        ctx.caches.runtimeMetadata.delete(app.appKey);
        const metadata = await ctx.getRuntimeMetadata(app);
        if (!metadata) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'copy_view_sharing_pages',
                sourceViewKey,
                error: 'COULD_NOT_VERIFY_VIEW',
                message:
                    'Runtime metadata could not be fetched from Knack, so the source view could not be read. Nothing was sent.',
            });
        }

        const scenes = parseRuntimeScenes(metadata);
        const resolvedSourceSceneKey =
            sourceSceneKey ??
            scenes.find((scene) =>
                scene.views.some((view) => view.viewKey === sourceViewKey),
            )?.sceneKey;
        const rawView = resolvedSourceSceneKey
            ? findRawViewInMetadata(
                  metadata,
                  resolvedSourceSceneKey,
                  sourceViewKey,
              )
            : null;
        const sourceAttributes = rawView
            ? resolveViewAttributes(rawView)
            : null;
        if (!resolvedSourceSceneKey || !sourceAttributes) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'copy_view_sharing_pages',
                sourceViewKey,
                error: 'VIEW_NOT_FOUND',
                message: `${sourceViewKey} was not found${sourceSceneKey ? ` in ${sourceSceneKey}` : ''} in this app's metadata. Nothing was sent.`,
            });
        }

        const plan = planSharedPageCopy(sourceAttributes, { name, title });
        if (!plan.ok) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'copy_view_sharing_pages',
                sourceSceneKey: resolvedSourceSceneKey,
                sourceViewKey,
                error: plan.code,
                message: plan.message,
            });
        }

        const sceneViewKeys = getSceneViewKeys(scenes, targetSceneKey);
        const layoutWarning = existingViewKeys
            ? describeLayoutKeyGap(existingViewKeys, sceneViewKeys)
            : null;
        // An explicit list is the caller stating the layout they want, so it is still
        // built verbatim. Derived, it is not a layout at all: `scene.views` is creation
        // order, and rebuilding from it restacked a real page on 2026-09-11. The page's
        // own stored layout is preserved instead.
        //
        // Either way the placement is applied to the rows, never assumed: building the
        // explicit list straight through `buildStarterPageGroups` would pin the copy to
        // the end of the page and report success for a position the caller did not ask
        // for — the same silent-success failure this change exists to remove.
        const layout =
            existingViewKeys && existingViewKeys.length > 0
                ? placeNewViewInLayout(
                      buildStarterLayoutRows(existingViewKeys),
                      placement,
                  )
                : buildPageGroupsPreservingLayout(
                      readSceneGroups(metadata, targetSceneKey),
                      sceneViewKeys,
                      placement,
                  );
        if (!layout.ok) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'copy_view_sharing_pages',
                sourceSceneKey: resolvedSourceSceneKey,
                sourceViewKey,
                targetSceneKey,
                error: layout.code,
                message: `${layout.message} Nothing was sent.`,
            });
        }
        const payload = JSON.stringify({
            ...plan.payload,
            pageGroups: layout.pageGroups,
        });

        const sharedPages = plan.linkedPageRefs.map((ref) => {
            const scene = scenes.find(
                (candidate) =>
                    candidate.sceneKey === ref || candidate.sceneSlug === ref,
            );
            return {
                ref,
                sceneKey: scene?.sceneKey ?? null,
                sceneName: scene?.sceneName ?? null,
            };
        });

        const outcome = await runViewMutationTool(
            ctx,
            app,
            {
                action: 'create_view',
                sceneKey: targetSceneKey,
                updates: payload,
            },
            () =>
                ctx.request(app, `/scenes/${targetSceneKey}/views`, {
                    method: 'POST',
                    body: payload,
                }),
            // Already fetched fresh above to resolve the source view — passing it on
            // avoids re-fetching the whole application payload a second time.
            { metadata },
        );

        // Knack's answer is the only account of whether the pages were shared.
        const verification =
            outcome.ok === true
                ? verifySharedPageCopy(plan.linkedPageRefs, outcome.body)
                : null;

        return makeTextResponse({
            sourceSceneKey: resolvedSourceSceneKey,
            sourceViewKey,
            targetSceneKey,
            sharedPages,
            ...(layoutWarning ? { layoutWarning } : {}),
            ...outcome,
            action: 'copy_view_sharing_pages',
            performedAs: 'create_view',
            ...(plan.ownershipRelease.released.length > 0
                ? { sharedPagesReleased: plan.ownershipRelease.released }
                : {}),
            ...(plan.ownershipRelease.unreleasable.length > 0
                ? {
                      sharedPagesStillOwned: plan.ownershipRelease.unreleasable,
                      sharedPagesOwnershipWarning: `${plan.ownershipRelease.unreleasable.length} page(s) are reached by menu links, which carry no remote flag, so the copy owns them alongside the source. Moving either view would take those pages with it.`,
                  }
                : {}),
            ...(verification
                ? {
                      sharedPagesVerified: verification.verified,
                      ...(verification.problems.length > 0
                          ? {
                                sharedPagesProblems: verification.problems,
                                warning:
                                    'The copy did not come back as a shared-page copy. Read the pages above back before relying on it.',
                            }
                          : {}),
                  }
                : {}),
        });
    },
});

export const moveView = defineTool({
    name: 'knack_move_view',
    description:
        'Move a view to another scene; child pages reached only through it are at risk.',
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sourceSceneKey: z.string(),
        targetSceneKey: z.string(),
        viewKey: z.string(),
        completeViewSchema: z
            .boolean()
            .default(false)
            .describe('Knack moveView flag'),
        insertAfterViewKey: z
            .string()
            .optional()
            .describe(
                'Place the moved view directly after this view on the target page. It joins the stack when this view shares its column, otherwise it gets a row of its own next to that row. Default: the end of the page',
            ),
        insertBeforeViewKey: z
            .string()
            .optional()
            .describe('As insertAfterViewKey, but before'),
        previewOnly: z.boolean().optional().describe(PREVIEW_DESCRIPTION),
    },
    handler: async (
        {
            appKey,
            sourceSceneKey,
            targetSceneKey,
            viewKey,
            completeViewSchema,
            insertAfterViewKey,
            insertBeforeViewKey,
            previewOnly,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        if (insertAfterViewKey && insertBeforeViewKey) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'move_view',
                viewKey,
                error: 'CONFLICTING_PLACEMENT',
                message:
                    'insertAfterViewKey and insertBeforeViewKey both name a position for the moved view. Pass one, or neither to add it at the end. Nothing was sent.',
            });
        }
        const placement: NewViewPlacement = insertAfterViewKey
            ? { at: 'after', viewKey: insertAfterViewKey }
            : insertBeforeViewKey
              ? { at: 'before', viewKey: insertBeforeViewKey }
              : { at: 'end' };

        // The anchor is checked against the target page *before* the move, not after.
        // The layout repair runs once the move has landed, so an anchor rejected there
        // would leave the view moved and unplaced — a worse state than refusing, and
        // one no caller asked for.
        if (placement.at !== 'end') {
            // Read fresh. The cached payload is up to five minutes old, and an anchor
            // removed or moved inside that window would pass here, let the move go, and
            // then fail the post-move repair that reads metadata properly — leaving the
            // view moved and unplaced, which is the state this check exists to prevent.
            ctx.caches.runtimeMetadata.delete(app.appKey);
            const groups = readSceneGroups(
                await ctx.getRuntimeMetadata(app),
                targetSceneKey,
            );
            const trial = placeViewInLayout(groups, viewKey, placement);
            if (groups.length > 0 && !trial.ok) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    action: 'move_view',
                    viewKey,
                    targetSceneKey,
                    error: trial.code,
                    message: `${trial.message} Nothing was sent.`,
                });
            }
            if (groups.length === 0) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    action: 'move_view',
                    viewKey,
                    targetSceneKey,
                    error: 'ANCHOR_NOT_IN_LAYOUT',
                    message: `${targetSceneKey} has no stored layout, so there is no row ${placement.at} ${placement.viewKey} to move ${viewKey} against. Knack renders every view on a page with no layout; omit the anchor to accept that order. Nothing was sent.`,
                });
            }
        }

        // Read before the move. Afterwards Knack has already taken the view out of this
        // page's layout, leaving a row that cannot be told apart from one that arrived
        // empty — so the only moment the residue is identifiable is now.
        ctx.caches.runtimeMetadata.delete(app.appKey);
        const metadataBeforeMove = await ctx.getRuntimeMetadata(app);
        const sourceGroupsBeforeMove = readSceneGroups(
            metadataBeforeMove,
            sourceSceneKey,
        );

        // The pages this view owns, captured now: after the move its links name the
        // rebuilt copies instead, so the originals can no longer be found from it.
        const rawBeforeMove = metadataBeforeMove
            ? findRawViewInMetadata(metadataBeforeMove, sourceSceneKey, viewKey)
            : null;
        const scenesBeforeMove = metadataBeforeMove
            ? parseRuntimeScenes(metadataBeforeMove)
            : [];
        const ownedBeforeMove = rawBeforeMove
            ? [
                  ...new Set(
                      collectLinkTargets(resolveViewAttributes(rawBeforeMove))
                          .linkColumns.filter(
                              (column) =>
                                  column.remote !== true &&
                                  column.childSceneRef !== null,
                          )
                          .map((column) => column.childSceneRef as string),
                  ),
              ]
                  .map(
                      (ref) =>
                          scenesBeforeMove.find(
                              (scene) =>
                                  scene.sceneKey === ref ||
                                  scene.sceneSlug === ref,
                          )?.sceneKey ?? null,
                  )
                  .filter((key): key is string => key !== null)
            : [];

        const outcome = await runViewMutationTool(
            ctx,
            app,
            {
                action: 'move_view',
                sceneKey: sourceSceneKey,
                viewKey,
                previewOnly,
            },
            () =>
                ctx.request(app, `/scenes/${sourceSceneKey}/copyview`, {
                    method: 'POST',
                    body: JSON.stringify({
                        action: 'move',
                        target_scene_key: targetSceneKey,
                        view_key: viewKey,
                        completeViewSchema,
                    }),
                }),
            undefined,
            // So the prompt can say who reaches the replacement pages under the
            // target, not only who reaches the pages being destroyed.
            { targetSceneKey },
        );

        // Only after the move actually landed. A refused or failed move has nothing
        // on the target page to put in its layout, and reading one back would report
        // a repair that never happened.
        const layout =
            outcome.ok === true
                ? await ensureMovedViewIsRendered(
                      ctx,
                      app,
                      targetSceneKey,
                      viewKey,
                      placement,
                  )
                : {};

        // The page it left needs looking at too: Knack takes the view out of that
        // page's layout and leaves the row standing, empty. `ensureMovedViewIsRendered`
        // has already refetched the metadata, so this reads the same fresh copy.
        const sourceLayout =
            outcome.ok === true
                ? await ensureMovedViewLeavesNoResidue(
                      ctx,
                      app,
                      sourceSceneKey,
                      viewKey,
                      sourceGroupsBeforeMove,
                  )
                : {};

        // Knack rebuilds the pages a moved view owns and deletes only some of the
        // originals. The survivors are linked by nothing and flagged by nothing, so
        // they are named here or not at all.
        const orphans =
            outcome.ok === true
                ? await findOrphansLeftByMove(ctx, app, ownedBeforeMove)
                : {};

        return makeTextResponse({
            // `sceneKey` is what the guard reports, but this tool has always named its
            // two scenes explicitly. Keep both so a caller written against the old
            // response shape still finds sourceSceneKey.
            sourceSceneKey,
            targetSceneKey,
            ...outcome,
            ...layout,
            ...sourceLayout,
            ...orphans,
        });
    },
});

export const deleteView = defineTool({
    name: 'knack_delete_view',
    description:
        'Delete a view; child pages reached only through it are destroyed with it.',
    access: 'view-delete',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        viewKey: z.string(),
        previewOnly: z.boolean().optional().describe(PREVIEW_DESCRIPTION),
    },
    handler: async ({ appKey, sceneKey, viewKey, previewOnly }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        return makeTextResponse(
            await runViewMutationTool(
                ctx,
                app,
                { action: 'delete_view', sceneKey, viewKey, previewOnly },
                () =>
                    ctx.request(app, `/scenes/${sceneKey}/views/${viewKey}`, {
                        method: 'DELETE',
                    }),
            ),
        );
    },
});

export const viewMutationTools: AnyToolDef[] = [
    createView,
    updateViewOrder,
    updateView,
    addViewColumns,
    addActionLink,
    addViewRules,
    addViewLinks,
    copyView,
    moveView,
    deleteView,
];
