/**
 * View mutation tools. Every one of them runs through runViewMutationTool, which owns the
 * guard: fresh metadata, the cascade check, the human confirmation, the snapshot and the
 * response shape. Nothing here re-implements any of that; each tool only names the
 * action and performs the Knack request the guard lets through.
 */
import { z } from 'zod';
import type { AppConfig } from '../config.js';
import type { KnackContext } from '../context.js';

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
import { ruleFieldRefusal } from '../lib/field-exclusion.js';
import { asRecord, parseJsonInput, parseJsonObjectArray } from '../lib/util.js';
import {
    applyRuleEdit,
    assignNumericRuleKeys,
    assignSubmitRuleKeys,
    readRuleArray,
} from '../lib/rule-edits.js';
import { deepEqual } from '../lib/structural-diff.js';
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
import { makeTextResponse, toolReplies } from '../response.js';
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
        previewOnly: z.boolean().optional().describe(PREVIEW_DESCRIPTION),
    },
    handler: async ({ appKey, sceneKey, payload, previewOnly }, ctx) => {
        const app = ctx.getApp(appKey);
        // Resolved before the guard runs any I/O: a missing key must refuse here, not
        // after a human has already been prompted or a snapshot written.
        ctx.getApiKey(app.appKey);

        return makeTextResponse(
            await runViewMutationTool(
                ctx,
                app,
                {
                    action: 'create_view',
                    sceneKey,
                    updates: payload,
                    previewOnly,
                },
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
 * The mirror image of `spliceNestedFields`: removes `insertedCount` items starting at
 * `insertIndex` from the exact same leaf position, rebuilding the same width-block,
 * group and sub-column wrappers above it and leaving every other one untouched.
 *
 * Exists only for `assertNestedSpliceIsClean` below — applying this to a splice's own
 * output should always reproduce what went in, and the caller does not need this
 * function for anything else.
 */
function unspliceNestedFields(
    columns: unknown[],
    location: {
        blockIndex: number;
        groupIndex: number;
        subColumnIndex: number;
    },
    insertIndex: number,
    insertedCount: number,
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
                            ...items.slice(insertIndex + insertedCount),
                        ];
                    }),
                };
            }),
        };
    });
}

/**
 * Confirms a flat-array splice (a table's `columns`, or a menu's `links`) touched only
 * the positions it meant to: removing exactly `insertedCount` items starting at
 * `insertIndex` from `after` reproduces `before` byte for byte.
 *
 * This is not a defense against a caller's input — the caller's new items are meant to
 * differ, that is the whole point of the call. It is a defense against *this tool*
 * regressing: if the splice above ever changed to (say) rebuild each item instead of
 * reusing it by reference, this is what would catch a column drifting away from what was
 * read, the same way the `GAP-Track` `view_3255` "Docs" column drifted under a different,
 * pre-this-tool workflow (see computeStructuralDiff's doc comment, lib/structural-diff.ts).
 * If this ever fails, it is this tool's bug, not the caller's — refusing to send is safer
 * than trusting a splice that did not do what it was built to do.
 */
// Exported for direct testing only: no legitimate call can make either assertion below
// fail — a failure means the splice logic itself regressed, which this file's own
// tools can never provoke through any input a caller controls. Testing that indirectly,
// through a tool call, would mean first breaking spliceNestedFields on purpose — these
// two are tested directly instead, the same way lib/view-safety.ts's pure helpers are.
export function assertFlatSpliceIsClean(
    before: unknown[],
    after: unknown[],
    insertIndex: number,
    insertedCount: number,
): { ok: true } | { ok: false; message: string } {
    const reconstructed = [
        ...after.slice(0, insertIndex),
        ...after.slice(insertIndex + insertedCount),
    ];
    if (deepEqual(reconstructed, before)) return { ok: true };
    return {
        ok: false,
        message: `internal check failed: after removing the ${insertedCount} item(s) this call inserted at position ${insertIndex}, the remaining array no longer matches what was read from Knack. Refusing to send — this is a bug in the tool, not in the request.`,
    };
}

/** As `assertFlatSpliceIsClean`, for the nested details/list `columns[].groups[].columns[][]` shape. */
export function assertNestedSpliceIsClean(
    before: unknown[],
    after: unknown[],
    location: {
        blockIndex: number;
        groupIndex: number;
        subColumnIndex: number;
    },
    insertIndex: number,
    insertedCount: number,
): { ok: true } | { ok: false; message: string } {
    const reconstructed = unspliceNestedFields(
        after,
        location,
        insertIndex,
        insertedCount,
    );
    if (deepEqual(reconstructed, before)) return { ok: true };
    return {
        ok: false,
        message: `internal check failed: after removing the ${insertedCount} item(s) this call inserted, the remaining layout no longer matches what was read from Knack. Refusing to send — this is a bug in the tool, not in the request.`,
    };
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

            const tableInsertIndex =
                anchorIndex === null
                    ? existingColumns.length
                    : insertAfterFieldKey
                      ? anchorIndex + 1
                      : anchorIndex;
            finalColumns = [
                ...existingColumns.slice(0, tableInsertIndex),
                ...newItems,
                ...existingColumns.slice(tableInsertIndex),
            ];
            const flatCheck = assertFlatSpliceIsClean(
                existingColumns,
                finalColumns,
                tableInsertIndex,
                newItems.length,
            );
            if (!flatCheck.ok) {
                return refuse('SPLICE_INVARIANT_VIOLATED', flatCheck.message);
            }
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
            const nestedCheck = assertNestedSpliceIsClean(
                existingColumns,
                finalColumns,
                location,
                insertIndex,
                newItems.length,
            );
            if (!nestedCheck.ok) {
                return refuse('SPLICE_INVARIANT_VIOLATED', nestedCheck.message);
            }
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
 * Shared splice/anchor engine behind knack_add_action_link and
 * knack_add_page_link_column (both append a caller-supplied JSON item to a table,
 * details or list view's existing columns, with no field key of their own to build
 * from — see each tool's own doc comment for what makes its item shape distinct).
 *
 * Owns everything the two calls used to duplicate line-for-line: the conflicting-
 * placement check, the fresh metadata read the mutation guard also relies on, the
 * table-vs-nested view-type gate, anchor lookup and both splice branches (flat
 * `columns[]` for a table, `columns[].groups[].columns[][]` for details/list, reusing
 * knack_add_view_columns's walker/splice helpers), the splice-invariant checks, and the
 * guarded update call. What's specific to one caller — its item shape, its default
 * `type`, its response's `action` field, the noun in its own messages — is supplied by
 * that caller through `buildItem` and the plain string options below; `buildItem` sees
 * `isTable` because the default `type` differs between a table's `link` and a nested
 * view's `scene_link`, and that isn't known until the view's own type is resolved here.
 *
 * `columnCountBefore`/`columnCountAfter` are reported for the nested branch too, even
 * though the new items carry no `key` `walkNestedFields` can count: `nestedFieldLocations`
 * is read *before* the splice, so its length is a valid pre-splice count regardless of
 * what the new items look like — knack_add_view_columns already reports the equivalent
 * for its own nested branch, and there is no reason the count should differ once this
 * logic lives in one place instead of two independently-reasoned tools.
 */
async function spliceColumnItems(
    ctx: KnackContext,
    app: AppConfig,
    {
        sceneKey,
        viewKey,
        rawItems,
        buildItem,
        insertAfterFieldKey,
        insertBeforeFieldKey,
        previewOnly,
        toolAction,
        toolName,
        itemNoun,
    }: {
        sceneKey: string;
        viewKey: string;
        rawItems: Record<string, unknown>[];
        buildItem: (
            record: Record<string, unknown>,
            isTable: boolean,
        ) => Record<string, unknown>;
        insertAfterFieldKey?: string;
        insertBeforeFieldKey?: string;
        previewOnly?: boolean;
        /** The `action` this tool reports on every response, e.g. `'add_action_link'`. */
        toolAction: string;
        /** The tool's own name, for the UNSUPPORTED_VIEW_TYPE message. */
        toolName: string;
        /** Singular noun for one item, e.g. `'action link'` or `'page link'`. */
        itemNoun: string;
    },
) {
    const refuse = (error: string, message: string) =>
        makeTextResponse({
            ok: false,
            appKey: app.appKey,
            action: toolAction,
            sceneKey,
            viewKey,
            error,
            message,
        });

    if (insertAfterFieldKey && insertBeforeFieldKey) {
        return refuse(
            'CONFLICTING_PLACEMENT',
            `insertAfterFieldKey and insertBeforeFieldKey both name a position for the new ${itemNoun}(s). Pass one, or neither to add them at the end. Nothing was sent.`,
        );
    }

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
            `${toolName} only supports table, details and list views. ${viewKey} is a "${viewType ?? 'unknown'}" view. A form's ${itemNoun}s live under groups[].columns[].inputs instead — a different shape this tool does not build. Use knack_update_view with a hand-built patch for those. Nothing was sent.`,
        );
    }

    const newItems = rawItems.map((record) => buildItem(record, isTable));

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
                    `${anchorKey} is not an existing field column on ${viewKey}, so the new ${itemNoun}(s) cannot be placed ${insertAfterFieldKey ? 'after' : 'before'} it. Nothing was sent.`,
                );
            }
        }

        const tableInsertIndex =
            anchorIndex === null
                ? existingColumns.length
                : insertAfterFieldKey
                  ? anchorIndex + 1
                  : anchorIndex;
        finalColumns = [
            ...existingColumns.slice(0, tableInsertIndex),
            ...newItems,
            ...existingColumns.slice(tableInsertIndex),
        ];
        const flatCheck = assertFlatSpliceIsClean(
            existingColumns,
            finalColumns,
            tableInsertIndex,
            newItems.length,
        );
        if (!flatCheck.ok) {
            return refuse('SPLICE_INVARIANT_VIOLATED', flatCheck.message);
        }
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
                    `${anchorKey} is not an existing field column on ${viewKey}, so the new ${itemNoun}(s) cannot be placed ${insertAfterFieldKey ? 'after' : 'before'} it. Nothing was sent.`,
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
        const nestedCheck = assertNestedSpliceIsClean(
            existingColumns,
            finalColumns,
            location,
            insertIndex,
            newItems.length,
        );
        if (!nestedCheck.ok) {
            return refuse('SPLICE_INVARIANT_VIOLATED', nestedCheck.message);
        }
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
        // The guard reports its own request as `update_view` — this tool's identity
        // wins, as it does for knack_add_view_columns.
        action: toolAction,
        addedCount: newItems.length,
        columnCountBefore,
        columnCountAfter,
    });
}

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
 * not this tool's to guess — and hands them to spliceColumnItems, which owns the shared
 * placement mechanics (see that function's doc comment). No raw JSON needs to reach the
 * caller, so allowViewMutation alone is enough.
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

        const rawActionLinks = parseJsonObjectArray(
            'actionLinks',
            actionLinks,
            'action-link',
        );

        return spliceColumnItems(ctx, app, {
            sceneKey,
            viewKey,
            rawItems: rawActionLinks,
            buildItem: (record) => ({ type: 'action_link', ...record }),
            insertAfterFieldKey,
            insertBeforeFieldKey,
            previewOnly,
            toolAction: 'add_action_link',
            toolName: 'knack_add_action_link',
            itemNoun: 'action link',
        });
    },
});

/**
 * Append page-link column(s) to a table, details or list view's existing columns —
 * links that either open an *existing* scene, or ask Knack to create a brand-new one as
 * part of this same call.
 *
 * `scene` takes either shape, distinguished by `isScenePageSpecification`
 * (lib/view-safety.ts): a plain key/slug string (or `{key: ...}`) *references* a page
 * that must already exist, but `{name, parent, views}` — no `key`/`scene`/`slug` of its
 * own — is a *specification* asking Knack to create one. This is exactly the shape
 * Knack's own builder posts for "+ Add New Page" on a link column. A well-formed
 * specification (one with a `views` array, even `[]`) is resolved to the new page's slug
 * on save — measured live on a *menu* link, per lib/view-safety.ts's
 * `isScenePageSpecification` doc comment, and separately measured live through this
 * tool's own table-column path (TESTING.md Tier 23, NPS Test App, 21 September 2026); the
 * nested details/list column path has not been measured live or by test, so treat it with
 * the same caution as any unmeasured shape in this file. A malformed specification
 * (missing `views`) is refused before anything is sent, by the same guard every other
 * knack_update_view-backed call in this file already goes through
 * (MALFORMED_PAGE_SPECIFICATION) — this tool adds no separate check of its own, so it
 * cannot drift from that one. That guard's own comments note its `type: "scene"`
 * malformation check is judged only on menu links and never fires for a column, so for a
 * column the only protection against the "page created, object never resolved, every
 * resave recreates it" failure mode is after the fact (STORED_PAGE_SPECIFICATION on a
 * later mutation), not preventative the way it is for a menu link.
 *
 * What a bare string reference to a page that does *not* exist yet does **not** do is
 * create it — Knack stores the string verbatim and the link opens nothing (confirmed
 * live in the GAP Track app, 2026-09-21; see the "Link column can't create a scene"
 * memory). Use a specification object to create a page, a reference to link to one that
 * already exists — never a slug guessed for a page that hasn't been made yet.
 *
 * A page-link column is not a field and carries no `field` key of its own, so
 * knack_add_view_columns cannot place one; it carries a `scene` instead of the
 * `action_rules[]` an action link has, so knack_add_action_link's forced
 * `type: 'action_link'` is the wrong shape too. This tool hands its item(s) to the same
 * spliceColumnItems engine knack_add_action_link uses (see that function's doc comment
 * for the shared mechanics), supplying only what differs: the input shape and the default
 * `type`.
 *
 * Knack writes `type: "link"` on a table's columns, `type: "scene_link"` on a details or
 * list view's nested fields, and `type: "scene_link"` on a calendar column too — measured
 * for table/details/list specifically in TESTING.md's Tier 17/18 (the `collectLinkTargets`
 * comment this codebase relies on elsewhere only says "details and calendar", but list was
 * independently confirmed live there as well). This tool sets that default itself so the
 * caller does not have to know which shape they are on, but a `type` the caller does
 * supply always wins, same as knack_add_action_link's own default.
 */
export const addPageLinkColumn = defineTool({
    name: 'knack_add_page_link_column',
    description:
        "Append page-link column(s) to a table, details or list view's existing columns — either linking to an existing scene, or creating a new one as part of this same call; reads the live columns itself so nothing else on the view is touched.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        viewKey: z.string(),
        pageLinks: z
            .string()
            .describe(
                'JSON array of page-link column objects to add. To link an existing page: [{"header":"Edit","link_text":"Edit","scene":"scene_123"}] ("scene" is its key or slug). To create a new page: [{"header":"Edit","link_text":"Edit","scene":{"name":"Edit Zone Rule","parent":"jobs2","views":[]}}] — "views" is required (Knack stores the object and creates nothing without it) and can be empty; Knack resolves it to the new page\'s slug on save. "type" is set to "link" (table) or "scene_link" (details/list) automatically if omitted',
            ),
        insertAfterFieldKey: z
            .string()
            .optional()
            .describe(
                'Place the new page link(s) directly after this existing field column',
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
            pageLinks,
            insertAfterFieldKey,
            insertBeforeFieldKey,
            previewOnly,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        const rawPageLinks = parseJsonObjectArray(
            'pageLinks',
            pageLinks,
            'page-link',
        );

        return spliceColumnItems(ctx, app, {
            sceneKey,
            viewKey,
            rawItems: rawPageLinks,
            // Knack's own type string for this shape differs by view type (see this
            // function's doc comment); a caller-supplied `type` always wins over it.
            buildItem: (record, isTable) => ({
                type: isTable ? 'link' : 'scene_link',
                ...record,
            }),
            insertAfterFieldKey,
            insertBeforeFieldKey,
            previewOnly,
            toolAction: 'add_page_link_column',
            toolName: 'knack_add_page_link_column',
            itemNoun: 'page link',
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

        const incomingRecordRules = recordRules
            ? parseJsonObjectArray('recordRules', recordRules, 'rule')
            : undefined;
        const incomingSubmitRules = submitRules
            ? parseJsonObjectArray('submitRules', submitRules, 'rule')
            : undefined;
        // A rule naming a hidden field, or reading a write-only one (a record rule copying
        // it through input, an email rule quoting {field_N}), would move or send a value
        // MCP must not reach.
        const refusal = ruleFieldRefusal(
            await ctx.getFieldExclusions(app),
            [...(incomingRecordRules ?? []), ...(incomingSubmitRules ?? [])],
            'a rule',
        );
        if (refusal) return refuse(refusal.error, refusal.message);

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

        // Each new rule gets a key in a Builder scheme (numeric for record rules, as the
        // older Builder mints them; submit_N for submit rules): a rule stored without one
        // (as this tool used to send them) can never be edited or removed by
        // knack_edit_view_rules. Found by the PR #71 retest on 25 September; the survey
        // behind the formats is on assignSubmitRuleKeys and in lib/rule-edits.ts.
        const parsedRecordRules =
            incomingRecordRules &&
            assignNumericRuleKeys(
                readRuleArray(existingRecordRules),
                incomingRecordRules,
                'recordRules',
            );
        const parsedSubmitRules =
            incomingSubmitRules &&
            assignSubmitRuleKeys(
                readRuleArray(existingSubmitRules),
                incomingSubmitRules,
                'submitRules',
            );

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

        // Only `records` and/or `submits` should differ from what was read, and only by
        // the rules just appended — see assertFlatSpliceIsClean's doc comment for why
        // this is checked rather than trusted.
        if (parsedRecordRules) {
            const check = assertFlatSpliceIsClean(
                existingRecordRules,
                mergedRules.records as unknown[],
                existingRecordRules.length,
                parsedRecordRules.length,
            );
            if (!check.ok) {
                return refuse('SPLICE_INVARIANT_VIOLATED', check.message);
            }
        }
        if (parsedSubmitRules) {
            const check = assertFlatSpliceIsClean(
                existingSubmitRules,
                mergedRules.submits as unknown[],
                existingSubmitRules.length,
                parsedSubmitRules.length,
            );
            if (!check.ok) {
                return refuse('SPLICE_INVARIANT_VIOLATED', check.message);
            }
        }
        const untouchedRuleKeys = Object.keys(existingRules).filter(
            (key) => key !== 'records' && key !== 'submits',
        );
        for (const key of untouchedRuleKeys) {
            if (!deepEqual(existingRules[key], mergedRules[key])) {
                return refuse(
                    'SPLICE_INVARIANT_VIOLATED',
                    `internal check failed: rules.${key} was not asked to change but differs from what was read. Refusing to send — this is a bug in the tool, not in the request.`,
                );
            }
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
                      recordRuleKeysAdded: parsedRecordRules.map(
                          (rule) => rule.key,
                      ),
                      recordRuleCountBefore: existingRecordRules.length,
                      recordRuleCountAfter:
                          existingRecordRules.length + parsedRecordRules.length,
                  }
                : {}),
            ...(parsedSubmitRules
                ? {
                      submitRulesAdded: parsedSubmitRules.length,
                      submitRuleKeysAdded: parsedSubmitRules.map(
                          (rule) => rule.key,
                      ),
                      submitRuleCountBefore: existingSubmitRules.length,
                      submitRuleCountAfter:
                          existingSubmitRules.length + parsedSubmitRules.length,
                  }
                : {}),
        });
    },
});

/** A view's rule sets as Knack stores them under `rules`, and what the Builder calls each. */
const VIEW_RULE_SETS = {
    records: 'record rules (form record actions)',
    submits: 'submit rules',
    fields: 'display rules',
    emails: 'email rules',
} as const;

/**
 * Remove or replace a view's existing rules by key, in one of its rule sets.
 *
 * The same clobbering hazard as knack_add_view_rules — the guard replaces the whole
 * top-level `rules` object — so this reads the live `rules` off fresh metadata, edits the
 * one named set (lib/rule-edits.ts), checks every other set is byte-for-byte what was
 * read, and sends the result through the guarded update path.
 *
 * A form's default submit rule (`is_default: true`) is what Knack runs when no other
 * submit rule matches. It cannot be removed here, and a replacement must stay the default.
 */
export const editViewRules = defineTool({
    name: 'knack_edit_view_rules',
    description:
        "Remove or replace a view's existing record, submit, display or email rules by key; nothing else on the view changes.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        viewKey: z.string(),
        ruleSet: z
            .enum(['records', 'submits', 'fields', 'emails'])
            .describe(
                'records = record actions, submits = submit rules, fields = display rules, emails = email rules',
            ),
        removeKeys: z
            .array(z.string())
            .optional()
            .describe('Keys of rules to remove, e.g. ["15"] or ["submit_1"]'),
        replaceRules: z
            .string()
            .optional()
            .describe(
                'JSON array of whole rules, each carrying the key of the stored rule it replaces',
            ),
        previewOnly: z.boolean().optional().describe(PREVIEW_DESCRIPTION),
    },
    handler: async (
        {
            appKey,
            sceneKey,
            viewKey,
            ruleSet,
            removeKeys,
            replaceRules,
            previewOnly,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        const { refuse } = toolReplies(app.appKey, 'edit_view_rules', {
            sceneKey,
            viewKey,
            ruleSet,
        });

        const replacements = replaceRules
            ? parseJsonObjectArray('replaceRules', replaceRules, 'rule')
            : undefined;
        // A rule naming a hidden field, or reading a write-only one (a record rule copying
        // it through input, an email rule quoting {field_N}), would move or send a value
        // MCP must not reach. Display rules only change what a person sees, so they may
        // use a write-only field.
        const refusal = ruleFieldRefusal(
            await ctx.getFieldExclusions(app),
            replacements ?? [],
            'a rule',
            { displayOnly: ruleSet === 'fields' },
        );
        if (refusal) return refuse(refusal.error, refusal.message);

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
        const existing = readRuleArray(existingRules[ruleSet]);

        let edited: ReturnType<typeof applyRuleEdit>;
        try {
            edited = applyRuleEdit(
                existing,
                { removeKeys, replaceRules: replacements },
                VIEW_RULE_SETS[ruleSet],
            );
        } catch (error) {
            return refuse('INVALID_EDIT', (error as Error).message);
        }

        if (ruleSet === 'submits') {
            const defaultRule = existing.find(
                (rule) => rule.is_default === true,
            );
            if (
                defaultRule &&
                edited.removedKeys.includes(defaultRule.key as string)
            ) {
                return refuse(
                    'DEFAULT_SUBMIT_RULE',
                    `${String(defaultRule.key)} is the form's default submit rule, which Knack runs when no other rule matches. Replace it instead of removing it. Nothing was sent.`,
                );
            }
            const replacedDefault = replacements?.find(
                (rule) => rule.key === defaultRule?.key,
            );
            if (replacedDefault && replacedDefault.is_default !== true) {
                return refuse(
                    'DEFAULT_SUBMIT_RULE',
                    `The replacement for ${String(defaultRule!.key)} must keep "is_default": true: it is the form's default submit rule. Nothing was sent.`,
                );
            }
        }

        const mergedRules: Record<string, unknown> = {
            ...existingRules,
            [ruleSet]: edited.rules,
        };
        for (const key of Object.keys(existingRules)) {
            if (key === ruleSet) continue;
            if (!deepEqual(existingRules[key], mergedRules[key])) {
                return refuse(
                    'SPLICE_INVARIANT_VIOLATED',
                    `internal check failed: rules.${key} was not asked to change but differs from what was read. Refusing to send — this is a bug in the tool, not in the request.`,
                );
            }
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
            action: 'edit_view_rules',
            ruleSet,
            ruleCountBefore: existing.length,
            ruleCountAfter: edited.rules.length,
            removedKeys: edited.removedKeys,
            replacedKeys: edited.replacedKeys,
            rulesBefore: existing,
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

        const newItems = parseJsonObjectArray('links', links, 'link');

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

        const linksInsertIndex = insertAtIndex ?? existingLinks.length;
        const finalLinks = [
            ...existingLinks.slice(0, linksInsertIndex),
            ...newItems,
            ...existingLinks.slice(linksInsertIndex),
        ];
        const linksCheck = assertFlatSpliceIsClean(
            existingLinks,
            finalLinks,
            linksInsertIndex,
            newItems.length,
        );
        if (!linksCheck.ok) {
            return refuse('SPLICE_INVARIANT_VIOLATED', linksCheck.message);
        }

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
                'Scene owning the view. Required when sharePages is false (a plain copy); when sharePages is true it is derived from the view itself if omitted',
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

            // Named up front rather than left for a caller to dig out of `changes` (or,
            // before this, only recoverable by regex over the snapshot file) — this is
            // the one fact every caller of a copy actually wants first.
            const newViewKeys = insertedViewKeysFromOutcome(outcome);

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
                          newViewKeys,
                      )
                    : {};

            return makeTextResponse({
                // `sceneKey` is what the guard reports, but this tool has always named
                // its two scenes explicitly. Keep both so a caller written against the
                // old response shape still finds sourceSceneKey.
                sourceSceneKey,
                targetSceneKey,
                ...(newViewKeys.length ? { newViewKeys } : {}),
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
    addPageLinkColumn,
    addViewRules,
    editViewRules,
    addViewLinks,
    copyView,
    moveView,
    deleteView,
];
