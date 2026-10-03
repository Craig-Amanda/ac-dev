/**
 * Declarative tool registry.
 *
 * A tool is data: a name, a one-sentence description, an access level, a zod input
 * shape and a handler that takes the parsed arguments and the context. Registration
 * does the rest once for every tool — gating by access level, resolving the app and
 * enforcing its permissions, logging the call, and turning a thrown error into a
 * compact JSON error response.
 */
import type { McpServer } from '@modelcontextprotocol/server';
import { z } from 'zod';

import { type ToolAccess, assertAccess, isAdvertised } from './access.js';
import type { KnackContext } from './context.js';
import {
    type TypedEmailOptions,
    readAppDefaultSender,
    typedEmailsInEmailRules,
} from './lib/exposure-audit.js';
import { debugLog } from './lib/log.js';
import { describeRequestCost } from './lib/rate-limit.js';
import { type ToolResult, makeErrorResponse } from './response.js';

export type ToolDef<S extends z.ZodRawShape = z.ZodRawShape> = {
    name: string;
    /** One sentence. This is sent to the model on every turn; guidance belongs in the README. */
    description: string;
    access: ToolAccess;
    input: S;
    handler: (
        args: z.infer<z.ZodObject<S>>,
        ctx: KnackContext,
    ) => Promise<ToolResult>;
};

/**
 * The registry holds tools of every shape. The handler's argument type is checked at
 * the definition site, so the erased form is safe to store and call.
 */
export type AnyToolDef = {
    name: string;
    description: string;
    access: ToolAccess;
    input: z.ZodRawShape;
    handler: (
        args: Record<string, unknown>,
        ctx: KnackContext,
    ) => Promise<ToolResult>;
};

/** Identity with inference, so `args` is typed from `input` at the definition. */
export function defineTool<S extends z.ZodRawShape>(
    def: ToolDef<S>,
): AnyToolDef {
    return def as unknown as AnyToolDef;
}

/** Whether a tool result reports a change made to Knack (it carries a `cacheNote`). */
function changedAnApp(result: ToolResult): boolean {
    return result.content.some(
        (block) => block.type === 'text' && block.text.includes('"cacheNote"'),
    );
}

export type RegistrationSummary = { advertised: string[]; withheld: string[] };

/**
 * Register every tool whose access level at least one app has opted into.
 * Each call still enforces the selected app's own toggles.
 */
export function registerTools(
    server: McpServer,
    ctx: KnackContext,
    tools: AnyToolDef[],
): RegistrationSummary {
    const summary: RegistrationSummary = { advertised: [], withheld: [] };
    const seen = new Set<string>();

    for (const def of tools) {
        if (seen.has(def.name))
            throw new Error(`Duplicate tool name: ${def.name}`);
        seen.add(def.name);

        if (!isAdvertised(def.access, ctx.apps, ctx.options)) {
            summary.withheld.push(def.name);
            continue;
        }
        summary.advertised.push(def.name);

        // v2 requires a Standard Schema object for `inputSchema`, not a raw zod shape —
        // wrapping it here (rather than at each ToolDef definition site) is also what
        // fixes the v1-SDK bug this migration exists for: the old zod-compat shim
        // re-wrapped a raw shape through zod/v4-mini's `object()`, whose `.default()`
        // handling treated every defaulted parameter as required. A real `z.object()`
        // has none of that, so a caller omitting a defaulted parameter now gets the
        // default rather than a "received undefined" error.
        server.registerTool(
            def.name,
            { description: def.description, inputSchema: z.object(def.input) },
            async (args: Record<string, unknown>) => {
                debugLog('tool_call', {
                    tool: def.name,
                    appKey:
                        typeof args.appKey === 'string'
                            ? args.appKey
                            : ctx.state.activeAppKey,
                    args: Object.keys(args),
                });
                try {
                    if (def.access !== 'read') {
                        const app = ctx.getApp(
                            typeof args.appKey === 'string'
                                ? args.appKey
                                : undefined,
                        );
                        assertAccess(app, def.access, ctx.options);
                    }
                    const before = ctx.usage.snapshot();
                    const result = withKeyNote(
                        ctx,
                        def.name,
                        args,
                        withTypedEmailNote(
                            def,
                            args,
                            await def.handler(args, ctx),
                            {
                                appDefaultSender: cachedDefaultSender(
                                    ctx,
                                    args,
                                ),
                            },
                        ),
                    );
                    // A successful change says so with a `cacheNote`; drop the app's
                    // cached metadata so the next read is not stale.
                    if (def.access !== 'read' && changedAnApp(result)) {
                        ctx.invalidate(
                            ctx.getApp(
                                typeof args.appKey === 'string'
                                    ? args.appKey
                                    : undefined,
                            ).appKey,
                        );
                    }
                    const cost = describeRequestCost(
                        ctx.usage,
                        before,
                        Date.now(),
                    );
                    return cost
                        ? {
                              ...result,
                              content: [
                                  ...result.content,
                                  { type: 'text' as const, text: cost },
                              ],
                          }
                        : result;
                } catch (error) {
                    debugLog('tool_error', {
                        tool: def.name,
                        error:
                            error instanceof Error
                                ? error.message
                                : String(error),
                    });
                    return withKeyNote(
                        ctx,
                        def.name,
                        args,
                        makeErrorResponse(error, def.name),
                    );
                }
            },
        );
    }
    return summary;
}

/**
 * When the app's metadata was withheld for its key, say so in the response itself, so
 * a caller does not mistake a key problem for Knack being down, or cached files on
 * disk for the live app. Added to the prose note, never to the JSON payload.
 * knack_list_apps reports every app's key status itself.
 */
export function withKeyNote(
    ctx: KnackContext,
    toolName: string,
    args: Record<string, unknown>,
    result: ToolResult,
): ToolResult {
    if (toolName === 'knack_list_apps') return result;
    const appKey =
        typeof args.appKey === 'string' ? args.appKey : ctx.state.activeAppKey;
    const app = appKey ? ctx.findApp(appKey) : undefined;
    const refusal = app ? ctx.metadataRefusal(app) : null;
    if (!refusal) return result;

    const note = `${refusal} Anything this response shows of the app's structure came from the cache files on disk, if there are any, and may be out of date.`;
    const [payload, existing, ...rest] = result.content;
    const content = existing
        ? [
              payload,
              { ...existing, text: `${existing.text}\n\n${note}` },
              ...rest,
          ]
        : [payload, { type: 'text' as const, text: note }];
    return { ...result, content };
}

/**
 * Tools that can put an email into an app: every view and page tool, and the task
 * writes. Record writes are left out on purpose: an email field's value is stored as
 * `{ email: ... }`, which is a record's data, not an email the app sends.
 */
function writesEmailSettings(
    def: Pick<AnyToolDef, 'name' | 'access'>,
): boolean {
    return (
        def.access === 'view' ||
        def.access === 'view-delete' ||
        def.name === 'knack_create_task' ||
        def.name === 'knack_update_task'
    );
}

/**
 * Flag a write that puts a typed email address into an email's settings: a rule's
 * recipients or text, or a task's email. The change still goes through; the note says
 * that anyone with the app ID can read the address, and suggests sending to an email
 * field on the record instead. Addresses are shown with the local part hidden.
 */
export function withTypedEmailNote(
    def: Pick<AnyToolDef, 'name' | 'access'>,
    args: Record<string, unknown>,
    result: ToolResult,
    options: TypedEmailOptions = {},
): ToolResult {
    if (!writesEmailSettings(def) || result.isError) return result;
    // A preview or dry run is the one refusal that is an answer rather than a stop, so
    // it keeps the note, saying what the change would do; any other refusal gets none.
    const payload = parsePayload(result);
    const preview =
        payload?.preview === true ||
        payload?.previewOnly === true ||
        payload?.error === 'PREVIEW_ONLY' ||
        payload?.dryRun === true;
    if (payload?.ok === false && !preview) return result;
    const hits = typedEmailsInEmailRules(args, {
        appDefaultSender: options.appDefaultSender,
    });
    if (!hits.length) return result;

    const listed = hits
        .slice(0, 5)
        .map((hit) => `${hit.address} at ${hit.path}`)
        .join('; ');
    const more = hits.length > 5 ? ` (+${hits.length - 5} more)` : '';
    const note = `This change ${preview ? 'would put' : 'puts'} a typed email address into an email: ${listed}${more}. Knack serves the app's structure to anyone with its application ID, so the address can be read without a login. Consider sending to an email field on the record, or a connected record, instead.`;
    const [first, existing, ...rest] = result.content;
    const content = existing
        ? [first, { ...existing, text: `${existing.text}\n\n${note}` }, ...rest]
        : [first, { type: 'text' as const, text: note }];
    return { ...result, content };
}

function parsePayload(result: ToolResult): Record<string, unknown> | null {
    try {
        const parsed: unknown = JSON.parse(result.content[0]?.text ?? '');
        return parsed && typeof parsed === 'object'
            ? (parsed as Record<string, unknown>)
            : null;
    } catch {
        return null;
    }
}

/**
 * The app's default sender from its cached metadata, so a rule's From left at the
 * Builder's pre-filled default is not flagged as a typed address. Read from the cache
 * only: a write has normally just read fresh metadata, and a note is not worth a fetch.
 */
function cachedDefaultSender(
    ctx: KnackContext,
    args: Record<string, unknown>,
): string | null {
    const appKey =
        typeof args.appKey === 'string' ? args.appKey : ctx.state.activeAppKey;
    if (!appKey) return null;
    return readAppDefaultSender(ctx.caches.runtimeMetadata.get(appKey)?.value);
}
