/**
 * Declarative tool registry.
 *
 * A tool is data: a name, a one-sentence description, an access level, a zod input
 * shape and a handler that takes the parsed arguments and the context. Registration
 * does the rest once for every tool — gating by access level, resolving the app and
 * enforcing its permissions, logging the call, and turning a thrown error into a
 * compact JSON error response.
 */
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { z } from 'zod';

import { type ToolAccess, assertAccess, isAdvertised } from './access.js';
import type { KnackContext } from './context.js';
import { debugLog } from './lib/log.js';
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
    handler: (args: Record<string, unknown>, ctx: KnackContext) => Promise<ToolResult>;
};

/** Identity with inference, so `args` is typed from `input` at the definition. */
export function defineTool<S extends z.ZodRawShape>(
    def: ToolDef<S>,
): AnyToolDef {
    return def as unknown as AnyToolDef;
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

        server.registerTool(
            def.name,
            { description: def.description, inputSchema: def.input },
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
                    return await def.handler(args, ctx);
                } catch (error) {
                    debugLog('tool_error', {
                        tool: def.name,
                        error:
                            error instanceof Error
                                ? error.message
                                : String(error),
                    });
                    return makeErrorResponse(error, def.name);
                }
            },
        );
    }
    return summary;
}
