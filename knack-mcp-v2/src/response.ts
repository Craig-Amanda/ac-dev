import { asRecord } from './lib/util.js';
import { readChangedScenes } from './lib/view-safety.js';
import {
    type AppConfig,
    MAX_INLINE_DETAIL_BYTES,
    MAX_TOOL_TEXT_BYTES,
    PRETTY_TOOL_JSON,
} from './config.js';

/**
 * Build a compact summary when a tool response would be too large to send efficiently.
 */
export function summariseLargeValue(value: unknown, depth = 0): unknown {
    if (value === null || value === undefined) return value;

    if (typeof value === 'string') {
        return {
            type: 'string',
            length: value.length,
            preview: value.length <= 240 ? value : `${value.slice(0, 240)}...`,
        };
    }

    if (typeof value === 'number' || typeof value === 'boolean') {
        return value;
    }

    if (Array.isArray(value)) {
        return {
            type: 'array',
            length: value.length,
            sample:
                depth >= 2
                    ? undefined
                    : value
                          .slice(0, 3)
                          .map((entry) =>
                              summariseLargeValue(entry, depth + 1),
                          ),
        };
    }

    const record = asRecord(value);
    if (!record) {
        return { type: typeof value };
    }

    const keys = Object.keys(record);
    const sampleEntries =
        depth >= 2
            ? undefined
            : Object.fromEntries(
                  keys
                      .slice(0, 8)
                      .map((key) => [
                          key,
                          summariseLargeValue(record[key], depth + 1),
                      ]),
              );

    return {
        type: 'object',
        keyCount: keys.length,
        keys: keys.slice(0, 30),
        sample: sampleEntries,
    };
}

/**
 * Serialise tool responses compactly and fall back to a structured overflow summary when needed.
 */
export function serialiseToolPayload(data: unknown): string {
    const spacing = PRETTY_TOOL_JSON ? 2 : undefined;
    const text = JSON.stringify(data, null, spacing);
    const sizeBytes = Buffer.byteLength(text, 'utf8');
    if (sizeBytes <= MAX_TOOL_TEXT_BYTES) return text;

    const payload = asRecord(data);
    const overflow = {
        ok: typeof payload?.ok === 'boolean' ? payload.ok : false,
        truncated: true,
        message: `Tool response exceeded ${MAX_TOOL_TEXT_BYTES} bytes after serialisation. Narrow the query or lower requested limits.`,
        sizeBytes,
        maxToolTextBytes: MAX_TOOL_TEXT_BYTES,
        topLevelKeys: payload ? Object.keys(payload) : [],
        summary: summariseLargeValue(data),
    };

    return JSON.stringify(overflow, null, spacing);
}

/**
 * Inline detailed payloads only when they are small enough to stay cheap for token-based clients.
 */
export function getInlineDetail(
    value: unknown,
    maxBytes = MAX_INLINE_DETAIL_BYTES,
): {
    included: boolean;
    sizeBytes: number;
    value?: unknown;
    summary?: unknown;
} {
    const text = JSON.stringify(value);
    const sizeBytes = Buffer.byteLength(text, 'utf8');
    if (sizeBytes <= maxBytes) {
        return {
            included: true,
            sizeBytes,
            value,
        };
    }

    return {
        included: false,
        sizeBytes,
        summary: summariseLargeValue(value),
    };
}

/**
 * Shape a tool response, optionally led by a human-readable note.
 *
 * The note is a separate block rather than a field inside the JSON, because prose
 * buried in a serialised payload is prose nobody reads. It goes *after* the payload,
 * not before: a client indexing `content[0].text` and parsing it as JSON is a contract
 * worth keeping, and a second text block is read either way — the original problem was
 * prose inside the serialisation, not prose in second position.
 *
 * @param data The structured payload.
 * @param note Optional plain-text summary to place above the payload.
 * @returns An MCP tool response.
 */
export function makeTextResponse(data: unknown, note?: string) {
    const payloadBlock = {
        type: 'text' as const,
        text: serialiseToolPayload(data),
    };

    const trimmedNote = note?.trim();
    if (!trimmedNote) {
        return { content: [payloadBlock] };
    }

    return {
        content: [payloadBlock, { type: 'text' as const, text: trimmedNote }],
    };
}

/**
 * Name a handful of apps without letting the banner grow with the folder.
 *
 * @param names App names to list.
 * @param limit How many to spell out before summarising the rest.
 * @returns A comma-separated list, truncated with a count of what was dropped.
 */
export function listAppNames(names: string[], limit = 6): string {
    if (!names.length) return 'none';
    if (names.length <= limit) return names.join(', ');
    return `${names.slice(0, limit).join(', ')} +${names.length - limit} more`;
}

/**
 * Build the plain-text banner that leads the knack_list_apps response.
 *
 * The structured payload already carries every fact here, but two of them decide
 * whether a change is even attemptable and are easy to miss inside a serialised blob:
 * which apps accept writes, and whether this client can put a cascade-delete
 * confirmation in front of a person or will simply be refused. Stating them in prose
 * means a caller learns the rule while orienting, not when a real mutation bounces.
 *
 * @param input Discovery results plus the client-dependent confirmation status.
 * @returns A short human-readable summary.
 */
export function describeAppListForHumans(input: {
    knackAppsDir: string;
    activeAppKey: string | null;
    apps: AppConfig[];
    enforcedReadOnly: boolean;
    humanConfirmation: { available: boolean; client: string | null };
    cascadeDeleteBehaviour: { summary: string };
    buildSummary: string;
}): string {
    const { apps, enforcedReadOnly, humanConfirmation } = input;

    const writable = apps
        .filter((app) => app.readonly === false)
        .map((app) => app.appName || app.appKey);
    const viewMutable = apps
        .filter((app) => app.allowViewMutation === true)
        .map((app) => app.appName || app.appKey);

    const lines = [
        `Knack apps: ${apps.length} discovered in ${input.knackAppsDir}. Active app: ${
            input.activeAppKey ?? 'none'
        }.`,
    ];

    if (enforcedReadOnly) {
        lines.push(
            'Writes: none. This server was started in enforced read-only mode, so every app is read-only whatever app.json says.',
        );
    } else {
        lines.push(
            `Writable: ${listAppNames(writable)}. View mutation allowed: ${listAppNames(
                viewMutable,
            )}.`,
        );
    }

    const clientLabel = humanConfirmation.client
        ? `Client "${humanConfirmation.client}"`
        : 'This client';
    const headline = humanConfirmation.available
        ? 'Cascade deletes: a human is prompted.'
        : 'Cascade deletes: refused.';
    const advertised = humanConfirmation.available
        ? `${clientLabel} advertised MCP elicitation.`
        : `${clientLabel} did not advertise MCP elicitation.`;
    // The consequence sentence is reused verbatim from describeCascadeBehaviour rather
    // than reworded here, so the prose cannot drift from the structured field.
    lines.push(
        `${headline} ${advertised} ${input.cascadeDeleteBehaviour.summary}`,
    );

    // Last line rather than first: it answers "which code am I talking to", which
    // matters only once something above it reads wrong.
    lines.push(input.buildSummary);

    return lines.join('\n');
}

export type ToolResult = {
    content: Array<{ type: 'text'; text: string }>;
    isError?: boolean;
};

/**
 * A failed tool call, as compact JSON the caller can parse like any other response.
 * `isError` is set so a client that distinguishes failures still can.
 */
export function makeErrorResponse(error: unknown, tool?: string): ToolResult {
    const message = error instanceof Error ? error.message : String(error);
    return {
        isError: true,
        content: [
            {
                type: 'text',
                text: serialiseToolPayload({
                    ok: false,
                    ...(tool ? { tool } : {}),
                    error: message,
                }),
            },
        ],
    };
}

/**
 * Compact the `changes` block Knack returns from a view mutation.
 *
 * Knack echoes the whole created or updated view under `changes.inserts.views[].view`
 * as well as under `view`, and sends empty `objects` and `fields` arrays under every
 * heading. A caller uses the keys and the page identities; the rest doubled the size of
 * every mutation response. The scenes Knack made keep key, name, slug and parent, since
 * those are how the caller finds them; everything else reduces to keys.
 *
 * @param body Knack's response body.
 * @returns A `body` override carrying the compacted block, or nothing to override.
 */
export function compactKnackChanges(
    body: unknown,
): { body: Record<string, unknown> } | Record<string, never> {
    const record = asRecord(body);
    const changes = asRecord(record?.changes);
    if (!record || !changes) return {};

    const keysOf = (list: unknown): string[] =>
        Array.isArray(list)
            ? list
                  .map((entry) => {
                      if (typeof entry === 'string') return entry;
                      const item = asRecord(entry);
                      return asRecord(item?.view)?.key ?? item?.key ?? null;
                  })
                  .filter((key): key is string => typeof key === 'string')
            : [];

    const compacted: Record<string, unknown> = {};
    for (const kind of ['deletes', 'inserts', 'updates'] as const) {
        const block = asRecord(changes[kind]);
        if (!block) continue;
        const out: Record<string, unknown> = {};
        const scenes =
            kind === 'inserts'
                ? readChangedScenes(body, 'inserts').map((scene) => ({
                      key: scene.sceneKey,
                      name: scene.sceneName,
                      slug: scene.sceneSlug,
                      parent: scene.parentRef,
                  }))
                : keysOf(block.scenes);
        if (scenes.length > 0) out.scenes = scenes;
        for (const other of ['views', 'objects', 'fields'] as const) {
            const keys = keysOf(block[other]);
            if (keys.length > 0) out[other] = keys;
        }
        if (Object.keys(out).length > 0) compacted[kind] = out;
    }

    return { body: { ...record, changes: compacted } };
}
