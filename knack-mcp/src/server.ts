import fs from 'node:fs';
import * as http from 'node:http';
import * as https from 'node:https';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { z } from 'zod';
import mammoth from 'mammoth';
import pdf from 'pdf-parse';

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';

import {
    collectNavigationRefs,
    resolveViewAttributes,
    runGuardedViewMutation,
    sanitiseFileNameComponent,
    type PageDeletionConfirmation,
    type SceneNode,
    type SceneViewLinks,
    type ViewMutationAction,
    type ViewMutationDeps,
    type ViewMutationRequest,
} from './view-safety.js';

/** How long to wait for a human to answer a cascade-delete prompt. */
const CASCADE_CONFIRMATION_TIMEOUT_MS = 300_000;

/** Makes snapshot filenames unique within a process, alongside the ms timestamp. */
let snapshotSequence = 1;

type AppConfig = {
    appKey: string;
    appName?: string;
    appId: string;
    apiBase?: string;
    notes?: string;
    builderAccountSlug?: string;
    builderAppSlug?: string;
    readonly?: boolean;
    allowViewMutation?: boolean;
    allowDelete?: boolean;
    allowDiagnostics?: boolean;

    /** Optional read policy for installations that handle sensitive data. */

    dataAccess?: {
        /** When present, only these objects can be read through record tools. */

        allowedObjectKeys?: string[];

        /** When present for an object, only these fields are returned from records. */

        allowedFieldKeys?: Record<string, string[]>;

        /** Fields that must never be returned, even when otherwise allowed. */

        redactedFieldKeys?: string[];

        /** Upper bound for records returned or scanned by one read tool call. */

        maxRecordsPerQuery?: number;
    };

    appFolder: string;
};

type ServerOptions = {
    /** A hard boundary used by the director-facing launcher. */

    readOnly?: boolean;
};

type SecretsMap = Record<string, string>;

type CachedField = {
    key: string;
    name?: string;
    type?: string;
    required?: boolean;
    description?: string;
    connectedObject?: string;
    choiceOptions?: string[];
    allowsMultiple?: boolean;
};

type CachedObject = {
    key: string;
    name?: string;
    fields?: CachedField[];
};

type CachedSchema = {
    objects?: CachedObject[];
};

type CachedFieldMapEntry = {
    fieldKey: string;
    fieldType?: string | null;
};

type CachedFieldMap = Record<string, CachedFieldMapEntry>;

type CachedViewMap = Record<string, Record<string, unknown>>;

type ViewFieldSettings = {
    fieldKey: string;
    fieldType?: string;
    label?: string;
    objectRequired?: boolean;
    readOnly?: boolean;
    defaults?: Record<string, unknown>;
    rules?: unknown[];
    layout: 'form-input' | 'search-field' | 'view-column';
    sourcePath: string;
};

type ViewFieldSettingsSummary = {
    configuredFieldCount: number;
    requiredFieldCount: number;
    readOnlyFieldCount: number;
    fields: ViewFieldSettings[];
    viewRules?: unknown;
};

type ViewContextMap = Record<
    string,
    { sceneKey?: string; sceneName?: string; sceneSlug?: string }
>;

type SceneViewInfo = {
    viewKey: string;
    viewName: string | undefined;
    viewType: string | undefined;
};

type SceneInfo = {
    sceneKey: string;
    sceneName: string | undefined;
    sceneSlug: string | undefined;

    /**
     * The scene this one hangs off, when it is a child page. Required to work out
     * which pages a cascade delete takes with it — a doomed child page may own
     * children of its own.
     *
     * Knack writes a **slug** here, not a `scene_N` key, so it must be resolved
     * through the slug index rather than compared against `sceneKey`.
     */

    parentRef: string | undefined;
    views: SceneViewInfo[];
};

type FieldReference = {
    fieldKey: string;
    sourceType: 'schema' | 'fieldMap' | 'viewMap';
    matchType: 'definition' | 'value' | 'propertyKey' | 'alias';
    path: string;
    classification: string[];
    containingText?: string | null;
    objectKey?: string;
    objectName?: string;
    fieldName?: string;
    alias?: string;
    viewKey?: string;
    viewName?: string;
    viewType?: string;
    sceneKey?: string;
    sceneName?: string;
    sceneSlug?: string;
};

type CachedFieldReferenceIndex = Record<string, FieldReference[]>;

type CacheSource = 'runtime' | 'file';

type CacheEntry<T> = {
    value: T;
    source: CacheSource;
    loadedAt: number;
    expiresAt: number;
};

type RuntimeMetadata = Record<string, unknown>;

type TemplateFieldDescriptor = {
    key: string;
    name: string;
    type: string;
    /**
     * The connection field this column reaches through, when the column shows a
     * field belonging to a connected record rather than to the view's own object.
     * Emitted as `connection: { key }` beside the column's own `field: { key }`.
     *
     * Measured 2026-09-04 from two builder copy requests: six of fourteen columns
     * in one table carried it, all through the same connection. It is the
     * reference a repoint most often misses, because changing the view's source
     * leaves these untouched and the columns keep rendering values from the old
     * relationship.
     */
    connectionKey?: string;
};

const NON_FORM_FIELD_TYPES = new Set([
    'auto_increment',
    'sum',
    'count',
    'average',
    'min',
    'max',
    'equation',
    'concatenation',
]);

const DEFAULT_API_BASE = 'https://api.knack.com/v1';
const DEFAULT_CACHE_TTL_MS = 5 * 60 * 1000;

const ENV_KNACK_APPS_DIR = process.env.KNACK_APPS_DIR; // e.g. C:\Work\KnackApps
const ENV_SECRETS_PATH = process.env.KNACK_MCP_SECRETS_PATH; // e.g. C:\Users\you\.knack-mcp-secrets.json
const ENV_DEBUG = process.env.DEBUG;
const ENV_CACHE_TTL_MS = process.env.KNACK_CACHE_TTL_MS;
const ENV_MAX_RESPONSE_BYTES = process.env.KNACK_MAX_RESPONSE_BYTES;
const ENV_COMPACT_TOOL_METADATA = process.env.KNACK_MCP_COMPACT_TOOL_METADATA;
const ENV_PRETTY_TOOL_JSON = process.env.KNACK_MCP_PRETTY_TOOL_JSON;
const ENV_MAX_TOOL_TEXT_BYTES = process.env.KNACK_MCP_MAX_TOOL_TEXT_BYTES;
const ENV_MAX_INLINE_DETAIL_BYTES =
    process.env.KNACK_MCP_MAX_INLINE_DETAIL_BYTES;
const ENV_MAX_EXTRACTED_TEXT_BYTES =
    process.env.KNACK_MCP_MAX_EXTRACTED_TEXT_BYTES;
const ENV_BATCH_CONCURRENCY = process.env.KNACK_MCP_BATCH_CONCURRENCY;

type NodeFetchHeaders = {
    get(name: string): string | null;
};

type NodeFetchResponseLike = {
    ok: boolean;
    status: number;
    headers: NodeFetchHeaders;
    body: null;
    text(): Promise<string>;
};

/**
 * Normalise supported Fetch header inputs for the legacy Node 16 fallback.
 *
 * @param headers Fetch request headers.
 * @returns Plain Node HTTP request headers.
 */
function normaliseNodeFetchHeaders(
    headers: RequestInit['headers'],
): Record<string, string> {
    if (!headers) return {};

    if (Array.isArray(headers)) {
        return headers.reduce<Record<string, string>>(
            (result, [key, value]) => {
                result[String(key)] = String(value);
                return result;
            },
            {},
        );
    }

    if (typeof Headers !== 'undefined' && headers instanceof Headers) {
        const result: Record<string, string> = {};
        headers.forEach((value, key) => {
            result[key] = value;
        });
        return result;
    }

    return Object.entries(headers).reduce<Record<string, string>>(
        (result, [key, value]) => {
            if (value === undefined) return result;
            result[key] = String(value);
            return result;
        },
        {},
    );
}

/**
 * Preserve best-effort compatibility for existing Node 16 MCP launchers while Node 18+ remains

 * the supported runtime. Newer Node releases already provide fetch and bypass this fallback.

 * The fallback supports the current text-only call sites; it intentionally has no abort,

 * JSON-body, or redirect handling.

 *
 * @returns void
 */
function installLegacyFetchFallback(): void {
    if (typeof globalThis.fetch === 'function') return;

    globalThis.fetch = (async (input: string | URL, init?: RequestInit) => {
        const requestUrl = String(input);
        const url = new URL(requestUrl);
        const transport = url.protocol === 'http:' ? http : https;
        const headers = normaliseNodeFetchHeaders(init?.headers);
        const body =
            init?.body == null
                ? undefined
                : typeof init.body === 'string'
                  ? init.body
                  : Buffer.isBuffer(init.body)
                    ? init.body
                    : init.body instanceof Uint8Array
                      ? Buffer.from(init.body)
                      : String(init.body);

        return await new Promise<NodeFetchResponseLike>((resolve, reject) => {
            const req = transport.request(
                requestUrl,
                {
                    method: init?.method || 'GET',
                    headers,
                },
                (res) => {
                    const chunks: Buffer[] = [];

                    res.on('data', (chunk) => {
                        chunks.push(
                            Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk),
                        );
                    });

                    res.on('end', () => {
                        const textBody = Buffer.concat(chunks).toString('utf8');

                        resolve({
                            ok:
                                !!res.statusCode &&
                                res.statusCode >= 200 &&
                                res.statusCode < 300,
                            status: res.statusCode || 0,
                            headers: {
                                get(name: string) {
                                    const headerValue =
                                        res.headers[name.toLowerCase()];

                                    if (Array.isArray(headerValue))
                                        return headerValue.join(', ');
                                    return headerValue == null
                                        ? null
                                        : String(headerValue);
                                },
                            },
                            body: null,
                            async text() {
                                return textBody;
                            },
                        });
                    });

                    res.on('error', reject);
                },
            );

            req.on('error', reject);

            if (body !== undefined) {
                req.write(body);
            }

            req.end();
        });
    }) as typeof fetch;
}

installLegacyFetchFallback();

function isEnabledEnv(
    value: string | undefined,
    defaultValue: boolean,
): boolean {
    if (!value) return defaultValue;
    const normalised = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalised)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalised)) return false;
    return defaultValue;
}

function getPositiveIntEnv(
    value: string | undefined,
    fallback: number,
): number {
    if (!value) return fallback;
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    return Math.trunc(parsed);
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Run `worker` over `items` with at most `concurrency` in flight at once. Results are
 * returned in the same order as `items` regardless of completion order. Used by batch
 * record tools to overlap several Knack API calls instead of running fully sequentially.
 */
async function runWithConcurrency<T, R>(
    items: T[],
    concurrency: number,
    worker: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
    const results: R[] = new Array(items.length);
    let nextIndex = 0;

    async function runNext(): Promise<void> {
        while (nextIndex < items.length) {
            const currentIndex = nextIndex;
            nextIndex += 1;
            results[currentIndex] = await worker(
                items[currentIndex],
                currentIndex,
            );
        }
    }

    const workerCount = Math.max(1, Math.min(concurrency, items.length));
    await Promise.all(Array.from({ length: workerCount }, () => runNext()));

    return results;
}

const DEBUG_ENABLED = isEnabledEnv(ENV_DEBUG, false);
const CACHE_TTL_MS = (() => {
    if (!ENV_CACHE_TTL_MS) return DEFAULT_CACHE_TTL_MS;
    const ttl = Number(ENV_CACHE_TTL_MS);
    if (!Number.isFinite(ttl) || ttl <= 0) return DEFAULT_CACHE_TTL_MS;
    return Math.trunc(ttl);
})();

const DEFAULT_MAX_RESPONSE_BYTES = 20 * 1024 * 1024;
const MAX_RESPONSE_BYTES = getPositiveIntEnv(
    ENV_MAX_RESPONSE_BYTES,
    DEFAULT_MAX_RESPONSE_BYTES,
);
const MAX_ATTACHMENT_REDIRECTS = 5;
const DEFAULT_MAX_TOOL_TEXT_BYTES = 256 * 1024;
const MAX_TOOL_TEXT_BYTES = getPositiveIntEnv(
    ENV_MAX_TOOL_TEXT_BYTES,
    DEFAULT_MAX_TOOL_TEXT_BYTES,
);
const DEFAULT_MAX_INLINE_DETAIL_BYTES = 48 * 1024;
const MAX_INLINE_DETAIL_BYTES = getPositiveIntEnv(
    ENV_MAX_INLINE_DETAIL_BYTES,
    DEFAULT_MAX_INLINE_DETAIL_BYTES,
);
const DEFAULT_MAX_EXTRACTED_TEXT_BYTES = 192 * 1024;
const MAX_EXTRACTED_TEXT_BYTES = getPositiveIntEnv(
    ENV_MAX_EXTRACTED_TEXT_BYTES,
    DEFAULT_MAX_EXTRACTED_TEXT_BYTES,
);
const COMPACT_TOOL_METADATA = isEnabledEnv(ENV_COMPACT_TOOL_METADATA, true);
const PRETTY_TOOL_JSON = isEnabledEnv(ENV_PRETTY_TOOL_JSON, false);
const DEFAULT_BATCH_CONCURRENCY = 5;
const BATCH_CONCURRENCY = Math.min(
    10,
    getPositiveIntEnv(ENV_BATCH_CONCURRENCY, DEFAULT_BATCH_CONCURRENCY),
);

/**
 * Build a compact summary when a tool response would be too large to send efficiently.
 */
function summariseLargeValue(value: unknown, depth = 0): unknown {
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
function serialiseToolPayload(data: unknown): string {
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
function getInlineDetail(
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
 * Shorten verbose manifest descriptions to keep the tool catalogue cheaper for token-based clients.
 */
function compactToolDescription(name: string, description: string): string {
    if (!COMPACT_TOOL_METADATA) return description;

    const trimmed = description.trim().replace(/\s+/g, ' ');
    if (trimmed.length <= 96) return trimmed;

    const label = name
        .replace(/^knack_/, '')
        .replace(/_/g, ' ')
        .trim();

    const compact = label ? `Knack ${label}.` : trimmed;
    return compact.length <= 96 ? compact : `${trimmed.slice(0, 93)}...`;
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

/**
 * Which code this process is actually running.
 *
 * Three separate incidents traced back to the same blind spot: a client showing an
 * older response shape than the checkout it was pointed at. A branch that never
 * merged, a `dist/` that was never rebuilt, and a long-lived server process that
 * predated a `git checkout` all present identically — a missing key — and none of
 * them can be told apart from the response itself. So the server states its own
 * identity, and a stale build says so instead of leaving it to be inferred.
 */

/** Captured once at module load: the moment this process started running. */
const SERVER_STARTED_AT = new Date().toISOString();

/** Directory this module was loaded from — `src/` under tsx, `dist/` when compiled. */
const SERVER_MODULE_PATH = fileURLToPath(import.meta.url);
const SERVER_MODULE_DIR = path.dirname(SERVER_MODULE_PATH);

/**
 * Feature markers, so a caller can ask "does this build have X" without knowing
 * commit hashes. Hand-maintained: add a marker when a feature a caller could
 * reasonably check for lands, and never remove one without removing the feature.
 */
const SERVER_FEATURES = [
    'cascade-delete-guard',
    'human-confirmation',
    'list-apps-banner',
    'mutation-snapshots',
    'server-build-identity',
];

/**
 * Read the package version without assuming a build layout.
 *
 * `package.json` sits one level above both `src/` and `dist/`, so the same relative
 * lookup works whether this is TypeScript under tsx or compiled JavaScript.
 *
 * @returns The declared version, or null if it cannot be read.
 */
function readPackageVersion(): string | null {
    const pkg = readJsonFile<{ version?: unknown }>(
        path.resolve(SERVER_MODULE_DIR, '..', 'package.json'),
    );
    return typeof pkg?.version === 'string' ? pkg.version : null;
}

/**
 * Resolve the `.git` directory for a checkout, following the worktree indirection.
 *
 * A linked worktree or submodule has `.git` as a file containing `gitdir: <path>`
 * rather than a directory, so the plain existence check is not enough.
 *
 * @param startDir Directory to start walking up from.
 * @returns Absolute path to the git directory, or null if none is found.
 */
function findGitDir(startDir: string): string | null {
    let current = path.resolve(startDir);

    for (let depth = 0; depth < 12; depth += 1) {
        const candidate = path.join(current, '.git');

        try {
            const stat = fs.statSync(candidate);
            if (stat.isDirectory()) return candidate;
            if (stat.isFile()) {
                const pointer = fs.readFileSync(candidate, 'utf8').trim();
                const match = /^gitdir:\s*(.+)$/.exec(pointer);
                if (match) return path.resolve(current, match[1].trim());
            }
        } catch {
            // Not here; keep walking up.
        }

        const parent = path.dirname(current);
        if (parent === current) break;
        current = parent;
    }

    return null;
}

/**
 * Explain a persist that was asked for and did not happen.
 *
 * `persistFiles` reads as an outcome and is only a request. Nothing can be written
 * until the metadata has been fetched, so the persist step lives inside the warm
 * branch — and with `warm: false` the caches are cleared, nothing is re-read, and
 * nothing reaches disk. The response echoed `persistFiles: true` beside `warm: false`
 * regardless, which claims files were written when none were. That is the failure this
 * server exists to prevent, in a diagnostic tool of all places: it cost a whole-app
 * referrer scan, which read a `viewMap.json` written before the fixture existed and
 * reported every count as zero.
 *
 * @param warm Whether the caller asked for the caches to be re-read.
 * @param persistFiles Whether the caller asked for the result to be written out.
 * @returns The reason nothing was written, or null when the question does not arise.
 */
export function describePersistOutcome(
    warm: boolean,
    persistFiles: boolean,
): string | null {
    if (!persistFiles || warm) return null;
    return 'Nothing was written. persistFiles only takes effect with warm: true — the caches were cleared, but no metadata was fetched to persist. Re-run with warm: true if you need the files on disk refreshed.';
}

/**
 * Report whether the running build is older than the checkout it sits in.
 *
 * `readGitIdentity` reads `.git` at call time, so on a compiled runtime the commit it
 * returns is the **checkout's**, not the build's — `dist/` can have been compiled from
 * an entirely different commit and nothing in the identity would say so. That gap is
 * not theoretical: a menu test was run against a build three commits behind the branch
 * it reported, and the reported commit is exactly what made it look current.
 *
 * Comparing modification times closes it without a build step. The running module file
 * is written by `tsc`; `.git/HEAD` and the ref it names are rewritten by checkout,
 * commit and pull. If either moved after the module was written, the source has
 * changed since this build and the commit above describes code that is not running.
 *
 * @param modulePath The file this process was loaded from.
 * @param runtime Whether that file is source or a build artefact.
 * @returns True when the checkout has moved on, false when it has not, null when it
 *     cannot be told — an unknown answer being better than a confident wrong one.
 */
export function detectStaleBuild(
    modulePath: string,
    runtime: 'typescript' | 'compiled',
): boolean | null {
    // Running the source directly means there is no build to be behind.
    if (runtime === 'typescript') return false;

    const gitDir = findGitDir(path.dirname(modulePath));
    if (!gitDir) return null;

    const mtime = (target: string): number | null => {
        try {
            return fs.statSync(target).mtimeMs;
        } catch {
            return null;
        }
    };

    const builtAt = mtime(modulePath);
    if (builtAt === null) return null;

    // HEAD alone is not enough. It changes when you switch branches, but a pull that
    // fast-forwards the branch you are already on rewrites the ref file instead — and
    // that is the common way to end up with a stale build.
    const candidates = [path.join(gitDir, 'HEAD')];
    try {
        const head = fs.readFileSync(path.join(gitDir, 'HEAD'), 'utf8').trim();
        const refMatch = /^ref:\s*(.+)$/.exec(head);
        if (refMatch) {
            candidates.push(
                path.join(gitDir, ...refMatch[1].trim().split('/')),
            );
        }
    } catch {
        // HEAD unreadable; the candidate list still holds its path, which will fail
        // its own stat below and leave the answer unknown rather than wrong.
    }

    const touched = candidates
        .map(mtime)
        .filter((value): value is number => value !== null);
    if (touched.length === 0) return null;

    return Math.max(...touched) > builtAt;
}

/**
 * Report the branch and commit this process's source was loaded from.
 *
 * Read from `.git` directly rather than by shelling out to `git`, so it cannot hang
 * startup or fail on a machine without the binary. Every failure degrades to null —
 * an unknown commit is a worse diagnostic than a known one, but it is not an error.
 *
 * @param startDir Directory to resolve the checkout from. Defaults to this module's.
 * @returns Branch and commit, or null when this is not a git checkout.
 */
export function readGitIdentity(
    startDir: string = SERVER_MODULE_DIR,
): { branch: string | null; commit: string | null } | null {
    const gitDir = findGitDir(startDir);
    if (!gitDir) return null;

    let head: string;
    try {
        head = fs.readFileSync(path.join(gitDir, 'HEAD'), 'utf8').trim();
    } catch {
        return null;
    }

    // Detached HEAD holds the commit itself rather than a ref to follow.
    if (/^[0-9a-f]{40}$/i.test(head)) {
        return { branch: null, commit: head.slice(0, 7) };
    }

    const refMatch = /^ref:\s*(.+)$/.exec(head);
    if (!refMatch) return null;

    const ref = refMatch[1].trim();
    const branch = ref.replace(/^refs\/heads\//, '');

    // A loose ref is a file; once packed, it only exists inside packed-refs.
    try {
        const loose = fs
            .readFileSync(path.join(gitDir, ...ref.split('/')), 'utf8')
            .trim();
        if (/^[0-9a-f]{40}$/i.test(loose)) {
            return { branch, commit: loose.slice(0, 7) };
        }
    } catch {
        // Fall through to packed-refs.
    }

    try {
        const packed = fs.readFileSync(
            path.join(gitDir, 'packed-refs'),
            'utf8',
        );
        for (const line of packed.split('\n')) {
            const [sha, name] = line.trim().split(/\s+/);
            if (name === ref && /^[0-9a-f]{40}$/i.test(sha ?? '')) {
                return { branch, commit: sha.slice(0, 7) };
            }
        }
    } catch {
        // No packed-refs either.
    }

    return { branch, commit: null };
}

export type ServerBuildIdentity = {
    name: string;
    version: string | null;
    mode: 'full' | 'readonly';
    runtime: 'typescript' | 'compiled';
    entryPath: string | null;
    moduleDir: string;
    git: { branch: string | null; commit: string | null } | null;
    /**
     * Whether `git` above describes code that is not actually running.
     *
     * True means the checkout moved after this build was compiled, so the commit is
     * the source tree's rather than the build's. Null means it could not be told.
     */
    sourceNewerThanBuild: boolean | null;
    startedAt: string;
    features: string[];
};

/**
 * Describe this process so a caller can tell a stale server from a current one.
 *
 * @param enforcedReadOnly Whether the server was started in enforced read-only mode.
 * @returns The build identity reported alongside every app listing.
 */
export function describeServerBuild(
    enforcedReadOnly: boolean,
): ServerBuildIdentity {
    return {
        name: 'knack-mcp',
        version: readPackageVersion(),
        mode: enforcedReadOnly ? 'readonly' : 'full',
        // The extension of this module is the only honest answer: a `dist/` build and
        // tsx running `src/` are exactly the confusion this field exists to settle.
        runtime: import.meta.url.endsWith('.ts') ? 'typescript' : 'compiled',
        entryPath: process.argv[1] ?? null,
        moduleDir: SERVER_MODULE_DIR,
        git: readGitIdentity(),
        sourceNewerThanBuild: detectStaleBuild(
            SERVER_MODULE_PATH,
            import.meta.url.endsWith('.ts') ? 'typescript' : 'compiled',
        ),
        startedAt: SERVER_STARTED_AT,
        features: [...SERVER_FEATURES],
    };
}

/**
 * Render the build identity as one line, for the banner and the startup log.
 *
 * @param build The identity to render.
 * @returns A single line naming version, mode, runtime, commit and start time.
 */
export function summariseServerBuild(build: ServerBuildIdentity): string {
    const parts = [
        `${build.name}${build.version ? ` ${build.version}` : ''}`,
        `${build.mode} mode`,
        build.runtime === 'typescript'
            ? 'TypeScript source'
            : 'compiled JavaScript',
    ];

    if (build.git) {
        const branch = build.git.branch ?? 'detached HEAD';
        parts.push(
            build.git.commit ? `${branch} @ ${build.git.commit}` : branch,
        );
    }

    parts.push(`started ${build.startedAt}`);

    // The commit is the checkout's, and on a stale build that is a different thing
    // from what is running. Saying so here matters more than in the payload: this
    // line is what goes to stderr at startup and leads the app listing.
    const staleWarning =
        build.sourceNewerThanBuild === true
            ? ` WARNING: the checkout has changed since this build was compiled, so ${
                  build.git?.commit ?? 'the commit above'
              } describes the source tree and not the code running. Rebuild before trusting it.`
            : '';

    return `Build: ${parts.join(', ')}. Loaded from ${build.moduleDir}.${staleWarning}`;
}

function normalisePath(p: string): string {
    // Normalise for Windows/Mac comparisons
    return path.resolve(p).replaceAll('\\', '/').toLowerCase();
}

function normaliseAppIdentity(value: string): string {
    return value
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '');
}

function readJsonFile<T>(filePath: string): T | null {
    try {
        const raw = fs.readFileSync(filePath, 'utf8');
        return JSON.parse(raw) as T;
    } catch {
        return null;
    }
}

function writeJsonFile(
    filePath: string,
    data: unknown,
): { ok: true } | { ok: false; error: string } {
    try {
        fs.mkdirSync(path.dirname(filePath), { recursive: true });
        fs.writeFileSync(
            filePath,
            `${JSON.stringify(data, null, 2)}\n`,
            'utf8',
        );
        return { ok: true };
    } catch (error) {
        return {
            ok: false,
            error: error instanceof Error ? error.message : String(error),
        };
    }
}

function asRecord(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value))
        return null;
    return value as Record<string, unknown>;
}

/**
 * Recursively merge plain-object properties (e.g. format, relationship) so a dry-run preview
 * of a partial update — {format: {precision: "2"}} — keeps sibling keys instead of replacing
 * the whole nested object, matching how a caller reads "merged" intuitively.
 *
 * @param base Current value (e.g. the live field definition).
 * @param updates Partial value to layer on top.
 * @returns A new object with updates applied, merging nested plain objects recursively.
 */
function deepMergeRecords(
    base: Record<string, unknown>,
    updates: Record<string, unknown>,
): Record<string, unknown> {
    const merged: Record<string, unknown> = { ...base };
    for (const [key, value] of Object.entries(updates)) {
        const baseRecord = asRecord(merged[key]);
        const updateRecord = asRecord(value);
        merged[key] =
            baseRecord && updateRecord
                ? deepMergeRecords(baseRecord, updateRecord)
                : value;
    }
    return merged;
}

type FieldPayloadPreflight = {
    payload: Record<string, unknown> | null;
    errors: string[];
};

/**
 * Parse a JSON object supplied to a field mutation tool without allowing arrays or primitives.
 *
 * @param value JSON text supplied by the MCP client.
 * @param label Input name used in validation feedback.
 * @returns The parsed object or a user-actionable validation error.
 */
function parseJsonObjectInput(
    value: string,
    label: string,
): FieldPayloadPreflight {
    try {
        const payload = asRecord(JSON.parse(value));
        return payload
            ? { payload, errors: [] }
            : { payload: null, errors: [`${label} must be a JSON object.`] };
    } catch (error) {
        return {
            payload: null,
            errors: [
                `${label} must be valid JSON: ${error instanceof Error ? error.message : String(error)}`,
            ],
        };
    }
}

/**
 * Check the minimum field payload contract locally before making a Builder API request.
 * Advanced Knack format settings remain pass-through so the MCP does not reject valid settings
 * that are not represented in its cached schema.
 *
 * @param payload Candidate field definition.
 * @param requireIdentity Whether both name and type are required, as they are for field creation.
 * @returns Validation errors. An empty array means the payload is safe to send to Knack.
 */
function validateFieldPayload(
    payload: Record<string, unknown>,
    requireIdentity: boolean,
): string[] {
    const errors: string[] = [];
    const hasName = Object.hasOwn(payload, 'name');
    const hasType = Object.hasOwn(payload, 'type');

    if (requireIdentity && !hasName) errors.push('Field name is required.');
    if (requireIdentity && !hasType) errors.push('Field type is required.');
    if (hasName && (typeof payload.name !== 'string' || !payload.name.trim())) {
        errors.push('Field name must be a non-empty string.');
    }
    if (hasType && (typeof payload.type !== 'string' || !payload.type.trim())) {
        errors.push('Field type must be a non-empty string.');
    }

    for (const property of ['format', 'relationship']) {
        if (
            Object.hasOwn(payload, property) &&
            asRecord(payload[property]) === null
        ) {
            errors.push(`${property} must be a JSON object when supplied.`);
        }
    }

    if (payload.type === 'connection') {
        const format = asRecord(payload.format);
        const relationship = asRecord(payload.relationship);
        const target = format?.object || relationship?.object;
        if (typeof target !== 'string' || !/^object_\d+$/i.test(target)) {
            errors.push(
                'Connection fields require format.object or relationship.object with an object key (for example object_12).',
            );
        }
    }

    return errors;
}

/**
 * Mirror a field's description into meta.description before it goes out over the wire.
 *
 * Knack's fields API does not reliably persist a bare top-level `description` on
 * create/update — verified in production use, where a top-level `description` silently
 * failed to stick and had to be resent under `meta.description` to actually take effect.
 * The runtime metadata endpoint (parseRuntimeSchema) already reads description from either
 * location, so writing to both keeps that read-side fallback correct while guaranteeing the
 * value actually persists. Mutates payload in place; a no-op when description isn't a string.
 *
 * @param payload Field create/update payload about to be sent to Knack.
 */
function normalizeFieldDescriptionForWrite(
    payload: Record<string, unknown>,
): void {
    if (typeof payload.description !== 'string') return;
    const existingMeta = asRecord(payload.meta) || {};
    payload.meta = { ...existingMeta, description: payload.description };
}

type AppOverviewRelationship = {
    fromObjectKey: string;
    fromObjectName: string | undefined;
    fieldKey: string;
    fieldName: string | undefined;
    toObjectKey: string;
    toObjectName: string;
};

type AppOverviewResult = {
    objectCount: number;
    totalFields: number;
    relationshipCount: number;
    objects: Array<Record<string, unknown>>;
    relationships: AppOverviewRelationship[];
};

/**
 * Build the object/field/connection summary shared by knack_get_app_overview and
 * knack_app_deep_dive, so the two tools cannot drift out of sync.
 *
 * @param schema Cached schema for the app.
 * @param includeFieldDetails When true, include every field's name/type per object (verbose).
 */
function buildAppOverview(
    schema: CachedSchema,
    includeFieldDetails: boolean,
): AppOverviewResult {
    const objects = schema.objects || [];
    const objectKeyToName = new Map<string, string>(
        objects.map((obj) => [obj.key, obj.name || obj.key]),
    );

    const relationships: AppOverviewRelationship[] = [];

    const objectSummaries = objects.map((obj) => {
        const fields = obj.fields || [];
        const typeCounts: Record<string, number> = {};
        for (const field of fields) {
            const t = field.type || 'unknown';
            typeCounts[t] = (typeCounts[t] || 0) + 1;
        }

        const connections = fields.filter((f) => f.type === 'connection');
        for (const cf of connections) {
            if (cf.connectedObject) {
                relationships.push({
                    fromObjectKey: obj.key,
                    fromObjectName: obj.name,
                    fieldKey: cf.key,
                    fieldName: cf.name,
                    toObjectKey: cf.connectedObject,
                    toObjectName:
                        objectKeyToName.get(cf.connectedObject) ||
                        cf.connectedObject,
                });
            }
        }

        const summary: Record<string, unknown> = {
            key: obj.key,
            name: obj.name,
            fieldCount: fields.length,
            connectionCount: connections.length,
            typeSummary: Object.entries(typeCounts)
                .map(([type, count]) => ({ type, count }))
                .sort((a, b) => b.count - a.count),
        };

        if (includeFieldDetails) {
            summary.fields = fields.map((f) => ({
                key: f.key,
                name: f.name,
                type: f.type,
                connectedObject: f.connectedObject || undefined,
            }));
        }

        return summary;
    });

    return {
        objectCount: objects.length,
        totalFields: objects.reduce(
            (sum, obj) => sum + (obj.fields || []).length,
            0,
        ),
        relationshipCount: relationships.length,
        objects: objectSummaries,
        relationships,
    };
}

type DataModelObjectMetric = {
    objectKey: string;
    objectName: string | undefined;
    fieldCount: number;
};

type DataModelAnalysis = {
    summary: {
        totalObjects: number;
        totalFields: number;
        avgFieldCount: number;
        minFieldCount: number;
        maxFieldCount: number;
        connectedObjectCount: number;
        isolatedObjectCount: number;
    };
    fieldTypeDistribution: Array<{
        type: string;
        count: number;
        percentage: number;
    }>;
    highFieldCountObjects: DataModelObjectMetric[];
    lowFieldCountObjects: DataModelObjectMetric[];
    isolatedObjects: DataModelObjectMetric[];
    observations: string[];
};

/**
 * Build the design-feedback analysis shared by knack_analyze_data_model and
 * knack_app_deep_dive, so the two tools cannot drift out of sync.
 *
 * @param schema Cached schema for the app.
 */
function buildDataModelAnalysis(schema: CachedSchema): DataModelAnalysis {
    const objects = schema.objects || [];
    const totalObjects = objects.length;
    const totalFields = objects.reduce(
        (sum, obj) => sum + (obj.fields || []).length,
        0,
    );

    const globalTypeCounts = new Map<string, number>();
    const objectMetrics = objects.map((obj) => {
        const fields = obj.fields || [];
        const typeCounts: Record<string, number> = {};
        for (const field of fields) {
            const t = field.type || 'unknown';
            typeCounts[t] = (typeCounts[t] || 0) + 1;
            globalTypeCounts.set(t, (globalTypeCounts.get(t) || 0) + 1);
        }
        const connectionCount = fields.filter(
            (f) => f.type === 'connection',
        ).length;
        return {
            objectKey: obj.key,
            objectName: obj.name,
            fieldCount: fields.length,
            connectionCount,
            typeCounts,
        };
    });

    const avgFieldCount = totalObjects
        ? Math.round(totalFields / totalObjects)
        : 0;
    const maxFieldCount = objectMetrics.reduce(
        (max, m) => Math.max(max, m.fieldCount),
        0,
    );
    const minFieldCount =
        objectMetrics.reduce(
            (min, m) => Math.min(min, m.fieldCount),
            Infinity,
        ) === Infinity
            ? 0
            : objectMetrics.reduce(
                  (min, m) => Math.min(min, m.fieldCount),
                  Infinity,
              );

    const connectedObjectKeys = new Set<string>(
        objects.flatMap((obj) =>
            (obj.fields || [])
                .filter((f) => f.type === 'connection' && f.connectedObject)
                .flatMap((f) => [obj.key, f.connectedObject as string]),
        ),
    );

    // Consistent with connectedObjectKeys above: an object counts as "connected" if it
    // owns a connection field OR is the target of one elsewhere in the schema. Using
    // m.connectionCount === 0 here (own outgoing fields only) would let a pure
    // connection target — e.g. a core "Users" object other objects point at but that
    // has no outgoing connections itself — be reported as both connected (in
    // connectedObjectCount) and isolated (here) in the same response.
    const isolatedObjects = objectMetrics
        .filter((m) => !connectedObjectKeys.has(m.objectKey))
        .map((m) => ({
            objectKey: m.objectKey,
            objectName: m.objectName,
            fieldCount: m.fieldCount,
        }));

    // Objects are flagged as high-field when they exceed twice the app average or the absolute
    // minimum of 30 fields, whichever is larger. 30 is chosen as a practical Knack threshold
    // above which a single object often becomes hard to maintain.
    const MIN_HIGH_FIELD_THRESHOLD = 30;
    const highFieldThreshold = Math.max(
        avgFieldCount * 2,
        MIN_HIGH_FIELD_THRESHOLD,
    );
    const highFieldCountObjects = objectMetrics
        .filter((m) => m.fieldCount >= highFieldThreshold)
        .map((m) => ({
            objectKey: m.objectKey,
            objectName: m.objectName,
            fieldCount: m.fieldCount,
        }))
        .sort((a, b) => b.fieldCount - a.fieldCount);

    // Objects with 2 or fewer fields are flagged as potentially stub/lookup tables.
    // Knack auto-creates a primary text field for every object, so ≤ 2 means only
    // that auto-field plus at most one user-added field — a likely placeholder or lookup list.
    const LOW_FIELD_COUNT_THRESHOLD = 2;
    const lowFieldCountObjects = objectMetrics
        .filter((m) => m.fieldCount <= LOW_FIELD_COUNT_THRESHOLD)
        .map((m) => ({
            objectKey: m.objectKey,
            objectName: m.objectName,
            fieldCount: m.fieldCount,
        }));

    const fieldTypeDistribution = [...globalTypeCounts.entries()]
        .map(([type, count]) => ({
            type,
            count,
            percentage: totalFields
                ? Math.round((count / totalFields) * 100)
                : 0,
        }))
        .sort((a, b) => b.count - a.count);

    const connectionPct = totalObjects
        ? Math.round((connectedObjectKeys.size / totalObjects) * 100)
        : 0;
    const observations: string[] = [];
    if (isolatedObjects.length > 0) {
        observations.push(
            `${isolatedObjects.length} object(s) have no connections at all (neither an outgoing connection field nor being the target of one elsewhere) — they may be standalone lookup tables or unused.`,
        );
    }
    if (highFieldCountObjects.length > 0) {
        observations.push(
            `${highFieldCountObjects.length} object(s) exceed ${highFieldThreshold} fields — consider whether any could be split into related objects.`,
        );
    }
    if (lowFieldCountObjects.length > 0) {
        observations.push(
            `${lowFieldCountObjects.length} object(s) have ≤ ${LOW_FIELD_COUNT_THRESHOLD} fields — these may be stub/placeholder tables or simple lookup lists.`,
        );
    }
    observations.push(
        `${connectionPct}% of objects participate in at least one connection relationship.`,
    );

    return {
        summary: {
            totalObjects,
            totalFields,
            avgFieldCount,
            minFieldCount,
            maxFieldCount,
            connectedObjectCount: connectedObjectKeys.size,
            isolatedObjectCount: isolatedObjects.length,
        },
        fieldTypeDistribution,
        highFieldCountObjects,
        lowFieldCountObjects,
        isolatedObjects,
        observations,
    };
}

/**
 * Reminder attached to schema-mutating tool responses: nothing in this server invalidates
 * the in-memory/on-disk schema cache automatically, so cached-schema tools can silently
 * return pre-mutation data until a refresh is run.
 */
const SCHEMA_CACHE_STALE_NOTE =
    'Schema cache not auto-invalidated — run knack_refresh_cache(warm:true) before trusting cached-schema tools.';

/**
 * Reminder attached to scene/view-mutating tool responses, for the same reason as
 * SCHEMA_CACHE_STALE_NOTE but for the scene/view cache.
 */
const VIEW_CACHE_STALE_NOTE =
    'View cache not auto-invalidated — run knack_refresh_cache(warm:true) before trusting cached-view tools.';

/**
 * Reminder attached to knack_update_field responses (dry-run and live) whenever the
 * update touches format/relationship: whether Knack's PUT merges or fully replaces a
 * partial nested object has not been independently verified.
 */
const NESTED_MERGE_UNCERTAINTY_NOTE =
    "Knack's merge behaviour for partial format/relationship objects is unverified — check knack_get_field afterwards.";

type FieldWriteMatchCriteria =
    { fieldKey: string } | { name: string; type: string };

/**
 * Locate the field a create/update field request just touched inside Knack's raw write
 * response. Most field writes return a compact `{ field: {...} }` body, but Knack's API
 * returns the full application schema (every object's field list) for connection-field
 * writes, since a connection also updates the cross-object relationship graph — that body
 * can run into tens of thousands of characters. This searches whichever shape the response
 * actually took so the caller can project a huge response down to just the touched field.
 *
 * @param body Raw Knack API response body.
 * @param objectKey Object the field write targeted.
 * @param criteria Match by fieldKey (updates, where the key is already known) or by
 *   name+type (creates, where Knack assigns the key).
 * @returns The matching field record, or undefined if the shape wasn't recognised.
 */
function findFieldInFieldWriteResponse(
    body: unknown,
    objectKey: string,
    criteria: FieldWriteMatchCriteria,
): Record<string, unknown> | undefined {
    const root = asRecord(body);
    if (!root) return undefined;

    const matchesCriteria = (field: Record<string, unknown>): boolean =>
        'fieldKey' in criteria
            ? field.key === criteria.fieldKey
            : field.name === criteria.name && field.type === criteria.type;

    const directField = asRecord(root.field);
    if (directField && matchesCriteria(directField)) return directField;

    const objectsContainer =
        asRecord(root.application)?.objects ?? root.objects;
    const objects = Array.isArray(objectsContainer) ? objectsContainer : [];
    for (const objEntry of objects) {
        const obj = asRecord(objEntry);
        if (!obj || obj.key !== objectKey) continue;
        const fields = Array.isArray(obj.fields) ? obj.fields : [];
        const matches = fields
            .map((f) => asRecord(f))
            .filter((f): f is Record<string, unknown> =>
                Boolean(f && matchesCriteria(f)),
            );
        // fieldKey is a genuine unique identifier, so a single match is trustworthy
        // (more than one would mean corrupted data, not a realistic case). name+type
        // is not unique within an object (Knack allows duplicate field names) — with
        // more than one match there is no reliable way to tell which entry is the one
        // just created, so return undefined rather than guess in either case.
        if (matches.length === 1) return matches[0];
    }

    return undefined;
}

type EquationTokenCheck = {
    errors: string[];
    warnings: string[];
};

const FIELD_KEY_PATTERN = /^field_\d+$/i;
const FIELD_ALIAS_OBJECT_FIELD_KEY_PATTERN = /^(object_\d+)\.(field_\d+)$/i;

/**
 * Validate the {...} reference tokens in an equation string against the cached schema.
 * Knack silently resolves an unmatched token to 0 rather than erroring, so catching bad
 * references here — before the write reaches a live app — is the only safety net available.
 *
 * @param schema Cached schema for the app the field belongs to.
 * @param objectKey Object the equation field lives on.
 * @param equation Raw equation string from format.equation.
 * @returns Errors for tokens that cannot resolve, and warnings for tokens that resolve unreliably.
 */
function validateEquationTokens(
    schema: CachedSchema,
    objectKey: string,
    equation: string,
): EquationTokenCheck {
    const errors: string[] = [];
    const warnings: string[] = [];

    const object = schema.objects?.find((entry) => entry.key === objectKey);
    if (!object) {
        warnings.push(
            `Could not validate equation tokens: object ${objectKey} was not found in the cached schema, so this write is going out unchecked. Run knack_refresh_cache and re-check if that is unexpected.`,
        );
        return { errors, warnings };
    }

    const fieldsByKey = new Map(
        (object.fields || []).map((field) => [field.key, field]),
    );
    const objectsByKey = new Map(
        (schema.objects || []).map((entry) => [entry.key, entry]),
    );

    const isCrossableConnection = (field: CachedField): boolean =>
        field.type === 'connection' &&
        Boolean(field.connectedObject) &&
        !field.allowsMultiple;

    const tokens = equation.match(/\{[^{}]+\}/g) || [];
    for (const rawToken of tokens) {
        const token = rawToken.slice(1, -1);
        const parts = token.split('.');

        if (parts.length === 1) {
            const [fieldKey] = parts;
            if (!FIELD_KEY_PATTERN.test(fieldKey)) {
                warnings.push(
                    `Token {${token}} looks name-based rather than a field key. Name-based tokens have been observed to resolve inconsistently (correct on one read, 0 on the next) — prefer {field_key}.`,
                );
                continue;
            }
            if (fieldsByKey.has(fieldKey)) continue;

            let hint = '';
            for (const field of object.fields || []) {
                if (!isCrossableConnection(field) || !field.connectedObject) {
                    continue;
                }
                const connectedObject = objectsByKey.get(field.connectedObject);
                if (
                    connectedObject?.fields?.some(
                        (candidate) => candidate.key === fieldKey,
                    )
                ) {
                    hint = ` It exists on connected object ${field.connectedObject} — did you mean {${field.key}.${fieldKey}}?`;
                    break;
                }
            }
            errors.push(
                `Token {${token}} does not match any field on ${objectKey}.${hint}`,
            );
            continue;
        }

        if (parts.length === 2) {
            const [connectionKey, targetKey] = parts;

            if (/^object_\d+$/i.test(connectionKey)) {
                errors.push(
                    `Token {${token}} qualifies by object key (${connectionKey}), which equations do not accept. Use {connection_field_key.target_field_key} instead — the connection *field* on ${objectKey} that points at ${connectionKey}, not the object key itself.`,
                );
                continue;
            }

            if (
                !FIELD_KEY_PATTERN.test(connectionKey) ||
                !FIELD_KEY_PATTERN.test(targetKey)
            ) {
                warnings.push(
                    `Token {${token}} looks name-based rather than {connection_field_key.target_field_key}. Name-based tokens have been observed to resolve inconsistently — prefer the field-key form.`,
                );
                continue;
            }

            const connectionField = fieldsByKey.get(connectionKey);
            if (!connectionField) {
                errors.push(
                    `Token {${token}}: ${connectionKey} is not a field on ${objectKey}.`,
                );
                continue;
            }
            if (connectionField.type !== 'connection') {
                errors.push(
                    `Token {${token}}: ${connectionKey} is a ${connectionField.type ?? 'non-connection'} field on ${objectKey}, not a connection — only many-to-one / one-to-one connections can be crossed in an equation.`,
                );
                continue;
            }
            if (connectionField.allowsMultiple) {
                errors.push(
                    `Token {${token}}: ${connectionKey} allows multiple connected records (many-to-many or one-to-many) — Knack equations can only cross many-to-one / one-to-one connections.`,
                );
                continue;
            }
            if (!connectionField.connectedObject) {
                warnings.push(
                    `Token {${token}}: could not verify — connection field ${connectionKey} has no resolvable target object in the cached schema.`,
                );
                continue;
            }

            const connectedObject = objectsByKey.get(
                connectionField.connectedObject,
            );
            if (!connectedObject) {
                warnings.push(
                    `Token {${token}}: could not verify — connected object ${connectionField.connectedObject} is not in the cached schema.`,
                );
                continue;
            }

            const hasTarget = (connectedObject.fields || []).some(
                (candidate) => candidate.key === targetKey,
            );
            if (!hasTarget) {
                errors.push(
                    `Token {${token}}: field ${targetKey} does not exist on connected object ${connectionField.connectedObject} (via ${connectionKey}).`,
                );
            }
            continue;
        }

        warnings.push(
            `Token {${token}} has more than one "." and could not be validated.`,
        );
    }

    return { errors, warnings };
}

function getObjectAtPath(root: unknown, ...keys: string[]): unknown {
    let current: unknown = root;
    for (const key of keys) {
        const rec = asRecord(current);
        if (!rec || !(key in rec)) return null;
        current = rec[key];
    }
    return current;
}

function isRuntimeMetadataPayload(value: unknown): value is RuntimeMetadata {
    const payload = asRecord(value);
    if (!payload) return false;

    const hasApplication = asRecord(payload.application) !== null;
    const hasObjects = Array.isArray(payload.objects);
    const hasScenes = Array.isArray(payload.scenes);

    return hasApplication || hasObjects || hasScenes;
}

function getPublicApiBase(apiBase?: string): string {
    const base = (apiBase || DEFAULT_API_BASE).trim().replace(/\/+$/, '');
    return base.replace(/\/v1$/i, '');
}

function slugifyForBuilder(value: string): string {
    return value
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
}

function getBuilderSlugs(
    app: AppConfig,
    runtimeMetadata?: RuntimeMetadata | null,
): { accountSlug: string; appSlug: string } {
    const runtimeApplication = asRecord(
        getObjectAtPath(runtimeMetadata, 'application'),
    );
    const runtimeAccount = asRecord(runtimeApplication?.account);

    const runtimeAppSlug =
        typeof runtimeApplication?.slug === 'string'
            ? runtimeApplication.slug
            : typeof runtimeApplication?.name === 'string'
              ? slugifyForBuilder(runtimeApplication.name)
              : null;

    const runtimeAccountSlug =
        typeof runtimeAccount?.slug === 'string'
            ? runtimeAccount.slug
            : typeof runtimeApplication?.account_slug === 'string'
              ? runtimeApplication.account_slug
              : null;

    const fallbackSlug = slugifyForBuilder(app.appName || app.appKey);

    return {
        accountSlug:
            app.builderAccountSlug || runtimeAccountSlug || fallbackSlug,
        appSlug: app.builderAppSlug || runtimeAppSlug || fallbackSlug,
    };
}

function makeBuilderBaseUrl(
    app: AppConfig,
    runtimeMetadata?: RuntimeMetadata | null,
): string {
    const { accountSlug, appSlug } = getBuilderSlugs(app, runtimeMetadata);
    return `https://builder.knack.com/${accountSlug}/${appSlug}`;
}

function makeSceneBuilderUrl(
    app: AppConfig,
    sceneKey?: string,
    runtimeMetadata?: RuntimeMetadata | null,
): string | null {
    if (!sceneKey) return null;
    return `${makeBuilderBaseUrl(app, runtimeMetadata)}/pages/${sceneKey}`;
}

function makeViewBuilderUrl(
    app: AppConfig,
    params: { sceneKey?: string; viewKey?: string; viewType?: string },
    runtimeMetadata?: RuntimeMetadata | null,
): string | null {
    if (!params.sceneKey || !params.viewKey) return null;
    const viewTypeSegment = (params.viewType || 'view').trim().toLowerCase();
    return `${makeBuilderBaseUrl(app, runtimeMetadata)}/pages/${params.sceneKey}/views/${params.viewKey}/${viewTypeSegment}`;
}

function makeFieldBuilderUrl(
    app: AppConfig,
    params: { objectKey?: string; fieldKey?: string },
    runtimeMetadata?: RuntimeMetadata | null,
): string | null {
    if (!params.objectKey || !params.fieldKey) return null;
    return `${makeBuilderBaseUrl(app, runtimeMetadata)}/schema/list/objects/${params.objectKey}/fields/${params.fieldKey}/settings`;
}

function fileExists(filePath: string): boolean {
    try {
        fs.accessSync(filePath, fs.constants.F_OK);
        return true;
    } catch {
        return false;
    }
}

function makeCacheEntry<T>(value: T, source: CacheSource): CacheEntry<T> {
    const loadedAt = Date.now();
    return {
        value,
        source,
        loadedAt,
        expiresAt: loadedAt + CACHE_TTL_MS,
    };
}

function getCacheEntry<T>(
    cache: Map<string, CacheEntry<T>>,
    key: string,
): CacheEntry<T> | null {
    const entry = cache.get(key);
    if (!entry) return null;
    if (Date.now() >= entry.expiresAt) {
        cache.delete(key);
        return null;
    }
    return entry;
}

function debugLog(message: string, payload?: unknown): void {
    if (!DEBUG_ENABLED) return;
    if (payload === undefined) {
        console.error(`[knack-mcp] ${message}`);
        return;
    }
    try {
        console.error(`[knack-mcp] ${message}`, JSON.stringify(payload));
    } catch {
        console.error(`[knack-mcp] ${message}`, String(payload));
    }
}

function normaliseAlias(text: string): string {
    return text
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
}

function getFieldTypeByKey(
    schema: CachedSchema | null,
): Record<string, string | null> {
    const fieldTypeByKey: Record<string, string | null> = {};
    for (const obj of schema?.objects || []) {
        for (const field of obj.fields || []) {
            fieldTypeByKey[field.key] = field.type || null;
        }
    }
    return fieldTypeByKey;
}

function generateStrictFieldMapFromSchema(
    schema: CachedSchema,
): CachedFieldMap {
    const map: CachedFieldMap = {};
    const collidingAliases = new Set<string>();

    for (const obj of schema.objects || []) {
        for (const field of obj.fields || []) {
            const fieldName = (field.name || '').trim();
            if (!fieldName) continue;

            const alias = `${obj.key}.${normaliseAlias(fieldName)}`;
            if (!alias || !/^object_\d+\.[a-z0-9_]+$/.test(alias)) continue;

            const existing = map[alias];
            if (!existing) {
                map[alias] = {
                    fieldKey: field.key,
                    fieldType: field.type || null,
                };
                continue;
            }

            if (existing.fieldKey !== field.key) {
                collidingAliases.add(alias);
            }
        }
    }

    if (collidingAliases.size > 0) {
        debugLog('strict_fieldmap_alias_collisions_detected', {
            collisionCount: collidingAliases.size,
            sample: [...collidingAliases].slice(0, 50),
        });
    }

    return map;
}

function coerceFieldMap(
    value: unknown,
    schema: CachedSchema | null,
): CachedFieldMap | null {
    const raw = asRecord(value);
    if (!raw) return null;

    const fieldTypeByKey = getFieldTypeByKey(schema);
    const map: CachedFieldMap = {};

    for (const [alias, entry] of Object.entries(raw)) {
        if (typeof entry === 'string') {
            if (!/^field_\d+$/i.test(entry)) continue;
            map[alias] = {
                fieldKey: entry,
                fieldType: fieldTypeByKey[entry] ?? null,
            };
            continue;
        }

        const rec = asRecord(entry);
        if (!rec) continue;
        const fieldKey = typeof rec.fieldKey === 'string' ? rec.fieldKey : null;
        if (!fieldKey || !/^field_\d+$/i.test(fieldKey)) continue;
        const fieldType =
            typeof rec.fieldType === 'string'
                ? rec.fieldType
                : (fieldTypeByKey[fieldKey] ?? null);

        map[alias] = {
            fieldKey,
            fieldType,
        };
    }

    return Object.keys(map).length ? map : null;
}

function resolveAliasToFieldKey(
    fieldMap: CachedFieldMap,
    alias: string,
): string | null {
    const entry = fieldMap[alias];
    if (!entry) return null;
    return entry.fieldKey;
}

function getTrimmedString(value: unknown): string | null {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
}

function parseJsonInput<T>(label: string, text: string): T {
    const trimmed = text.trim();
    if (!trimmed) {
        throw new Error(`${label} cannot be empty.`);
    }
    return JSON.parse(trimmed) as T;
}

function cloneJsonValue<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
}

/**
 * Warn when an explicit existingViewKeys list is missing views the page already has.
 *
 * `pageGroups` is the page's whole layout, not an addition to it, so creating a view
 * with a list that omits an existing key drops that view out of the layout. It still
 * exists and is still reachable by its builder URL — it simply has nowhere to appear.
 *
 * Measured the hard way on 2026-09-03: two views were created back to back with the
 * same existingViewKeys, captured before either existed. The second create rebuilt the
 * layout without the first, which vanished from the page while remaining a live view.
 * Nothing in the response said so, which is what this note is for.
 *
 * @param explicitKeys The caller's existingViewKeys, as passed.
 * @param sceneViewKeys The keys the scene actually holds right now.
 * @returns A note to add to the response, or null when the list is complete.
 */
export function describeLayoutKeyGap(
    explicitKeys: string[],
    sceneViewKeys: string[],
): string | null {
    if (explicitKeys.length === 0 || sceneViewKeys.length === 0) return null;

    const passed = new Set(explicitKeys);
    const missing = sceneViewKeys.filter((key) => !passed.has(key));
    if (missing.length === 0) return null;

    return `existingViewKeys omits ${missing.length} view(s) the page currently holds (${missing.join(', ')}). pageGroups replaces the page layout rather than adding to it, so creating this view will drop those from the page — they will still exist and stay reachable by their builder URL, but nothing will show them. Omit existingViewKeys to have them derived from ${'the scene'} instead, or include every key above.`;
}

/**
 * Which canonical view types carry `no_data_text`.
 *
 * Measured on 2026-09-04 across a 738-view export: the key appears only on
 * `table` (217 of 224) and `list` (6 of 6). It is absent from every details,
 * form, menu, calendar, report, login, registration and rich_text view. A
 * `search` view carries it too — its builder save request writes one — but
 * search is not a template branch here.
 *
 * Of the 223 views that hold the key, **all 223 are non-empty**: nobody leaves
 * it blank on purpose. Knack stores no template tokens in it either (0 of 223
 * contain a placeholder), so the value cannot be dynamic at render time — the
 * most that can be done is derive it from the object at build time.
 *
 * Corrected 2026-09-04: this once said every stored value was "exactly two
 * words". That was true of all 223 in the first app and false in general — two
 * builder copy requests from a second app carried three- and four-word values,
 * neither containing the word "Records". So the derived default here is a
 * sensible floor rather than a house style, and `noDataText` is the way to match
 * an app whose convention differs. Length is not a rule; non-empty is.
 */
const NO_DATA_TEXT_VIEW_TYPES = new Set(['table', 'list']);

export function viewTypeCarriesNoDataText(canonicalType: string): boolean {
    return NO_DATA_TEXT_VIEW_TYPES.has(canonicalType);
}

/**
 * The empty-state line for a record-backed view.
 *
 * Left unset, Knack's builder fills the key with an empty string and the view
 * renders its stock message. Naming the object is strictly more useful, so the
 * object's own name is used when the schema was loaded; appending `Records`
 * rather than pluralising the name keeps a singular object name reading
 * correctly ("No Booking Records", not "No Bookings" guessed from "Booking").
 * Without a name — no appKey passed, or the object missing from the schema —
 * it falls back to a bare `No records`.
 */
export function buildNoDataText(objectName?: string | null): string {
    const name = typeof objectName === 'string' ? objectName.trim() : '';
    return name ? `No ${name} Records` : 'No records';
}

/**
 * What a reference found inside a view schema is for.
 *
 * The split that matters is **scope versus display**, and it was measured rather
 * than assumed. A builder before-and-after pair on 4 September added
 * `connection_key`, `relationship_type`, `authenticated_user` and `parent_source`
 * to a source that previously had none — and left every `columns[].connection.key`
 * exactly as it was. Those columns already named a connection field while the source
 * had no connection at all, which is the proof: a display connection is the path
 * from the view's **own object** out to a connected record, and it has nothing to do
 * with how the view's records are scoped.
 *
 * So:
 *
 * - `scope-connection` decides **which records** appear. Change it to rescope.
 * - `display-connection` decides **where a shown value is read from**. A rescope
 *   leaves these correct, and rewriting them alongside one is a bug, not diligence.
 * - What does invalidate them is changing `source.object`: every field, display
 *   connection, filter, sort and rule then names a field on an object the view no
 *   longer lists.
 */
export type ViewReferenceKind =
    | 'scope-connection'
    | 'display-connection'
    | 'scoped-field'
    | 'navigation'
    | 'object'
    | 'other';

export type ViewReference = {
    /** JSON path into the view schema, e.g. `columns[1].connection.key`. */
    path: string;
    value: string;
    kind: ViewReferenceKind;
};

const KNACK_KEY_PATTERN = /^(?:field|object|view|scene)_\d+$/;
// `object_44.field_1029`, the form edit_rules use for a connection.
const DOTTED_CONNECTION_PATTERN = /^object_\d+\.field_\d+$/;
// `field_784-field_74`, the pair form a record rule's connection_field uses. Without
// this it fell through to the prose branch and was reported as two loose keys, losing
// the display connection it actually names.
const HYPHENATED_PAIR_PATTERN = /^field_\d+-field_\d+$/;
// KTL directives embed bare keys in prose: `_bulk_actions=[label, field_1029]`.
const EMBEDDED_KEY_PATTERN = /\b(?:field|view|scene)_\d+\b/g;

/**
 * Classify a reference by where it sits rather than by what it looks like.
 *
 * Path-based on purpose. `field_1029` means something different in
 * `source.connection_key` than in `source.sort[0].field`, and only the path can
 * tell them apart.
 */
function classifyViewReference(path: string): ViewReferenceKind {
    // Scope: what decides which records the view lists.
    if (
        /connection_key$/.test(path) ||
        /parent_source\.connection$/.test(path)
    ) {
        return 'scope-connection';
    }

    // Display: what decides where a shown value is read from. Measured to survive
    // a rescope untouched, and to exist independently of one.
    if (
        /\.connection\.key$/.test(path) ||
        /edit_rules\[\d+\]\.connection$/.test(path) ||
        /connection_field$/.test(path) ||
        /source\.connections\[\d+\]/.test(path)
    ) {
        return 'display-connection';
    }

    // A `scene` may be a slug string or a reference object — `{key}`, `{scene}` or
    // `{slug}`, all of which readSceneReference resolves. The nested forms end the path
    // one segment deeper, and matching only `.scene` classified `{key}` as `other` and
    // dropped a `{slug}` value from the scan entirely.
    if (/(?:^|\.)scene(\.(?:key|scene|slug))?$/.test(path)) {
        return 'navigation';
    }

    if (/object$/.test(path)) {
        return 'object';
    }

    // A field named inside any filter, rule, sort or value block. These are the
    // references that keep pointing at the old object after a repoint and still
    // look valid, which is the failure this whole scan exists to surface.
    if (
        /\.field$/.test(path) ||
        /\.field\.key$/.test(path) ||
        // A column's `id` mirrors its field key, so it has to follow the field.
        /\.id$/.test(path) ||
        /criteria\[\d+\]/.test(path) ||
        /filters\[\d+\]/.test(path) ||
        /sort\[\d+\]/.test(path) ||
        /rules\[\d+\]/.test(path) ||
        /values\[\d+\]/.test(path)
    ) {
        return 'scoped-field';
    }

    return 'other';
}

/**
 * Every Knack key held anywhere in a view schema, with the path that holds it.
 *
 * Walks the structure generically instead of reading a list of known paths. That
 * is deliberate, and it is the same lesson the cascade guard learned about
 * `links` and `columns`: an enumerated path list only finds the shapes someone
 * thought of, and Knack keeps putting references in new places — a column's own
 * `source.filters`, an `edit_rules` entry's dotted `object_N.field_N`, a KTL
 * directive inside `description`. A generic walk finds those without being told,
 * and covers details, list, form and search layouts for free.
 *
 * Read-only. It reports; it changes nothing.
 *
 * @param schema A complete view schema, as returned by view metadata or posted
 *   to a copy request.
 * @returns Every reference found, in walk order, de-duplicated by path.
 */
export function collectViewReferences(schema: unknown): ViewReference[] {
    const found: ViewReference[] = [];

    const walk = (node: unknown, path: string): void => {
        if (typeof node === 'string') {
            if (KNACK_KEY_PATTERN.test(node)) {
                found.push({
                    path,
                    value: node,
                    kind: classifyViewReference(path),
                });
                return;
            }

            if (
                DOTTED_CONNECTION_PATTERN.test(node) ||
                HYPHENATED_PAIR_PATTERN.test(node)
            ) {
                // Recorded whole rather than split: the stored value is the
                // compound string, so that is what a repoint has to rewrite.
                found.push({
                    path,
                    value: node,
                    kind: classifyViewReference(path),
                });
                return;
            }

            // A slug like `cancel-appointment3` in a `scene` position is a real
            // navigation reference even though it is not a `scene_N` key.
            if (classifyViewReference(path) === 'navigation' && node !== '') {
                found.push({ path, value: node, kind: 'navigation' });
                return;
            }

            // Prose that embeds keys — KTL directives in `description` are the
            // known case, and they are invisible to every other check.
            const embedded = node.match(EMBEDDED_KEY_PATTERN);
            if (embedded) {
                for (const key of new Set(embedded)) {
                    found.push({
                        path: `${path} (embedded)`,
                        value: key,
                        kind: 'other',
                    });
                }
            }

            return;
        }

        if (Array.isArray(node)) {
            node.forEach((item, index) => walk(item, `${path}[${index}]`));
            return;
        }

        if (node && typeof node === 'object') {
            for (const [key, value] of Object.entries(node)) {
                walk(value, path ? `${path}.${key}` : key);
            }
        }
    };

    walk(schema, '');
    return found;
}

/**
 * Group a view's references by what each kind of change would invalidate.
 *
 * Two different edits are called "repointing" and they have almost nothing in
 * common:
 *
 * - **Rescope** — change `connection_key`, `parent_source` or `authenticated_user`.
 *   Only `scopeConnections` are involved. Display connections, fields, filters and
 *   sorts all stay valid, because they name fields on an object that has not
 *   changed. Measured: a builder rescope left all six display connections untouched.
 * - **Retarget** — change `source.object`. Now everything is suspect: every field,
 *   display connection, filter, sort and rule names a field on the old object.
 *
 * @param schema A complete view schema.
 * @returns References split by kind, plus the distinct connection fields in each.
 */
export function planViewRepoint(schema: unknown): {
    scopeConnections: ViewReference[];
    displayConnections: ViewReference[];
    scopedFields: ViewReference[];
    navigation: ViewReference[];
    other: ViewReference[];
    distinctScopeKeys: string[];
    distinctDisplayKeys: string[];
} {
    const references = collectViewReferences(schema);
    const byKind = (kind: ViewReferenceKind) =>
        references.filter((reference) => reference.kind === kind);

    const scopeConnections = byKind('scope-connection');
    const displayConnections = byKind('display-connection');

    // The dotted `object_N.field_N` form an edit rule uses reduces to its field.
    const fieldsOf = (list: ViewReference[]) => [
        ...new Set(
            list.map((reference) => reference.value.split('.').pop() as string),
        ),
    ];

    return {
        scopeConnections,
        displayConnections,
        scopedFields: byKind('scoped-field'),
        navigation: byKind('navigation'),
        other: [...byKind('object'), ...byKind('other')],
        distinctScopeKeys: fieldsOf(scopeConnections),
        distinctDisplayKeys: fieldsOf(displayConnections),
    };
}

function buildStarterPageGroups(
    existingViewKeys: string[],
): Array<{ columns: Array<{ keys: string[]; width: number }> }> {
    const rows = existingViewKeys.map((viewKey) => ({
        columns: [{ keys: [viewKey], width: 100 }],
    }));
    rows.push({ columns: [{ keys: ['new'], width: 100 }] });
    return rows;
}

type ViewTemplatePayloadOptions = {
    canonicalType: string;
    displayName: string;
    resolvedTitle: string;
    viewSource: Record<string, unknown>;
    fieldDescriptors: TemplateFieldDescriptor[];
    pageGroups: unknown[];
    noDataText: string;
};

/**
 * The starter payload for each template type.
 *
 * Extracted from the tool handler so the payload a caller actually receives can
 * be asserted, rather than only the helpers that feed it. Two defects in this
 * file were invisible to helper-level tests for exactly that reason: a header
 * fell back to the raw field key because the call site passed an empty field
 * list, and `no_data_text` was never written at all because no branch set it.
 * A test of `buildNoDataText` passes either way; a test of this does not.
 */
export function buildViewTemplatePayload({
    canonicalType,
    displayName,
    resolvedTitle,
    viewSource,
    fieldDescriptors,
    pageGroups,
    noDataText,
}: ViewTemplatePayloadOptions): Record<string, unknown> {
    if (canonicalType === 'table') {
        return {
            name: displayName,
            type: 'table',
            title: resolvedTitle,
            links: [],
            groups: [],
            inputs: [],
            source: viewSource,
            columns: fieldDescriptors.map((field) =>
                buildViewFieldColumn(field),
            ),
            no_data_text: noDataText,
            pageGroups,
        };
    }

    if (canonicalType === 'form') {
        return {
            name: displayName,
            type: 'form',
            title: resolvedTitle,
            action: 'insert',
            links: [],
            groups: [
                {
                    columns: [
                        {
                            width: 100,
                            inputs: fieldDescriptors.map((field) =>
                                buildFormInputField(field),
                            ),
                        },
                    ],
                },
            ],
            rules: {
                emails: [],
                fields: [],
                records: [],
                submits: [
                    {
                        key: 'submit_1',
                        action: 'message',
                        message: '<p>Form successfully submitted.</p>',
                        is_default: true,
                        reload_show: true,
                    },
                ],
            },
            source: viewSource,
            pageGroups,
        };
    }

    if (canonicalType === 'details') {
        return {
            name: displayName,
            type: 'details',
            title: resolvedTitle,
            links: [],
            groups: [],
            inputs: [],
            layout: 'full',
            source: viewSource,
            columns: [
                {
                    width: 100,
                    groups: [
                        {
                            columns: [
                                fieldDescriptors.map((field) =>
                                    buildViewGroupField(field),
                                ),
                            ],
                        },
                    ],
                },
            ],
            pageGroups,
        };
    }

    return {
        name: displayName,
        type: 'list',
        title: resolvedTitle,
        links: [],
        groups: [],
        inputs: [],
        layout: 'full',
        source: viewSource,
        columns: [
            {
                width: 100,
                groups: [
                    {
                        columns: [
                            fieldDescriptors.map((field) =>
                                buildViewGroupField(field),
                            ),
                        ],
                    },
                ],
            },
        ],
        reportType: null,
        allow_limit: true,
        filter_type: 'none',
        hide_fields: false,
        no_data_text: noDataText,
        pageGroups,
    };
}

export function buildTemplateFieldDescriptors(
    fieldKeys: string[],
    objectFields: CachedField[] = [],
    maxFields = 12,
): TemplateFieldDescriptor[] {
    const objectFieldsByKey = new Map(
        objectFields.map((field) => [field.key, field]),
    );
    const selectedFields =
        fieldKeys.length > 0
            ? fieldKeys.map(
                  (fieldKey) =>
                      objectFieldsByKey.get(fieldKey) || { key: fieldKey },
              )
            : objectFields.slice(0, Math.max(maxFields, 1));

    return selectedFields.map((field) => ({
        key: field.key,
        name: field.name || field.key,
        type: field.type || 'text',
    }));
}

function isEligibleFormField(field: CachedField): boolean {
    const fieldType = (field.type || '').trim().toLowerCase();
    if (!fieldType) return true;
    return !NON_FORM_FIELD_TYPES.has(fieldType);
}

function getSceneViewKeys(scenes: SceneInfo[], sceneKey?: string): string[] {
    if (!sceneKey) return [];
    return (
        scenes
            .find((scene) => scene.sceneKey === sceneKey)
            ?.views.map((view) => view.viewKey) || []
    );
}

/**
 * One filter rule inside a view's source criteria.
 *
 * Measured across 466 source criteria blocks in a production app export (738 views,
 * 31 August 2026): 339 of 345 rules were exactly `{ field, operator, value }`. Three
 * rarer variants exist and are deliberately not modelled — a rule adding `object_key`
 * to name a field on another object, one adding a display `label`, and a date-range
 * rule shaped `{ type: 'week', field, operator: 'is during the current', value: {
 * date: '', all_day: false } }`. Build those by hand until one is measured on purpose.
 */
export type ViewSourceFilterRule = {
    field: string;
    operator: string;
    value?: unknown;
};

/**
 * A view's source criteria: one first block plus any number of groups.
 *
 * `match` governs the first block, and a group's internal operator is the *inverse* of
 * it. See KNACK_VIEW_SOURCE_SHAPE for the observation that settles that, which is not
 * a reading of documentation but of a real filter that only parses one way.
 */
export type ViewSourceFilters = {
    match?: 'all' | 'any';
    rules?: ViewSourceFilterRule[];
    groups?: ViewSourceFilterRule[][];
};

export type ViewSourceSort = { field: string; order: 'asc' | 'desc' };

export type ViewSourceOptions = {
    objectKey: string;
    connectionKey?: string;
    relationshipType?: 'foreign' | 'local';
    authenticatedUser?: boolean;
    parentSource?: { object: string; connection: string };
    filters?: ViewSourceFilters;
    /**
     * The view's default sort. Omitted means `[]`, which is a real stored state —
     * 36 stored `sort: []` and 156 with no sort key at all in the export. Passing
     * one matters when rebuilding an existing view: two real builder copy requests
     * each carried a sort, and different ones, so a rebuild that hardcodes `[]`
     * silently drops the ordering the view was designed around.
     */
    sort?: ViewSourceSort[];
};

/**
 * Assemble a view's `source` block, including connection scoping and filters.
 *
 * The templates used to emit a flat source — object, empty criteria, empty sort — which
 * is the only shape they could produce, because nothing in this server knew what a
 * connected or filtered source looked like. Both are now measured, so both can be built
 * rather than guessed at by the caller.
 *
 * Two invariants are enforced rather than trusted, because each has a silent failure
 * mode on the far side:
 *
 * `connection_key` and `relationship_type` always travel together — 102 of 102
 * connected sources carried both, none carried one alone. A connection with no
 * relationship type does not describe a direction, and Knack is left to pick one.
 *
 * `authenticated_user` was `true` in all 28 occurrences and never `false`. It reads as
 * a flag whose presence is the meaning, so `false` omits the key rather than writing
 * it — writing `false` would assert something never observed.
 *
 * @param options Source object, optional connection scoping, optional filters.
 * @returns The `source` block, ready to drop into a view payload.
 */
export function buildViewSource(
    options: ViewSourceOptions,
): Record<string, unknown> {
    const {
        objectKey,
        connectionKey,
        relationshipType,
        authenticatedUser,
        parentSource,
        filters,
        sort,
    } = options;

    if (!objectKey) {
        throw new Error('objectKey is required to build a view source.');
    }

    // Validated at runtime, not just in the type. `parseJsonInput` casts, so
    // `ViewSourceSort[]` promises nothing about what a caller actually sent — a review
    // pointed out that `order: "sideways"` and a non-string field both reached Knack.
    if (sort !== undefined && !Array.isArray(sort)) {
        throw new Error('sort must be an array of { field, order } entries.');
    }

    for (const entry of sort ?? []) {
        const record = entry as unknown;
        if (!record || typeof record !== 'object' || Array.isArray(record)) {
            throw new Error(
                'Every sort entry must be an object of the form { field, order }.',
            );
        }

        const { field, order } = record as { field?: unknown; order?: unknown };

        if (typeof field !== 'string' || field.trim() === '') {
            throw new Error(
                'Every sort entry needs a field, as a non-empty string. A sort with no field is stored but orders nothing, which reads as a working sort in the builder.',
            );
        }

        if (order !== undefined && order !== 'asc' && order !== 'desc') {
            throw new Error(
                `Sort order must be "asc" or "desc", not ${JSON.stringify(order)}. Knack stores only those two, so anything else is written and then silently ignored.`,
            );
        }
    }

    if (connectionKey && !relationshipType) {
        throw new Error(
            'relationshipType is required alongside connectionKey. Use "foreign" when the connection field lives on the view\'s own object, "local" when it lives on the other object and points back — that split held for all 102 connected sources measured.',
        );
    }

    if (relationshipType && !connectionKey) {
        throw new Error(
            'connectionKey is required alongside relationshipType. The relationship type describes a direction; without the connection field there is nothing for it to describe.',
        );
    }

    if (parentSource && (!parentSource.object || !parentSource.connection)) {
        throw new Error(
            'parentSource needs both object and connection. It names the record context the page supplies, so a half-specified hop cannot be resolved.',
        );
    }

    const criteria = buildViewSourceCriteria(filters);

    const source: Record<string, unknown> = {
        object: objectKey,
        criteria,
        sort: sort ?? [],
        limit: '',
    };

    if (connectionKey) {
        source.connection_key = connectionKey;
        source.relationship_type = relationshipType;
    }

    if (parentSource) {
        source.parent_source = {
            object: parentSource.object,
            connection: parentSource.connection,
        };
    }

    // Presence is the meaning; see the note above on never writing `false`.
    if (authenticatedUser === true) {
        source.authenticated_user = true;
    }

    return source;
}

/**
 * Normalise the criteria half of a source, defaulting an absent filter to match-all.
 *
 * An empty first block with `match: "all"` is what every unfiltered view in the export
 * carried, so it is the right shape for "no filter" rather than omitting `criteria`.
 */
function buildViewSourceCriteria(
    filters: ViewSourceFilters | undefined,
): Record<string, unknown> {
    const match = filters?.match ?? 'all';

    if (match !== 'all' && match !== 'any') {
        throw new Error(
            `filters.match must be "all" or "any", received ${JSON.stringify(match)}.`,
        );
    }

    const rules = filters?.rules ?? [];
    const groups = filters?.groups ?? [];

    if (!Array.isArray(rules)) {
        throw new Error('filters.rules must be an array of rules.');
    }

    if (!Array.isArray(groups) || groups.some((g) => !Array.isArray(g))) {
        throw new Error(
            'filters.groups must be an array of arrays: each group is a list of rules, and a group carries no match of its own.',
        );
    }

    const check = (rule: unknown, where: string): ViewSourceFilterRule => {
        const record = rule as Partial<ViewSourceFilterRule> | null;
        if (!record || typeof record !== 'object') {
            throw new Error(`${where} must be an object.`);
        }
        if (!record.field || !record.operator) {
            throw new Error(`${where} needs both field and operator.`);
        }
        return {
            field: record.field,
            operator: record.operator,
            value: record.value ?? '',
        };
    };

    return {
        match,
        rules: rules.map((rule, i) => check(rule, `filters.rules[${i}]`)),
        groups: groups.map((group, gi) =>
            group.map((rule, i) => check(rule, `filters.groups[${gi}][${i}]`)),
        ),
    };
}

/**
 * What a view's `source` block looks like, measured rather than inferred.
 *
 * Recorded in the same spirit as KNACK_CONDITIONAL_RULES_SHAPE: the shapes that were
 * actually observed, with the counts behind them and the gaps stated plainly. Read from
 * a production app export of 738 views on 31 August 2026. Keys and values below are
 * placeholders; only the structure is from the export.
 */
export const KNACK_VIEW_SOURCE_SHAPE = {
    summary:
        "A view's source decides which records it shows. The scoping keys are INDEPENDENT and compose freely — do not read the patterns below as a closed set of four. Verified against a production app export (738 views) on 2026-08-31, then corrected on 2026-09-04 by two real builder copy requests from a second app that carried connection_key, relationship_type, authenticated_user AND parent_source in one block, which is none of the named patterns.",
    composition:
        'Treat each key as an independent switch on top of `{ object, criteria, sort }`: connection_key+relationship_type scope through a connection, authenticated_user scopes to the logged-in account, parent_source adds a hop through the record the page supplies. Any combination is legal, including all of them at once. The named patterns below are the four combinations the first export happened to contain, not the four that exist.',
    patterns: {
        plain: '{ "object": "object_1", "criteria": { "match": "all", "rules": [], "groups": [] }, "sort": [{ "field": "field_1", "order": "asc" }], "limit": "" }',
        connectionScoped:
            '{ "object": "object_1", "criteria": { ... }, "sort": [], "limit": "", "connection_key": "field_2", "relationship_type": "foreign" }',
        loggedInUser:
            '{ "object": "object_1", "criteria": { ... }, "sort": [], "limit": "", "connection_key": "field_2", "relationship_type": "foreign", "authenticated_user": true }',
        multiHop:
            '{ "object": "object_1", "criteria": { ... }, "sort": [], "limit": "", "connection_key": "field_2", "relationship_type": "foreign", "parent_source": { "object": "object_2", "connection": "field_3" } }',
    },
    allKeysAtOnce:
        '{ "object": "object_1", "criteria": { ... }, "sort": [{ "field": "field_9", "order": "desc" }], "connection_key": "field_2", "relationship_type": "foreign", "authenticated_user": true, "parent_source": { "object": "object_2", "connection": "field_3" } } — observed twice in a second app, on two sibling views of one object. Note there is no `limit` key at all: the builder omits it where buildViewSource always writes `limit: ""`. Knack accepted our explicit empty string in a round-trip, so both forms work, but do not treat limit as mandatory.',
    counts: 'plain 325 views · connection-scoped 57 · logged-in user 16 · multi-hop 6 · authenticated_user seen 28 times in total across variants. Those counts are one app; a second app supplied the all-keys-at-once combination absent from them.',
    notes: [
        'relationship_type is decided by which object owns the connection field, and the split was clean across all 102 connected sources: "foreign" (84) where the connection field lives on the view\'s own object and points outward, "local" (18) where it lives on the other object and points back. This is the value to recompute when a copied view is repointed at a different connection — carrying the original over is how a copy silently returns the wrong rows.',
        'connection_key and relationship_type always appeared together. Neither was ever present alone.',
        "authenticated_user was true in every occurrence and never false. It also appears without connection_key at all — a form on the logged-in user's own record carried `{ object, sort, authenticated_user: true }` and nothing else — so it is not solely a modifier on a connection.",
        'parent_source names the record context the page supplies, as `{ object, connection }`. In 3 of 8 cases its connection was the same field as connection_key; in the other 5 it named an earlier, different hop, which is the case that cannot be reconstructed from connection_key alone. Every occurrence sat alongside relationship_type "foreign".',
        'source.type is unrelated to the above: "registration" on registration views (120) and "database" on a handful of others (6). It is not a filter or a connection.',
        'Sorting lives in source.sort as `[{ field, order }]` — a separate array from criteria. Notably `value_field` never appeared inside any source in the export (0 of 738 views); all 55 of its occurrences were in view rule criteria (records, emails and submits), paired with `value_type: "custom"` for field-to-field comparison. Do not look for a sort field inside a criteria rule.',
        'Scoping to the logged-in user has a second, separate mechanism: a criteria rule with `operator: "user"` and an empty value, applied to a connection field (60 occurrences). That is a filter rule rather than a source flag, and the two can be used independently.',
        'Knack stores these keys as posted rather than rewriting them. Measured on 2026-09-03 by creating a connection-scoped table on two unrelated objects and reading each back: object, connection_key and relationship_type came back byte-for-byte in both. So a source built here is what the view ends up with — but the read-back is still the honest way to confirm a shape this file has not seen, since only these four patterns have been round-tripped.',
        'Knack does not validate relationship_type against the connection. A view repointed at a connection whose field lives on the other object was stored with "foreign" and accepted without error, where ownership makes it "local". Nothing downstream will tell you; that silence is why buildViewSource refuses a connection with no relationship type rather than guessing one.',
    ],
    criteria: {
        summary:
            'criteria is an object, not an array. `match` governs the first block; each group is an array of rules and carries no match of its own.',
        shape: '{ "match": "all", "rules": [{ "field": "field_1", "operator": "is", "value": "x" }], "groups": [[{ "field": "field_2", "operator": "is", "value": "a" }, { "field": "field_2", "operator": "is", "value": "b" }]] }',
        semantics:
            'A group\'s internal operator is the inverse of match. With match "all": rules AND together and each group is an OR. With match "any": rules OR together and each group is an AND.',
        evidence:
            'Observed rather than documented. One table carried match "all", four AND-ed top-level rules, and a single group of five equality tests on the *same* field with five different values. AND-ing five equality tests on one field matches nothing, so the group can only be an OR — which is the inverse of the enclosing match. The same view also proves the first block is `rules` and not group zero, since it populates both at once.',
        counts: '466 criteria blocks: match "all" 439, "any" 27. 135 carried rules, 30 carried groups. Every one of the 42 groups seen was an array of `{ field, operator, value }`.',
        operators:
            'Observed in source criteria: is, user, is not, contains, does not contain, is blank, is not blank, is after, is before, is after today, is today or after, is during the current, higher than. Not exhaustive — it is what this app happened to use.',
    },
} as const;

/**
 * Decide which fields a view template shows, and what each column is called.
 *
 * Extracted so the call site is testable rather than only the helper beneath it. The
 * bug this replaces lived entirely in the wiring: explicit `fieldKeys` were passed
 * with an empty schema, so `buildTemplateFieldDescriptors` could only fall back to the
 * key and every generated column read as `field_196` in the builder. A test of that
 * helper passed either way, which is why the decision now has a seam of its own.
 *
 * @param options Caller-supplied keys, the object's schema fields, and the view type.
 * @returns The descriptors, whether they were derived, and notes for the response.
 */
export function resolveTemplateFields(options: {
    fieldKeys: string[];
    allObjectFields: CachedField[];
    objectKey: string;
    canonicalType: string;
    maxFields?: number;
}): {
    fieldDescriptors: TemplateFieldDescriptor[];
    derivedFromSchema: boolean;
    notes: string[];
} {
    const {
        fieldKeys,
        allObjectFields,
        objectKey,
        canonicalType,
        maxFields = 12,
    } = options;
    const notes: string[] = [];

    if (fieldKeys.length > 0) {
        // Explicit keys still decide *which* fields appear; the schema only supplies
        // each one's label and type. A key the schema does not know keeps falling back
        // to itself rather than failing the call.
        const fieldDescriptors = buildTemplateFieldDescriptors(
            fieldKeys,
            allObjectFields,
            maxFields,
        );
        const named = fieldDescriptors.filter(
            (field) => field.name !== field.key,
        ).length;

        if (allObjectFields.length === 0) {
            notes.push(
                `No schema fields were available for ${objectKey}, so column headers fall back to field keys. Pass appKey, or expect headers like "field_123" in the builder.`,
            );
        } else if (named < fieldDescriptors.length) {
            notes.push(
                `${fieldDescriptors.length - named} of ${fieldDescriptors.length} field(s) were not found in ${objectKey}'s schema, so those column headers fall back to the field key.`,
            );
        }

        return { fieldDescriptors, derivedFromSchema: false, notes };
    }

    if (allObjectFields.length === 0) {
        notes.push(
            `No schema fields were found for ${objectKey}. Pass fieldKeys explicitly or ensure schema/runtime metadata is available.`,
        );
        return { fieldDescriptors: [], derivedFromSchema: false, notes };
    }

    const candidateFields =
        canonicalType === 'form'
            ? allObjectFields.filter((field) => isEligibleFormField(field))
            : allObjectFields;
    const fieldDescriptors = buildTemplateFieldDescriptors(
        [],
        candidateFields,
        maxFields,
    );
    notes.push(
        `Derived ${fieldDescriptors.length} field(s) from object metadata for ${objectKey}.`,
    );

    if (canonicalType === 'form') {
        const excludedCount = allObjectFields.length - candidateFields.length;
        if (excludedCount > 0) {
            notes.push(
                `Excluded ${excludedCount} non-input field(s) from the derived form template.`,
            );
        }
    }

    return { fieldDescriptors, derivedFromSchema: true, notes };
}

function buildViewFieldColumn(field: TemplateFieldDescriptor) {
    const column: Record<string, unknown> = {
        id: field.key,
        type: 'field',
        align: 'left',
        field: { key: field.key },
        rules: [],
        width: {
            type: 'default',
            units: 'px',
            amount: '50',
        },
        header: field.name,
        grouping: false,
        conn_link: '',
        link_text: '',
        link_type: 'text',
        group_sort: 'asc',
        link_field: '',
        ignore_edit: false,
        img_gallery: '',
        conn_separator: '',
        ignore_summary: false,
        link_design_active: false,
        icon: {
            icon: '',
            align: 'left',
        },
    };

    // Only present when the column reaches through a connection. A column on the
    // view's own object carries no `connection` key at all, so writing an empty
    // one would invent a shape rather than reproduce the measured two.
    if (field.connectionKey) {
        column.connection = { key: field.connectionKey };
    }

    return column;
}

function buildViewGroupField(field: TemplateFieldDescriptor) {
    const item: Record<string, unknown> = {
        key: field.key,
        copy: '',
        type: 'field',
        value: '',
        name: field.name,
        show_map: false,
        conn_link: '',
        link_text: '',
        link_type: 'text',
        map_width: 400,
        link_field: '',
        map_height: 300,
        img_gallery: '',
        conn_separator: '',
        link_design_active: false,
        icon: {
            icon: '',
            align: 'left',
        },
        format: {
            styles: [],
            label_custom: true,
            label_format: 'left',
        },
    };

    // A details view carries the same `connection: { key }` a table column does, on
    // field items nested inside columns[].groups[].columns[][] — measured from a real
    // builder copy request. Without this, columnConnections was accepted for a details
    // or list template, silently dropped, and then reported as applied.
    if (field.connectionKey) {
        item.connection = { key: field.connectionKey };
    }

    return item;
}

function buildFormInputField(field: TemplateFieldDescriptor) {
    return {
        id: field.key,
        key: field.key,
        type: field.type,
        label: field.name,
        instructions: '',
        field: { key: field.key },
    };
}

function getOptionLabel(value: unknown): string | null {
    const direct = getTrimmedString(value);
    if (direct) return direct;

    const rec = asRecord(value);
    if (!rec) return null;

    const candidates = [
        rec.label,
        rec.name,
        rec.text,
        rec.value,
        rec.identifier,
    ];
    for (const candidate of candidates) {
        const label = getTrimmedString(candidate);
        if (label) return label;
    }

    return null;
}

function collectOptionLabels(
    value: unknown,
    output: string[],
    seen: Set<string>,
): void {
    if (Array.isArray(value)) {
        for (const item of value) {
            const label = getOptionLabel(item);
            if (label) {
                const dedupeKey = label.toLowerCase();
                if (!seen.has(dedupeKey)) {
                    seen.add(dedupeKey);
                    output.push(label);
                }
                continue;
            }

            const rec = asRecord(item);
            if (!rec) continue;
            for (const nestedKey of ['options', 'choices', 'values']) {
                if (nestedKey in rec) {
                    collectOptionLabels(rec[nestedKey], output, seen);
                }
            }
        }
        return;
    }

    const rec = asRecord(value);
    if (!rec) return;
    for (const nestedKey of ['options', 'choices', 'values']) {
        if (nestedKey in rec) {
            collectOptionLabels(rec[nestedKey], output, seen);
        }
    }
}

function extractChoiceOptions(...candidates: unknown[]): string[] {
    const output: string[] = [];
    const seen = new Set<string>();
    for (const candidate of candidates) {
        collectOptionLabels(candidate, output, seen);
    }
    return output;
}

function extractBoolean(...candidates: unknown[]): boolean | undefined {
    for (const candidate of candidates) {
        if (typeof candidate === 'boolean') return candidate;
        if (typeof candidate === 'number') return candidate !== 0;
        if (typeof candidate !== 'string') continue;
        const normalised = candidate.trim().toLowerCase();
        if (['true', 'yes', 'y', '1'].includes(normalised)) return true;
        if (['false', 'no', 'n', '0'].includes(normalised)) return false;
    }
    return undefined;
}

function parseRuntimeSchema(body: unknown): CachedSchema | null {
    const directObjects = getObjectAtPath(body, 'objects');
    const nestedObjects = getObjectAtPath(body, 'application', 'objects');
    const objectsRaw = Array.isArray(directObjects)
        ? directObjects
        : Array.isArray(nestedObjects)
          ? nestedObjects
          : null;

    if (!objectsRaw) return null;

    const objects: NonNullable<CachedSchema['objects']> = [];

    for (const objectItem of objectsRaw) {
        const obj = asRecord(objectItem);
        if (!obj) continue;

        const objectKey = typeof obj.key === 'string' ? obj.key : null;
        if (!objectKey) continue;

        const objectName = typeof obj.name === 'string' ? obj.name : undefined;
        const fieldsRaw = Array.isArray(obj.fields) ? obj.fields : [];
        const fields: CachedField[] = [];

        for (const fieldItem of fieldsRaw) {
            const field = asRecord(fieldItem);
            if (!field) continue;
            const fieldKey = typeof field.key === 'string' ? field.key : null;
            if (!fieldKey) continue;
            const fieldMeta = asRecord(field.meta);
            const fieldDescription =
                typeof field.description === 'string'
                    ? field.description
                    : typeof fieldMeta?.description === 'string'
                      ? fieldMeta.description
                      : undefined;

            const fieldFormat = asRecord(field.format);
            const fieldRelationship = asRecord(field.relationship);
            const connectedObject =
                (typeof fieldFormat?.object === 'string'
                    ? fieldFormat.object
                    : undefined) ||
                (typeof fieldRelationship?.object === 'string'
                    ? fieldRelationship.object
                    : undefined);
            const choiceOptions = extractChoiceOptions(
                field.options,
                fieldFormat?.options,
                fieldFormat?.choices,
                fieldMeta?.options,
                fieldMeta?.choices,
            );
            // Knack's real connection cardinality lives at relationship.has /
            // relationship.belongs_to ('one'|'many'), not any of the boolean-ish keys
            // below (those were never observed on a live connection field). Treat either
            // side reporting 'many' as multiple; only count as one-to-one when both sides
            // explicitly say 'one'.
            const relationshipCardinality =
                fieldRelationship?.has === 'many' ||
                fieldRelationship?.belongs_to === 'many'
                    ? true
                    : fieldRelationship?.has === 'one' &&
                        fieldRelationship?.belongs_to === 'one'
                      ? false
                      : undefined;
            const allowsMultiple = extractBoolean(
                relationshipCardinality,
                field.multiple,
                field.allow_multiple,
                field.allowMultiple,
                fieldFormat?.multiple,
                fieldFormat?.allow_multiple,
                fieldFormat?.allowMultiple,
                fieldMeta?.multiple,
                fieldMeta?.allow_multiple,
                fieldMeta?.allowMultiple,
                fieldRelationship?.multiple,
                fieldRelationship?.hasMany,
                fieldRelationship?.many,
            );
            const required = extractBoolean(
                field.required,
                fieldFormat?.required,
                fieldMeta?.required,
            );

            fields.push({
                key: fieldKey,
                name: typeof field.name === 'string' ? field.name : undefined,
                type: typeof field.type === 'string' ? field.type : undefined,
                required,
                description: fieldDescription,
                connectedObject,
                choiceOptions: choiceOptions.length ? choiceOptions : undefined,
                allowsMultiple,
            });
        }

        objects.push({ key: objectKey, name: objectName, fields });
    }

    return objects.length ? { objects } : null;
}

function parseRuntimeFieldMap(body: unknown): CachedFieldMap | null {
    const schema = parseRuntimeSchema(body);
    if (schema?.objects?.length) {
        const strictMap = generateStrictFieldMapFromSchema(schema);
        if (Object.keys(strictMap).length) return strictMap;
    }

    const direct = getObjectAtPath(body, 'fieldMap');
    const nested = getObjectAtPath(body, 'application', 'fieldMap');
    return coerceFieldMap(direct ?? nested, schema);
}

function parseRuntimeViewMap(body: unknown): CachedViewMap | null {
    const direct = getObjectAtPath(body, 'viewMap');
    const nested = getObjectAtPath(body, 'application', 'viewMap');
    const rawMap = asRecord(direct) || asRecord(nested);

    if (rawMap) {
        const parsed: CachedViewMap = {};
        for (const [viewKey, attrs] of Object.entries(rawMap)) {
            const attributes = asRecord(attrs);
            if (!attributes) continue;
            parsed[viewKey] = attributes;
        }
        if (Object.keys(parsed).length) return parsed;
    }

    const directScenes = getObjectAtPath(body, 'scenes');
    const nestedScenes = getObjectAtPath(body, 'application', 'scenes');
    const scenesRaw = Array.isArray(directScenes)
        ? directScenes
        : Array.isArray(nestedScenes)
          ? nestedScenes
          : null;

    if (!scenesRaw) return null;

    const viewMap: CachedViewMap = {};
    for (const sceneItem of scenesRaw) {
        const scene = asRecord(sceneItem);
        if (!scene) continue;

        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];
        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (!view) continue;

            const viewKey = typeof view.key === 'string' ? view.key : null;
            if (!viewKey) continue;

            const attributes = asRecord(view.attributes) || view;
            viewMap[viewKey] = attributes;
        }
    }

    return Object.keys(viewMap).length ? viewMap : null;
}

/**
 * Resolve a field key from a Knack view layout item.
 *
 * @param item A form input, search field, or displayed view column.
 * @returns The configured Knack field key when the item represents a field.
 */
function getViewLayoutFieldKey(
    item: Record<string, unknown>,
): string | undefined {
    const field = item.field;
    if (typeof field === 'string' && /^field_\d+$/i.test(field)) {
        return field;
    }

    const fieldRecord = asRecord(field);
    if (
        fieldRecord &&
        typeof fieldRecord.key === 'string' &&
        /^field_\d+$/i.test(fieldRecord.key)
    ) {
        return fieldRecord.key;
    }

    return typeof item.id === 'string' && /^field_\d+$/i.test(item.id)
        ? item.id
        : undefined;
}

/**
 * Resolve the object-field metadata that applies to a record-backed view.
 *
 * @param attributes Raw Knack view attributes.
 * @param schema Cached object schema.
 * @returns Field metadata keyed by field key, or an empty map when the view object is unknown.
 */
function getViewObjectFields(
    attributes: Record<string, unknown>,
    schema: CachedSchema | null | undefined,
): Map<string, CachedField> {
    const source = asRecord(attributes.source);
    const objectKey = typeof source?.object === 'string' ? source.object : null;
    const object = schema?.objects?.find((entry) => entry.key === objectKey);
    return new Map((object?.fields || []).map((field) => [field.key, field]));
}

/**
 * Extract configured default values while retaining false, zero, and empty-string defaults.
 *
 * @param item A Knack view layout field item.
 * @returns The explicitly configured defaults, if any.
 */
function getViewFieldDefaults(
    item: Record<string, unknown>,
): Record<string, unknown> | undefined {
    const defaults: Record<string, unknown> = {};
    const format = asRecord(item.format);
    const candidates = [item, format].filter(
        (candidate): candidate is Record<string, unknown> => Boolean(candidate),
    );

    for (const candidate of candidates) {
        for (const [key, value] of Object.entries(candidate)) {
            if (
                key === 'default' ||
                key === 'conn_default' ||
                key.startsWith('default_')
            ) {
                defaults[key] = value;
            }
        }
    }

    return Object.keys(defaults).length ? defaults : undefined;
}

/**
 * Extract the configured field settings from a view layout without interpreting conditional rules.
 *
 * Requiredness is resolved from the owning object schema. Defaults and read-only state are view
 * settings. A missing value is intentionally omitted so callers do not confuse an absent setting
 * with an explicit false value.
 *
 * @param attributes Raw Knack view attributes.
 * @returns A compact field-settings summary suitable for MCP tool responses.
 */
function getViewFieldSettings(
    attributes: Record<string, unknown>,
    fieldsByKey: Map<string, CachedField> = new Map(),
): ViewFieldSettingsSummary {
    const fields: ViewFieldSettings[] = [];
    const seen = new Set<string>();

    const addField = (
        value: unknown,
        layout: ViewFieldSettings['layout'],
        sourcePath: string,
    ): void => {
        const item = asRecord(value);
        if (!item) return;

        const fieldKey = getViewLayoutFieldKey(item);
        if (!fieldKey) return;

        const dedupeKey = `${sourcePath}:${fieldKey}`;
        if (seen.has(dedupeKey)) return;
        seen.add(dedupeKey);

        const format = asRecord(item.format);
        const rules = Array.isArray(item.rules)
            ? item.rules
            : Array.isArray(item.visibility_rules)
              ? item.visibility_rules
              : Array.isArray(item.visibilityRules)
                ? item.visibilityRules
                : undefined;

        fields.push({
            fieldKey,
            fieldType:
                fieldsByKey.get(fieldKey)?.type ??
                (typeof item.type === 'string' ? item.type : undefined),

            label:
                typeof item.label === 'string'
                    ? item.label
                    : typeof item.name === 'string'
                      ? item.name
                      : undefined,
            objectRequired: fieldsByKey.get(fieldKey)?.required,
            readOnly: extractBoolean(
                item.read_only,
                item.readOnly,
                format?.read_only,
                format?.readOnly,
            ),
            defaults: getViewFieldDefaults(item),
            rules,
            layout,
            sourcePath,
        });
    };

    const visitContainer = (value: unknown, path: string): void => {
        const container = asRecord(value);
        if (!container) return;

        const inputs = Array.isArray(container.inputs) ? container.inputs : [];
        inputs.forEach((input, index) =>
            addField(input, 'form-input', `${path}.inputs[${index}]`),
        );

        const searchFields = Array.isArray(container.fields)
            ? container.fields
            : [];
        searchFields.forEach((field, index) =>
            addField(field, 'search-field', `${path}.fields[${index}]`),
        );

        const groups = Array.isArray(container.groups) ? container.groups : [];
        groups.forEach((group, index) =>
            visitContainer(group, `${path}.groups[${index}]`),
        );

        const columns = Array.isArray(container.columns)
            ? container.columns
            : [];
        columns.forEach((column, index) => {
            const columnPath = `${path}.columns[${index}]`;
            addField(column, 'view-column', columnPath);
            visitContainer(column, columnPath);
        });
    };

    visitContainer(attributes, '$');

    return {
        configuredFieldCount: fields.length,
        requiredFieldCount: fields.filter(
            (field) => field.objectRequired === true,
        ).length,
        readOnlyFieldCount: fields.filter((field) => field.readOnly === true)
            .length,
        fields,
        ...(Object.hasOwn(attributes, 'rules')
            ? { viewRules: attributes.rules }
            : {}),
    };
}

function parseRuntimeViewContextMap(body: unknown): ViewContextMap {
    const directScenes = getObjectAtPath(body, 'scenes');
    const nestedScenes = getObjectAtPath(body, 'application', 'scenes');
    const scenesRaw = Array.isArray(directScenes)
        ? directScenes
        : Array.isArray(nestedScenes)
          ? nestedScenes
          : null;

    if (!scenesRaw) return {};

    const contextMap: ViewContextMap = {};
    for (const sceneItem of scenesRaw) {
        const scene = asRecord(sceneItem);
        if (!scene) continue;

        const sceneKey = typeof scene.key === 'string' ? scene.key : undefined;
        const sceneName =
            typeof scene.name === 'string' ? scene.name : undefined;
        const sceneSlug =
            typeof scene.slug === 'string' ? scene.slug : undefined;
        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];

        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (!view) continue;
            const viewKey = typeof view.key === 'string' ? view.key : null;
            if (!viewKey) continue;
            contextMap[viewKey] = { sceneKey, sceneName, sceneSlug };
        }
    }

    return contextMap;
}

/**
 * Find one view's raw definition inside a runtime metadata payload.
 *
 * The guard's preflight needs a view's declared type and the layout key carrying its
 * link columns. Knack serves no per-view route to a REST API key — every candidate host
 * answers `scenes/<scene>/views/<view>` with a web-server HTML 404, so the preflight
 * failed with COULD_NOT_VERIFY_VIEW on every mutation and the menu blocks, the cascade
 * check and the human confirmation were all unreachable.
 *
 * The application payload carries the whole definition, on a route that does work and
 * that this server already reads. Sourcing the preflight from it needs no new endpoint
 * and no builder session.
 *
 * Returns the view object as it appears in the payload — `{key, attributes: {...}}` —
 * which `resolveViewAttributes` already unwraps.
 *
 * @param body Runtime metadata payload.
 * @param sceneKey Scene holding the view.
 * @param viewKey View to find.
 * @returns The raw view record, or null when either key is absent.
 */
export function findRawViewInMetadata(
    body: unknown,
    sceneKey: string,
    viewKey: string,
): Record<string, unknown> | null {
    const directScenes = getObjectAtPath(body, 'scenes');
    const nestedScenes = getObjectAtPath(body, 'application', 'scenes');
    const scenesRaw = Array.isArray(directScenes)
        ? directScenes
        : Array.isArray(nestedScenes)
          ? nestedScenes
          : null;

    if (!scenesRaw) return null;

    for (const sceneItem of scenesRaw) {
        const scene = asRecord(sceneItem);
        if (!scene || scene.key !== sceneKey) continue;

        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];
        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (view && view.key === viewKey) return view;
        }
        // The scene was found and the view was not in it. Keep scanning rather than
        // returning: a duplicate scene key would otherwise mask a later match, and a
        // wrong "not found" here becomes a refusal on a legitimate mutation.
    }

    return null;
}

/**
 * Collect, for every view in the app, the pages that view links to.
 *
 * The cascade rule needs the app's whole link graph, not the mutating view's corner of
 * it: Knack deletes a child page when its **last** referring link goes, and nothing in
 * one view's definition says whether another view still points at the same page. Built
 * from the same runtime payload the preflight reads, so the referrer count and the
 * view being changed cannot disagree about what links where.
 *
 * @param body Runtime metadata, in either the bare or `application`-wrapped shape.
 * @returns Per-scene view links, keyed by scene key. Empty when the payload carries no
 *     scenes — callers must treat that as "not measured" rather than "nothing links".
 */
export function collectSceneViewLinks(
    body: unknown,
): Map<string, SceneViewLinks[]> {
    const directScenes = getObjectAtPath(body, 'scenes');
    const nestedScenes = getObjectAtPath(body, 'application', 'scenes');
    const scenesRaw = Array.isArray(directScenes)
        ? directScenes
        : Array.isArray(nestedScenes)
          ? nestedScenes
          : null;

    const linksByScene = new Map<string, SceneViewLinks[]>();
    if (!scenesRaw) return linksByScene;

    for (const sceneItem of scenesRaw) {
        const scene = asRecord(sceneItem);
        if (!scene) continue;
        const sceneKey = typeof scene.key === 'string' ? scene.key : null;
        if (!sceneKey) continue;

        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];
        const views: SceneViewLinks[] = [];
        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (!view) continue;
            const viewKey = typeof view.key === 'string' ? view.key : null;
            if (!viewKey) continue;

            const attributes = resolveViewAttributes(view);
            if (!attributes) continue;
            // The same collector the guard runs on the view being mutated, so a link
            // shape it can see in one place it can see everywhere. A shape it cannot
            // read contributes no referrer, which keeps the count conservative: an
            // uncounted referrer leaves a page doomed, never spares one.
            // Navigation only. The broad collector is right for the view being
            // mutated and wrong here: an extra "link" makes a page look
            // multi-referenced, which spares it and skips the prompt.
            views.push({
                viewKey,
                childSceneRefs: collectNavigationRefs(attributes),
            });
        }

        // A duplicate scene key would otherwise drop the first scene's views, and a
        // dropped referrer is a page reported as doomed that is not.
        const existing = linksByScene.get(sceneKey);
        linksByScene.set(sceneKey, existing ? [...existing, ...views] : views);
    }

    return linksByScene;
}

function parseRuntimeScenes(body: unknown): SceneInfo[] {
    const directScenes = getObjectAtPath(body, 'scenes');
    const nestedScenes = getObjectAtPath(body, 'application', 'scenes');
    const scenesRaw = Array.isArray(directScenes)
        ? directScenes
        : Array.isArray(nestedScenes)
          ? nestedScenes
          : null;

    if (!scenesRaw) return [];

    const scenes: SceneInfo[] = [];
    for (const sceneItem of scenesRaw) {
        const scene = asRecord(sceneItem);
        if (!scene) continue;

        const sceneKey = typeof scene.key === 'string' ? scene.key : null;
        if (!sceneKey) continue;

        const sceneName =
            typeof scene.name === 'string' ? scene.name : undefined;
        const sceneSlug =
            typeof scene.slug === 'string' ? scene.slug : undefined;
        const parentRef =
            typeof scene.parent === 'string' && scene.parent.trim()
                ? scene.parent.trim()
                : undefined;
        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];

        const views: SceneViewInfo[] = [];
        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (!view) continue;
            const viewKey = typeof view.key === 'string' ? view.key : null;
            if (!viewKey) continue;
            const attributes = asRecord(view.attributes) || view;
            const viewName =
                typeof attributes.name === 'string'
                    ? attributes.name
                    : undefined;
            const viewType =
                typeof attributes.type === 'string'
                    ? attributes.type
                    : undefined;
            views.push({ viewKey, viewName, viewType });
        }

        scenes.push({
            sceneKey,
            sceneName,
            sceneSlug,
            parentRef,
            views,
        });
    }

    return scenes;
}

function getStringFromUnknown(value: unknown): string | null {
    if (typeof value === 'string') {
        const trimmed = value.trim();
        return trimmed ? trimmed : null;
    }

    if (Array.isArray(value)) {
        const strings = value
            .map((entry) => getStringFromUnknown(entry))
            .filter((entry): entry is string => Boolean(entry));
        if (!strings.length) return null;
        return strings.join(', ');
    }

    if (value && typeof value === 'object') {
        const rec = value as Record<string, unknown>;
        const candidates = [
            'value',
            'text',
            'email',
            'to',
            'message',
            'subject',
            'name',
        ];
        for (const key of candidates) {
            if (!(key in rec)) continue;
            const candidate = getStringFromUnknown(rec[key]);
            if (candidate) return candidate;
        }
    }

    return null;
}

function truncateText(text: string | null, maxLength = 2000): string | null {
    if (!text) return null;
    if (text.length <= maxLength) return text;
    return `${text.slice(0, maxLength)}…`;
}

function extractKtlKeywordsFromText(
    text: string,
): Array<{ keyword: string; snippet: string }> {
    // Boundary is "start of string or any non-word character" rather than just
    // whitespace/'>' — otherwise a keyword wrapped in punctuation (parentheses,
    // quotes, a leading colon/comma) is silently missed.
    const regex = /(?:^|[^a-zA-Z0-9_])(_[a-zA-Z0-9_]+)/g;
    const hits: Array<{ keyword: string; snippet: string }> = [];
    let match: RegExpExecArray | null;

    while ((match = regex.exec(text)) !== null) {
        const keyword = match[1];
        const start = Math.max(0, (match.index || 0) - 40);
        const end = Math.min(text.length, (match.index || 0) + 200);
        const snippet = text.slice(start, end).replace(/\s+/g, ' ').trim();
        hits.push({ keyword, snippet });
    }

    return hits;
}

function escapeRegExpLiteral(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * True when `text` contains `keyword` as a whole token (bounded by start/end of
 * string or a non-word character on both sides), not merely as a substring of a
 * longer word. A plain `.includes()` check would treat "_hideField" as still
 * "kept" if the new text instead contains an unrelated "_hideFieldWasRemoved".
 */
function containsKtlKeywordToken(text: string, keyword: string): boolean {
    const pattern = new RegExp(
        `(?:^|[^a-zA-Z0-9_])${escapeRegExpLiteral(keyword)}(?:$|[^a-zA-Z0-9_])`,
    );
    return pattern.test(text);
}

function extractFieldKeysFromString(text: string): string[] {
    const matches = text.match(/field_\d+/gi) || [];
    return [...new Set(matches.map((match) => match.toLowerCase()))];
}

function truncateReferenceText(text: string, maxLength = 300): string {
    const normalised = text.replace(/\s+/g, ' ').trim();
    if (normalised.length <= maxLength) return normalised;
    return `${normalised.slice(0, maxLength)}...`;
}

function classifyFieldReference(
    sourceType: FieldReference['sourceType'],
    pathParts: string[],
): string[] {
    const joined = pathParts.join('.').toLowerCase();
    const classes = new Set<string>([sourceType]);

    if (sourceType === 'schema') {
        classes.add('schemaMetadata');
    }

    if (sourceType === 'fieldMap') {
        classes.add('fieldAlias');
    }

    if (sourceType === 'viewMap') {
        classes.add('view');
    }

    if (
        /(rule|rules|filter|filters|criteria|condition|conditions)/.test(joined)
    ) {
        classes.add('rule');
    }

    if (/(record|records)/.test(joined)) {
        classes.add('record');
    }

    if (classes.has('view') && classes.has('rule') && classes.has('record')) {
        classes.add('viewRecordRule');
    }

    return [...classes];
}

function addFieldReference(
    index: CachedFieldReferenceIndex,
    dedupe: Set<string>,
    reference: FieldReference,
): void {
    const dedupeKey = JSON.stringify({
        fieldKey: reference.fieldKey,
        sourceType: reference.sourceType,
        matchType: reference.matchType,
        path: reference.path,
        alias: reference.alias || null,
        objectKey: reference.objectKey || null,
        viewKey: reference.viewKey || null,
    });

    if (dedupe.has(dedupeKey)) return;
    dedupe.add(dedupeKey);

    if (!index[reference.fieldKey]) {
        index[reference.fieldKey] = [];
    }

    index[reference.fieldKey].push(reference);
}

function scanNodeForFieldReferences(
    node: unknown,
    context: {
        sourceType: FieldReference['sourceType'];
        pathParts: string[];
        dedupe: Set<string>;
        index: CachedFieldReferenceIndex;
        objectKey?: string;
        objectName?: string;
        fieldName?: string;
        alias?: string;
        viewKey?: string;
        viewName?: string;
        viewType?: string;
        sceneKey?: string;
        sceneName?: string;
        sceneSlug?: string;
        seen?: WeakSet<object>;
    },
): void {
    if (node === null || node === undefined) return;

    if (typeof node === 'string') {
        const fieldKeys = extractFieldKeysFromString(node);
        if (!fieldKeys.length) return;

        for (const fieldKey of fieldKeys) {
            addFieldReference(context.index, context.dedupe, {
                fieldKey,
                sourceType: context.sourceType,
                matchType: 'value',
                path: context.pathParts.join('.'),
                classification: classifyFieldReference(
                    context.sourceType,
                    context.pathParts,
                ),
                containingText: truncateReferenceText(node),
                objectKey: context.objectKey,
                objectName: context.objectName,
                fieldName: context.fieldName,
                alias: context.alias,
                viewKey: context.viewKey,
                viewName: context.viewName,
                viewType: context.viewType,
                sceneKey: context.sceneKey,
                sceneName: context.sceneName,
                sceneSlug: context.sceneSlug,
            });
        }
        return;
    }

    if (Array.isArray(node)) {
        node.forEach((entry, index) => {
            scanNodeForFieldReferences(entry, {
                ...context,
                pathParts: [...context.pathParts, String(index)],
            });
        });
        return;
    }

    if (typeof node !== 'object') return;

    const seen = context.seen || new WeakSet<object>();
    if (seen.has(node)) return;
    seen.add(node);

    for (const [key, value] of Object.entries(
        node as Record<string, unknown>,
    )) {
        const nextPathParts = [...context.pathParts, key];

        if (/^field_\d+$/i.test(key)) {
            const fieldKey = key.toLowerCase();
            addFieldReference(context.index, context.dedupe, {
                fieldKey,
                sourceType: context.sourceType,
                matchType: 'propertyKey',
                path: nextPathParts.join('.'),
                classification: [
                    ...classifyFieldReference(
                        context.sourceType,
                        nextPathParts,
                    ),
                    'propertyKey',
                ],
                containingText: null,
                objectKey: context.objectKey,
                objectName: context.objectName,
                fieldName: context.fieldName,
                alias: context.alias,
                viewKey: context.viewKey,
                viewName: context.viewName,
                viewType: context.viewType,
                sceneKey: context.sceneKey,
                sceneName: context.sceneName,
                sceneSlug: context.sceneSlug,
            });
        }

        scanNodeForFieldReferences(value, {
            ...context,
            pathParts: nextPathParts,
            seen,
        });
    }
}

function buildFieldReferenceIndex(params: {
    schema: CachedSchema | null;
    fieldMap: CachedFieldMap | null;
    viewMap: CachedViewMap | null;
    viewContextMap: ViewContextMap;
}): CachedFieldReferenceIndex {
    const index: CachedFieldReferenceIndex = {};
    const dedupe = new Set<string>();

    for (const obj of params.schema?.objects || []) {
        for (const field of obj.fields || []) {
            addFieldReference(index, dedupe, {
                fieldKey: field.key.toLowerCase(),
                sourceType: 'schema',
                matchType: 'definition',
                path: `schema.objects.${obj.key}.fields.${field.key}`,
                classification: ['schema', 'schemaMetadata', 'fieldDefinition'],
                containingText: field.name || null,
                objectKey: obj.key,
                objectName: obj.name,
                fieldName: field.name,
            });

            scanNodeForFieldReferences(field, {
                sourceType: 'schema',
                pathParts: ['schema', 'objects', obj.key, 'fields', field.key],
                dedupe,
                index,
                objectKey: obj.key,
                objectName: obj.name,
                fieldName: field.name,
            });
        }
    }

    for (const [alias, entry] of Object.entries(params.fieldMap || {})) {
        addFieldReference(index, dedupe, {
            fieldKey: entry.fieldKey.toLowerCase(),
            sourceType: 'fieldMap',
            matchType: 'alias',
            path: `fieldMap.${alias}`,
            classification: ['fieldMap', 'fieldAlias'],
            containingText: alias,
            alias,
        });

        scanNodeForFieldReferences(entry, {
            sourceType: 'fieldMap',
            pathParts: ['fieldMap', alias],
            dedupe,
            index,
            alias,
        });
    }

    for (const [viewKey, viewAttrs] of Object.entries(params.viewMap || {})) {
        const sceneContext = params.viewContextMap[viewKey] || {};
        const viewName =
            typeof viewAttrs.name === 'string' ? viewAttrs.name : undefined;
        const viewType =
            typeof viewAttrs.type === 'string' ? viewAttrs.type : undefined;

        scanNodeForFieldReferences(viewAttrs, {
            sourceType: 'viewMap',
            pathParts: ['viewMap', viewKey],
            dedupe,
            index,
            viewKey,
            viewName,
            viewType,
            sceneKey: sceneContext.sceneKey,
            sceneName: sceneContext.sceneName,
            sceneSlug: sceneContext.sceneSlug,
        });
    }

    for (const references of Object.values(index)) {
        references.sort((left, right) => left.path.localeCompare(right.path));
    }

    return index;
}

function collectEmailNodes(
    node: unknown,
    pathParts: string[] = [],
    out: Array<{
        path: string;
        action: string | null;
        to: string | null;
        cc: string | null;
        bcc: string | null;
        subject: string | null;
        message: string | null;
    }> = [],
    seen = new WeakSet<object>(),
) {
    if (!node || typeof node !== 'object') return out;
    if (seen.has(node)) return out;
    seen.add(node);

    if (Array.isArray(node)) {
        node.forEach((item, index) =>
            collectEmailNodes(item, [...pathParts, String(index)], out, seen),
        );
        return out;
    }

    const rec = node as Record<string, unknown>;
    const action = typeof rec.action === 'string' ? rec.action : null;
    const to = getStringFromUnknown(
        rec.to ?? rec.to_email ?? rec.recipient ?? rec.recipients ?? rec.email,
    );
    const cc = getStringFromUnknown(rec.cc);
    const bcc = getStringFromUnknown(rec.bcc);
    const subject = getStringFromUnknown(
        rec.subject ?? rec.email_subject ?? rec.title,
    );
    const message = getStringFromUnknown(
        rec.message ?? rec.email_message ?? rec.body ?? rec.text,
    );

    const hasRecipientKey = [
        'to',
        'to_email',
        'recipient',
        'recipients',
        'email',
        'cc',
        'bcc',
    ].some((key) => key in rec);
    const isEmailAction = (action || '').toLowerCase() === 'email';
    if (isEmailAction || hasRecipientKey) {
        out.push({
            path: pathParts.length ? pathParts.join('.') : '$',
            action,
            to,
            cc,
            bcc,
            subject,
            message,
        });
    }

    for (const [key, value] of Object.entries(rec)) {
        if (value && typeof value === 'object') {
            collectEmailNodes(value, [...pathParts, key], out, seen);
        }
    }

    return out;
}

function getDefaultSecretsPath(): string {
    // ~/.knack-mcp-secrets.json (cross-platform)
    return path.join(os.homedir(), '.knack-mcp-secrets.json');
}

function loadSecrets(): SecretsMap {
    const secretsPath = ENV_SECRETS_PATH || getDefaultSecretsPath();
    const secrets = readJsonFile<SecretsMap>(secretsPath);
    if (!secrets) {
        debugLog('secrets_unavailable', {
            message:
                'Secrets file not found/readable. API-key tools will fail until secrets are configured.',
            secretsPath,
        });
        return {};
    }
    return secrets;
}

function discoverApps(knackAppsDir: string): AppConfig[] {
    const entries = fs
        .readdirSync(knackAppsDir, { withFileTypes: true })
        .filter((e) => e.isDirectory())
        .map((e) => e.name);

    const apps: AppConfig[] = [];

    for (const dirName of entries) {
        const appFolder = path.join(knackAppsDir, dirName);
        const appJsonCandidates = [
            path.join(appFolder, 'schema', 'app.json'),
            path.join(appFolder, 'app.json'),
        ];
        const appJsonPath = appJsonCandidates.find((candidate) =>
            fileExists(candidate),
        );
        const config = appJsonPath
            ? readJsonFile<Omit<AppConfig, 'appFolder'>>(appJsonPath)
            : null;
        if (!config?.appKey || !config?.appId) {
            continue;
        }
        apps.push({
            ...config,
            apiBase: config.apiBase || DEFAULT_API_BASE,
            appFolder,
        });
    }

    return apps;
}

async function readResponseTextWithLimit(
    res: Response,
    maxBytes: number,
): Promise<{ text: string; sizeBytes: number; tooLarge: boolean }> {
    const bodyAny = res.body as unknown as {
        getReader?: () => ReadableStreamDefaultReader<Uint8Array>;
    } | null;
    if (!bodyAny || typeof bodyAny.getReader !== 'function') {
        const text = await res.text();
        const sizeBytes = Buffer.byteLength(text, 'utf8');
        return {
            text: sizeBytes > maxBytes ? '' : text,
            sizeBytes,
            tooLarge: sizeBytes > maxBytes,
        };
    }

    const reader = bodyAny.getReader();
    const decoder = new TextDecoder();
    let sizeBytes = 0;
    let text = '';

    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (!value) continue;

        sizeBytes += value.byteLength;
        if (sizeBytes > maxBytes) {
            try {
                await reader.cancel();
            } catch {}
            return { text: '', sizeBytes, tooLarge: true };
        }

        text += decoder.decode(value, { stream: true });
    }

    text += decoder.decode();
    try {
        reader.releaseLock();
    } catch {}

    return { text, sizeBytes, tooLarge: false };
}

type KnackApiResult = {
    ok: boolean;
    status: number;
    body: unknown;
};

async function knackFetchJson(
    url: string,
    init: RequestInit,
): Promise<KnackApiResult> {
    const res = await fetch(url, init);
    const contentLength = Number(res.headers.get('content-length') || 0);
    if (contentLength && contentLength > MAX_RESPONSE_BYTES) {
        if (DEBUG_ENABLED) {
            console.error(
                '[knack-mcp] response_too_large',
                JSON.stringify({
                    url,
                    status: res.status,
                    contentLength,
                    maxResponseBytes: MAX_RESPONSE_BYTES,
                    precheck: true,
                }),
            );
        }
        return {
            ok: false,
            status: 413,
            body: {
                error: 'response_too_large',
                limited: true,
                url,
                upstreamStatus: res.status,
                sizeBytes: contentLength,
                maxResponseBytes: MAX_RESPONSE_BYTES,
                precheck: true,
            },
        };
    }

    const { text, sizeBytes, tooLarge } = await readResponseTextWithLimit(
        res,
        MAX_RESPONSE_BYTES,
    );
    if (tooLarge) {
        if (DEBUG_ENABLED) {
            console.error(
                '[knack-mcp] response_too_large',
                JSON.stringify({
                    url,
                    status: res.status,
                    sizeBytes,
                    maxResponseBytes: MAX_RESPONSE_BYTES,
                }),
            );
        }
        return {
            ok: false,
            status: 413,
            body: {
                error: 'response_too_large',
                limited: true,
                url,
                upstreamStatus: res.status,
                sizeBytes,
                maxResponseBytes: MAX_RESPONSE_BYTES,
            },
        };
    }

    let body: unknown = text;
    try {
        body = text ? JSON.parse(text) : null;
    } catch {
        // keep as text
    }
    return { ok: res.ok, status: res.status, body };
}

type FieldShapeInfo = {
    summary: string;
    formattedShape: unknown;
    rawShape: unknown;
    notes?: string;
    /**
     * The format/relationship object to send to knack_create_field / knack_update_field
     * when creating or editing this field type — as opposed to formattedShape/rawShape,
     * which describe what a record's *value* looks like once the field exists.
     */
    definitionShape?: string;
    definitionNotes?: string;
};

export const KNACK_CONDITIONAL_RULES_SHAPE = {
    summary:
        'Conditional field rules (dynamic default values) live in the "rules" array on a field definition, not in "format". Verified against a live app on 2026-08-14, and re-checked against a 1,911-field schema export from a second app on 2026-09-03.',
    copyAnotherFieldShape:
        '{ "key": "1", "values": [{ "type": "record", "field": "<target_field_key>", "input": "<source_field_key>", "value": "", "connection_field": null }], "criteria": [{ "field": "<test_field_key>", "value": "No", "operator": "is", "value_type": "custom", "value_field": "<auto_increment_field_key>" }] }',
    setFixedValueShape:
        '{ "key": "1", "values": [{ "type": "value", "field": "<target_field_key>", "value": 1, "connection_field": null }], "criteria": [{ "field": "<test_field_key>", "value": "Cat 1", "operator": "is", "value_type": "custom", "value_field": "<auto_increment_field_key>" }] }',
    notes: [
        'To copy another field\'s value, put the source field key in values[].input, not values[].value — putting it in "value" fails silently. Corroborated across 126 record-type values: 117 carried input with an empty value, 9 carried both, none relied on value alone.',
        'criteria[].value_type decides whether value_field means anything, and this note previously said its purpose was unclear. With value_type "field" (7 of 223 observed) value_field names the field being compared against, and was never the auto_increment key. With value_type "custom" (216) the literal in "value" is used and value_field is inert — which is why it usually holds the object\'s auto_increment key, a Builder default rather than a meaningful target. So mirroring an auto_increment key stays harmless, but a field-to-field comparison needs value_type "field" and a real target.',
        'That auto_increment observation was also narrower than it read: 200 of 223 occurrences, not all of them. The other 23 pointed at connection and concatenation fields while still on value_type "custom", so do not treat an auto_increment value_field as a required shape.',
        'A rule\'s "key" is a string when present, is not sequential (28 of 39 rule-bearing fields had keys that were not 1..n), and can be absent altogether — 17 of 236 rules carried no key at all. Builder-assigned either way; never compute it.',
        'values[] entries carry { type, field, value, connection_field } and, for type "record", an "input". One observation added an "action" key, so treat the set as open rather than closed.',
        'Conditional rules only re-evaluate on record save. A schema change alone will not re-run rules against existing records; force a save (e.g. write an unrelated field) to see the effect.',
        'The counts above come from a second app: a schema export of 1,911 fields read on 2026-09-03, which is what corrected the two claims in this list that were true of their original sample and not in general.',
    ],
};

const KNACK_FIELD_SHAPES: Record<string, FieldShapeInfo> = {
    short_text: {
        summary: 'Plain string.',
        formattedShape: '"Hello World"',
        rawShape: '"Hello World"',
    },
    paragraph_text: {
        summary: 'Multi-line text value.',
        formattedShape: '"Line one<br />Line two"',
        rawShape: '"Line one\\nLine two"',
        notes: 'Formatted output can contain HTML line breaks. Raw preserves newline characters.',
    },
    email: {
        summary: 'Email value with optional label metadata.',
        formattedShape:
            '"<a href="mailto:user@example.com">user@example.com</a>"',
        rawShape: '{ "email": "user@example.com", "label": "Work" }',
        notes: 'Formatted output is typically a mailto anchor. Raw is an object with email and label.',
    },
    phone: {
        summary: 'Phone value with structured number parts.',
        formattedShape: '"<a href="tel:07543423538">07543423538</a>"',
        rawShape:
            '{ "area": null, "number": "07543423538", "ext": null, "full": "07543423538", "country": null, "formatted": "07543423538" }',
        notes: 'Formatted output is typically a tel anchor. Raw is an object containing number parts and preformatted variants.',
    },
    number: {
        summary: 'Numeric value.',
        formattedShape: '"$1,234.50"',
        rawShape: 1234.5,
        notes: 'Raw is a JS number. Formatted output depends on the field display settings and may include prefixes or suffixes.',
    },
    currency: {
        summary: 'Currency value.',
        formattedShape: '"$1,234.56"',
        rawShape: '"1234.56"',
        notes: 'Formatted includes currency symbols and separators. Raw is commonly a numeric string rather than a JS number.',
    },
    auto_increment: {
        summary: 'Auto-incrementing integer.',
        formattedShape: '"42"',
        rawShape: 42,
    },
    boolean: {
        summary: 'Yes/No field. Also referred to as yes_no.',
        formattedShape: '"Yes"',
        rawShape: true,
        notes: 'Raw is a JS boolean. Formatted is typically "Yes" or "No".',
    },
    yes_no: {
        summary: 'Yes/No boolean field.',
        formattedShape: '"Yes"',
        rawShape: true,
        notes: 'Alias for boolean. Raw is a JS boolean.',
    },
    rating: {
        summary: 'Numeric rating value.',
        formattedShape: '"3"',
        rawShape: 3,
    },
    equation: {
        summary:
            'Computed equation result whose shape depends on the configured return type.',
        formattedShape: '"(-42.00)" | "05/01/2026"',
        rawShape:
            '42 | "2026-01-05" | { "date": "01/05/2026", "date_formatted": "05/01/2026", "unix_timestamp": 1767571200000 }',
        notes: 'Equation fields can return numbers, plain strings, or date-like values depending on configuration. For date-returning equations, raw may be a scalar date string or a structured date object, while formatted applies the field display format.',
        definitionShape:
            '{ "equation": "{field_1387.field_1761}*{field_1394.field_439}+{field_1387.field_1762}*{field_1394.field_440}", "equation_type": "numeric", "date_type": "", "date_result": "", "date_format": "mm/dd/yyyy", "time_format": "Ignore Time", "count_field": "Connection", "formula_field": "Field", "rounding": "none", "precision": "2", "mark_decimal": ".", "mark_thousands": "", "pre": "£", "post": "", "format": "" }',
        definitionNotes:
            'Reference local fields as {field_key} and fields on connected records as {connection_field_key.target_field_key} — the qualified form only, since bare names like {Cat 1 Price} have been observed to resolve correctly on one read and silently to 0 on the next with no error either way. One equation can cross more than one connection field on the same object. Only many-to-one / one-to-one connections can be crossed this way; many-to-many connections are not exposed to equations. Equation values recalculate on record save — allow ~15s after a schema change before asserting against them, and always assert against a known non-zero expected value, since an unresolved reference returns 0 rather than an error and a vacuous test would still pass. knack_create_field / knack_update_field now reject or warn on unresolvable {...} tokens before the write reaches the app. Corroborated on a 1,911-field export from a second app on 2026-09-03: all 185 equation tokens used the field-key form and none were name-based, and of 39 connections crossed by an equation none was many-to-many — 38 one-to-many and 1 one-to-one. The date_type, date_result, date_format and time_format keys in the shape above are optional: 85 of 90 equation fields carried them and 5 did not.',
    },
    sum: {
        summary: 'Numeric aggregate (sum of connected records).',
        formattedShape: '"100"',
        rawShape: 100,
    },
    count: {
        summary: 'Numeric count of connected records.',
        formattedShape: '"5"',
        rawShape: 5,
    },
    average: {
        summary: 'Numeric average of connected records.',
        formattedShape: '"3.5"',
        rawShape: 3.5,
    },
    min: {
        summary: 'Minimum value from connected records.',
        formattedShape: '"1"',
        rawShape: 1,
    },
    max: {
        summary: 'Maximum value from connected records.',
        formattedShape: '"10"',
        rawShape: 10,
    },
    concatenation: {
        summary: 'Concatenated string from other fields.',
        formattedShape: '"John Smith - Manager"',
        rawShape: '"John Smith - Manager"',
    },
    name: {
        summary: 'Full name composed of title, first, middle, last, suffix.',
        formattedShape: '"John A. Smith"',
        rawShape:
            '{ "title": "Mr", "first": "John", "middle": "A", "last": "Smith", "full": "John A. Smith" }',
        notes: 'Raw is an object with individual name parts. Optional keys such as middle or suffix may be omitted or blank.',
    },
    address: {
        summary: 'Postal address with geocoordinates.',
        formattedShape: '"123 Main St<br />Springfield, IL 62701"',
        rawShape:
            '{ "street": "123 Main St", "street2": null, "city": "Springfield", "state": "IL", "zip": "62701", "country": null, "longitude": null, "latitude": null, "full": "123 Main St Springfield, IL 62701" }',
        notes: 'Formatted output can contain HTML line breaks. Raw includes address components plus a full string; geo fields are often null.',
    },
    date_time: {
        summary: 'Date and/or time value.',
        formattedShape: '"01/15/2024 10:30 am"',
        rawShape:
            '{ "date": "01/15/2024", "date_formatted": "January 15, 2024", "hours": "10", "minutes": "30", "am_pm": "AM", "unix_timestamp": 1705316400000, "iso_timestamp": "2024-01-15T10:30:00.000Z", "timestamp": "01/15/2024 10:30 am" }',
        notes: 'Formatted output depends on the field configuration and may be date-only, time-only, or a range. Raw for native date/time fields is typically a structured object with date/time parts, proper_* timestamp keys, and an optional to object for ranges rather than a scalar string.',
    },
    timer: {
        summary: 'Time tracking timer with start/stop times.',
        formattedShape: '"2:30:00"',
        rawShape:
            '{ "times": [{ "from": { "date": "01/15/2024", "hours": "10", "minutes": "00", "am_pm": "AM" }, "to": { "date": "01/15/2024", "hours": "12", "minutes": "30", "am_pm": "PM" } }], "running": false, "hours": 2.5, "minutes": 150, "seconds": 9000 }',
        notes: 'Formatted is human-readable elapsed time. Raw contains an array of from/to time pairs plus totals.',
    },
    multiple_choice: {
        summary: 'One or more selected options.',
        formattedShape: '"Option A, Option B"',
        rawShape: '"Option A" | ["Option A", "Option B"]',
        notes: 'Raw is a string for single-select controls and an array for multi-select controls. Formatted is a display string.',
    },
    connection: {
        summary: 'Reference to one or more records in another object.',
        formattedShape:
            '"<span class="abc123def456" data-kn="connection-value">Record Label A</span>"',
        rawShape:
            '[{ "id": "abc123def456", "identifier": "Record Label A" }, { "id": "789xyz", "identifier": "Record Label B" }]',
        notes: 'Raw is an array of objects with id and identifier. Formatted output is HTML, usually one span per connected record, not a plain comma-joined string.',
        definitionShape:
            '{ "relationship": { "object": "object_12", "has": "one", "belongs_to": "many" } }',
        definitionNotes:
            'format.object / relationship.object must be an object key (e.g. object_12), not a name. "has"/"belongs_to" describe cardinality from this object\'s perspective — only many-to-one / one-to-one connections can later be referenced from an equation field; many-to-many connections cannot.',
    },
    file: {
        summary: 'Uploaded file attachment.',
        formattedShape: '"document.pdf"',
        rawShape:
            '{ "id": "abc123", "filename": "document.pdf", "url": "https://...", "thumb_url": null, "size": 204800, "mime_type": "application/pdf" }',
        notes: 'Raw includes the download URL and file metadata.',
    },
    image: {
        summary: 'Uploaded image attachment.',
        formattedShape: '"<img src=\'...\' />"',
        rawShape:
            '{ "id": "abc123", "filename": "photo.jpg", "url": "https://...photo.jpg", "thumb_url": "https://...photo_thumb.jpg", "size": 102400, "mime_type": "image/jpeg" }',
        notes: 'Raw includes full-size and thumbnail URLs. Formatted is an HTML img tag.',
    },
    signature: {
        summary: 'Captured signature.',
        formattedShape: '"<img src="data:image/svg+xml;base64,..." />"',
        rawShape: '{ "svg": "<svg ...></svg>", "base30": "2OZ9jcd..." }',
        notes: 'Observed raw payload contains SVG markup plus a base30 stroke encoding rather than hosted image URLs or timestamp metadata.',
    },
    link: {
        summary: 'Hyperlink with URL and display label.',
        formattedShape: '"<a href=\'https://example.com\'>Example</a>"',
        rawShape: '{ "url": "https://example.com", "label": "Example" }',
        notes: 'Raw has url and label. Formatted is an HTML anchor tag.',
    },
    rich_text: {
        summary: 'HTML rich text content.',
        formattedShape: '"<p>Hello <strong>World</strong></p>"',
        rawShape: '"<p>Hello <strong>World</strong></p>"',
        notes: 'Both formatted and raw are HTML strings.',
    },
    user_roles: {
        summary: 'User role assignments (array of role names).',
        formattedShape: '"Admin, Manager"',
        rawShape: '["Admin", "Manager"]',
        notes: 'Raw is an array of role name strings.',
    },
    password: {
        summary: 'Password validation status only (never the actual password).',
        formattedShape: '""',
        rawShape: '{ "validation": "good" }',
        notes: 'Knack never returns the password value. Raw only indicates validation strength.',
    },
};

function getFieldShapeInfo(fieldType: string): FieldShapeInfo | null {
    return KNACK_FIELD_SHAPES[fieldType.toLowerCase()] || null;
}

type SeedCsvObject = {
    objectKey: string;
    objectName: string;
    suggestedUniqueImportKey: string;
    csvContent: string;
    notes: string[];
};

type SeedCsvWorkbook = {
    importOrder: Array<{
        objectKey: string;
        objectName: string;
        suggestedUniqueImportKey: string;
    }>;
    objects: SeedCsvObject[];
};

type ExternalConnectionLookup = {
    objectKey: string;
    objectName?: string;
    values: string[];
    source: 'api';
    lookupField: 'identifier';
};

const CONNECTION_DISPLAY_VALUE_PRIORITY = [
    'identifier',
    'display',
    'name',
    'label',
    'id',
] as const;

type SeedObjectMeta = {
    object: CachedObject;
    objectName: string;
    uniqueImportKey: string;
    uniqueImportField?: CachedField;
    labelField?: CachedField;
    syntheticLabelField?: string;
    rowCount: number;
    uniqueValues: string[];
    usedPlaceholderChoiceFields: string[];
    skippedFields: string[];
};

const NON_IMPORTABLE_FIELD_TYPES = new Set([
    'auto_increment',
    'equation',
    'sum',
    'count',
    'average',
    'min',
    'max',
    'concatenation',
    'file',
    'image',
    'signature',
    'timer',
    'password',
]);

const SAMPLE_FIRST_NAMES = [
    'Avery',
    'Jordan',
    'Casey',
    'Morgan',
    'Riley',
    'Taylor',
];
const SAMPLE_LAST_NAMES = [
    'Bennett',
    'Carter',
    'Diaz',
    'Foster',
    'Hayes',
    'Morgan',
];
const SAMPLE_COMPANY_PREFIXES = [
    'Acme',
    'Bluebird',
    'Cedar',
    'Northwind',
    'Summit',
    'Harbor',
];
const SAMPLE_COMPANY_SUFFIXES = [
    'Logistics',
    'Health',
    'Supply',
    'Advisory',
    'Labs',
    'Services',
];
const SAMPLE_STREETS = [
    '100 Main St',
    '245 Oak Ave',
    '18 Market St',
    '77 River Rd',
    '910 Sunset Blvd',
    '62 Cedar Ln',
];
const SAMPLE_CITIES = [
    'Austin',
    'Denver',
    'Madison',
    'Phoenix',
    'Raleigh',
    'Seattle',
];
const SAMPLE_STATES = ['TX', 'CO', 'WI', 'AZ', 'NC', 'WA'];

function toSnakeCase(value: string): string {
    return value
        .trim()
        .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
        .replace(/[^a-zA-Z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '')
        .replace(/_+/g, '_')
        .toLowerCase();
}

function singularize(value: string): string {
    const trimmed = value.trim();
    if (trimmed.endsWith('ies') && trimmed.length > 3)
        return `${trimmed.slice(0, -3)}y`;
    if (trimmed.endsWith('ses') && trimmed.length > 3)
        return trimmed.slice(0, -2);
    if (trimmed.endsWith('s') && !trimmed.endsWith('ss') && trimmed.length > 1)
        return trimmed.slice(0, -1);
    return trimmed;
}

function humanizeObjectName(value: string): string {
    return singularize(value.replace(/[_-]+/g, ' ')).trim() || 'Record';
}

function makeSyntheticImportKey(objectName: string): string {
    const slug = toSnakeCase(singularize(objectName)) || 'record';
    return /(_id|_code|_sku|_key|_email)$/.test(slug) ? slug : `${slug}_code`;
}

function makeSyntheticLabelField(objectName: string): string {
    const slug = toSnakeCase(singularize(objectName)) || 'record';
    return slug.endsWith('_name') ? slug : `${slug}_name`;
}

function makeKeyPrefix(objectName: string): string {
    const parts = toSnakeCase(singularize(objectName))
        .split('_')
        .filter(Boolean);
    const base =
        parts.length > 1
            ? parts.map((part) => part[0]).join('')
            : (parts[0] || 'rec').slice(0, 4);
    return base.toUpperCase();
}

function makeUniqueValue(objectName: string, index: number): string {
    return `${makeKeyPrefix(objectName)}-${String(index + 1).padStart(3, '0')}`;
}

function inferLabelValue(objectName: string, rowIndex: number): string {
    const lowerName = objectName.toLowerCase();
    if (
        /(company|client|customer|vendor|supplier|partner|agency|business|organization|organisation)/.test(
            lowerName,
        )
    ) {
        return `${SAMPLE_COMPANY_PREFIXES[rowIndex % SAMPLE_COMPANY_PREFIXES.length]} ${SAMPLE_COMPANY_SUFFIXES[rowIndex % SAMPLE_COMPANY_SUFFIXES.length]}`;
    }
    if (/(employee|user|contact|person|member|staff|owner)/.test(lowerName)) {
        return `${SAMPLE_FIRST_NAMES[rowIndex % SAMPLE_FIRST_NAMES.length]} ${SAMPLE_LAST_NAMES[rowIndex % SAMPLE_LAST_NAMES.length]}`;
    }
    const humanName = humanizeObjectName(objectName);
    return `${humanName} ${rowIndex + 1}`;
}

function escapeCsvCell(value: string): string {
    if (/[",\n]/.test(value)) {
        return `"${value.replace(/"/g, '""')}"`;
    }
    return value;
}

function buildCsv(
    headers: string[],
    rows: Array<Record<string, string>>,
): string {
    const headerLine = headers.map(escapeCsvCell).join(',');
    const dataLines = rows.map((row) =>
        headers.map((header) => escapeCsvCell(row[header] || '')).join(','),
    );
    return [headerLine, ...dataLines].join('\n');
}

function isImportableField(field: CachedField): boolean {
    return !NON_IMPORTABLE_FIELD_TYPES.has((field.type || '').toLowerCase());
}

function getFieldHeader(field: CachedField): string {
    return field.name?.trim() || field.key;
}

function getMultipartHeaders(field: CachedField): string[] {
    const header = getFieldHeader(field);
    switch ((field.type || '').toLowerCase()) {
        case 'name':
            return [
                `${header} Title`,
                `${header} First`,
                `${header} Middle`,
                `${header} Last`,
                `${header} Suffix`,
            ];
        case 'address':
            return [
                `${header} Street`,
                `${header} Street 2`,
                `${header} City`,
                `${header} State`,
                `${header} Zip`,
                `${header} Country`,
            ];
        default:
            return [header];
    }
}

function chooseUniqueImportField(
    fields: CachedField[],
): CachedField | undefined {
    const pattern =
        /\b(code|sku|external id|external_id|import key|import_key|unique key|unique_key|email|record key|record_key|id)\b/i;
    return fields.find((field) => {
        const type = (field.type || '').toLowerCase();
        return (
            !['connection', 'multiple_choice', 'address', 'name'].includes(
                type,
            ) && pattern.test(getFieldHeader(field))
        );
    });
}

function chooseLabelField(fields: CachedField[]): CachedField | undefined {
    const preferred = fields.find((field) =>
        /\b(name|title|label)\b/i.test(getFieldHeader(field)),
    );
    if (preferred) return preferred;
    return fields.find((field) =>
        ['short_text', 'paragraph_text', 'email', 'name'].includes(
            (field.type || '').toLowerCase(),
        ),
    );
}

function getDefaultChoiceOptions(field: CachedField): string[] {
    if ((field.type || '').toLowerCase() === 'user_roles') {
        return ['Admin', 'Manager', 'Viewer'];
    }
    return ['Option A', 'Option B', 'Option C'];
}

function getSeedRowCount(fields: CachedField[], minimumRows: number): number {
    const optionCount = fields.reduce(
        (max, field) => Math.max(max, field.choiceOptions?.length || 0),
        0,
    );
    return Math.max(minimumRows, Math.min(optionCount || minimumRows, 6));
}

function buildSeedObjectMeta(
    object: CachedObject,
    minimumRows: number,
): SeedObjectMeta {
    const objectName = object.name || object.key;
    const importableFields = (object.fields || []).filter(isImportableField);
    const uniqueImportField = chooseUniqueImportField(importableFields);
    const labelField = chooseLabelField(importableFields);
    const uniqueImportKey = uniqueImportField
        ? getFieldHeader(uniqueImportField)
        : makeSyntheticImportKey(objectName);
    const syntheticLabelField = labelField
        ? undefined
        : makeSyntheticLabelField(objectName);
    const rowCount = getSeedRowCount(importableFields, minimumRows);

    return {
        object,
        objectName,
        uniqueImportKey,
        uniqueImportField,
        labelField,
        syntheticLabelField,
        rowCount,
        uniqueValues: Array.from({ length: rowCount }, (_, index) =>
            makeUniqueValue(objectName, index),
        ),
        usedPlaceholderChoiceFields: [],
        skippedFields: (object.fields || [])
            .filter((field) => !isImportableField(field))
            .map((field) => getFieldHeader(field)),
    };
}

function topologicallySortObjects(objects: CachedObject[]): CachedObject[] {
    const objectsByKey = new Map(objects.map((object) => [object.key, object]));
    const dependents = new Map<string, Set<string>>();
    const indegree = new Map<string, number>(
        objects.map((object) => [object.key, 0]),
    );

    for (const object of objects) {
        for (const field of object.fields || []) {
            if (
                (field.type || '').toLowerCase() !== 'connection' ||
                !field.connectedObject ||
                !objectsByKey.has(field.connectedObject)
            )
                continue;
            if (!dependents.has(field.connectedObject))
                dependents.set(field.connectedObject, new Set());
            const downstream = dependents.get(field.connectedObject);
            if (!downstream?.has(object.key)) {
                downstream?.add(object.key);
                indegree.set(object.key, (indegree.get(object.key) || 0) + 1);
            }
        }
    }

    const queue = objects
        .filter((object) => (indegree.get(object.key) || 0) === 0)
        .sort((a, b) => (a.name || a.key).localeCompare(b.name || b.key));
    const ordered: CachedObject[] = [];

    while (queue.length) {
        const next = queue.shift();
        if (!next) continue;
        ordered.push(next);
        for (const dependentKey of dependents.get(next.key) || []) {
            const remaining = (indegree.get(dependentKey) || 0) - 1;
            indegree.set(dependentKey, remaining);
            if (remaining === 0) {
                const dependent = objectsByKey.get(dependentKey);
                if (dependent) {
                    queue.push(dependent);
                    queue.sort((a, b) =>
                        (a.name || a.key).localeCompare(b.name || b.key),
                    );
                }
            }
        }
    }

    if (ordered.length === objects.length) return ordered;

    const seen = new Set(ordered.map((object) => object.key));
    const remaining = objects
        .filter((object) => !seen.has(object.key))
        .sort((a, b) => (a.name || a.key).localeCompare(b.name || b.key));
    return [...ordered, ...remaining];
}

function populateMultipartField(
    row: Record<string, string>,
    headers: string[],
    rowIndex: number,
): void {
    if (headers.length === 5) {
        row[headers[0]] = rowIndex % 2 === 0 ? 'Ms' : 'Mr';
        row[headers[1]] =
            SAMPLE_FIRST_NAMES[rowIndex % SAMPLE_FIRST_NAMES.length];
        row[headers[2]] = '';
        row[headers[3]] =
            SAMPLE_LAST_NAMES[rowIndex % SAMPLE_LAST_NAMES.length];
        row[headers[4]] = '';
        return;
    }

    row[headers[0]] = SAMPLE_STREETS[rowIndex % SAMPLE_STREETS.length];
    row[headers[1]] = rowIndex % 3 === 0 ? `Suite ${rowIndex + 100}` : '';
    row[headers[2]] = SAMPLE_CITIES[rowIndex % SAMPLE_CITIES.length];
    row[headers[3]] = SAMPLE_STATES[rowIndex % SAMPLE_STATES.length];
    row[headers[4]] = `78${String(rowIndex).padStart(3, '0')}`;
    row[headers[5]] = 'USA';
}

function populateScalarField(
    row: Record<string, string>,
    field: CachedField,
    meta: SeedObjectMeta,
    metasByKey: Map<string, SeedObjectMeta>,
    externalConnectionLookups: Map<string, ExternalConnectionLookup>,
    rowIndex: number,
): void {
    const header = getFieldHeader(field);
    const fieldType = (field.type || '').toLowerCase();
    const lowerHeader = header.toLowerCase();
    const shouldUseMultipleValuesOnAlternatingRows = Boolean(
        field.allowsMultiple && rowIndex % 2 === 1,
    );

    if (meta.uniqueImportField?.key === field.key) {
        row[header] = meta.uniqueValues[rowIndex];
        return;
    }

    if (meta.labelField?.key === field.key) {
        row[header] = inferLabelValue(meta.objectName, rowIndex);
        return;
    }

    switch (fieldType) {
        case 'connection': {
            const connectedMeta = field.connectedObject
                ? metasByKey.get(field.connectedObject)
                : undefined;
            const externalLookup = field.connectedObject
                ? externalConnectionLookups.get(field.connectedObject)
                : undefined;
            if (!connectedMeta && externalLookup?.values.length) {
                const selectedValues = [
                    externalLookup.values[
                        rowIndex % externalLookup.values.length
                    ],
                ];
                if (
                    shouldUseMultipleValuesOnAlternatingRows &&
                    externalLookup.values.length > 1
                ) {
                    selectedValues.push(
                        externalLookup.values[
                            (rowIndex + 1) % externalLookup.values.length
                        ],
                    );
                }
                row[header] = selectedValues.join(',');
                return;
            }

            if (!connectedMeta) {
                row[header] = makeUniqueValue(
                    field.connectedObject || header,
                    0,
                );
                return;
            }

            const selectedValues = [
                connectedMeta.uniqueValues[
                    rowIndex % connectedMeta.uniqueValues.length
                ],
            ];
            if (
                shouldUseMultipleValuesOnAlternatingRows &&
                connectedMeta.uniqueValues.length > 1
            ) {
                selectedValues.push(
                    connectedMeta.uniqueValues[
                        (rowIndex + 1) % connectedMeta.uniqueValues.length
                    ],
                );
            }
            row[header] = selectedValues.join(',');
            return;
        }
        case 'multiple_choice':
        case 'user_roles': {
            const options = field.choiceOptions?.length
                ? field.choiceOptions
                : getDefaultChoiceOptions(field);
            if (!field.choiceOptions?.length) {
                meta.usedPlaceholderChoiceFields.push(header);
            }
            const selectedValues = [options[rowIndex % options.length]];
            if (
                shouldUseMultipleValuesOnAlternatingRows &&
                options.length > 1
            ) {
                selectedValues.push(options[(rowIndex + 1) % options.length]);
            }
            row[header] = selectedValues.join(',');
            return;
        }
        case 'email':
            row[header] =
                `${toSnakeCase(singularize(meta.objectName)) || 'record'}${rowIndex + 1}@example.com`;
            return;
        case 'phone':
            row[header] = `555010${String(rowIndex + 1).padStart(3, '0')}`;
            return;
        case 'number':
        case 'currency':
            row[header] = ((rowIndex + 1) * 1250).toFixed(
                fieldType === 'currency' ? 2 : 0,
            );
            return;
        case 'boolean':
        case 'yes_no':
            row[header] = rowIndex % 2 === 0 ? 'Yes' : 'No';
            return;
        case 'rating':
            row[header] = String((rowIndex % 5) + 1);
            return;
        case 'date_time':
            row[header] = `2026-01-${String(rowIndex + 5).padStart(2, '0')}`;
            return;
        case 'paragraph_text':
        case 'rich_text':
            row[header] =
                `Sample ${humanizeObjectName(meta.objectName).toLowerCase()} notes for workflow testing row ${rowIndex + 1}.`;
            return;
        case 'link':
            row[header] =
                `https://example.com/${toSnakeCase(singularize(meta.objectName)) || 'record'}/${rowIndex + 1}`;
            return;
        case 'short_text':
        default:
            row[header] = lowerHeader.includes('status')
                ? `Active ${rowIndex + 1}`
                : lowerHeader.includes('code') ||
                    lowerHeader.includes('sku') ||
                    lowerHeader.includes('id')
                  ? meta.uniqueValues[rowIndex]
                  : `${inferLabelValue(meta.objectName, rowIndex)} ${header}`;
            return;
    }
}

export function generateSeedCsvWorkbook(
    schema: CachedSchema,
    options?: {
        objectKeys?: string[];
        rowsPerObject?: number;
        externalConnectionLookups?: Record<string, ExternalConnectionLookup>;
    },
): SeedCsvWorkbook {
    const requestedKeys = options?.objectKeys?.length
        ? new Set(options.objectKeys)
        : null;
    const selectedObjects = (schema.objects || []).filter(
        (object) => !requestedKeys || requestedKeys.has(object.key),
    );
    const orderedObjects = topologicallySortObjects(selectedObjects);
    const metas = orderedObjects.map((object) =>
        buildSeedObjectMeta(object, Math.max(options?.rowsPerObject || 4, 2)),
    );
    const metasByKey = new Map(metas.map((meta) => [meta.object.key, meta]));
    const externalConnectionLookups = new Map(
        Object.entries(options?.externalConnectionLookups || {}),
    );

    const objects: SeedCsvObject[] = metas.map((meta) => {
        const headers: string[] = [];
        const rows = Array.from(
            { length: meta.rowCount },
            () => ({}) as Record<string, string>,
        );
        const importableFields = (meta.object.fields || []).filter(
            isImportableField,
        );

        const pushHeader = (header: string) => {
            if (!headers.includes(header)) headers.push(header);
        };

        pushHeader(meta.uniqueImportKey);
        if (meta.syntheticLabelField) {
            pushHeader(meta.syntheticLabelField);
        }

        for (const field of importableFields) {
            for (const header of getMultipartHeaders(field)) {
                pushHeader(header);
            }
        }

        rows.forEach((row, rowIndex) => {
            row[meta.uniqueImportKey] = meta.uniqueValues[rowIndex];
            if (meta.syntheticLabelField) {
                row[meta.syntheticLabelField] = inferLabelValue(
                    meta.objectName,
                    rowIndex,
                );
            }

            for (const field of importableFields) {
                const multipartHeaders = getMultipartHeaders(field);
                if (multipartHeaders.length > 1) {
                    populateMultipartField(row, multipartHeaders, rowIndex);
                } else {
                    populateScalarField(
                        row,
                        field,
                        meta,
                        metasByKey,
                        externalConnectionLookups,
                        rowIndex,
                    );
                }
            }
        });

        const notes: string[] = [];
        if (!meta.uniqueImportField) {
            notes.push(
                `Suggested unique import key "${meta.uniqueImportKey}" is synthetic so child CSVs have a stable lookup value.`,
            );
        }
        for (const field of importableFields.filter(
            (entry) => (entry.type || '').toLowerCase() === 'connection',
        )) {
            const connectedMeta = field.connectedObject
                ? metasByKey.get(field.connectedObject)
                : undefined;
            const externalLookup = field.connectedObject
                ? externalConnectionLookups.get(field.connectedObject)
                : undefined;
            if (connectedMeta) {
                notes.push(
                    `Connection field "${getFieldHeader(field)}" uses ${connectedMeta.objectName}.${connectedMeta.uniqueImportKey} as the import lookup value.`,
                );
                continue;
            }
            if (externalLookup) {
                notes.push(
                    `Connection field "${getFieldHeader(field)}" uses existing ${externalLookup.objectName || field.connectedObject || 'connected object'} display values fetched from the API (${externalLookup.lookupField}).`,
                );
                continue;
            }
            notes.push(
                `Connection field "${getFieldHeader(field)}" uses ${field.connectedObject || 'the connected object'} via an existing unique lookup field as the import lookup value.`,
            );
        }
        if (meta.usedPlaceholderChoiceFields.length) {
            const uniquePlaceholderFields = Array.from(
                new Set(meta.usedPlaceholderChoiceFields),
            );
            notes.push(
                `Schema metadata did not expose exact option labels for ${uniquePlaceholderFields.join(', ')}; placeholder option labels were used and should be replaced before import if needed.`,
            );
        }
        if (meta.skippedFields.length) {
            notes.push(
                `Skipped non-importable/system fields: ${meta.skippedFields.join(', ')}.`,
            );
        }

        return {
            objectKey: meta.object.key,
            objectName: meta.objectName,
            suggestedUniqueImportKey: meta.uniqueImportKey,
            csvContent: buildCsv(headers, rows),
            notes,
        };
    });

    return {
        importOrder: metas.map((meta) => ({
            objectKey: meta.object.key,
            objectName: meta.objectName,
            suggestedUniqueImportKey: meta.uniqueImportKey,
        })),
        objects,
    };
}

type ShapeValidationStatus = 'match' | 'mismatch' | 'skipped' | 'unknown';

type ShapeValidationResult = {
    status: ShapeValidationStatus;
    observedFormattedShape: string;
    observedRawShape: string;
    findings: string[];
};

function isBlankKnackValue(value: unknown): boolean {
    return (
        value === null ||
        value === undefined ||
        value === '' ||
        (Array.isArray(value) && value.length === 0)
    );
}

function isHtmlLikeString(value: string): boolean {
    return /<[^>]+>/.test(value);
}

function getObservedShape(value: unknown): string {
    if (value === null) return 'null';
    if (value === undefined) return 'undefined';
    if (Array.isArray(value)) {
        if (!value.length) return 'array(empty)';
        const firstNonBlank = value.find((entry) => !isBlankKnackValue(entry));
        if (firstNonBlank === undefined) return 'array(empty-like)';
        return `array(${getObservedShape(firstNonBlank)})`;
    }
    if (typeof value === 'string') {
        return isHtmlLikeString(value) ? 'html-string' : 'string';
    }
    if (typeof value === 'number' || typeof value === 'boolean') {
        return typeof value;
    }

    const rec = asRecord(value);
    if (rec) {
        const keys = Object.keys(rec).slice(0, 6);
        return `object(${keys.join(', ')})`;
    }

    return typeof value;
}

function getValuePreview(value: unknown): unknown {
    if (typeof value === 'string') {
        return truncateText(value, 160);
    }
    if (Array.isArray(value)) {
        return value.slice(0, 2);
    }

    const rec = asRecord(value);
    if (rec) {
        return Object.fromEntries(Object.entries(rec).slice(0, 8));
    }

    return value;
}

function rawHasKeys(value: unknown, keys: string[]): boolean {
    const rec = asRecord(value);
    return Boolean(rec) && keys.some((key) => key in rec!);
}

function rawIsConnectionArray(value: unknown): boolean {
    if (!Array.isArray(value)) return false;
    return value.every((entry) => {
        const rec = asRecord(entry);
        if (!rec) return false;
        return typeof rec.id === 'string' || typeof rec.identifier === 'string';
    });
}

function rawIsStringArray(value: unknown): boolean {
    return (
        Array.isArray(value) &&
        value.every((entry) => typeof entry === 'string')
    );
}

function extractRecordList(body: unknown): Record<string, unknown>[] {
    if (Array.isArray(body)) {
        return body
            .map((entry) => asRecord(entry))
            .filter((entry): entry is Record<string, unknown> =>
                Boolean(entry),
            );
    }

    const rec = asRecord(body);
    const records = rec?.records;
    if (!Array.isArray(records)) return [];
    return records
        .map((entry) => asRecord(entry))
        .filter((entry): entry is Record<string, unknown> => Boolean(entry));
}

function extractConnectionDisplayValues(body: unknown): string[] {
    const values: string[] = [];
    const seen = new Set<string>();

    for (const record of extractRecordList(body)) {
        const value = CONNECTION_DISPLAY_VALUE_PRIORITY.map((key) =>
            getStringFromUnknown(record[key]),
        ).find((candidate): candidate is string => Boolean(candidate));
        if (!value) continue;
        const dedupeKey = value.toLowerCase();
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);
        values.push(value);
    }

    return values;
}

function validateFieldShape(
    fieldType: string,
    formatted: unknown,
    raw: unknown,
): ShapeValidationResult {
    const observedFormattedShape = getObservedShape(formatted);
    const observedRawShape = getObservedShape(raw);

    if (isBlankKnackValue(formatted) && isBlankKnackValue(raw)) {
        return {
            status: 'skipped',
            observedFormattedShape,
            observedRawShape,
            findings: [],
        };
    }

    const findings: string[] = [];
    const addFinding = (condition: boolean, message: string) => {
        if (!condition) findings.push(message);
    };

    switch (fieldType.toLowerCase()) {
        case 'short_text':
        case 'paragraph_text':
        case 'concatenation':
        case 'rich_text':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                typeof raw === 'string',
                'Raw value should be a string.',
            );
            break;
        case 'email':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string or HTML anchor.',
            );
            addFinding(
                rawHasKeys(raw, ['email']),
                'Raw value should be an object containing an email key.',
            );
            break;
        case 'phone':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string or HTML anchor.',
            );
            addFinding(
                rawHasKeys(raw, ['number', 'full', 'formatted']),
                'Raw value should be a phone object with number/full/formatted keys.',
            );
            break;
        case 'number':
            addFinding(
                typeof formatted === 'string' || typeof formatted === 'number',
                'Formatted value should be a string or number.',
            );
            addFinding(
                typeof raw === 'number' || typeof raw === 'string',
                'Raw value should be a number or numeric string.',
            );
            break;
        case 'currency':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                typeof raw === 'number' || typeof raw === 'string',
                'Raw value should be a number or numeric string.',
            );
            break;
        case 'auto_increment':
        case 'rating':
        case 'sum':
        case 'count':
        case 'average':
        case 'min':
        case 'max':
            addFinding(
                typeof formatted === 'string' || typeof formatted === 'number',
                'Formatted value should be numeric-like.',
            );
            addFinding(
                typeof raw === 'number',
                'Raw value should be a number.',
            );
            break;
        case 'boolean':
        case 'yes_no':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a display string such as Yes/No.',
            );
            addFinding(
                typeof raw === 'boolean',
                'Raw value should be a boolean.',
            );
            break;
        case 'equation':
            addFinding(
                typeof formatted === 'string' || typeof formatted === 'number',
                'Formatted value should be a string or number.',
            );
            addFinding(
                typeof raw === 'number' ||
                    typeof raw === 'string' ||
                    asRecord(raw) !== null,
                'Raw value should be a number, string, or structured date-like object.',
            );
            break;
        case 'name':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, [
                    'first',
                    'last',
                    'full',
                    'title',
                    'middle',
                    'suffix',
                ]),
                'Raw value should be an object containing name parts.',
            );
            break;
        case 'address':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string, often with HTML line breaks.',
            );
            addFinding(
                rawHasKeys(raw, ['street', 'city', 'zip', 'full']),
                'Raw value should be an object containing address components.',
            );
            break;
        case 'date_time':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, [
                    'date',
                    'timestamp',
                    'unix_timestamp',
                    'iso_timestamp',
                    'to',
                ]),
                'Raw value should be a structured date/time object.',
            );
            break;
        case 'timer':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, ['times', 'hours', 'minutes', 'seconds']),
                'Raw value should be a timer object containing time segments or totals.',
            );
            break;
        case 'multiple_choice':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a display string.',
            );
            addFinding(
                typeof raw === 'string' || Array.isArray(raw),
                'Raw value should be a string or an array of strings.',
            );
            if (Array.isArray(raw)) {
                addFinding(
                    rawIsStringArray(raw),
                    'Raw multiple choice arrays should contain strings.',
                );
            }
            break;
        case 'connection':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string, usually HTML.',
            );
            addFinding(
                rawIsConnectionArray(raw),
                'Raw value should be an array of connection objects with id and/or identifier.',
            );
            break;
        case 'file':
        case 'image':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, [
                    'id',
                    'filename',
                    'url',
                    'thumb_url',
                    'mime_type',
                ]),
                'Raw value should be an attachment object with file metadata.',
            );
            break;
        case 'signature':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, [
                    'svg',
                    'base30',
                    'base64',
                    'url',
                    'thumb_url',
                    'timestamp',
                    'date',
                ]),
                'Raw value should be a signature object with stroke/image metadata.',
            );
            break;
        case 'link':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string, often HTML.',
            );
            addFinding(
                rawHasKeys(raw, ['url', 'label']),
                'Raw value should be an object containing url/label.',
            );
            break;
        case 'user_roles':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a display string.',
            );
            addFinding(
                rawIsStringArray(raw),
                'Raw value should be an array of role name strings.',
            );
            break;
        case 'password':
            addFinding(
                typeof formatted === 'string',
                'Formatted value should be a string.',
            );
            addFinding(
                rawHasKeys(raw, ['validation']),
                'Raw value should be an object containing password validation metadata.',
            );
            break;
        default:
            return {
                status: 'unknown',
                observedFormattedShape,
                observedRawShape,
                findings: [
                    `No automated verifier is defined for field type ${fieldType}.`,
                ],
            };
    }

    return {
        status: findings.length ? 'mismatch' : 'match',
        observedFormattedShape,
        observedRawShape,
        findings,
    };
}

type SessionState = {
    activeAppKey: string | null;
    lastContextPath: string | null;
};

type AppInferenceResult = {
    appKey: string | null;
    inferenceMode:
        | 'direct-folder'
        | 'segment-alias'
        | 'basename-alias'
        | 'explicit-appkey'
        | null;
    candidateAppKeys: string[];
};

function createServer(options: ServerOptions = {}) {
    const knackAppsDir = ENV_KNACK_APPS_DIR;
    if (!knackAppsDir) {
        throw new Error(
            'Missing env var KNACK_APPS_DIR (absolute path to your KnackApps folder).',
        );
    }

    const apps = discoverApps(knackAppsDir);
    if (!apps.length) {
        throw new Error(
            `No apps discovered in ${knackAppsDir}. Ensure KnackApps/*/schema/app.json (or legacy KnackApps/*/app.json) exists.`,
        );
    }
    const HAS_MUTATION_TOOLS =
        !options.readOnly && apps.some((app) => app.readonly === false);

    const HAS_VIEW_MUTATION_TOOLS =
        !options.readOnly && apps.some((app) => app.allowViewMutation === true);

    const HAS_DIAGNOSTIC_TOOLS =
        !options.readOnly && apps.some((app) => app.allowDiagnostics === true);

    let secrets = loadSecrets();

    const appsByKey = new Map<string, AppConfig>();
    for (const app of apps) appsByKey.set(app.appKey, app);

    function rescanApps(): AppConfig[] {
        const freshApps = discoverApps(knackAppsDir as string);
        appsByKey.clear();
        for (const app of freshApps) appsByKey.set(app.appKey, app);
        secrets = loadSecrets();
        return freshApps;
    }

    const runtimeMetadataCache = new Map<string, CacheEntry<RuntimeMetadata>>();
    const schemaCache = new Map<string, CacheEntry<CachedSchema>>();
    const fieldMapCache = new Map<string, CacheEntry<CachedFieldMap>>();
    const viewMapCache = new Map<string, CacheEntry<CachedViewMap>>();
    const fieldReferenceCache = new Map<
        string,
        CacheEntry<CachedFieldReferenceIndex>
    >();

    // Simple in-memory session state (works well for local usage)
    const state: SessionState = {
        activeAppKey: null,
        lastContextPath: null,
    };

    function getAppOrThrow(appKey?: string): AppConfig {
        const key = appKey || state.activeAppKey;
        if (!key) {
            throw new Error(
                'No app selected. Call knack_set_context or pass appKey.',
            );
        }
        const app = appsByKey.get(key);
        if (!app) {
            throw new Error(
                `Unknown appKey: ${key}. Call knack_list_apps to see available apps.`,
            );
        }
        return app;
    }

    function getApiKeyOrThrow(appKey: string): string {
        const apiKey = secrets[appKey];
        if (!apiKey) {
            throw new Error(
                `No API key found for appKey "${appKey}" in your secrets file.`,
            );
        }
        return apiKey;
    }

    function getAppAliases(app: AppConfig): string[] {
        const aliases = new Set<string>();
        const candidates = [
            app.appKey,
            app.appName,
            path.basename(app.appFolder),
        ];
        for (const candidate of candidates) {
            if (!candidate) continue;
            const normalised = normaliseAppIdentity(candidate);
            if (normalised) aliases.add(normalised);
        }
        return [...aliases];
    }

    function assertWritable(app: AppConfig): void {
        if (options.readOnly) {
            throw new Error(
                'This MCP server was started in enforced read-only mode.',
            );
        }

        if (app.readonly !== false) {
            throw new Error(
                `App "${app.appKey}" is readonly. Set "readonly": false in app.json to enable writes.`,
            );
        }
    }

    /**
     * Guard diagnostic tools so they only run for apps that opt in via app.json.
     *
     * @param app The app configuration resolved for the current tool call.
     * @returns void
     */
    function assertDiagnosticAccess(app: AppConfig): void {
        if (options.readOnly) {
            throw new Error(
                'This MCP server was started in enforced read-only mode without diagnostic tools.',
            );
        }

        if (app.allowDiagnostics !== true) {
            throw new Error(
                `App "${app.appKey}" does not allow diagnostic tools. Set "allowDiagnostics": true in app.json to enable raw inspection helpers.`,
            );
        }
    }

    function assertViewWritable(app: AppConfig): void {
        assertWritable(app);
        if (app.allowViewMutation !== true) {
            throw new Error(
                `App "${app.appKey}" does not allow view mutations. Set "allowViewMutation": true in app.json to enable create/update view operations.`,
            );
        }
    }

    function assertDeletable(app: AppConfig): void {
        assertWritable(app);
        if (app.allowDelete !== true) {
            throw new Error(
                `App "${app.appKey}" does not allow deletions. Set "allowDelete": true in app.json to enable delete operations.`,
            );
        }
    }

    function assertViewDeletable(app: AppConfig): void {
        assertViewWritable(app);
        if (app.allowDelete !== true) {
            throw new Error(
                `App "${app.appKey}" does not allow deletions. Set "allowDelete": true in app.json to enable delete operations.`,
            );
        }
    }

    function inferAppKeyFromPath(contextPath: string): AppInferenceResult {
        const nContext = normalisePath(contextPath);

        // If the file is inside KnackApps/<AppKey>/... we can infer directly
        // Example: .../KnackApps/ARC/somefile.js -> ARC
        for (const app of apps) {
            const nFolder = normalisePath(app.appFolder);
            if (nContext.startsWith(nFolder + '/')) {
                return {
                    appKey: app.appKey,
                    inferenceMode: 'direct-folder',
                    candidateAppKeys: [app.appKey],
                };
            }
        }

        const pathSegments = nContext.split('/').filter(Boolean);
        const normalisedSegments = pathSegments
            .map((segment) => normaliseAppIdentity(segment))
            .filter(Boolean);
        const segmentMatches = apps.filter((app) =>
            getAppAliases(app).some((alias) =>
                normalisedSegments.includes(alias),
            ),
        );
        if (segmentMatches.length === 1) {
            return {
                appKey: segmentMatches[0].appKey,
                inferenceMode: 'segment-alias',
                candidateAppKeys: [segmentMatches[0].appKey],
            };
        }

        const basename = path.basename(contextPath, path.extname(contextPath));
        const basenameAlias = normaliseAppIdentity(basename);
        if (basenameAlias) {
            const basenameMatches = apps.filter((app) =>
                getAppAliases(app).includes(basenameAlias),
            );
            if (basenameMatches.length === 1) {
                return {
                    appKey: basenameMatches[0].appKey,
                    inferenceMode: 'basename-alias',
                    candidateAppKeys: [basenameMatches[0].appKey],
                };
            }
            if (basenameMatches.length > 1) {
                return {
                    appKey: null,
                    inferenceMode: null,
                    candidateAppKeys: basenameMatches.map((app) => app.appKey),
                };
            }
        }

        return {
            appKey: null,
            inferenceMode: null,
            candidateAppKeys: segmentMatches.map((app) => app.appKey),
        };
    }

    async function knackRequest(
        app: AppConfig,
        apiKey: string,
        apiPath: string,
        init?: RequestInit,
    ) {
        const url = `${app.apiBase || DEFAULT_API_BASE}${apiPath}`;
        debugLog('knack_request', {
            appKey: app.appKey,
            method: init?.method || 'GET',
            apiPath,
        });
        const result = await knackFetchJson(url, {
            ...init,
            headers: {
                'X-Knack-Application-Id': app.appId,
                'X-Knack-REST-API-Key': apiKey,
                'Content-Type': 'application/json',
                ...(init?.headers || {}),
            },
        });
        return result;
    }

    /**
     * Like knackRequest, but retries with exponential backoff on a 429 (rate limited) or
     * 5xx response. Batch record tools run several of these concurrently, so backoff
     * protects against tripping Knack's per-second rate limit under concurrent load.
     *
     * Knack has no client-supplied idempotency key, so a 5xx is genuinely ambiguous for a
     * non-idempotent write — the request may have already been applied server-side and
     * only the response was lost/delayed. Retrying a POST (create) on 5xx risks silently
     * creating a duplicate record, so POST only retries on 429 (an unambiguous rejection
     * that never reached processing), never on 5xx. PUT/DELETE are safe to retry on 5xx
     * since re-applying them is a no-op. For DELETE specifically, a 404 immediately after
     * a 5xx-triggered retry almost certainly means the first attempt's delete actually
     * succeeded and only its response was lost — that's reported back as success rather
     * than a false failure.
     *
     * @param maxAttempts Total attempts including the first, before giving up.
     */
    async function knackRequestWithRetry(
        app: AppConfig,
        apiKey: string,
        apiPath: string,
        init: RequestInit | undefined,
        maxAttempts = 4,
    ): Promise<KnackApiResult> {
        const method = (init?.method || 'GET').toUpperCase();
        const canRetryOn5xx = method !== 'POST';

        let lastResult: KnackApiResult = await knackRequest(
            app,
            apiKey,
            apiPath,
            init,
        );

        for (let attempt = 2; attempt <= maxAttempts; attempt++) {
            const retryingAfter5xx = lastResult.status >= 500;
            const shouldRetry =
                lastResult.status === 429 ||
                (canRetryOn5xx && retryingAfter5xx);
            if (!shouldRetry) break;
            await sleep(500 * 2 ** (attempt - 2));
            lastResult = await knackRequest(app, apiKey, apiPath, init);
            if (
                method === 'DELETE' &&
                retryingAfter5xx &&
                lastResult.status === 404
            ) {
                return {
                    ok: true,
                    status: 200,
                    body: {
                        inferredSuccess: true,
                        message:
                            'Treated as a successful delete: a 5xx on the first attempt was retried and came back 404, which almost certainly means the delete already applied and only its response was lost — not that the record never existed.',
                        upstreamStatus: lastResult.status,
                        upstreamBody: lastResult.body,
                    },
                };
            }
        }

        return lastResult;
    }

    /**
     * Resolve a file or image attachment from an approved record field.
     *
     * @param app Selected Knack application.
     * @param objectKey Object containing the attachment.
     * @param recordId Record containing the attachment.
     * @param fieldKey File or image field to read.
     * @returns Attachment metadata and its record-derived download URL.
     */
    async function getRecordAttachment(
        app: AppConfig,
        objectKey: string,
        recordId: string,
        fieldKey: string,
    ) {
        const { object } = await getPermittedReadFields(app, objectKey, [
            fieldKey,
        ]);
        const field = (object.fields || []).find(
            (entry) => entry.key === fieldKey,
        );
        if (!field || !['file', 'image'].includes(field.type || '')) {
            throw new Error(
                `Field ${fieldKey} is not a file or image field on ${objectKey}.`,
            );
        }

        const apiKey = getApiKeyOrThrow(app.appKey);
        const result = await knackRequest(
            app,
            apiKey,
            `/objects/${objectKey}/records/${recordId}`,
        );
        const record = asRecord(result.body);
        if (!result.ok || !record) {
            throw new Error(
                `Unable to fetch record ${recordId} from ${objectKey}.`,
            );
        }

        const attachment = asRecord(record[`${fieldKey}_raw`]);
        const url = typeof attachment?.url === 'string' ? attachment.url : null;
        const filename =
            typeof attachment?.filename === 'string'
                ? attachment.filename
                : null;
        if (!attachment || !url || !filename) {
            throw new Error(
                `Field ${fieldKey} does not contain an uploaded attachment.`,
            );
        }

        return {
            url,
            filename,
            mimeType:
                typeof attachment.mime_type === 'string'
                    ? attachment.mime_type
                    : 'application/octet-stream',
            sizeBytes:
                typeof attachment.size === 'number' ? attachment.size : null,
        };
    }

    /**
     * Download an attachment to an application-specific temporary directory with a hard byte limit.
     *
     * @param app Selected Knack application.
     * @param recordId Source record identifier.
     * @param attachment Record-derived attachment metadata.
     * @returns The local file path and observed byte count.
     */
    async function downloadRecordAttachment(
        app: AppConfig,
        recordId: string,
        attachment: {
            url: string;
            filename: string;
            mimeType: string;
            sizeBytes: number | null;
        },
    ): Promise<{ filePath: string; sizeBytes: number }> {
        const attachmentUrl = new URL(attachment.url);
        if (attachmentUrl.protocol !== 'https:') {
            throw new Error('Knack attachment URLs must use HTTPS.');
        }

        const safeFilename =
            path
                .basename(attachment.filename)
                .replace(/[^a-zA-Z0-9._-]/g, '_') || 'attachment';
        const safeAppKey = app.appKey.replace(/[^a-zA-Z0-9._-]/g, '_') || 'app';
        const safeRecordId =
            recordId.replace(/[^a-zA-Z0-9._-]/g, '_') || 'record';
        const downloadDirectory = path.join(
            os.tmpdir(),
            'knack-mcp-downloads',
            safeAppKey,
            safeRecordId,
        );
        const filePath = path.join(downloadDirectory, safeFilename);
        fs.mkdirSync(downloadDirectory, { recursive: true });

        return new Promise((resolve, reject) => {
            const download = (url: URL, redirectsRemaining: number) => {
                const request = https.get(url, (response) => {
                    const statusCode = response.statusCode || 0;
                    const location = response.headers.location;
                    if (
                        [301, 302, 303, 307, 308].includes(statusCode) &&
                        typeof location === 'string'
                    ) {
                        response.resume();
                        if (redirectsRemaining === 0) {
                            reject(
                                new Error(
                                    `Attachment download exceeded the ${MAX_ATTACHMENT_REDIRECTS}-redirect limit.`,
                                ),
                            );
                            return;
                        }

                        const redirectUrl = new URL(location, url);
                        if (redirectUrl.protocol !== 'https:') {
                            reject(
                                new Error(
                                    'Knack attachment URLs must use HTTPS.',
                                ),
                            );
                            return;
                        }

                        download(redirectUrl, redirectsRemaining - 1);
                        return;
                    }

                    const contentLength = Number(
                        response.headers['content-length'] || 0,
                    );
                    if (statusCode < 200 || statusCode >= 300) {
                        response.resume();
                        reject(
                            new Error(
                                `Attachment download failed with HTTP ${statusCode}.`,
                            ),
                        );
                        return;
                    }
                    if (contentLength && contentLength > MAX_RESPONSE_BYTES) {
                        response.resume();
                        reject(
                            new Error(
                                `Attachment exceeds the ${MAX_RESPONSE_BYTES}-byte download limit.`,
                            ),
                        );
                        return;
                    }

                    const output = fs.createWriteStream(filePath, {
                        flags: 'w',
                    });
                    let sizeBytes = 0;
                    response.on('data', (chunk: Buffer) => {
                        sizeBytes += chunk.length;
                        if (sizeBytes > MAX_RESPONSE_BYTES) {
                            request.destroy(
                                new Error(
                                    `Attachment exceeds the ${MAX_RESPONSE_BYTES}-byte download limit.`,
                                ),
                            );
                        }
                    });
                    output.on('error', (error) => {
                        try {
                            fs.unlinkSync(filePath);
                        } catch {}
                        reject(error);
                    });
                    response.pipe(output);
                    output.on('finish', () =>
                        output.close(() => resolve({ filePath, sizeBytes })),
                    );
                });
                request.setTimeout(30_000, () =>
                    request.destroy(
                        new Error(
                            'Attachment download timed out after 30 seconds.',
                        ),
                    ),
                );
                request.on('error', (error) => {
                    try {
                        fs.unlinkSync(filePath);
                    } catch {}
                    reject(error);
                });
            };

            download(attachmentUrl, MAX_ATTACHMENT_REDIRECTS);
        });
    }

    /**
     * Extract bounded plain text from a downloaded attachment for AI review.
     *
     * @param filePath Local attachment path.
     * @param mimeType Attachment MIME type.
     * @returns Text extraction result or a reason the format is unsupported.
     */
    async function extractAttachmentText(
        filePath: string,
        mimeType: string,
    ): Promise<{ text: string; truncated: boolean; supported: boolean }> {
        const extension = path.extname(filePath).toLowerCase();
        let text: string;

        if (mimeType === 'application/pdf' || extension === '.pdf') {
            const parsed = await pdf(fs.readFileSync(filePath));
            text = parsed.text;
        } else if (
            mimeType ===
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
            extension === '.docx'
        ) {
            const parsed = await mammoth.extractRawText({ path: filePath });
            text = parsed.value;
        } else if (
            mimeType.startsWith('text/') ||
            ['application/json', 'application/xml', 'text/csv'].includes(
                mimeType,
            ) ||
            ['.csv', '.json', '.md', '.txt', '.xml'].includes(extension)
        ) {
            text = fs.readFileSync(filePath, 'utf8');
        } else {
            return { text: '', truncated: false, supported: false };
        }

        const textBytes = Buffer.byteLength(text, 'utf8');
        if (textBytes <= MAX_EXTRACTED_TEXT_BYTES) {
            return { text, truncated: false, supported: true };
        }

        return {
            text: Buffer.from(text, 'utf8')
                .subarray(0, MAX_EXTRACTED_TEXT_BYTES)
                .toString('utf8'),
            truncated: true,
            supported: true,
        };
    }

    function getMetadataFilePaths(app: AppConfig, fileName: string): string[] {
        return [
            path.join(app.appFolder, 'schema', fileName),
            path.join(app.appFolder, fileName),
        ];
    }

    function resolveMetadataFilePath(app: AppConfig, fileName: string): string {
        const candidates = getMetadataFilePaths(app, fileName);
        return (
            candidates.find((candidate) => fileExists(candidate)) ||
            candidates[0]
        );
    }

    function metadataFileExists(app: AppConfig, fileName: string): boolean {
        return getMetadataFilePaths(app, fileName).some((candidate) =>
            fileExists(candidate),
        );
    }

    function readMetadataJson<T>(app: AppConfig, fileName: string): T | null {
        const candidates = getMetadataFilePaths(app, fileName);
        for (const candidate of candidates) {
            const parsed = readJsonFile<T>(candidate);
            if (parsed) return parsed;
        }
        return null;
    }

    function writeMetadataJson(
        app: AppConfig,
        fileName: string,
        data: unknown,
    ) {
        const targetPath = resolveMetadataFilePath(app, fileName);
        const writeResult = writeJsonFile(targetPath, data);
        if (!writeResult.ok) {
            return {
                ok: false as const,
                path: targetPath,
                error: writeResult.error,
            };
        }

        return {
            ok: true as const,
            path: targetPath,
        };
    }

    function readSchemaFromDisk(app: AppConfig): CachedSchema | null {
        return readMetadataJson<CachedSchema>(app, 'schema.json');
    }

    function readFieldMapFromDisk(
        app: AppConfig,
        schema: CachedSchema | null,
    ): CachedFieldMap | null {
        const raw = readMetadataJson<unknown>(app, 'fieldMap.json');
        return coerceFieldMap(raw, schema);
    }

    function readViewMapFromDisk(app: AppConfig): CachedViewMap | null {
        return readMetadataJson<CachedViewMap>(app, 'viewMap.json');
    }

    function readFieldReferenceIndexFromDisk(
        app: AppConfig,
    ): CachedFieldReferenceIndex | null {
        return readMetadataJson<CachedFieldReferenceIndex>(
            app,
            'fieldReferenceIndex.json',
        );
    }

    async function getRuntimeMetadata(
        app: AppConfig,
    ): Promise<RuntimeMetadata | null> {
        const cached = getCacheEntry(runtimeMetadataCache, app.appKey);
        if (cached) {
            return cached.value;
        }

        const publicBase = getPublicApiBase(app.apiBase);
        const url = `${publicBase}/v1/applications/${encodeURIComponent(app.appId)}`;

        debugLog('runtime_metadata_attempt', { appKey: app.appKey, url });
        const result = await knackFetchJson(url, { method: 'GET' });
        if (!result.ok) {
            return null;
        }

        const payload = asRecord(result.body);
        if (!payload || !isRuntimeMetadataPayload(payload)) {
            debugLog('runtime_metadata_invalid_shape', {
                appKey: app.appKey,
                url,
                bodyType: typeof result.body,
                topLevelKeys: payload
                    ? Object.keys(payload).slice(0, 30)
                    : null,
            });
            return null;
        }

        runtimeMetadataCache.set(
            app.appKey,
            makeCacheEntry(payload, 'runtime'),
        );
        return payload;
    }

    async function getSchemaForApp(
        app: AppConfig,
    ): Promise<{ schema: CachedSchema | null; source: CacheSource | null }> {
        const cached = getCacheEntry(schemaCache, app.appKey);
        if (cached) return { schema: cached.value, source: cached.source };

        const runtimeMetadata = await getRuntimeMetadata(app);
        const runtimeSchema = parseRuntimeSchema(runtimeMetadata);
        if (runtimeSchema?.objects?.length) {
            schemaCache.set(
                app.appKey,
                makeCacheEntry(runtimeSchema, 'runtime'),
            );
            return { schema: runtimeSchema, source: 'runtime' };
        }

        const diskSchema = readSchemaFromDisk(app);
        if (diskSchema?.objects?.length) {
            schemaCache.set(app.appKey, makeCacheEntry(diskSchema, 'file'));
            return { schema: diskSchema, source: 'file' };
        }

        return { schema: null, source: null };
    }

    function getExternalSeedConnectionTargets(
        schema: CachedSchema,
        objectKeys?: string[],
    ): CachedObject[] {
        const selectedKeys = new Set(
            objectKeys?.length
                ? objectKeys
                : (schema.objects || []).map((object) => object.key),
        );
        const objectsByKey = new Map(
            (schema.objects || []).map((object) => [object.key, object]),
        );
        const targets = new Map<string, CachedObject>();

        for (const object of schema.objects || []) {
            if (!selectedKeys.has(object.key)) continue;
            for (const field of object.fields || []) {
                if (
                    (field.type || '').toLowerCase() !== 'connection' ||
                    !field.connectedObject ||
                    selectedKeys.has(field.connectedObject)
                )
                    continue;
                const target = objectsByKey.get(field.connectedObject);
                if (target) {
                    targets.set(target.key, target);
                }
            }
        }

        return [...targets.values()].sort((left, right) =>
            (left.name || left.key).localeCompare(right.name || right.key),
        );
    }

    async function fetchExternalSeedConnectionLookups(
        app: AppConfig,
        targets: CachedObject[],
        rowsPerObject: number,
    ): Promise<{
        lookups: Record<string, ExternalConnectionLookup>;
        fetches: Array<{
            objectKey: string;
            objectName?: string;
            apiPath: string;
            fetchedValues: number;
            ok: boolean;
            message?: string;
        }>;
    }> {
        const apiKey = getApiKeyOrThrow(app.appKey);
        const lookups: Record<string, ExternalConnectionLookup> = {};
        const fetches: Array<{
            objectKey: string;
            objectName?: string;
            apiPath: string;
            fetchedValues: number;
            ok: boolean;
            message?: string;
        }> = [];

        for (const target of targets) {
            const params = new URLSearchParams();
            params.set('page', '1');
            params.set('rows_per_page', String(Math.max(rowsPerObject, 2)));
            const apiPath = `/objects/${target.key}/records?${params.toString()}`;
            const result = await knackRequest(app, apiKey, apiPath);
            const values = result.ok
                ? extractConnectionDisplayValues(result.body)
                : [];

            if (values.length) {
                lookups[target.key] = {
                    objectKey: target.key,
                    objectName: target.name,
                    values,
                    source: 'api',
                    lookupField: 'identifier',
                };
            }

            fetches.push({
                objectKey: target.key,
                objectName: target.name,
                apiPath,
                fetchedValues: values.length,
                ok: result.ok,
                message: result.ok
                    ? values.length
                        ? undefined
                        : 'No display values were returned from the first page of records.'
                    : `Request failed with status ${result.status}.`,
            });
        }

        return { lookups, fetches };
    }

    async function getFieldMapForApp(app: AppConfig): Promise<{
        fieldMap: CachedFieldMap | null;
        source: CacheSource | null;
    }> {
        const cached = getCacheEntry(fieldMapCache, app.appKey);
        if (cached) return { fieldMap: cached.value, source: cached.source };

        const runtimeMetadata = await getRuntimeMetadata(app);
        const runtimeFieldMap = parseRuntimeFieldMap(runtimeMetadata);
        if (runtimeFieldMap && Object.keys(runtimeFieldMap).length) {
            fieldMapCache.set(
                app.appKey,
                makeCacheEntry(runtimeFieldMap, 'runtime'),
            );
            return { fieldMap: runtimeFieldMap, source: 'runtime' };
        }

        const schemaResult = await getSchemaForApp(app);
        const diskFieldMap = readFieldMapFromDisk(app, schemaResult.schema);
        if (diskFieldMap && Object.keys(diskFieldMap).length) {
            fieldMapCache.set(app.appKey, makeCacheEntry(diskFieldMap, 'file'));
            return { fieldMap: diskFieldMap, source: 'file' };
        }

        return { fieldMap: null, source: null };
    }

    async function getViewMapForApp(
        app: AppConfig,
    ): Promise<{ viewMap: CachedViewMap | null; source: CacheSource | null }> {
        const cached = getCacheEntry(viewMapCache, app.appKey);
        if (cached) return { viewMap: cached.value, source: cached.source };

        const runtimeMetadata = await getRuntimeMetadata(app);
        const runtimeViewMap = parseRuntimeViewMap(runtimeMetadata);
        if (runtimeViewMap && Object.keys(runtimeViewMap).length) {
            viewMapCache.set(
                app.appKey,
                makeCacheEntry(runtimeViewMap, 'runtime'),
            );
            return { viewMap: runtimeViewMap, source: 'runtime' };
        }

        const diskViewMap = readViewMapFromDisk(app);
        if (diskViewMap && Object.keys(diskViewMap).length) {
            viewMapCache.set(app.appKey, makeCacheEntry(diskViewMap, 'file'));
            return { viewMap: diskViewMap, source: 'file' };
        }

        return { viewMap: null, source: null };
    }

    async function getViewContextMapForApp(
        app: AppConfig,
    ): Promise<ViewContextMap> {
        const runtimeMetadata = await getRuntimeMetadata(app);
        return parseRuntimeViewContextMap(runtimeMetadata);
    }

    async function getScenesForApp(app: AppConfig): Promise<SceneInfo[]> {
        const runtimeMetadata = await getRuntimeMetadata(app);
        return parseRuntimeScenes(runtimeMetadata);
    }

    /**
     * Re-read the app's scene tree, bypassing the cache, for destructive preflight.
     *
     * getScenesForApp serves a five-minute cache and returns [] when runtime metadata
     * cannot be fetched — so a page added minutes ago, or an unreachable API, both look
     * identical to "this page has no children". Confirmation prompts are built from this
     * list, so a stale or empty answer under-reports what a delete destroys. Force the
     * refresh and report failure as failure.
     *
     * @param app Selected Knack application.
     * @returns The scene tree, or why it could not be read.
     */
    async function getFreshSceneTree(
        app: AppConfig,
    ): Promise<
        { ok: true; scenes: SceneInfo[] } | { ok: false; reason: string }
    > {
        runtimeMetadataCache.delete(app.appKey);
        const metadata = await getRuntimeMetadata(app);
        if (!metadata) {
            return {
                ok: false,
                reason: 'runtime metadata could not be fetched from Knack',
            };
        }

        const scenes = parseRuntimeScenes(metadata);
        if (scenes.length === 0) {
            return {
                ok: false,
                reason: 'the runtime metadata contained no scenes, which cannot be right for an app being mutated',
            };
        }

        // Full SceneInfo, views included: the snapshot stores this verbatim, and a
        // restore point without each scene's view list cannot rebuild a page.
        return { ok: true, scenes };
    }

    /**
     * Write a timestamped restore point for one app.
     *
     * knack_refresh_cache overwrites schema.json/viewMap.json in place and never persists
     * scenes at all, so it cannot be used to recover from a cascade delete. This keeps the
     * full scene tree — routes, slugs and parents — alongside the target view's complete
     * definition, so columns, filters and links can be rebuilt exactly.
     *
     * @param app Selected Knack application.
     * @param params What is about to happen, and the view it happens to.
     * @returns The snapshot path, or the reason it could not be written.
     */
    async function writeMutationSnapshot(
        app: AppConfig,
        params: {
            action: ViewMutationAction | 'manual';
            sceneKey?: string;
            viewKey?: string;
            view?: unknown;
            /** A tree the caller already fetched, so it is not fetched twice. */
            sceneTree?:
                | { ok: true; scenes: SceneInfo[] }
                | { ok: false; reason: string };
        },
    ): Promise<{ ok: true; path: string } | { ok: false; error: string }> {
        try {
            const takenAt = new Date().toISOString();
            // Milliseconds are kept, and a per-process counter added on top. Truncating
            // to whole seconds let two mutations of the same view inside one second
            // produce the same filename, and writeJsonFile overwrites — so the second
            // snapshot destroyed the first, losing the restore point for the change
            // that had just been applied.
            const stamp = takenAt.replaceAll(':', '-').replace('.', '-');
            const subject = sanitiseFileNameComponent(
                params.viewKey || params.sceneKey || 'app',
            );
            const fileName = `${stamp}-${params.action}-${subject}-${snapshotSequence++}.json`;

            // Force the refetch rather than serving getScenesForApp's five-minute
            // cache. Nothing invalidates that cache after a mutation, so a second
            // mutation within the window would otherwise snapshot the tree as it stood
            // before the first — a restore point describing a state that no longer
            // exists is worse than an obvious failure.
            // `sceneTree` is passed in by the guard, which has already taken a fresh
            // one for the confirmation prompt. On a large app that tree is several
            // megabytes, and fetching it twice per mutation was pure duplication.
            const sceneTree =
                params.sceneTree ?? (await getFreshSceneTree(app));

            // A file with no scene tree is not a restore point, and writing one while
            // reporting success would let a mutation proceed believing it is recoverable.
            // Scenes come back empty whenever runtime metadata cannot be fetched.
            if (!sceneTree.ok) {
                return {
                    ok: false,
                    error: `the app scene tree could not be read (${sceneTree.reason}), so the snapshot would contain no pages to restore from`,
                };
            }
            const scenes = sceneTree.scenes;

            const targetPath = path.join(
                app.appFolder,
                'schema',
                'snapshots',
                fileName,
            );

            // The object/field schema is deliberately not embedded. Rebuilding a
            // cascade-deleted page needs the scene tree and the view definitions; the
            // schema is context, and it does not change when a page is deleted. Copying
            // it into every snapshot added hundreds of KB per file, to files nothing
            // prunes. A pointer to the app's own schema.json carries the same
            // information without the duplication.
            const writeResult = writeJsonFile(targetPath, {
                snapshotVersion: 2,
                takenAt,
                appKey: app.appKey,
                appId: app.appId,
                action: params.action,
                sceneKey: params.sceneKey ?? null,
                viewKey: params.viewKey ?? null,
                scenes,
                view: params.view ?? null,
                schemaPath: path.join(app.appFolder, 'schema', 'schema.json'),
            });

            if (!writeResult.ok) {
                return { ok: false, error: writeResult.error };
            }

            debugLog('mutation_snapshot', {
                appKey: app.appKey,
                action: params.action,
                path: targetPath,
                scenes: scenes.length,
            });
            return { ok: true, path: targetPath };
        } catch (error) {
            return {
                ok: false,
                error: error instanceof Error ? error.message : String(error),
            };
        }
    }

    /**
     * Build the injected I/O the view-safety guard runs on.
     *
     * The preflight takes no REST API key. It reads the view from runtime metadata
     * rather than a per-view route, because Knack serves no per-view route to a REST
     * key — every candidate host answers with a web-server HTML 404. The mutation that
     * follows resolves its own key at the call site.
     *
     * @param app Selected Knack application.
     * @returns Preflight read, scene tree, snapshot writer and builder deep links.
     */
    async function makeViewMutationDeps(
        app: AppConfig,
    ): Promise<ViewMutationDeps> {
        // The five-minute cache is wrong here. A preflight immediately before a
        // destructive mutation must see the app as it is now, not as it was up to five
        // minutes ago — so the cache is dropped and the payload read once, here, for
        // everything this run needs it for.
        runtimeMetadataCache.delete(app.appKey);
        const runtimeMetadata = await getRuntimeMetadata(app);

        // One payload, read once, shared by everything in this guard run: the view
        // the preflight examines, the scene tree the cascade is worked out from, the
        // link graph behind the referrer count, and the snapshot written before the
        // mutation. On a large app that payload is several megabytes, and this used to
        // be fetched three times — `getFreshSceneTree` cleared the cache and refetched
        // after the preflight already had, so the view and the tree came from
        // *different* reads and could straddle someone else's edit. The comment here
        // claimed a single consistent snapshot while the code took two.
        //
        const freshMetadataForThisRun = async () => runtimeMetadata;

        const sceneTreeForThisRun = async (): Promise<
            { ok: true; scenes: SceneInfo[] } | { ok: false; reason: string }
        > => {
            const metadata = await freshMetadataForThisRun();
            if (!metadata) {
                return {
                    ok: false,
                    reason: 'runtime metadata could not be fetched from Knack',
                };
            }

            const scenes = parseRuntimeScenes(metadata);
            if (scenes.length === 0) {
                return {
                    ok: false,
                    reason: 'the runtime metadata contained no scenes, which cannot be right for an app being mutated',
                };
            }

            return { ok: true, scenes };
        };

        return {
            fetchView: async (sceneKey, viewKey) => {
                const metadata = await freshMetadataForThisRun();
                if (!metadata) {
                    // Statuses are reported to the caller in the refusal, so they have
                    // to mean something. 502: the upstream read failed, as distinct
                    // from the view genuinely not existing.
                    return {
                        ok: false,
                        status: 502,
                        body: {
                            error: 'runtime metadata could not be fetched from Knack, so the view could not be verified',
                        },
                    };
                }

                const view = findRawViewInMetadata(metadata, sceneKey, viewKey);
                if (!view) {
                    return {
                        ok: false,
                        status: 404,
                        body: {
                            error: `${viewKey} was not found in ${sceneKey} in this app's metadata`,
                        },
                    };
                }

                return { ok: true, status: 200, body: view };
            },
            listScenes: async () => {
                const tree = await sceneTreeForThisRun();
                if (!tree.ok) return tree;

                // The link graph the referrer count runs on, read from the same fresh
                // payload as the view being mutated. Left off entirely when that read
                // failed: `views: []` would say "nothing links to this page", which is
                // the one wrong answer available here — it would spare nothing and
                // doom nothing, but it would do so on invented evidence.
                const metadata = await freshMetadataForThisRun();
                const linksByScene = metadata
                    ? collectSceneViewLinks(metadata)
                    : null;

                return {
                    ok: true as const,
                    scenes: tree.scenes.map((scene): SceneNode => ({
                        sceneKey: scene.sceneKey,
                        sceneName: scene.sceneName,
                        sceneSlug: scene.sceneSlug,
                        parentRef: scene.parentRef,
                        ...(linksByScene
                            ? { views: linksByScene.get(scene.sceneKey) ?? [] }
                            : {}),
                    })),
                };
            },
            writeSnapshot: async (input) =>
                writeMutationSnapshot(app, {
                    ...input,
                    sceneTree: await sceneTreeForThisRun(),
                }),
            builderUrlForScene: (sceneKey) =>
                makeSceneBuilderUrl(app, sceneKey, runtimeMetadata),
            confirmPageDeletion: (input) =>
                askHumanToConfirmPageDeletion(app, input),
        };
    }

    /**
     * Describe what a cascade delete would actually do, given the connected client.
     *
     * It turns on one thing a caller cannot otherwise see: whether this client can put a
     * prompt in front of a person. There is no per-app variation — a client that cannot
     * prompt cannot cascade-delete through this server, on any app.
     *
     * @param humanConfirmationAvailable Whether this client advertised elicitation.
     * @returns A stable mode string and a sentence explaining it.
     */
    function describeCascadeBehaviour(humanConfirmationAvailable: boolean): {
        mode: string;
        summary: string;
    } {
        if (humanConfirmationAvailable) {
            return {
                mode: 'prompts-human',
                summary:
                    'A mutation that would delete child pages is put to the user for confirmation. The calling model cannot answer it.',
            };
        }
        return {
            mode: 'refuses',
            summary:
                'No human can be prompted, so a mutation that would delete child pages is refused outright. There is no override — make the change in the Knack builder.',
        };
    }

    /**
     * Report whether this MCP client can put a confirmation prompt in front of a human.
     *
     * Elicitation is an optional, client-declared capability, so whether a cascade delete
     * can be confirmed by a person — rather than refused outright — depends on what the
     * connected client advertised at handshake. Surfacing it means a caller can find out
     * before hitting a refusal on a real change.
     *
     * @returns Availability, the connected client, and what that means.
     */
    function getHumanConfirmationStatus() {
        const capabilities = server.server.getClientCapabilities();
        const client = server.server.getClientVersion();
        const available = Boolean(capabilities?.elicitation);

        return {
            available,
            client: client
                ? `${client.name}${client.version ? ` ${client.version}` : ''}`
                : null,
            message: available
                ? 'This client can prompt a human, so a mutation that would delete child pages is put to the user directly. The calling model cannot answer that prompt.'
                : 'This client did not advertise the elicitation capability, so no human can be prompted. Any mutation that would delete child pages is refused, with no override. Make such changes in the Knack builder.',
        };
    }

    /**
     * Ask the person operating the MCP client to confirm a cascade delete.
     *
     * Uses MCP elicitation, so the prompt is rendered by the client and answered by a
     * human. The calling model never sees it and cannot answer it — which is the whole
     * point: a typed acknowledgement only proves the agent read the preflight, while
     * this proves somebody agreed.
     *
     * Any failure is reported as `supported: false` rather than as an acceptance, so a
     * broken or silent client degrades to the app's configured fallback instead of
     * waving the deletion through.
     *
     * @param app Selected Knack application.
     * @param input What would be destroyed.
     * @returns Whether a human could be asked, and what they said.
     */
    async function askHumanToConfirmPageDeletion(
        app: AppConfig,
        input: {
            action: string;
            sceneKey: string;
            viewKey?: string;
            childPages: Array<{
                sceneKey: string;
                sceneName: string | null;
                depth: number;
            }>;
            externalPages?: Array<{
                sceneKey: string | null;
                sceneName: string | null;
                reason: string;
            }>;
            transferredPages?: Array<{
                sceneKey: string | null;
                sceneName: string | null;
                otherReferrers: Array<{ sceneKey: string; viewKey: string }>;
            }>;
            unresolvedLinkCount: number;
        },
    ): Promise<PageDeletionConfirmation> {
        if (!server.server.getClientCapabilities()?.elicitation) {
            return {
                supported: false,
                reason: 'the client did not advertise the elicitation capability',
            };
        }

        const pageList = input.childPages
            .map(
                (page) =>
                    `  - ${page.sceneKey}${page.sceneName ? ` (${page.sceneName})` : ''}${
                        page.depth > 0 ? ' — child of a page above' : ''
                    }`,
            )
            .join('\n');

        // A prompt reaches a human either because pages were named or because links
        // could not be read — and with only the latter, the count is zero and the list
        // is blank. "Knack will permanently delete 0 page(s)" above an empty list is
        // the one artefact in this server that has to be clear, so the unnamed case
        // gets its own wording rather than a template that degenerates.
        const named = input.childPages.length;
        const headline = named
            ? `Knack will permanently delete ${named} page(s) if this ${input.action} goes ahead on ${input.viewKey ?? input.sceneKey} in "${app.appKey}".\n\nPages that would be destroyed:\n${pageList}`
            : `This ${input.action} on ${input.viewKey ?? input.sceneKey} in "${app.appKey}" removes ${input.unresolvedLinkCount} link(s) whose target page this server could not identify.\n\nNo page can be named, so none can be listed — but a link that cannot be read is not a link to nothing, and accepting this may destroy pages that do not appear anywhere in this prompt.`;

        // Stated in the prompt because it is the other half of the consequence. A
        // person shown only what dies cannot tell a navigation edit from a destructive
        // one, and the earlier behaviour — counting these as doomed — made the prompt
        // overstate by enough to train people to click through it.
        const externalNote = input.externalPages?.length
            ? `\n\nAlso losing their link, but NOT being deleted (these pages live elsewhere in the app):\n${input.externalPages
                  .map(
                      (page) =>
                          `  - ${page.sceneKey ?? '?'}${
                              page.sceneName ? ` (${page.sceneName})` : ''
                          }`,
                  )
                  .join('\n')}`
            : '';

        // The other survival case, and the one a person is most likely to be caught
        // out by: the page is not deleted, but it is not where it was either. Naming
        // the view it lands under is the difference between "nothing happened to it"
        // and being able to go and find it.
        const transferredNote = input.transferredPages?.length
            ? `\n\nAlso losing their link here, but NOT being deleted — another view still links to each of these, so Knack moves the page under that view instead:\n${input.transferredPages
                  .map(
                      (page) =>
                          `  - ${page.sceneKey ?? '?'}${
                              page.sceneName ? ` (${page.sceneName})` : ''
                          } → now reached from ${
                              page.otherReferrers
                                  .map((entry) => entry.viewKey)
                                  .join(', ') || 'another view'
                          }`,
                  )
                  .join('\n')}`
            : '';

        const unresolvedNote =
            input.unresolvedLinkCount > 0
                ? `\n\nWARNING: ${input.unresolvedLinkCount} further link(s) point at pages this server could not identify, so they are not listed above. More pages than shown may be destroyed.`
                : '';

        try {
            const result = await server.server.elicitInput(
                {
                    message: `${headline}\n${named ? `\n${unresolvedNote}\n` : ''}\nThis cannot be undone from here. A snapshot is written first, but rebuilding from it is manual.${externalNote}${transferredNote}`,
                    requestedSchema: {
                        type: 'object',
                        properties: {
                            confirm: {
                                type: 'boolean',
                                title: named
                                    ? `Delete these ${named} page(s)`
                                    : `Proceed, and accept that unnamed pages may be destroyed`,
                                description:
                                    'Leave unticked to cancel. Nothing is sent to Knack unless this is ticked.',
                            },
                        },
                        required: ['confirm'],
                    },
                },
                { timeout: CASCADE_CONFIRMATION_TIMEOUT_MS },
            );

            if (result.action !== 'accept') {
                return {
                    supported: true,
                    accepted: false,
                    outcome: result.action,
                };
            }

            return {
                supported: true,
                accepted: result.content?.confirm === true,
                outcome:
                    result.content?.confirm === true ? 'accept' : 'decline',
            };
        } catch (error) {
            debugLog('elicitation_failed', {
                appKey: app.appKey,
                error: error instanceof Error ? error.message : String(error),
            });
            return {
                supported: false,
                reason: `the elicitation request failed: ${
                    error instanceof Error ? error.message : String(error)
                }`,
            };
        }
    }

    /**
     * Run a view mutation through the safety guard and shape the tool response.
     *
     * All six view tools go through here, so the rules hold regardless of which tool a
     * caller reaches for. Source mutations that can remove a view or child page only
     * invoke `perform` once a snapshot is on disk.
     *
     * @param app Selected Knack application.
     * @param apiKey Resolved REST API key.
     * @param request The mutation being attempted.
     * @param perform Sends the real Knack request.
     * @returns A tool payload carrying either the result or the refusal.
     */
    async function runViewMutationTool(
        app: AppConfig,
        apiKey: string,
        request: ViewMutationRequest,
        perform: (context: {
            outgoingBody: Record<string, unknown> | null;
            currentAttributes: Record<string, unknown> | null;
        }) => Promise<KnackApiResult>,
    ): Promise<Record<string, unknown>> {
        const deps = await makeViewMutationDeps(app);
        const identity = {
            appKey: app.appKey,
            sceneKey: request.sceneKey,
            ...(request.viewKey ? { viewKey: request.viewKey } : {}),
            action: request.action,
        };

        const outcome = await runGuardedViewMutation(deps, request, perform);

        if (!outcome.ok) {
            debugLog('view_mutation_blocked', {
                ...identity,
                error: outcome.code,
            });
            return {
                ok: false,
                ...identity,
                error: outcome.code,
                message: outcome.message,
                ...(outcome.details ?? {}),
            };
        }

        // Knack reports what it actually destroyed in the response body. That is the
        // only account of the damage that does not come from this server's own
        // prediction — surface it so a caller can see where the two differ.
        const reportedDeletes = readDeletedScenes(outcome.result);

        return {
            ...identity,
            ...(outcome.snapshotPath
                ? { snapshotPath: outcome.snapshotPath }
                : {}),
            ...(outcome.acknowledgedPages.length > 0
                ? { pagesExpectedToBeDeleted: outcome.acknowledgedPages }
                : {}),
            // Reported so the caller can say what it removed. On the prompt-free path
            // nobody was told anything by definition, and "done" is a poor account of
            // a change that severed navigation to a page that still exists.
            ...(outcome.externalPages.length > 0
                ? {
                      linksRemovedPagesKept: outcome.externalPages.map(
                          (page) => ({
                              sceneKey: page.sceneKey,
                              sceneName: page.sceneName,
                              sceneSlug: page.sceneSlug,
                              parentSceneKey: page.parentSceneKey,
                          }),
                      ),
                  }
                : {}),
            // Not deleted, but not where they were. A caller that reports only "done"
            // leaves someone hunting for a page that has quietly changed parent.
            ...(outcome.transferredPages.length > 0
                ? {
                      pagesMovedToAnotherLink: outcome.transferredPages.map(
                          (page) => ({
                              sceneKey: page.sceneKey,
                              sceneName: page.sceneName,
                              sceneSlug: page.sceneSlug,
                              previousParentSceneKey: page.parentSceneKey,
                              nowReachedFrom: page.otherReferrers,
                          }),
                      ),
                  }
                : {}),
            ...(reportedDeletes
                ? { pagesKnackReportsDeleted: reportedDeletes }
                : {}),
            ...outcome.result,
            ...(outcome.result.ok ? { cacheNote: VIEW_CACHE_STALE_NOTE } : {}),
        };
    }

    /**
     * Read the scenes Knack says it deleted out of a view-mutation response.
     *
     * @param result A KnackApiResult from a view PUT/POST/DELETE.
     * @returns The reported scene keys, or null when the response carries none.
     */
    function readDeletedScenes(result: KnackApiResult): string[] | null {
        const scenes = getObjectAtPath(
            result.body,
            'changes',
            'deletes',
            'scenes',
        );
        if (!Array.isArray(scenes) || scenes.length === 0) return null;

        const keys = scenes
            .map((scene) =>
                typeof scene === 'string'
                    ? scene
                    : ((asRecord(scene)?.key ?? null) as string | null),
            )
            .filter((key): key is string => typeof key === 'string');

        return keys.length > 0 ? keys : null;
    }

    async function getFieldReferenceIndexForApp(app: AppConfig): Promise<{
        index: CachedFieldReferenceIndex | null;
        source: CacheSource | null;
    }> {
        const cached = getCacheEntry(fieldReferenceCache, app.appKey);
        if (cached) return { index: cached.value, source: cached.source };

        const [schemaResult, fieldMapResult, viewMapResult, viewContextMap] =
            await Promise.all([
                getSchemaForApp(app),
                getFieldMapForApp(app),
                getViewMapForApp(app),
                getViewContextMapForApp(app),
            ]);

        if (
            schemaResult.schema ||
            fieldMapResult.fieldMap ||
            viewMapResult.viewMap
        ) {
            const index = buildFieldReferenceIndex({
                schema: schemaResult.schema,
                fieldMap: fieldMapResult.fieldMap,
                viewMap: viewMapResult.viewMap,
                viewContextMap,
            });

            if (Object.keys(index).length) {
                const source: CacheSource = [
                    schemaResult.source,
                    fieldMapResult.source,
                    viewMapResult.source,
                ].every((entry) => entry === 'runtime')
                    ? 'runtime'
                    : 'file';
                fieldReferenceCache.set(
                    app.appKey,
                    makeCacheEntry(index, source),
                );
                return { index, source };
            }
        }

        const diskIndex = readFieldReferenceIndexFromDisk(app);
        if (diskIndex && Object.keys(diskIndex).length) {
            fieldReferenceCache.set(
                app.appKey,
                makeCacheEntry(diskIndex, 'file'),
            );
            return { index: diskIndex, source: 'file' };
        }

        return { index: null, source: null };
    }

    async function getBuilderLinksForApp(
        app: AppConfig,
        params: {
            sceneKey?: string;
            viewKey?: string;
            viewType?: string;
            objectKey?: string;
            fieldKey?: string;
        },
    ) {
        const runtimeMetadata = await getRuntimeMetadata(app);
        return {
            base: makeBuilderBaseUrl(app, runtimeMetadata),
            scene: makeSceneBuilderUrl(app, params.sceneKey, runtimeMetadata),
            view: makeViewBuilderUrl(
                app,
                {
                    sceneKey: params.sceneKey,
                    viewKey: params.viewKey,
                    viewType: params.viewType,
                },
                runtimeMetadata,
            ),
            field: makeFieldBuilderUrl(
                app,
                {
                    objectKey: params.objectKey,
                    fieldKey: params.fieldKey,
                },
                runtimeMetadata,
            ),
        };
    }

    async function findFieldOwnerForApp(
        app: AppConfig,
        fieldKey: string,
    ): Promise<{
        objectKey?: string;
        objectName?: string;
        fieldName?: string;
    } | null> {
        const schemaResult = await getSchemaForApp(app);
        const schema = schemaResult.schema;
        if (!schema?.objects?.length) return null;

        for (const obj of schema.objects) {
            for (const field of obj.fields || []) {
                if (field.key !== fieldKey) continue;
                return {
                    objectKey: obj.key,
                    objectName: obj.name,
                    fieldName: field.name,
                };
            }
        }

        return null;
    }

    function buildRecordSearchParams({
        page,
        rowsPerPage,
        q,
        filters,
        sortField,
        sortOrder,
    }: {
        page: number;
        rowsPerPage: number;
        q?: string;
        filters?: string | Record<string, unknown>;
        sortField?: string;
        sortOrder?: 'asc' | 'desc';
    }): URLSearchParams {
        const params = new URLSearchParams();
        params.set('page', String(page));
        params.set('rows_per_page', String(rowsPerPage));
        if (q) params.set('q', q);
        const trimmedSortField = sortField?.trim();
        if (sortField !== undefined && !trimmedSortField) {
            throw new Error('sortField cannot be empty.');
        }
        if (sortOrder !== undefined && !trimmedSortField) {
            throw new Error('sortOrder requires sortField.');
        }
        if (trimmedSortField) {
            params.set('sort_field', trimmedSortField);
            params.set('sort_order', sortOrder === 'desc' ? 'desc' : 'asc');
        }

        if (filters !== undefined) {
            if (typeof filters === 'string') {
                const trimmed = filters.trim();
                if (!trimmed) {
                    throw new Error('filters string cannot be empty.');
                }
                if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
                    JSON.parse(trimmed);
                }
                params.set('filters', trimmed);
            } else {
                params.set('filters', JSON.stringify(filters));
            }
        }

        return params;
    }

    /**
     * Resolve and enforce the optional app-level read policy before exposing record data.
     * @param app Selected Knack application.
     * @param objectKey Object whose records are requested.
     * @param requestedFieldKeys Fields requested by the caller.
     * @returns The validated object metadata and permitted field keys.
     */
    async function getPermittedReadFields(
        app: AppConfig,
        objectKey: string,
        requestedFieldKeys: string[],
    ) {
        const policy = app.dataAccess;
        if (
            policy?.allowedObjectKeys &&
            !policy.allowedObjectKeys.includes(objectKey)
        ) {
            throw new Error(
                `Read access to ${objectKey} is not allowed by this app's dataAccess policy.`,
            );
        }

        const schemaResult = await getSchemaForApp(app);
        const object = schemaResult.schema?.objects?.find(
            (entry) => entry.key === objectKey,
        );
        if (!object)
            throw new Error(
                `Object ${objectKey} was not found in the available schema.`,
            );

        const knownFields = new Set(
            (object.fields || []).map((field) => field.key),
        );
        const policyFields = policy?.allowedFieldKeys?.[objectKey];
        const redactedFields = new Set(policy?.redactedFieldKeys || []);
        const fields = requestedFieldKeys
            .map((fieldKey) => fieldKey.trim())
            .filter(Boolean);

        for (const fieldKey of fields) {
            if (!knownFields.has(fieldKey))
                throw new Error(
                    `Field ${fieldKey} does not belong to ${objectKey}.`,
                );

            if (policyFields && !policyFields.includes(fieldKey)) {
                throw new Error(
                    `Field ${fieldKey} is not allowed by this app's dataAccess policy.`,
                );
            }

            if (redactedFields.has(fieldKey)) {
                throw new Error(
                    `Field ${fieldKey} is redacted by this app's dataAccess policy.`,
                );
            }
        }

        return {
            object,
            fields,
            maxRecords: policy?.maxRecordsPerQuery || 1000,
        };
    }

    /**

     * Collect field keys used by a Knack filter tree.
     *
     * @param filters Structured Knack filters or their JSON representation.
     * @returns Referenced field keys.
     */
    function getFilterFieldKeys(
        filters: string | Record<string, unknown> | undefined,
    ): string[] {
        if (filters === undefined) return [];
        const parsed =
            typeof filters === 'string' ? JSON.parse(filters) : filters;
        const fields = new Set<string>();
        const visit = (value: unknown): void => {
            if (Array.isArray(value)) {
                value.forEach(visit);
                return;
            }
            const record = asRecord(value);
            if (!record) return;
            if (typeof record.field === 'string') fields.add(record.field);
            Object.values(record).forEach(visit);
        };
        visit(parsed);
        return [...fields];
    }

    /**
     * Validate all fields that influence a record query before it is sent to Knack.
     *
     * @param app Selected Knack application.
     * @param objectKey Queried object.
     * @param options Query inputs that can reveal data through filtering or ordering.
     * @returns Maximum records permitted for the app.
     */
    async function validateReadQuery(
        app: AppConfig,
        objectKey: string,
        options: {
            filters?: string | Record<string, unknown>;
            q?: string;
            sortField?: string;
        },
    ): Promise<number> {
        if (!app.dataAccess) {
            return (await getPermittedReadFields(app, objectKey, []))
                .maxRecords;
        }
        const filterFields = getFilterFieldKeys(options.filters);
        const requestedFields = [
            ...filterFields,
            ...(options.sortField ? [options.sortField] : []),
        ];
        const { maxRecords } = await getPermittedReadFields(
            app,
            objectKey,
            requestedFields,
        );

        if (app.dataAccess && options.q?.trim()) {
            throw new Error(
                'Free-text search is disabled for apps with a dataAccess policy because it can search unapproved fields. Use approved structured filters instead.',
            );
        }
        return maxRecords;
    }

    /**

     * Return a record with only the fields explicitly approved for the tool call.

     *

     * @param value Raw Knack record payload.

     * @param fieldKeys Approved field keys.

     * @returns Minimal record representation safe to return to the MCP client.

     */

    function projectRecordFields(
        value: unknown,
        fieldKeys: string[],
    ): Record<string, unknown> {
        const record = asRecord(value) || {};

        const projected: Record<string, unknown> = {
            id: record.id || record._id || null,
        };

        for (const fieldKey of fieldKeys) {
            projected[fieldKey] = record[fieldKey] ?? null;

            if (`${fieldKey}_raw` in record)
                projected[`${fieldKey}_raw`] = record[`${fieldKey}_raw`];
        }

        return projected;
    }

    /**

     * Apply an app's data policy to existing generic record-read responses without changing

     * the response shape for installations that have not opted into a policy.

     *

     * @param app Selected Knack application.

     * @param objectKey Object represented by the response.

     * @param result Knack API response.

     * @returns Original response or a response with record values projected to approved fields.

     */

    async function applyRecordReadPolicy(
        app: AppConfig,
        objectKey: string,
        result: KnackApiResult,
    ): Promise<KnackApiResult> {
        if (!app.dataAccess) return result;

        const schemaResult = await getSchemaForApp(app);

        const object = schemaResult.schema?.objects?.find(
            (entry) => entry.key === objectKey,
        );

        const defaultFields = (object?.fields || [])
            .map((field) => field.key)

            .filter(
                (fieldKey) =>
                    !app.dataAccess?.redactedFieldKeys?.includes(fieldKey),
            );

        const policyFields = app.dataAccess.allowedFieldKeys?.[objectKey];

        const { fields } = await getPermittedReadFields(
            app,
            objectKey,
            policyFields || defaultFields,
        );

        const body = asRecord(result?.body);

        if (!body) return result;

        if (Array.isArray(body.records)) {
            return {
                ...result,

                body: {
                    ...body,
                    records: body.records.map((record) =>
                        projectRecordFields(record, fields),
                    ),
                },
            };
        }

        return { ...result, body: projectRecordFields(body, fields) };
    }

    /**

     * Extract Knack records from either a list or single-record API response.

     *

     * @param result Knack API response.

     * @returns Normalised record array.

     */

    function getRecordsFromResponse(
        result: unknown,
    ): Record<string, unknown>[] {
        const body = asRecord(asRecord(result)?.body);

        const records = body?.records;

        if (Array.isArray(records))
            return records
                .map(asRecord)
                .filter((record): record is Record<string, unknown> =>
                    Boolean(record),
                );

        if (body) return [body];

        return [];
    }

    /**

     * Convert a plain Knack numeric or formatted currency value into a number.

     *

     * @param value Knack field value.

     * @returns Numeric value, or null when it cannot be safely interpreted.

     */

    function getNumericValue(value: unknown): number | null {
        if (typeof value === 'number' && Number.isFinite(value)) return value;

        if (typeof value !== 'string') return null;

        const parsed = Number(value.replace(/[^0-9.-]/g, ''));

        return Number.isFinite(parsed) ? parsed : null;
    }

    /**

     * Create a stable date bucket from common Knack display and raw date shapes.

     *

     * @param value Knack date value.

     * @param granularity Required reporting bucket size.

     * @returns ISO-like bucket label, or null when no date is available.

     */

    function bucketDate(
        value: unknown,
        granularity: 'day' | 'month' | 'year',
    ): string | null {
        const raw = asRecord(value);

        const text =
            typeof value === 'string'
                ? value
                : typeof raw?.iso === 'string'
                  ? raw.iso
                  : typeof raw?.date === 'string'
                    ? raw.date
                    : null;

        const isoMatch = text?.match(/(\d{4})[-/](\d{1,2})(?:[-/](\d{1,2}))?/);

        const ukMatch = text?.match(/(\d{1,2})[-/](\d{1,2})[-/](\d{4})/);

        const year = isoMatch?.[1] || ukMatch?.[3];

        const monthValue = isoMatch?.[2] || ukMatch?.[2];

        const dayValue = isoMatch?.[3] || ukMatch?.[1];

        if (!year || !monthValue) return null;

        const month = monthValue.padStart(2, '0');

        const day = dayValue?.padStart(2, '0');

        if (granularity === 'year') return year;

        if (granularity === 'month') return `${year}-${month}`;

        return day ? `${year}-${month}-${day}` : null;
    }

    const server = new McpServer({
        name: 'knack-mcp-multi',

        version: '1.0.0',
    });

    type ToolRegistrationFn = (...args: unknown[]) => unknown;
    const baseToolRegistration = server.tool.bind(
        server,
    ) as unknown as ToolRegistrationFn;
    (server as unknown as { tool: ToolRegistrationFn }).tool = ((
        ...args: unknown[]
    ) => {
        const [name, description, inputSchema, handler] = args;
        return baseToolRegistration(
            name,
            compactToolDescription(String(name), String(description)),
            inputSchema,
            handler,
        );
    }) as ToolRegistrationFn;

    // -----------------------
    // MCP tool index (canonical naming)
    // -----------------------
    // Context/discovery:
    // - knack_list_apps
    // - knack_set_context
    // - knack_cache_status
    // - knack_refresh_cache
    //
    // Data reads:
    // - knack_get_record
    // - knack_find_records
    // - knack_get_object_records_with_schema
    // - knack_get_raw_object_metadata
    //
    // Schema/field helpers:
    // - knack_get_object_fields
    // - knack_get_object
    // - knack_list_fields
    // - knack_get_field_type
    // - knack_list_field_types
    // - knack_resolve_field_alias
    // - knack_resolve_any
    // - validateFieldMapping
    // - generateSnapshotStructure
    // - checkForDuplicateFieldUsage
    // - knack_list_objects
    // - knack_describe_field_shape
    // - knack_get_object_connections
    // - knack_get_app_overview
    // - knack_generate_seed_csvs
    //
    // View/search helpers:
    // - knack_get_view_context
    // - knack_get_view_attributes
    // - knack_search_ktl_keywords
    // - knack_search_emails
    // - knack_find_views_with_record_rule_field
    // - knack_list_field_references
    // - knack_list_scenes
    // - knack_list_views
    // - knack_analyze_data_model
    // - knack_app_deep_dive

    // -----------------------
    // Tools: context + discovery
    // -----------------------

    server.tool(
        'knack_list_apps',
        'List all Knack apps discovered from the KnackApps folder. Re-scans the directory each time so newly added apps appear immediately.',
        {},
        async () => {
            debugLog('tool_call', { tool: 'knack_list_apps' });
            const freshApps = rescanApps();
            const humanConfirmation = getHumanConfirmationStatus();
            debugLog('human_confirmation_status', humanConfirmation);
            // Reported once rather than per app: this depends only on the connected
            // client, and no app.json setting can change it.
            const cascadeDeleteBehaviour = describeCascadeBehaviour(
                humanConfirmation.available,
            );
            // Led by prose because the elicitation rule is client-dependent and decides
            // whether a cascade delete is confirmable at all. The structured fields below
            // stay unchanged for callers that parse the payload.
            // Reported so a caller can tell a stale server from a current one
            // without inferring it from which keys are missing.
            const serverBuild = describeServerBuild(options.readOnly === true);
            const humanSummary = describeAppListForHumans({
                knackAppsDir: knackAppsDir as string,
                activeAppKey: state.activeAppKey,
                apps: freshApps,
                enforcedReadOnly: options.readOnly === true,
                humanConfirmation,
                cascadeDeleteBehaviour,
                buildSummary: summariseServerBuild(serverBuild),
            });
            return makeTextResponse(
                {
                    ok: true,
                    serverBuild,
                    knackAppsDir,
                    activeAppKey: state.activeAppKey,
                    humanConfirmation,
                    cascadeDeleteBehaviour,
                    apps: freshApps.map((a) => ({
                        appKey: a.appKey,
                        appName: a.appName,
                        appId: a.appId,
                        appFolder: a.appFolder,
                        readonly: a.readonly !== false,
                        allowViewMutation: a.allowViewMutation === true,
                        allowDelete: a.allowDelete === true,
                        allowDiagnostics: a.allowDiagnostics === true,
                        notes: a.notes,
                    })),
                },
                humanSummary,
            );
        },
    );

    server.tool(
        'knack_set_context',
        'Set the active Knack app using either an explicit appKey or a file/folder path. The server can infer the app from KnackApps paths and common app-name aliases.',
        {
            appKey: z
                .string()
                .optional()
                .describe(
                    'Explicit app key to activate immediately, e.g. GAP-Track.',
                ),
            contextPath: z
                .string()
                .optional()
                .describe(
                    'A file path (preferred) or folder path within your workspace.',
                ),
        },
        async (args: { appKey?: string; contextPath?: string }) => {
            debugLog('tool_call', { tool: 'knack_set_context', args });
            const { appKey, contextPath } = args;

            if (appKey) {
                const app = appsByKey.get(appKey);
                if (!app) {
                    return makeTextResponse({
                        ok: false,
                        message: `Unknown appKey: ${appKey}`,
                        availableApps: apps.map((a) => a.appKey),
                    });
                }

                state.activeAppKey = app.appKey;
                if (contextPath) state.lastContextPath = contextPath;

                return makeTextResponse({
                    ok: true,
                    activeAppKey: state.activeAppKey,
                    contextPath: contextPath || null,
                    inferenceMode: 'explicit-appkey',
                });
            }

            if (!contextPath) {
                return makeTextResponse({
                    ok: false,
                    message: 'Provide either appKey or contextPath.',
                    availableApps: apps.map((a) => a.appKey),
                });
            }

            const inferred = inferAppKeyFromPath(contextPath);

            if (!inferred.appKey) {
                return makeTextResponse({
                    ok: false,
                    message:
                        'Could not infer appKey from the given contextPath.',
                    contextPath,
                    hint: 'Use a file inside KnackApps/<AppKey>/..., a path/basename containing the app name, or pass appKey directly.',
                    candidateAppKeys: inferred.candidateAppKeys,
                    availableApps: apps.map((a) => a.appKey),
                });
            }

            state.activeAppKey = inferred.appKey;
            state.lastContextPath = contextPath;

            return makeTextResponse({
                ok: true,
                activeAppKey: state.activeAppKey,
                contextPath,
                inferenceMode: inferred.inferenceMode,
            });
        },
    );

    server.tool(
        'knack_cache_status',
        'Report active app context, local schema/fieldMap/viewMap file presence, and cache status.',
        {
            appKey: z.string().optional(),
        },
        async (args: { appKey?: string }) => {
            debugLog('tool_call', { tool: 'knack_cache_status', args });
            const app = getAppOrThrow(args.appKey);
            const schemaPath = resolveMetadataFilePath(app, 'schema.json');
            const fieldMapPath = resolveMetadataFilePath(app, 'fieldMap.json');
            const viewMapPath = resolveMetadataFilePath(app, 'viewMap.json');
            const fieldReferenceIndexPath = resolveMetadataFilePath(
                app,
                'fieldReferenceIndex.json',
            );

            const schemaEntry = getCacheEntry(schemaCache, app.appKey);
            const fieldMapEntry = getCacheEntry(fieldMapCache, app.appKey);
            const viewMapEntry = getCacheEntry(viewMapCache, app.appKey);
            const metadataEntry = getCacheEntry(
                runtimeMetadataCache,
                app.appKey,
            );
            const fieldReferenceEntry = getCacheEntry(
                fieldReferenceCache,
                app.appKey,
            );

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                activeAppKey: state.activeAppKey,
                lastContextPath: state.lastContextPath,
                cacheTtlMs: CACHE_TTL_MS,
                files: {
                    schemaPath,
                    schemaExists: metadataFileExists(app, 'schema.json'),
                    schemaPathCandidates: getMetadataFilePaths(
                        app,
                        'schema.json',
                    ),
                    fieldMapPath,
                    fieldMapExists: metadataFileExists(app, 'fieldMap.json'),
                    fieldMapPathCandidates: getMetadataFilePaths(
                        app,
                        'fieldMap.json',
                    ),
                    viewMapPath,
                    viewMapExists: metadataFileExists(app, 'viewMap.json'),
                    viewMapPathCandidates: getMetadataFilePaths(
                        app,
                        'viewMap.json',
                    ),
                    fieldReferenceIndexPath,
                    fieldReferenceIndexExists: metadataFileExists(
                        app,
                        'fieldReferenceIndex.json',
                    ),
                    fieldReferenceIndexPathCandidates: getMetadataFilePaths(
                        app,
                        'fieldReferenceIndex.json',
                    ),
                },
                cache: {
                    schema: schemaEntry
                        ? {
                              cached: true,
                              source: schemaEntry.source,
                              loadedAt: new Date(
                                  schemaEntry.loadedAt,
                              ).toISOString(),
                              expiresAt: new Date(
                                  schemaEntry.expiresAt,
                              ).toISOString(),
                              expiresInMs: Math.max(
                                  0,
                                  schemaEntry.expiresAt - Date.now(),
                              ),
                          }
                        : { cached: false },
                    fieldMap: fieldMapEntry
                        ? {
                              cached: true,
                              source: fieldMapEntry.source,
                              loadedAt: new Date(
                                  fieldMapEntry.loadedAt,
                              ).toISOString(),
                              expiresAt: new Date(
                                  fieldMapEntry.expiresAt,
                              ).toISOString(),
                              expiresInMs: Math.max(
                                  0,
                                  fieldMapEntry.expiresAt - Date.now(),
                              ),
                          }
                        : { cached: false },
                    viewMap: viewMapEntry
                        ? {
                              cached: true,
                              source: viewMapEntry.source,
                              loadedAt: new Date(
                                  viewMapEntry.loadedAt,
                              ).toISOString(),
                              expiresAt: new Date(
                                  viewMapEntry.expiresAt,
                              ).toISOString(),
                              expiresInMs: Math.max(
                                  0,
                                  viewMapEntry.expiresAt - Date.now(),
                              ),
                          }
                        : { cached: false },
                    runtimeMetadata: metadataEntry
                        ? {
                              cached: true,
                              loadedAt: new Date(
                                  metadataEntry.loadedAt,
                              ).toISOString(),
                              expiresAt: new Date(
                                  metadataEntry.expiresAt,
                              ).toISOString(),
                              expiresInMs: Math.max(
                                  0,
                                  metadataEntry.expiresAt - Date.now(),
                              ),
                          }
                        : { cached: false },
                    fieldReferences: fieldReferenceEntry
                        ? {
                              cached: true,
                              source: fieldReferenceEntry.source,
                              loadedAt: new Date(
                                  fieldReferenceEntry.loadedAt,
                              ).toISOString(),
                              expiresAt: new Date(
                                  fieldReferenceEntry.expiresAt,
                              ).toISOString(),
                              expiresInMs: Math.max(
                                  0,
                                  fieldReferenceEntry.expiresAt - Date.now(),
                              ),
                          }
                        : { cached: false },
                },
            });
        },
    );

    server.tool(
        'knack_refresh_cache',
        'Clear runtime/schema/fieldMap/viewMap caches for one app or all apps, optionally warming immediately and persisting runtime metadata to local files. persistFiles requires warm: true — on its own it clears the caches and writes nothing, because there is no fetched metadata to write.',
        {
            appKey: z.string().optional(),
            warm: z.boolean().default(false),
            persistFiles: z.boolean().default(true),
        },
        async (args: {
            appKey?: string;
            warm: boolean;
            persistFiles: boolean;
        }) => {
            debugLog('tool_call', { tool: 'knack_refresh_cache', args });

            const { appKey, warm, persistFiles } = args;
            const targetApps = appKey
                ? [getAppOrThrow(appKey)]
                : [...appsByKey.values()];

            const getSizes = () => ({
                runtimeMetadata: runtimeMetadataCache.size,
                schema: schemaCache.size,
                fieldMap: fieldMapCache.size,
                viewMap: viewMapCache.size,
                fieldReferences: fieldReferenceCache.size,
            });

            const beforeSizes = getSizes();

            if (appKey) {
                runtimeMetadataCache.delete(appKey);
                schemaCache.delete(appKey);
                fieldMapCache.delete(appKey);
                viewMapCache.delete(appKey);
                fieldReferenceCache.delete(appKey);
            } else {
                runtimeMetadataCache.clear();
                schemaCache.clear();
                fieldMapCache.clear();
                viewMapCache.clear();
                fieldReferenceCache.clear();
            }

            const warmed: Array<Record<string, unknown>> = [];
            if (warm) {
                for (const app of targetApps) {
                    try {
                        const metadata = await getRuntimeMetadata(app);
                        const schemaResult = await getSchemaForApp(app);
                        const fieldMapResult = await getFieldMapForApp(app);
                        const viewMapResult = await getViewMapForApp(app);
                        const fieldReferenceResult =
                            await getFieldReferenceIndexForApp(app);

                        const persisted: Record<string, unknown> = {
                            enabled: persistFiles,
                        };

                        if (persistFiles) {
                            if (
                                schemaResult.source === 'runtime' &&
                                schemaResult.schema
                            ) {
                                persisted.schema = writeMetadataJson(
                                    app,
                                    'schema.json',
                                    schemaResult.schema,
                                );
                            }
                            if (
                                fieldMapResult.source === 'runtime' &&
                                fieldMapResult.fieldMap
                            ) {
                                persisted.fieldMap = writeMetadataJson(
                                    app,
                                    'fieldMap.json',
                                    fieldMapResult.fieldMap,
                                );
                            }
                            if (
                                viewMapResult.source === 'runtime' &&
                                viewMapResult.viewMap
                            ) {
                                persisted.viewMap = writeMetadataJson(
                                    app,
                                    'viewMap.json',
                                    viewMapResult.viewMap,
                                );
                            }
                            if (fieldReferenceResult.index) {
                                persisted.fieldReferenceIndex =
                                    writeMetadataJson(
                                        app,
                                        'fieldReferenceIndex.json',
                                        fieldReferenceResult.index,
                                    );
                            }
                        }

                        warmed.push({
                            appKey: app.appKey,
                            ok: true,
                            runtimeMetadataLoaded: Boolean(metadata),
                            schemaSource: schemaResult.source,
                            fieldMapSource: fieldMapResult.source,
                            viewMapSource: viewMapResult.source,
                            fieldReferenceSource: fieldReferenceResult.source,
                            persisted,
                        });
                    } catch (error) {
                        warmed.push({
                            appKey: app.appKey,
                            ok: false,
                            error:
                                error instanceof Error
                                    ? error.message
                                    : String(error),
                        });
                    }
                }
            }

            const persistSkipped = describePersistOutcome(warm, persistFiles);

            return makeTextResponse({
                ok: true,
                target: appKey || 'all',
                warm,
                persistFiles,
                ...(persistSkipped ? { persistSkipped } : {}),
                appCount: targetApps.length,
                beforeSizes,
                afterSizes: getSizes(),
                warmed,
            });
        },
    );

    server.tool(
        'knack_get_context_bundle',
        'Fetch a bounded, targeted bundle of selected object schema, field aliases, and view context/details in one call.',
        {
            appKey: z.string().optional(),
            objectKeys: z
                .array(z.string())
                .min(1)
                .max(20)
                .optional()
                .describe(
                    'Exact object keys to include. Omit when only aliases or views are needed.',
                ),
            fieldAliases: z
                .array(z.string())
                .min(1)
                .max(100)
                .optional()
                .describe(
                    'Field references to resolve and include. Either a direct "object_key.field_key" (e.g. object_2.field_123), or a fieldMap alias in "object_key.normalised_field_name" form (e.g. object_2.name).',
                ),
            viewKeys: z
                .array(z.string())
                .min(1)
                .max(20)
                .optional()
                .describe('Exact view keys to include.'),
            includeViewAttributes: z
                .boolean()
                .default(false)
                .describe(
                    'Include guarded raw view attributes for the requested views.',
                ),
        },
        async ({
            appKey,
            objectKeys,
            fieldAliases,
            viewKeys,
            includeViewAttributes,
        }) => {
            const requestedObjectKeys = [...new Set(objectKeys || [])];
            const requestedAliases = [...new Set(fieldAliases || [])];
            const requestedViewKeys = [...new Set(viewKeys || [])];

            if (
                !requestedObjectKeys.length &&
                !requestedAliases.length &&
                !requestedViewKeys.length
            ) {
                return makeTextResponse({
                    ok: false,
                    message:
                        'Provide at least one objectKey, fieldAlias, or viewKey. This tool intentionally does not return an unbounded app dump.',
                });
            }

            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_get_context_bundle',
                args: {
                    appKey: app.appKey,
                    objectCount: requestedObjectKeys.length,
                    aliasCount: requestedAliases.length,
                    viewCount: requestedViewKeys.length,
                    includeViewAttributes,
                },
            });

            const hasQualifiedFieldKeyAlias = requestedAliases.some((alias) =>
                FIELD_ALIAS_OBJECT_FIELD_KEY_PATTERN.test(alias),
            );

            const [
                schemaResult,
                fieldMapResult,
                viewMapResult,
                runtimeMetadata,
            ] = await Promise.all([
                requestedObjectKeys.length ||
                requestedViewKeys.length ||
                hasQualifiedFieldKeyAlias
                    ? getSchemaForApp(app)
                    : Promise.resolve(null),

                requestedAliases.length
                    ? getFieldMapForApp(app)
                    : Promise.resolve(null),

                requestedViewKeys.length
                    ? getViewMapForApp(app)
                    : Promise.resolve(null),

                requestedObjectKeys.length || requestedViewKeys.length
                    ? getRuntimeMetadata(app)
                    : Promise.resolve(null),
            ]);

            const viewContextMap = parseRuntimeViewContextMap(runtimeMetadata);

            const schemaObjects = schemaResult?.schema?.objects || [];

            const objectByKey = new Map(
                schemaObjects.map((object) => [object.key, object]),
            );

            const fieldMap = fieldMapResult?.fieldMap || {};

            const viewMap = viewMapResult?.viewMap || {};

            const objects = requestedObjectKeys.map((objectKey) => {
                const object = objectByKey.get(objectKey);
                return object
                    ? {
                          found: true,
                          key: object.key,
                          name: object.name,
                          fields: (object.fields || []).map((field) => ({
                              key: field.key,
                              name: field.name,
                              type: field.type,
                              required: field.required,
                              description: field.description,
                              connectedObject: field.connectedObject,
                              builderUrl: makeFieldBuilderUrl(
                                  app,
                                  {
                                      objectKey: object.key,
                                      fieldKey: field.key,
                                  },
                                  runtimeMetadata,
                              ),
                          })),
                      }
                    : { found: false, key: objectKey };
            });

            const aliases = requestedAliases.map((alias) => {
                const qualifiedKeyMatch = alias.match(
                    FIELD_ALIAS_OBJECT_FIELD_KEY_PATTERN,
                );
                if (qualifiedKeyMatch) {
                    const [, objectKey, fieldKey] = qualifiedKeyMatch;
                    const object = objectByKey.get(objectKey);
                    const field = object?.fields?.find(
                        (entry) => entry.key === fieldKey,
                    );
                    if (field) {
                        return {
                            found: true,
                            alias,
                            fieldKey: field.key,
                            fieldType: field.type || null,
                        };
                    }
                    return {
                        found: false,
                        alias,
                        message: object
                            ? `${fieldKey} was not found on ${objectKey}.`
                            : `${objectKey} was not found in the cached schema for this app. Confirm the object key is correct, or run knack_refresh_cache if it was added or renamed recently — the schema is loaded in full regardless of which objectKeys were requested.`,
                    };
                }

                const entry = fieldMap[alias];
                if (entry) {
                    return {
                        found: true,
                        alias,
                        fieldKey: entry.fieldKey,
                        fieldType: entry.fieldType || null,
                    };
                }
                return {
                    found: false,
                    alias,
                    message:
                        'Alias not found. fieldAliases accepts either a direct "object_key.field_key" reference (e.g. object_2.field_123) or a fieldMap alias in "object_key.normalised_field_name" form (e.g. object_2.name) — a bare field name or field key without the object_key prefix will not resolve.',
                };
            });

            const views = requestedViewKeys.map((viewKey) => {
                const attributes = viewMap[viewKey];
                const context = viewContextMap[viewKey] || {};
                const viewName =
                    typeof attributes?.name === 'string'
                        ? attributes.name
                        : undefined;
                const viewType =
                    typeof attributes?.type === 'string'
                        ? attributes.type
                        : undefined;
                const attributesDetail =
                    includeViewAttributes && attributes
                        ? getInlineDetail(attributes)
                        : null;
                const fieldSettings = attributes
                    ? getViewFieldSettings(
                          attributes,
                          getViewObjectFields(attributes, schemaResult?.schema),
                      )
                    : null;

                return {
                    found: Boolean(attributes || context.sceneKey),

                    viewKey,

                    ...context,

                    viewName,

                    viewType,

                    builderUrl: makeViewBuilderUrl(
                        app,
                        {
                            sceneKey: context.sceneKey,
                            viewKey,
                            viewType,
                        },
                        runtimeMetadata,
                    ),
                    attributesIncluded: attributesDetail?.included || false,
                    attributes: attributesDetail?.value,
                    attributesSummary: attributesDetail?.summary,
                    attributesSizeBytes: attributesDetail?.sizeBytes,
                    fieldSettings,
                };
            });

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                requested: {
                    objectKeys: requestedObjectKeys,
                    fieldAliases: requestedAliases,
                    viewKeys: requestedViewKeys,
                    includeViewAttributes,
                },
                sources: {
                    schema: schemaResult?.source || null,

                    fieldMap: fieldMapResult?.source || null,

                    viewMap: viewMapResult?.source || null,

                    viewContext: runtimeMetadata ? 'runtime' : null,
                },
                objects,
                aliases,
                views,
            });
        },
    );

    // -----------------------
    // Tools: Knack reads (safe)
    // -----------------------

    server.tool(
        'knack_get_record',
        'Fetch a single Knack record by object key and record id. Uses appKey if provided, otherwise the active app context.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
            recordId: z.string(),
        },
        async (args: {
            appKey?: string;
            objectKey: string;
            recordId: string;
        }) => {
            debugLog('tool_call', { tool: 'knack_get_record', args });
            const { appKey, objectKey, recordId } = args;
            const app = getAppOrThrow(appKey);
            await getPermittedReadFields(app, objectKey, []);
            const apiKey = getApiKeyOrThrow(app.appKey);
            const result = await knackRequest(
                app,
                apiKey,
                `/objects/${objectKey}/records/${recordId}`,
            );
            const safeResult = await applyRecordReadPolicy(
                app,
                objectKey,
                result,
            );
            return makeTextResponse({
                appKey: app.appKey,
                ...safeResult,
                ...(safeResult.ok
                    ? {
                          tip: 'Prefer field_xxx_raw for connections/dates — see knack_describe_field_shape.',
                      }
                    : {}),
            });
        },
    );

    server.tool(
        'knack_find_records',
        'Search Knack records (basic query + paging + sorting). Uses appKey if provided, otherwise the active app context.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
            page: z.number().int().min(1).default(1),
            rowsPerPage: z.number().int().min(1).max(1000).default(25),
            q: z.string().optional().describe('Free text search (q=)'),
            filters: z
                .union([z.string(), z.record(z.string(), z.unknown())])
                .optional()
                .describe(
                    'Structured Knack filters object (recommended) or JSON string.',
                ),
            sortField: z
                .string()
                .optional()
                .describe('Field key to sort by (sort_field=), e.g. field_66'),
            sortOrder: z
                .enum(['asc', 'desc'])
                .optional()
                .describe('Sort direction (sort_order=), default asc'),
        },
        async ({
            appKey,
            objectKey,
            page,
            rowsPerPage,
            q,
            filters,
            sortField,
            sortOrder,
        }) => {
            debugLog('tool_call', {
                tool: 'knack_find_records',
                args: {
                    appKey,
                    objectKey,
                    page,
                    rowsPerPage,
                    q,
                    filters,
                    sortField,
                    sortOrder,
                },
            });
            const app = getAppOrThrow(appKey);
            const maxRecords = await validateReadQuery(app, objectKey, {
                filters,
                q,
                sortField,
            });
            const apiKey = getApiKeyOrThrow(app.appKey);
            const params = buildRecordSearchParams({
                page,
                rowsPerPage: Math.min(rowsPerPage, maxRecords),
                q,
                filters,
                sortField,
                sortOrder,
            });

            const result = await knackRequest(
                app,
                apiKey,
                `/objects/${objectKey}/records?${params.toString()}`,
            );
            const safeResult = await applyRecordReadPolicy(
                app,
                objectKey,
                result,
            );

            return makeTextResponse({
                appKey: app.appKey,
                ...safeResult,
                ...(safeResult.ok
                    ? {
                          tip: 'Prefer field_xxx_raw for connections/dates — see knack_describe_field_shape.',
                      }
                    : {}),
            });
        },
    );

    server.tool(
        'knack_get_object_records_with_schema',

        'Fetch records for an object (paging + sorting) and include that object schema in the same response. Defaults to ARC object_294.',
        {
            appKey: z.string().default('ARC'),
            objectKey: z.string().default('object_294'),
            page: z.number().int().min(1).default(1),
            rowsPerPage: z.number().int().min(1).max(1000).default(25),
            q: z.string().optional().describe('Free text search (q=)'),
            filters: z
                .union([z.string(), z.record(z.string(), z.unknown())])
                .optional()
                .describe(
                    'Structured Knack filters object (recommended) or JSON string.',
                ),
            sortField: z
                .string()
                .optional()
                .describe('Field key to sort by (sort_field=), e.g. field_66'),
            sortOrder: z
                .enum(['asc', 'desc'])
                .optional()
                .describe('Sort direction (sort_order=), default asc'),
        },
        async ({
            appKey,
            objectKey,
            page,
            rowsPerPage,
            q,
            filters,
            sortField,
            sortOrder,
        }) => {
            debugLog('tool_call', {
                tool: 'knack_get_object_records_with_schema',
                args: {
                    appKey,
                    objectKey,
                    page,
                    rowsPerPage,
                    q,
                    filters,
                    sortField,
                    sortOrder,
                },
            });
            const app = getAppOrThrow(appKey);
            const maxRecords = await validateReadQuery(app, objectKey, {
                filters,
                q,
                sortField,
            });
            const apiKey = getApiKeyOrThrow(app.appKey);
            const params = buildRecordSearchParams({
                page,
                rowsPerPage: Math.min(rowsPerPage, maxRecords),
                q,
                filters,
                sortField,
                sortOrder,
            });

            const [schemaResult, recordsResult] = await Promise.all([
                getSchemaForApp(app),
                knackRequest(
                    app,
                    apiKey,
                    `/objects/${objectKey}/records?${params.toString()}`,
                ),
            ]);

            const object =
                schemaResult.schema?.objects?.find(
                    (entry) => entry.key === objectKey,
                ) || null;

            const safeRecordsResult = await applyRecordReadPolicy(
                app,
                objectKey,
                recordsResult,
            );

            return makeTextResponse({
                ok: Boolean(object) && recordsResult.ok,
                appKey: app.appKey,
                objectKey,
                objectName: object?.name || null,
                schemaSource: schemaResult.source,
                schemaAvailable: Boolean(object),
                schemaMessage: object
                    ? null
                    : schemaResult.schema?.objects?.length
                      ? `Object not found in schema: ${objectKey}`
                      : 'No schema available from runtime API or schema.json.',
                schema: object
                    ? {
                          key: object.key,
                          name: object.name,
                          fieldCount: (object.fields || []).length,
                          fields: (object.fields || []).map((field) => ({
                              key: field.key,
                              name: field.name,
                              type: field.type,
                              required: field.required,
                              description: field.description,
                          })),
                      }
                    : null,
                recordsResponse: safeRecordsResult,
            });
        },
    );

    server.tool(
        'knack_get_related_records',

        'Fetch approved fields from records connected to a selected record, following a connection forward or in reverse.',

        {
            appKey: z.string().optional(),

            sourceObjectKey: z.string(),

            sourceRecordId: z.string(),

            direction: z.enum(['forward', 'reverse']),

            connectionFieldKey: z
                .string()
                .describe(
                    'Connection field on the source object (forward) or related object (reverse).',
                ),

            relatedObjectKey: z
                .string()
                .optional()
                .describe(
                    'Required for reverse lookups. Forward lookups derive this from the connection field.',
                ),

            fieldKeys: z
                .array(z.string())
                .min(1)
                .max(50)
                .describe('Only these approved fields are returned.'),

            limit: z.number().int().min(1).max(100).default(25),

            sortField: z.string().optional(),

            sortOrder: z.enum(['asc', 'desc']).optional(),
        },

        async ({
            appKey,
            sourceObjectKey,
            sourceRecordId,
            direction,
            connectionFieldKey,
            relatedObjectKey,
            fieldKeys,
            limit,
            sortField,
            sortOrder,
        }) => {
            const app = getAppOrThrow(appKey);

            const apiKey = getApiKeyOrThrow(app.appKey);

            const sourceSchema = await getSchemaForApp(app);

            const sourceObject = sourceSchema.schema?.objects?.find(
                (entry) => entry.key === sourceObjectKey,
            );

            if (!sourceObject)
                throw new Error(
                    `Object ${sourceObjectKey} was not found in the available schema.`,
                );

            const effectiveLimit = Math.min(
                limit,
                app.dataAccess?.maxRecordsPerQuery || 1000,
            );

            let targetObjectKey = relatedObjectKey;

            let records: Record<string, unknown>[] = [];

            if (direction === 'forward') {
                const connection = (sourceObject.fields || []).find(
                    (field) => field.key === connectionFieldKey,
                );

                if (!connection?.connectedObject) {
                    throw new Error(
                        `${connectionFieldKey} is not a recognised connection field on ${sourceObjectKey}.`,
                    );
                }

                targetObjectKey = connection.connectedObject;

                await getPermittedReadFields(app, sourceObjectKey, [
                    connectionFieldKey,
                ]);

                const target = await getPermittedReadFields(
                    app,
                    targetObjectKey,
                    fieldKeys,
                );

                const sourceResult = await knackRequest(
                    app,
                    apiKey,
                    `/objects/${sourceObjectKey}/records/${sourceRecordId}`,
                );

                const sourceRecord = getRecordsFromResponse(sourceResult)[0];

                const connectionValue =
                    sourceRecord?.[`${connectionFieldKey}_raw`] ??
                    sourceRecord?.[connectionFieldKey];

                const relatedIds = (
                    Array.isArray(connectionValue) ? connectionValue : []
                )

                    .map((entry) => asRecord(entry)?.id)

                    .filter((id): id is string => typeof id === 'string')

                    .slice(0, effectiveLimit);

                for (const recordId of relatedIds) {
                    const result = await knackRequest(
                        app,
                        apiKey,
                        `/objects/${targetObjectKey}/records/${recordId}`,
                    );

                    const record = getRecordsFromResponse(result)[0];

                    if (record)
                        records.push(
                            projectRecordFields(record, target.fields),
                        );
                }
            } else {
                if (!targetObjectKey)
                    throw new Error(
                        'relatedObjectKey is required for reverse related-record lookups.',
                    );

                const target = await getPermittedReadFields(
                    app,
                    targetObjectKey,
                    fieldKeys,
                );

                const targetField = (target.object.fields || []).find(
                    (field) => field.key === connectionFieldKey,
                );

                if (
                    !targetField ||
                    targetField.connectedObject !== sourceObjectKey
                ) {
                    throw new Error(
                        `${connectionFieldKey} must be a connection from ${targetObjectKey} to ${sourceObjectKey}.`,
                    );
                }

                await getPermittedReadFields(app, targetObjectKey, [
                    connectionFieldKey,
                ]);

                if (sortField)
                    await getPermittedReadFields(app, targetObjectKey, [
                        sortField,
                    ]);

                const params = buildRecordSearchParams({
                    page: 1,

                    rowsPerPage: effectiveLimit,

                    filters: {
                        match: 'and',
                        rules: [
                            {
                                field: connectionFieldKey,
                                operator: 'is',
                                value: sourceRecordId,
                            },
                        ],
                    },

                    sortField,

                    sortOrder,
                });

                const result = await knackRequest(
                    app,
                    apiKey,
                    `/objects/${targetObjectKey}/records?${params.toString()}`,
                );

                records = getRecordsFromResponse(result)
                    .slice(0, effectiveLimit)
                    .map((record) =>
                        projectRecordFields(record, target.fields),
                    );
            }

            return makeTextResponse({
                ok: true,

                appKey: app.appKey,

                source: {
                    objectKey: sourceObjectKey,
                    recordId: sourceRecordId,
                },

                direction,

                relatedObjectKey: targetObjectKey,

                returned: records.length,

                limit: effectiveLimit,

                records,
            });
        },
    );

    server.tool(
        'knack_aggregate_records',

        'Count or sum approved records with filters and optional grouping. Returns aggregates, never individual records.',

        {
            appKey: z.string().optional(),

            objectKey: z.string(),

            filters: z
                .union([z.string(), z.record(z.string(), z.unknown())])
                .optional(),

            groupByFieldKeys: z.array(z.string()).max(3).default([]),

            dateBucket: z
                .object({
                    fieldKey: z.string(),
                    granularity: z.enum(['day', 'month', 'year']),
                })
                .optional(),

            metrics: z
                .array(
                    z.object({
                        type: z.enum(['count', 'sum']),
                        fieldKey: z.string().optional(),
                    }),
                )
                .min(1)
                .max(10)
                .default([{ type: 'count' }]),

            maxRecords: z
                .number()
                .int()
                .min(1)
                .max(10000)
                .default(1000)
                .describe(
                    'Maximum records to scan; a capped result is clearly reported.',
                ),
        },

        async ({
            appKey,
            objectKey,
            filters,
            groupByFieldKeys,
            dateBucket,
            metrics,
            maxRecords,
        }) => {
            const app = getAppOrThrow(appKey);

            const requestedFields = [
                ...groupByFieldKeys,

                ...(dateBucket ? [dateBucket.fieldKey] : []),

                ...metrics.flatMap((metric) =>
                    metric.fieldKey ? [metric.fieldKey] : [],
                ),
            ];

            for (const metric of metrics) {
                if (metric.type === 'sum' && !metric.fieldKey)
                    throw new Error('A sum metric requires fieldKey.');
            }
            const { fields } = await getPermittedReadFields(
                app,
                objectKey,
                requestedFields,
            );
            const policyMaximum = await validateReadQuery(app, objectKey, {
                filters,
            });
            const scanLimit = Math.min(maxRecords, policyMaximum);

            const apiKey = getApiKeyOrThrow(app.appKey);

            const groups = new Map<string, Record<string, unknown>>();

            let scanned = 0;

            let page = 1;

            let hasMore = true;

            while (hasMore && scanned < scanLimit) {
                const rowsPerPage = Math.min(1000, scanLimit - scanned);

                const params = buildRecordSearchParams({
                    page,
                    rowsPerPage,
                    filters,
                });

                const result = await knackRequest(
                    app,
                    apiKey,
                    `/objects/${objectKey}/records?${params.toString()}`,
                );

                if (!result.ok)
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        status: result.status,
                        body: result.body,
                    });

                const records = getRecordsFromResponse(result);

                for (const record of records) {
                    const dimensions: Record<string, unknown> = {};

                    for (const fieldKey of groupByFieldKeys)
                        dimensions[fieldKey] = record[fieldKey] ?? null;

                    if (dateBucket)
                        dimensions[dateBucket.fieldKey] =
                            bucketDate(
                                record[dateBucket.fieldKey],
                                dateBucket.granularity,
                            ) || 'Unknown';

                    const key = JSON.stringify(dimensions);

                    const group = groups.get(key) || {
                        dimensions,
                        metrics: {},
                    };

                    const values = group.metrics as Record<string, number>;

                    for (const metric of metrics) {
                        const metricKey =
                            metric.type === 'count'
                                ? 'count'
                                : `sum:${metric.fieldKey}`;

                        if (metric.type === 'count')
                            values[metricKey] = (values[metricKey] || 0) + 1;
                        else {
                            const numeric = getNumericValue(
                                record[metric.fieldKey!],
                            );

                            if (numeric !== null)
                                values[metricKey] =
                                    (values[metricKey] || 0) + numeric;
                        }
                    }

                    groups.set(key, group);
                }

                scanned += records.length;

                hasMore = records.length === rowsPerPage;

                page += 1;
            }

            const capped = scanned >= scanLimit && hasMore;

            return makeTextResponse({
                ok: true,

                appKey: app.appKey,

                objectKey,

                scanned,

                capped,

                scanLimit,

                fields,

                groups: [...groups.values()],

                ...(capped
                    ? {
                          warning: `Only the first ${scanned} matching record(s) were scanned (scanLimit: ${scanLimit}); more records exist. These counts/sums are PARTIAL, not the true total — raise maxRecords or narrow filters before treating them as final.`,
                      }
                    : {}),
            });
        },
    );

    if (HAS_DIAGNOSTIC_TOOLS) {
        server.tool(
            'knack_get_raw_object_metadata',
            'Return the raw runtime metadata object payload for a Knack object before schema normalization. Useful for diagnosing fields that may not survive parser transforms.',
            {
                appKey: z.string().optional(),
                objectKey: z.string(),
            },
            async ({ appKey, objectKey }) => {
                const app = getAppOrThrow(appKey);
                assertDiagnosticAccess(app);
                debugLog('tool_call', {
                    tool: 'knack_get_raw_object_metadata',
                    args: { appKey, objectKey },
                });

                const runtimeMetadata = await getRuntimeMetadata(app);
                if (!runtimeMetadata) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        message:
                            'No runtime metadata available from Knack application metadata endpoint.',
                    });
                }

                const directObjects = getObjectAtPath(
                    runtimeMetadata,
                    'objects',
                );
                const nestedObjects = getObjectAtPath(
                    runtimeMetadata,
                    'application',
                    'objects',
                );
                const objectsRaw = Array.isArray(directObjects)
                    ? directObjects
                    : Array.isArray(nestedObjects)
                      ? nestedObjects
                      : null;

                if (!objectsRaw) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        message:
                            'Runtime metadata did not contain an objects array.',
                    });
                }

                const rawObject = objectsRaw.find((entry) => {
                    const obj = asRecord(entry);
                    return obj && obj.key === objectKey;
                });

                if (!rawObject) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        message: `Object not found in runtime metadata: ${objectKey}`,
                        availableObjectKeys: objectsRaw
                            .map((entry) => {
                                const obj = asRecord(entry);
                                return typeof obj?.key === 'string'
                                    ? obj.key
                                    : null;
                            })
                            .filter((key): key is string => Boolean(key)),
                    });
                }

                const rawObjectDetail = getInlineDetail(rawObject);

                return makeTextResponse({
                    ok: true,
                    appKey: app.appKey,
                    source: 'runtime',
                    objectKey,
                    rawObjectIncluded: rawObjectDetail.included,
                    rawObjectSizeBytes: rawObjectDetail.sizeBytes,
                    rawObject: rawObjectDetail.value,
                    rawObjectSummary: rawObjectDetail.summary,
                });
            },
        );
    }

    // -----------------------
    // Tools: schema helpers (local, fast)
    // -----------------------

    server.tool(
        'knack_get_object_fields',
        'Return fields for an object from the cached schema.json (recommended) for the selected app, including descriptions when available.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_get_object_fields',
                args: { appKey, objectKey },
            });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const obj = schema.objects.find((o) => o.key === objectKey);
            if (!obj) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: `Object not found in schema.json: ${objectKey}`,
                });
            }

            const runtimeMetadata = await getRuntimeMetadata(app);

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                objectKey,
                objectName: obj.name,
                fields: (obj.fields || []).map((f) => ({
                    key: f.key,
                    name: f.name,
                    type: f.type,
                    required: f.required,
                    description: f.description,
                    builderUrl: makeFieldBuilderUrl(
                        app,
                        { objectKey: obj.key, fieldKey: f.key },
                        runtimeMetadata,
                    ),
                })),
            });
        },
    );

    server.tool(
        'knack_resolve_field_alias',
        'Resolve a friendly alias (from fieldMap.json) to a Knack field key (e.g. field_123).',
        {
            appKey: z.string().optional(),
            alias: z.string(),
        },
        async ({ appKey, alias }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_resolve_field_alias',
                args: { appKey, alias },
            });
            const { fieldMap, source } = await getFieldMapForApp(app);

            if (!fieldMap) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No field map available from runtime API or fieldMap.json.',
                });
            }

            const fieldKey = resolveAliasToFieldKey(fieldMap, alias);
            if (!fieldKey) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: `Alias not found in fieldMap.json: ${alias}`,
                    availableAliases: Object.keys(fieldMap),
                });
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                alias,
                fieldKey,
                ...(source === 'file'
                    ? {
                          note: 'Resolved from the on-disk fieldMap.json cache. If a field was created, renamed, or deleted recently, this map may be stale until knack_refresh_cache is run with warm: true, persistFiles: true.',
                      }
                    : {}),
            });
        },
    );

    server.tool(
        'knack_get_object',
        'Return a Knack object definition (object metadata + fields) from cached schema data, including field descriptions when available.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_get_object',
                args: { appKey, objectKey },
            });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const obj = schema.objects.find((entry) => entry.key === objectKey);
            if (!obj) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    source,
                    message: `Object not found in schema: ${objectKey}`,
                    availableObjectKeys: schema.objects.map(
                        (entry) => entry.key,
                    ),
                });
            }

            const runtimeMetadata = await getRuntimeMetadata(app);

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                object: {
                    key: obj.key,
                    name: obj.name,
                    fieldCount: (obj.fields || []).length,
                    fields: (obj.fields || []).map((field) => ({
                        key: field.key,
                        name: field.name,
                        type: field.type,
                        required: field.required,
                        description: field.description,
                        builderUrl: makeFieldBuilderUrl(
                            app,
                            { objectKey: obj.key, fieldKey: field.key },
                            runtimeMetadata,
                        ),
                    })),
                },
            });
        },
    );

    server.tool(
        'knack_list_fields',
        'List all fields for a Knack object (field key, name, type, description when available).',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_list_fields',
                args: { appKey, objectKey },
            });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const obj = schema.objects.find((entry) => entry.key === objectKey);
            if (!obj) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    source,
                    message: `Object not found in schema: ${objectKey}`,
                });
            }

            const runtimeMetadata = await getRuntimeMetadata(app);

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                objectKey: obj.key,
                objectName: obj.name,
                fields: (obj.fields || []).map((field) => ({
                    key: field.key,
                    name: field.name,
                    type: field.type,
                    required: field.required,
                    description: field.description,
                    builderUrl: makeFieldBuilderUrl(
                        app,
                        { objectKey: obj.key, fieldKey: field.key },
                        runtimeMetadata,
                    ),
                })),
            });
        },
    );

    server.tool(
        'knack_get_field',
        'Return the complete, unprojected definition for a single field, including format (equation strings, connection/sum/count settings) and conditional rules — properties that knack_list_fields, knack_get_object_fields, and knack_get_object omit. Reads the object directly from the Knack API, so it requires an API key for the app.',
        {
            appKey: z.string().optional(),
            objectKey: z.string().describe('The object key, e.g. object_2'),
            fieldKey: z.string().describe('The field key, e.g. field_123'),
        },
        async ({ appKey, objectKey, fieldKey }) => {
            const app = getAppOrThrow(appKey);
            const apiKey = getApiKeyOrThrow(app.appKey);
            debugLog('tool_call', {
                tool: 'knack_get_field',
                args: { appKey: app.appKey, objectKey, fieldKey },
            });

            const result = (await knackRequest(
                app,
                apiKey,
                `/objects/${objectKey}`,
            )) as {
                ok: boolean;
                status: number;
                body?: { object?: { fields?: Array<Record<string, unknown>> } };
            };

            if (!result.ok) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    objectKey,
                    fieldKey,
                    action: 'get_field',
                    status: result.status,
                    message: `Could not fetch object ${objectKey} from the Knack API.`,
                });
            }

            const fields = result.body?.object?.fields || [];
            const field = fields.find((entry) => entry.key === fieldKey);
            if (!field) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    objectKey,
                    fieldKey,
                    action: 'get_field',
                    message: `Field ${fieldKey} not found on ${objectKey}.`,
                    availableFieldKeys: fields
                        .map((entry) =>
                            typeof entry.key === 'string' ? entry.key : null,
                        )
                        .filter((key): key is string => Boolean(key)),
                });
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                objectKey,
                fieldKey,
                action: 'get_field',
                field,
            });
        },
    );

    server.tool(
        'validateFieldMapping',
        'Validate a mapping object by resolving aliases/field keys and checking field existence.',
        {
            appKey: z.string().optional(),
            mappingObject: z.record(z.string(), z.string()),
        },
        async ({ appKey, mappingObject }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'validateFieldMapping',
                args: {
                    appKey,
                    mappingSize: Object.keys(mappingObject).length,
                },
            });

            const schemaResult = await getSchemaForApp(app);
            const fieldMapResult = await getFieldMapForApp(app);
            const schema = schemaResult.schema;
            const fieldMap = fieldMapResult.fieldMap || {};

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const validFieldKeys = new Set(
                schema.objects
                    .flatMap((obj) =>
                        (obj.fields || []).map((field) => field.key),
                    )
                    .filter((key): key is string => Boolean(key)),
            );

            const resolvedMapping: Record<string, string> = {};
            const invalidMappings: Array<{
                mappingKey: string;
                input: string;
                reason: string;
            }> = [];

            for (const [mappingKey, value] of Object.entries(mappingObject)) {
                const directFieldKey = /^field_\d+$/i.test(value)
                    ? value
                    : null;
                const resolvedFieldKey =
                    directFieldKey ||
                    resolveAliasToFieldKey(fieldMap, value) ||
                    null;

                if (!resolvedFieldKey) {
                    invalidMappings.push({
                        mappingKey,
                        input: value,
                        reason: 'Not a field key and alias was not found in fieldMap.',
                    });
                    continue;
                }

                if (!validFieldKeys.has(resolvedFieldKey)) {
                    invalidMappings.push({
                        mappingKey,
                        input: value,
                        reason: `Resolved to ${resolvedFieldKey}, but that field does not exist in schema.`,
                    });
                    continue;
                }

                resolvedMapping[mappingKey] = resolvedFieldKey;
            }

            const resolvedEntries = Object.entries(resolvedMapping);
            const usageByField = new Map<string, string[]>();
            for (const [mappingKey, fieldKey] of resolvedEntries) {
                usageByField.set(fieldKey, [
                    ...(usageByField.get(fieldKey) || []),
                    mappingKey,
                ]);
            }

            const duplicateResolvedFields = [...usageByField.entries()]
                .filter(([, mappingKeys]) => mappingKeys.length > 1)
                .map(([fieldKey, mappingKeys]) => ({ fieldKey, mappingKeys }));

            return makeTextResponse({
                ok: invalidMappings.length === 0,
                appKey: app.appKey,
                schemaSource: schemaResult.source,
                fieldMapSource: fieldMapResult.source,
                totalMappings: Object.keys(mappingObject).length,
                validMappings: resolvedEntries.length,
                invalidMappings,
                duplicateResolvedFields,
                resolvedMapping,
            });
        },
    );

    server.tool(
        'generateSnapshotStructure',
        'Generate a snapshot object structure for a Knack object using schema fields.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'generateSnapshotStructure',
                args: { appKey, objectKey },
            });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const obj = schema.objects.find((entry) => entry.key === objectKey);
            if (!obj) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    source,
                    message: `Object not found in schema: ${objectKey}`,
                });
            }

            const snapshotByFieldKey: Record<string, null> = {};
            const snapshotByFieldName: Record<string, null> = {};

            for (const field of obj.fields || []) {
                snapshotByFieldKey[field.key] = null;
                if (field.name) {
                    snapshotByFieldName[field.name] = null;
                }
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                objectKey: obj.key,
                objectName: obj.name,
                fieldCount: (obj.fields || []).length,
                snapshotByFieldKey,
                snapshotByFieldName,
            });
        },
    );

    server.tool(
        'checkForDuplicateFieldUsage',
        'Check duplicate field usage in fieldMap aliases and optionally in a provided mappingObject.',
        {
            appKey: z.string().optional(),
            mappingObject: z.record(z.string(), z.string()).optional(),
        },
        async ({ appKey, mappingObject }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'checkForDuplicateFieldUsage',
                args: {
                    appKey,
                    mappingSize: mappingObject
                        ? Object.keys(mappingObject).length
                        : 0,
                },
            });

            const schemaResult = await getSchemaForApp(app);
            const fieldMapResult = await getFieldMapForApp(app);
            const schema = schemaResult.schema;
            const fieldMap = fieldMapResult.fieldMap || {};

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const validFieldKeys = new Set(
                schema.objects
                    .flatMap((obj) =>
                        (obj.fields || []).map((field) => field.key),
                    )
                    .filter((key): key is string => Boolean(key)),
            );

            const aliasUsageByField = new Map<string, string[]>();
            for (const [alias, entry] of Object.entries(fieldMap)) {
                const fieldKey = entry.fieldKey;
                if (!validFieldKeys.has(fieldKey)) continue;
                aliasUsageByField.set(fieldKey, [
                    ...(aliasUsageByField.get(fieldKey) || []),
                    alias,
                ]);
            }

            const fieldMapDuplicates = [...aliasUsageByField.entries()]
                .filter(([, aliases]) => aliases.length > 1)
                .map(([fieldKey, aliases]) => ({ fieldKey, aliases }));

            let mappingDuplicates: Array<{
                fieldKey: string;
                mappingKeys: string[];
            }> = [];
            const mappingInvalidEntries: Array<{
                mappingKey: string;
                input: string;
                reason: string;
            }> = [];

            if (mappingObject) {
                const mappingUsageByField = new Map<string, string[]>();

                for (const [mappingKey, value] of Object.entries(
                    mappingObject,
                )) {
                    const directFieldKey = /^field_\d+$/i.test(value)
                        ? value
                        : null;
                    const resolvedFieldKey =
                        directFieldKey ||
                        resolveAliasToFieldKey(fieldMap, value) ||
                        null;

                    if (!resolvedFieldKey) {
                        mappingInvalidEntries.push({
                            mappingKey,
                            input: value,
                            reason: 'Not a field key and alias was not found in fieldMap.',
                        });
                        continue;
                    }

                    if (!validFieldKeys.has(resolvedFieldKey)) {
                        mappingInvalidEntries.push({
                            mappingKey,
                            input: value,
                            reason: `Resolved to ${resolvedFieldKey}, but that field does not exist in schema.`,
                        });
                        continue;
                    }

                    mappingUsageByField.set(resolvedFieldKey, [
                        ...(mappingUsageByField.get(resolvedFieldKey) || []),
                        mappingKey,
                    ]);
                }

                mappingDuplicates = [...mappingUsageByField.entries()]
                    .filter(([, mappingKeys]) => mappingKeys.length > 1)
                    .map(([fieldKey, mappingKeys]) => ({
                        fieldKey,
                        mappingKeys,
                    }));
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                schemaSource: schemaResult.source,
                fieldMapSource: fieldMapResult.source,
                fieldMapDuplicateCount: fieldMapDuplicates.length,
                fieldMapDuplicates,
                mappingProvided: Boolean(mappingObject),
                mappingDuplicateCount: mappingDuplicates.length,
                mappingDuplicates,
                mappingInvalidEntries,
            });
        },
    );

    server.tool(
        'knack_get_field_type',
        'Return the field type for a field key or alias from schema data.',
        {
            appKey: z.string().optional(),
            fieldKey: z
                .string()
                .optional()
                .describe('Knack field key, e.g. field_1234'),
            alias: z
                .string()
                .optional()
                .describe('Alias from fieldMap.json, e.g. object_2.name'),
            objectKey: z
                .string()
                .optional()
                .describe(
                    'Optional object key to scope the lookup, e.g. object_2',
                ),
        },
        async ({ appKey, fieldKey, alias, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_get_field_type',
                args: { appKey, fieldKey, alias, objectKey },
            });

            if (!fieldKey && !alias) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'Provide either fieldKey or alias.',
                });
            }

            let resolvedFieldKey = fieldKey || null;
            let fieldMapSource: CacheSource | null = null;

            if (!resolvedFieldKey && alias) {
                const fieldMapResult = await getFieldMapForApp(app);
                fieldMapSource = fieldMapResult.source;
                const fieldMap = fieldMapResult.fieldMap;

                if (!fieldMap) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        message:
                            'No field map available from runtime API or fieldMap.json; cannot resolve alias.',
                    });
                }

                resolvedFieldKey = resolveAliasToFieldKey(fieldMap, alias);
                if (!resolvedFieldKey) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        fieldMapSource,
                        message: `Alias not found in fieldMap.json: ${alias}`,
                    });
                }
            }

            const schemaResult = await getSchemaForApp(app);
            const schema = schemaResult.schema;
            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const matches: Array<{
                objectKey: string;
                objectName?: string;
                fieldKey: string;
                fieldName?: string;
                fieldType?: string;
                builderUrl: string | null;
            }> = [];

            const runtimeMetadata = await getRuntimeMetadata(app);

            for (const obj of schema.objects) {
                if (objectKey && obj.key !== objectKey) continue;
                for (const field of obj.fields || []) {
                    if (field.key !== resolvedFieldKey) continue;
                    matches.push({
                        objectKey: obj.key,
                        objectName: obj.name,
                        fieldKey: field.key,
                        fieldName: field.name,
                        fieldType: field.type,
                        builderUrl: makeFieldBuilderUrl(
                            app,
                            { objectKey: obj.key, fieldKey: field.key },
                            runtimeMetadata,
                        ),
                    });
                }
            }

            if (!matches.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    schemaSource: schemaResult.source,
                    fieldMapSource,
                    message: objectKey
                        ? `Field not found in schema for object ${objectKey}: ${resolvedFieldKey}`
                        : `Field not found in schema: ${resolvedFieldKey}`,
                });
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                schemaSource: schemaResult.source,
                fieldMapSource,
                input: {
                    fieldKey: fieldKey || null,
                    alias: alias || null,
                    objectKey: objectKey || null,
                },
                resolvedFieldKey,
                matchCount: matches.length,
                matches,
            });
        },
    );

    server.tool(
        'knack_resolve_any',
        'Resolve an identifier (field key or alias) to field key + name + type + object key.',
        {
            appKey: z.string().optional(),
            identifier: z.string(),
            objectKey: z
                .string()
                .optional()
                .describe('Optional object key to narrow lookup.'),
        },
        async ({ appKey, identifier, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_resolve_any',
                args: { appKey, identifier, objectKey },
            });

            const schemaResult = await getSchemaForApp(app);
            const schema = schemaResult.schema;
            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const trimmed = identifier.trim();
            if (!trimmed) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'identifier cannot be empty.',
                });
            }

            let resolvedFieldKey: string | null;
            let resolvedBy: 'fieldKey' | 'alias';
            let fieldMapSource: CacheSource | null = null;

            if (/^field_\d+$/i.test(trimmed)) {
                resolvedFieldKey = trimmed;
                resolvedBy = 'fieldKey';
            } else {
                const fieldMapResult = await getFieldMapForApp(app);
                fieldMapSource = fieldMapResult.source;
                const fieldMap = fieldMapResult.fieldMap;

                if (!fieldMap) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        schemaSource: schemaResult.source,
                        message:
                            'No field map available from runtime API or fieldMap.json; cannot resolve alias identifier.',
                    });
                }

                resolvedFieldKey = resolveAliasToFieldKey(fieldMap, trimmed);
                if (!resolvedFieldKey) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        schemaSource: schemaResult.source,
                        fieldMapSource,
                        identifier: trimmed,
                        message: 'Identifier not found as alias or field key.',
                    });
                }
                resolvedBy = 'alias';
            }

            const matches: Array<{
                objectKey: string;
                objectName?: string;
                fieldKey: string;
                fieldName?: string;
                fieldType?: string;
                builderUrl: string | null;
            }> = [];

            const runtimeMetadata = await getRuntimeMetadata(app);

            for (const obj of schema.objects) {
                if (objectKey && obj.key !== objectKey) continue;
                for (const field of obj.fields || []) {
                    if (field.key !== resolvedFieldKey) continue;
                    matches.push({
                        objectKey: obj.key,
                        objectName: obj.name,
                        fieldKey: field.key,
                        fieldName: field.name,
                        fieldType: field.type,
                        builderUrl: makeFieldBuilderUrl(
                            app,
                            { objectKey: obj.key, fieldKey: field.key },
                            runtimeMetadata,
                        ),
                    });
                }
            }

            if (!matches.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    schemaSource: schemaResult.source,
                    fieldMapSource,
                    resolvedFieldKey,
                    message: objectKey
                        ? `Resolved field not found in schema for object ${objectKey}: ${resolvedFieldKey}`
                        : `Resolved field not found in schema: ${resolvedFieldKey}`,
                });
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                schemaSource: schemaResult.source,
                fieldMapSource,
                identifier: trimmed,
                resolvedBy,
                resolvedFieldKey,
                matchCount: matches.length,
                matches,
                primary: matches[0],
            });
        },
    );

    server.tool(
        'knack_list_field_types',
        'List field keys, names, and types for a Knack object, plus a grouped type summary.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_list_field_types',
                args: { appKey, objectKey },
            });

            const schemaResult = await getSchemaForApp(app);
            const schema = schemaResult.schema;
            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const obj = schema.objects.find((entry) => entry.key === objectKey);
            if (!obj) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    schemaSource: schemaResult.source,
                    message: `Object not found in schema: ${objectKey}`,
                    availableObjectKeys: schema.objects.map(
                        (entry) => entry.key,
                    ),
                });
            }

            const fields = (obj.fields || []).map((field) => ({
                fieldKey: field.key,
                fieldName: field.name,
                fieldType: field.type || null,
            }));

            const typeCounts = new Map<string, number>();
            fields.forEach((field) => {
                const typeKey = field.fieldType || 'unknown';
                typeCounts.set(typeKey, (typeCounts.get(typeKey) || 0) + 1);
            });

            const typeSummary = [...typeCounts.entries()]
                .map(([fieldType, count]) => ({ fieldType, count }))
                .sort(
                    (a, b) =>
                        b.count - a.count ||
                        a.fieldType.localeCompare(b.fieldType),
                );

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                schemaSource: schemaResult.source,
                objectKey: obj.key,
                objectName: obj.name,
                fieldCount: fields.length,
                typeSummary,
                fields,
            });
        },
    );

    server.tool(
        'knack_get_view_context',
        'Return scene context for a view key (sceneKey, sceneName, sceneSlug).',
        {
            appKey: z.string().optional(),
            viewKey: z.string(),
        },
        async ({ appKey, viewKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_get_view_context',
                args: { appKey, viewKey },
            });

            const contextMap = await getViewContextMapForApp(app);
            const context = contextMap[viewKey];

            if (!context) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: `View context not found for view key: ${viewKey}`,
                    availableViewKeyCount: Object.keys(contextMap).length,
                });
            }

            const viewMapResult = await getViewMapForApp(app);
            const viewType =
                typeof viewMapResult.viewMap?.[viewKey]?.type === 'string'
                    ? (viewMapResult.viewMap[viewKey].type as string)
                    : undefined;

            const builderUrls = await getBuilderLinksForApp(app, {
                sceneKey: context.sceneKey,
                viewKey,
                viewType,
            });

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                viewKey,
                context,
                builderUrls,
            });
        },
    );

    server.tool(
        'knack_plan_view_repoint',
        'List every reference a view holds, split into the two edits that get called repointing. Read-only — it reports and changes nothing. A RESCOPE (connection_key, parent_source, authenticated_user, or the filter criteria) touches the scope references only: display connections, fields, filters and sorts stay valid, because the object the view lists has not changed, and rewriting them alongside a rescope is a bug rather than diligence. A RETARGET (source.object) invalidates everything, since every reference then names a field on the old object — and this server refuses one outright. Use this before copying a view and rescoping it, to see which references are which.',
        {
            appKey: z.string().optional(),
            viewKey: z.string(),
            includeScopedFields: z
                .boolean()
                .optional()
                .default(false)
                .describe(
                    'Include every field named in a filter, rule, sort or value block. There are usually many and they are rarely all relevant; the connection list is the actionable part.',
                ),
        },
        async ({ appKey, viewKey, includeScopedFields }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_plan_view_repoint',
                args: { appKey: app.appKey, viewKey },
            });

            const { viewMap, source } = await getViewMapForApp(app);

            if (!viewMap) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No view map available from runtime API or viewMap.json.',
                });
            }

            const attributes = asRecord(viewMap[viewKey]);
            if (!attributes) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    viewKey,
                    source,
                    message: `View not found in view metadata: ${viewKey}`,
                });
            }

            const plan = planViewRepoint(attributes);
            const sourceObject = asRecord(attributes.source)?.object;

            const notes = [
                'Two different edits get called "repointing", and they invalidate different things. Read the two lists below accordingly.',
                `RESCOPE (change connection_key / parent_source / authenticated_user): ${plan.scopeConnections.length} reference(s), field(s) ${plan.distinctScopeKeys.join(', ') || 'none'}. Change these and nothing else — a rescope leaves display connections, fields, filters and sorts valid, because the object the view lists has not changed.`,
                `RETARGET (change source.object, currently ${typeof sourceObject === 'string' ? sourceObject : 'unknown'}): everything below is then suspect, because every field, display connection, filter, sort and rule names a field on the old object.`,
            ];

            if (plan.displayConnections.length > 0) {
                notes.push(
                    `${plan.displayConnections.length} DISPLAY connection(s), field(s) ${plan.distinctDisplayKeys.join(', ') || 'none'}: these read a shown value from a connected record, out from this view's own object. Measured on 2026-09-04 — a builder rescope that added connection_key, relationship_type, authenticated_user and parent_source left every one of them untouched, and they were already set while the source had no connection at all. So do NOT rewrite them for a rescope. Revisit them only on a retarget.`,
                );
            }

            if (plan.navigation.length > 0) {
                notes.push(
                    `${plan.navigation.length} navigation reference(s), listed separately: the cascade guard's concern rather than a repoint's. Note that copying a view appears to duplicate the child pages it owns and point the copy at the duplicates — see P1/P5 in TESTING.md — so a copy's links may not name the same pages the original's did.`,
                );
            }

            const embedded = plan.other.filter((reference) =>
                reference.path.endsWith('(embedded)'),
            );
            if (embedded.length > 0) {
                notes.push(
                    `${embedded.length} key(s) are embedded in prose (a description's KTL directives). Nothing else in this server reads those, so a copy carries them verbatim and they keep naming the original's fields and views.`,
                );
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                viewKey,
                source,
                viewType:
                    typeof attributes.type === 'string'
                        ? attributes.type
                        : null,
                sourceObject:
                    typeof sourceObject === 'string' ? sourceObject : null,
                scopeConnections: plan.scopeConnections,
                distinctScopeKeys: plan.distinctScopeKeys,
                displayConnections: plan.displayConnections,
                distinctDisplayKeys: plan.distinctDisplayKeys,
                navigation: plan.navigation,
                other: plan.other,
                scopedFieldCount: plan.scopedFields.length,
                scopedFields: includeScopedFields
                    ? plan.scopedFields
                    : undefined,
                notes,
            });
        },
    );

    if (HAS_DIAGNOSTIC_TOOLS) {
        server.tool(
            'knack_get_view_attributes',
            'Return attributes for a view key from runtime metadata or cached viewMap.json. Returns fieldSettings (a compact per-field summary) by default; pass includeRawAttributes: true for the full raw view JSON as well.',
            {
                appKey: z.string().optional(),
                viewKey: z.string(),
                includeRawAttributes: z
                    .boolean()
                    .optional()
                    .default(false)
                    .describe(
                        'Include the full raw view attributes payload alongside fieldSettings. Off by default: fieldSettings already covers per-field key/type/label/rules/defaults in a much smaller payload. Turn this on only when you need the raw view JSON itself (e.g. layout/pageGroups/rules structure not covered by fieldSettings).',
                    ),
            },
            async ({ appKey, viewKey, includeRawAttributes }) => {
                const app = getAppOrThrow(appKey);
                assertDiagnosticAccess(app);
                debugLog('tool_call', {
                    tool: 'knack_get_view_attributes',
                    args: { appKey, viewKey, includeRawAttributes },
                });
                const { viewMap, source } = await getViewMapForApp(app);

                if (!viewMap) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        message:
                            'No view map available from runtime API or viewMap.json.',
                    });
                }

                const attributes = viewMap[viewKey];
                if (!attributes) {
                    const allViewKeys = Object.keys(viewMap);
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        source,
                        message: `View not found in viewMap.json: ${viewKey}`,
                        availableViewKeyCount: allViewKeys.length,
                        availableViewKeySample: allViewKeys.slice(0, 200),
                    });
                }

                const schemaResult = await getSchemaForApp(app);
                const viewContextMap = await getViewContextMapForApp(app);
                const context = viewContextMap[viewKey] || {};

                const builderUrls = await getBuilderLinksForApp(app, {
                    sceneKey: context.sceneKey,
                    viewKey,
                    viewType:
                        typeof attributes.type === 'string'
                            ? attributes.type
                            : undefined,
                });

                const fieldSettings = getViewFieldSettings(
                    attributes,
                    getViewObjectFields(attributes, schemaResult.schema),
                );

                if (!includeRawAttributes) {
                    return makeTextResponse({
                        ok: true,
                        appKey: app.appKey,
                        source,
                        schemaSource: schemaResult.source,
                        viewKey,
                        fieldSettings,
                        builderUrls,
                        note: 'Pass includeRawAttributes: true for the full raw view JSON (layout, pageGroups, rules) — fieldSettings above already covers per-field key/type/label/rules/defaults.',
                    });
                }

                const attributeDetail = getInlineDetail(attributes);

                return makeTextResponse({
                    ok: true,
                    appKey: app.appKey,
                    source,
                    schemaSource: schemaResult.source,
                    viewKey,
                    attributesIncluded: attributeDetail.included,
                    attributesSizeBytes: attributeDetail.sizeBytes,
                    attributes: attributeDetail.value,
                    attributeSummary: attributeDetail.summary,
                    fieldSettings,
                    builderUrls,
                });
            },
        );
    }

    server.tool(
        'knack_list_view_fields',
        'List configured fields for a view, including required, defaults, read-only settings, and stored rules.',
        {
            appKey: z.string().optional(),
            viewKey: z.string(),
        },
        async ({ appKey, viewKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_list_view_fields',
                args: { appKey: app.appKey, viewKey },
            });

            const { viewMap, source } = await getViewMapForApp(app);
            if (!viewMap) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No view map available from runtime API or viewMap.json.',
                });
            }

            const attributes = viewMap[viewKey];
            if (!attributes) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    source,
                    message: `View not found in viewMap.json: ${viewKey}`,
                    availableViewKeyCount: Object.keys(viewMap).length,
                    availableViewKeySample: Object.keys(viewMap).slice(0, 200),
                });
            }

            const schemaResult = await getSchemaForApp(app);

            const fieldSettings = getViewFieldSettings(
                attributes,
                getViewObjectFields(attributes, schemaResult.schema),
            );

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                schemaSource: schemaResult.source,
                viewKey,
                viewName:
                    typeof attributes.name === 'string'
                        ? attributes.name
                        : null,
                viewType:
                    typeof attributes.type === 'string'
                        ? attributes.type
                        : null,
                fieldSettings,
            });
        },
    );

    server.tool(
        'knack_find_views_with_record_rule_field',
        'Find all views whose record-rule-related metadata references a specific field id.',
        {
            appKey: z.string().optional(),
            fieldKey: z.string().regex(/^field_\d+$/i),
            maxResults: z.number().int().min(1).max(5000).default(100),
        },
        async ({ appKey, fieldKey, maxResults }) => {
            const app = getAppOrThrow(appKey);
            const normalisedFieldKey = fieldKey.toLowerCase();
            debugLog('tool_call', {
                tool: 'knack_find_views_with_record_rule_field',
                args: { appKey, fieldKey: normalisedFieldKey, maxResults },
            });

            const fieldReferenceResult =
                await getFieldReferenceIndexForApp(app);
            const references =
                fieldReferenceResult.index?.[normalisedFieldKey] || [];
            const recordRuleRefs = references
                .filter(
                    (reference) =>
                        reference.viewKey &&
                        reference.classification.includes('viewRecordRule'),
                )
                .slice(0, maxResults);

            const viewsByKey = new Map<
                string,
                {
                    viewKey: string;
                    viewName?: string;
                    viewType?: string;
                    sceneKey?: string;
                    sceneName?: string;
                    sceneSlug?: string;
                    matchedPaths: string[];
                    matches: FieldReference[];
                }
            >();

            for (const reference of recordRuleRefs) {
                if (!reference.viewKey) continue;
                const existing = viewsByKey.get(reference.viewKey) || {
                    viewKey: reference.viewKey,
                    viewName: reference.viewName,
                    viewType: reference.viewType,
                    sceneKey: reference.sceneKey,
                    sceneName: reference.sceneName,
                    sceneSlug: reference.sceneSlug,
                    matchedPaths: [],
                    matches: [],
                };

                existing.matchedPaths.push(reference.path);
                existing.matches.push(reference);
                viewsByKey.set(reference.viewKey, existing);
            }

            const runtimeMetadata = await getRuntimeMetadata(app);
            const fieldOwner = await findFieldOwnerForApp(
                app,
                normalisedFieldKey,
            );

            const results = [...viewsByKey.values()].map((entry) => ({
                ...entry,
                matchedPaths: [...new Set(entry.matchedPaths)].sort(
                    (left, right) => left.localeCompare(right),
                ),
                matchCount: entry.matches.length,
                builderUrls: {
                    scene: makeSceneBuilderUrl(
                        app,
                        entry.sceneKey,
                        runtimeMetadata,
                    ),
                    view: makeViewBuilderUrl(
                        app,
                        {
                            sceneKey: entry.sceneKey,
                            viewKey: entry.viewKey,
                            viewType: entry.viewType,
                        },
                        runtimeMetadata,
                    ),
                },
            }));

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source: fieldReferenceResult.source,
                fieldKey: normalisedFieldKey,
                builderUrls: {
                    field: makeFieldBuilderUrl(
                        app,
                        {
                            objectKey: fieldOwner?.objectKey,
                            fieldKey: normalisedFieldKey,
                        },
                        runtimeMetadata,
                    ),
                },
                totalMatches: recordRuleRefs.length,
                totalViews: results.length,
                results,
            });
        },
    );

    server.tool(
        'knack_list_field_references',
        'List all cached schema, alias, and view references for a specific field id.',
        {
            appKey: z.string().optional(),
            fieldKey: z.string().regex(/^field_\d+$/i),
            maxResults: z.number().int().min(1).max(10000).default(200),
        },
        async ({ appKey, fieldKey, maxResults }) => {
            const app = getAppOrThrow(appKey);
            const normalisedFieldKey = fieldKey.toLowerCase();
            debugLog('tool_call', {
                tool: 'knack_list_field_references',
                args: { appKey, fieldKey: normalisedFieldKey, maxResults },
            });

            const fieldReferenceResult =
                await getFieldReferenceIndexForApp(app);
            const references = (
                fieldReferenceResult.index?.[normalisedFieldKey] || []
            ).slice(0, maxResults);
            const runtimeMetadata = await getRuntimeMetadata(app);
            const fieldOwner = await findFieldOwnerForApp(
                app,
                normalisedFieldKey,
            );

            const countsBySource = new Map<string, number>();
            const countsByClassification = new Map<string, number>();

            for (const reference of references) {
                countsBySource.set(
                    reference.sourceType,
                    (countsBySource.get(reference.sourceType) || 0) + 1,
                );
                for (const classification of reference.classification) {
                    countsByClassification.set(
                        classification,
                        (countsByClassification.get(classification) || 0) + 1,
                    );
                }
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source: fieldReferenceResult.source,
                fieldKey: normalisedFieldKey,
                builderUrls: {
                    field: makeFieldBuilderUrl(
                        app,
                        {
                            objectKey: fieldOwner?.objectKey,
                            fieldKey: normalisedFieldKey,
                        },
                        runtimeMetadata,
                    ),
                },
                totalReferences:
                    fieldReferenceResult.index?.[normalisedFieldKey]?.length ||
                    0,
                returnedReferences: references.length,
                countsBySource: [...countsBySource.entries()]
                    .map(([sourceType, count]) => ({ sourceType, count }))
                    .sort(
                        (left, right) =>
                            right.count - left.count ||
                            left.sourceType.localeCompare(right.sourceType),
                    ),
                countsByClassification: [...countsByClassification.entries()]
                    .map(([classification, count]) => ({
                        classification,
                        count,
                    }))
                    .sort(
                        (left, right) =>
                            right.count - left.count ||
                            left.classification.localeCompare(
                                right.classification,
                            ),
                    ),
                references: references.map((reference) => ({
                    ...reference,
                    builderUrls: {
                        scene: makeSceneBuilderUrl(
                            app,
                            reference.sceneKey,
                            runtimeMetadata,
                        ),
                        view: makeViewBuilderUrl(
                            app,
                            {
                                sceneKey: reference.sceneKey,
                                viewKey: reference.viewKey,
                                viewType: reference.viewType,
                            },
                            runtimeMetadata,
                        ),
                        field: makeFieldBuilderUrl(
                            app,
                            {
                                objectKey: reference.objectKey,
                                fieldKey: reference.fieldKey,
                            },
                            runtimeMetadata,
                        ),
                    },
                })),
            });
        },
    );

    server.tool(
        'knack_search_ktl_keywords',
        'Search KTL-style underscore keywords in view title/description across the selected app.',
        {
            appKey: z.string().optional(),
            keyword: z
                .string()
                .optional()
                .describe('Optional keyword filter (e.g. _sth).'),
            maxResults: z.number().int().min(1).max(5000).default(100),
        },
        async ({ appKey, keyword, maxResults }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_search_ktl_keywords',
                args: { appKey, keyword, maxResults },
            });

            const { viewMap, source } = await getViewMapForApp(app);
            if (!viewMap) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No view map available from runtime API or viewMap.json.',
                });
            }

            const keywordFilter = keyword ? keyword.trim().toLowerCase() : null;
            const viewContextMap = await getViewContextMapForApp(app);
            const matches: Array<Record<string, unknown>> = [];
            const keywordCounts = new Map<string, number>();

            for (const [viewKey, viewAttrs] of Object.entries(viewMap)) {
                const title =
                    typeof viewAttrs.title === 'string' ? viewAttrs.title : '';
                const description =
                    typeof viewAttrs.description === 'string'
                        ? viewAttrs.description
                        : '';
                const viewName =
                    typeof viewAttrs.name === 'string'
                        ? viewAttrs.name
                        : undefined;
                const viewType =
                    typeof viewAttrs.type === 'string'
                        ? viewAttrs.type
                        : undefined;

                const titleHits = extractKtlKeywordsFromText(title).map(
                    (entry) => ({
                        ...entry,
                        source: 'title',
                    }),
                );
                const descriptionHits = extractKtlKeywordsFromText(
                    description,
                ).map((entry) => ({ ...entry, source: 'description' }));
                const allHits = [...titleHits, ...descriptionHits];
                if (!allHits.length) continue;

                const filteredHits = keywordFilter
                    ? allHits.filter(
                          (hit) =>
                              hit.keyword.toLowerCase() === keywordFilter ||
                              hit.keyword.toLowerCase().includes(keywordFilter),
                      )
                    : allHits;

                if (!filteredHits.length) continue;

                const uniqueKeywords = [
                    ...new Set(filteredHits.map((hit) => hit.keyword)),
                ];
                uniqueKeywords.forEach((kw) =>
                    keywordCounts.set(kw, (keywordCounts.get(kw) || 0) + 1),
                );

                const sceneContext = viewContextMap[viewKey] || {};
                matches.push({
                    viewKey,
                    viewName,
                    viewType,
                    sceneKey: sceneContext.sceneKey,
                    sceneName: sceneContext.sceneName,
                    sceneSlug: sceneContext.sceneSlug,
                    matchedKeywords: uniqueKeywords,
                    hitCount: filteredHits.length,
                    snippets: filteredHits.slice(0, 20),
                });

                if (matches.length >= maxResults) break;
            }

            const topKeywords = [...keywordCounts.entries()]
                .map(([kw, count]) => ({ keyword: kw, viewCount: count }))
                .sort((a, b) => b.viewCount - a.viewCount)
                .slice(0, 200);

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                keywordFilter: keyword || null,
                totalMatches: matches.length,
                topKeywords,
                results: matches,
            });
        },
    );

    server.tool(
        'knack_search_emails',
        'Search views for email-related rules/actions and return recipient (to) plus subject/message context.',
        {
            appKey: z.string().optional(),
            query: z
                .string()
                .optional()
                .describe(
                    'Optional text filter applied to to/cc/bcc/subject/message/path.',
                ),
            includeMessage: z.boolean().default(false),
            maxResults: z.number().int().min(1).max(5000).default(100),
        },
        async ({ appKey, query, includeMessage, maxResults }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_search_emails',
                args: { appKey, query, includeMessage, maxResults },
            });

            const { viewMap, source } = await getViewMapForApp(app);
            if (!viewMap) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No view map available from runtime API or viewMap.json.',
                });
            }

            const viewContextMap = await getViewContextMapForApp(app);
            const filter = query ? query.trim().toLowerCase() : null;
            const matches: Array<Record<string, unknown>> = [];

            for (const [viewKey, viewAttrs] of Object.entries(viewMap)) {
                const sceneContext = viewContextMap[viewKey] || {};
                const emailNodes = collectEmailNodes(viewAttrs, ['$']);
                if (!emailNodes.length) continue;

                for (const node of emailNodes) {
                    const searchable = [
                        node.path,
                        node.to,
                        node.cc,
                        node.bcc,
                        node.subject,
                        node.message,
                        node.action,
                    ]
                        .filter((part): part is string => Boolean(part))
                        .join(' || ')
                        .toLowerCase();

                    if (filter && !searchable.includes(filter)) continue;

                    matches.push({
                        viewKey,
                        viewName:
                            typeof viewAttrs.name === 'string'
                                ? viewAttrs.name
                                : undefined,
                        viewType:
                            typeof viewAttrs.type === 'string'
                                ? viewAttrs.type
                                : undefined,
                        sceneKey: sceneContext.sceneKey,
                        sceneName: sceneContext.sceneName,
                        sceneSlug: sceneContext.sceneSlug,
                        path: node.path,
                        action: node.action,
                        to: node.to,
                        cc: node.cc,
                        bcc: node.bcc,
                        subject: truncateText(node.subject, 2000),
                        message: includeMessage
                            ? truncateText(node.message, 4000)
                            : undefined,
                    });

                    if (matches.length >= maxResults) break;
                }

                if (matches.length >= maxResults) break;
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                query: query || null,
                includeMessage,
                totalMatches: matches.length,
                results: matches,
            });
        },
    );

    // -----------------------
    // Tools: schema overview + database design helpers
    // -----------------------

    server.tool(
        'knack_list_objects',
        'List all objects in the app schema with their key, name, and field count. Use this to get a high-level map of the data model before diving into individual objects.',
        {
            appKey: z.string().optional(),
        },
        async ({ appKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_list_objects',
                args: { appKey },
            });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                objectCount: schema.objects.length,
                objects: schema.objects.map((obj) => ({
                    key: obj.key,
                    name: obj.name,
                    fieldCount: (obj.fields || []).length,
                })),
            });
        },
    );

    server.tool(
        'knack_describe_field_shape',
        'Describe a Knack field type for two different jobs: reading records (the formatted/raw API response shape) and authoring fields (the format/relationship object knack_create_field or knack_update_field expects, when a verified example is available). Also returns the general conditional-rules shape, since rules apply across field types. Use this before writing an equation or connection field definition, not just before reading one.',
        {
            fieldType: z
                .string()
                .describe(
                    'Knack field type, e.g. connection, date_time, name, address, multiple_choice.',
                ),
        },
        async ({ fieldType }) => {
            debugLog('tool_call', {
                tool: 'knack_describe_field_shape',
                args: { fieldType },
            });
            const info = getFieldShapeInfo(fieldType);

            if (!info) {
                const knownTypes = Object.keys(KNACK_FIELD_SHAPES).sort();
                return makeTextResponse({
                    ok: false,
                    fieldType,
                    message: `Unknown field type: ${fieldType}. See knownTypes for the full list.`,
                    knownTypes,
                });
            }

            return makeTextResponse({
                ok: true,
                fieldType,
                summary: info.summary,
                valueShape: {
                    formattedShape: info.formattedShape,
                    rawShape: info.rawShape,
                    notes: info.notes || null,
                    tip: 'Knack returns both field_xxx (formatted) and field_xxx_raw (raw) for every field. Prefer raw values when you need machine-readable data (numbers, IDs, arrays).',
                },
                definitionShape: info.definitionShape
                    ? {
                          format: info.definitionShape,
                          notes: info.definitionNotes || null,
                          tip: 'This is the format/relationship payload for knack_create_field or knack_update_field — not what a record value looks like. Use knack_get_field on a working example field of this type to see a live comparison.',
                      }
                    : {
                          format: null,
                          notes: `No verified definition example is recorded yet for "${fieldType}". Use knack_get_field on a working example field of this type on your app to read one instead of guessing.`,
                      },
                conditionalRules: KNACK_CONDITIONAL_RULES_SHAPE,
            });
        },
    );

    if (HAS_DIAGNOSTIC_TOOLS) {
        server.tool(
            'knack_verify_record_field_shapes',
            "Fetch a live Knack record and compare each field's observed formatted/raw values against the documented field shape heuristics. Use this to validate or refine KNACK_FIELD_SHAPES with real data.",
            {
                appKey: z.string().optional(),
                objectKey: z.string(),
                recordId: z.string(),
                includeBlankFields: z
                    .boolean()
                    .optional()
                    .describe(
                        'Include fields whose formatted and raw values are both blank. Defaults to false.',
                    ),
            },
            async ({
                appKey,
                objectKey,
                recordId,
                includeBlankFields = false,
            }) => {
                const app = getAppOrThrow(appKey);
                assertDiagnosticAccess(app);
                debugLog('tool_call', {
                    tool: 'knack_verify_record_field_shapes',
                    args: { appKey, objectKey, recordId, includeBlankFields },
                });
                const apiKey = getApiKeyOrThrow(app.appKey);

                const [schemaResult, recordResult] = await Promise.all([
                    getSchemaForApp(app),
                    knackRequest(
                        app,
                        apiKey,
                        `/objects/${objectKey}/records/${recordId}`,
                    ),
                ]);

                const schema = schemaResult.schema;
                const obj =
                    schema?.objects?.find((entry) => entry.key === objectKey) ||
                    null;
                const record = asRecord(recordResult.body);

                if (!recordResult.ok || !record) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        recordId,
                        message: 'Unable to fetch the requested record.',
                        recordResponse: recordResult,
                    });
                }

                if (!obj) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        recordId,
                        schemaSource: schemaResult.source,
                        message:
                            'Object was not found in the available schema, so field types could not be verified.',
                    });
                }

                const results = (obj.fields || []).map((field) => {
                    const formatted = record[field.key];
                    const raw = record[`${field.key}_raw`];
                    const validation = validateFieldShape(
                        field.type || '',
                        formatted,
                        raw,
                    );
                    const shapeInfo = field.type
                        ? getFieldShapeInfo(field.type)
                        : null;

                    return {
                        fieldKey: field.key,
                        fieldName: field.name || null,
                        fieldType: field.type || null,
                        status: validation.status,
                        observedFormattedShape:
                            validation.observedFormattedShape,
                        observedRawShape: validation.observedRawShape,
                        formattedPreview: getValuePreview(formatted),
                        rawPreview: getValuePreview(raw),
                        expectedSummary: shapeInfo?.summary || null,
                        findings: validation.findings,
                    };
                });

                const filteredResults = includeBlankFields
                    ? results
                    : results.filter((entry) => entry.status !== 'skipped');

                const summary = {
                    checkedFieldCount: filteredResults.length,
                    matchCount: filteredResults.filter(
                        (entry) => entry.status === 'match',
                    ).length,
                    mismatchCount: filteredResults.filter(
                        (entry) => entry.status === 'mismatch',
                    ).length,
                    skippedCount: results.filter(
                        (entry) => entry.status === 'skipped',
                    ).length,
                    unknownCount: filteredResults.filter(
                        (entry) => entry.status === 'unknown',
                    ).length,
                };

                return makeTextResponse({
                    ok: true,
                    appKey: app.appKey,
                    objectKey,
                    objectName: obj.name || null,
                    recordId,
                    schemaSource: schemaResult.source,
                    includeBlankFields,
                    summary,
                    results: filteredResults,
                });
            },
        );
    }

    server.tool(
        'knack_get_object_connections',
        'Return all connection fields for a Knack object showing which other objects they link to. Essential for understanding relationships between objects when designing or coding against the data model.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_get_object_connections',
                args: { appKey, objectKey },
            });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const obj = schema.objects.find((entry) => entry.key === objectKey);
            if (!obj) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    source,
                    message: `Object not found in schema: ${objectKey}`,
                    availableObjectKeys: schema.objects.map(
                        (entry) => entry.key,
                    ),
                });
            }

            const connectionFields = (obj.fields || [])
                .filter((field) => field.type === 'connection')
                .map((field) => {
                    const connectedObjectKey = field.connectedObject || null;
                    const connectedObject = connectedObjectKey
                        ? schema.objects?.find(
                              (o) => o.key === connectedObjectKey,
                          ) || null
                        : null;
                    return {
                        fieldKey: field.key,
                        fieldName: field.name,
                        connectedObjectKey,
                        connectedObjectName: connectedObject?.name || null,
                    };
                });

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                objectKey: obj.key,
                objectName: obj.name,
                connectionCount: connectionFields.length,
                connections: connectionFields,
                note: connectionFields.some((c) => !c.connectedObjectKey)
                    ? 'Some connection targets are unknown. Run knack_refresh_cache with warm:true to load fresh runtime metadata which includes relationship details.'
                    : null,
            });
        },
    );

    server.tool(
        'knack_get_app_overview',
        'Return a full overview of the app schema: all objects with field counts, field type summaries, and cross-object connection relationships. Use this to understand the data model at a glance and get database design advice.',
        {
            appKey: z.string().optional(),
            includeFieldDetails: z
                .boolean()
                .default(false)
                .describe(
                    'When true, include all field names and types for each object (verbose).',
                ),
        },
        async ({ appKey, includeFieldDetails }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_get_app_overview',
                args: { appKey, includeFieldDetails },
            });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const overview = buildAppOverview(schema, includeFieldDetails);

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                objectCount: overview.objectCount,
                totalFields: overview.totalFields,
                relationshipCount: overview.relationshipCount,
                objects: overview.objects,
                relationships: overview.relationships,
            });
        },
    );

    server.tool(
        'knack_generate_seed_csvs',
        'Generate Knack import-ready seed CSV content for new-object imports. Produces one CSV per object with headers, realistic example rows, connection lookup values that match generated parent rows, and comma-separated multi-select values. If you opt into API-backed existing parent lookups, the tool first returns a rough API-call estimate and requires explicit confirmation before using the API key.',
        {
            appKey: z.string().optional(),
            objectKeys: z
                .array(z.string())
                .optional()
                .describe(
                    'Optional subset of object keys to include. Defaults to all objects in the schema.',
                ),
            rowsPerObject: z
                .number()
                .int()
                .min(2)
                .max(10)
                .default(4)
                .describe(
                    'Minimum number of example rows to generate per object.',
                ),
            useExistingConnectionValues: z
                .boolean()
                .default(false)
                .describe(
                    'When true, fetch first-page display values from existing connected parent objects that are not included in objectKeys.',
                ),
            confirmExistingConnectionValueFetch: z
                .boolean()
                .default(false)
                .describe(
                    'Required before any API-key-backed parent lookup fetches are executed.',
                ),
        },
        async ({
            appKey,
            objectKeys,
            rowsPerObject,
            useExistingConnectionValues,
            confirmExistingConnectionValueFetch,
        }) => {
            const app = getAppOrThrow(appKey);
            const effectiveRowsPerObject = Math.max(rowsPerObject, 2);
            debugLog('tool_call', {
                tool: 'knack_generate_seed_csvs',
                args: {
                    appKey,
                    objectKeys,
                    rowsPerObject,
                    useExistingConnectionValues,
                    confirmExistingConnectionValueFetch,
                },
            });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json.',
                });
            }

            const externalTargets = useExistingConnectionValues
                ? getExternalSeedConnectionTargets(schema, objectKeys)
                : [];
            const apiCallEstimate = {
                requiresApiKey:
                    useExistingConnectionValues && externalTargets.length > 0,
                estimatedCalls: externalTargets.length,
                basis: useExistingConnectionValues
                    ? `One authenticated records-list request per connected parent object not included in objectKeys, limited to the first page with up to ${effectiveRowsPerObject} rows.`
                    : 'No authenticated API calls requested.',
                targets: externalTargets.map((target) => ({
                    objectKey: target.key,
                    objectName: target.name,
                    plannedApiPath: `/objects/${target.key}/records?page=1&rows_per_page=${effectiveRowsPerObject}`,
                })),
            };

            if (
                apiCallEstimate.requiresApiKey &&
                !confirmExistingConnectionValueFetch
            ) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    source,
                    confirmationRequired: true,
                    message:
                        'Authenticated API fetches for existing parent connection values were requested. Review the estimated call count and re-run with confirmExistingConnectionValueFetch:true to proceed.',
                    apiCallEstimate,
                });
            }

            const externalLookupResult = apiCallEstimate.requiresApiKey
                ? await fetchExternalSeedConnectionLookups(
                      app,
                      externalTargets,
                      effectiveRowsPerObject,
                  )
                : { lookups: {}, fetches: [] };

            const workbook = generateSeedCsvWorkbook(schema, {
                objectKeys,
                rowsPerObject: effectiveRowsPerObject,
                externalConnectionLookups: externalLookupResult.lookups,
            });

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                objectCount: workbook.objects.length,
                importOrder: workbook.importOrder,
                objects: workbook.objects,
                apiCallEstimate,
                externalConnectionFetches: externalLookupResult.fetches,
                note: apiCallEstimate.requiresApiKey
                    ? 'Connection values use generated unique keys for included parent objects and API-fetched existing display values for connected parent objects outside objectKeys.'
                    : 'Connection values reference each object’s suggested unique import key. Import parent/lookup objects before child objects that connect to them.',
            });
        },
    );

    server.tool(
        'knack_list_scenes',
        'List all scenes (pages) in the app with their key, name, slug, and views. Use this to explore the UI structure of a Knack application.',
        {
            appKey: z.string().optional(),
            includeViews: z
                .boolean()
                .default(false)
                .describe(
                    'When true, include the list of views for each scene with their key, name, and type.',
                ),
        },
        async ({ appKey, includeViews }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_list_scenes',
                args: { appKey, includeViews },
            });

            const scenes = await getScenesForApp(app);

            if (!scenes.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No scene data available. Run knack_refresh_cache with warm: true to load runtime metadata.',
                });
            }

            const runtimeMetadata = await getRuntimeMetadata(app);
            const sceneSummaries = scenes.map((scene) => {
                const summary: Record<string, unknown> = {
                    sceneKey: scene.sceneKey,
                    sceneName: scene.sceneName,
                    sceneSlug: scene.sceneSlug,
                    viewCount: scene.views.length,
                    builderUrl: makeSceneBuilderUrl(
                        app,
                        scene.sceneKey,
                        runtimeMetadata,
                    ),
                };

                if (includeViews) {
                    summary.views = scene.views;
                }

                return summary;
            });

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                sceneCount: scenes.length,
                totalViewCount: scenes.reduce(
                    (sum, s) => sum + s.views.length,
                    0,
                ),
                scenes: sceneSummaries,
            });
        },
    );

    server.tool(
        'knack_list_views',
        'List views across the app with scene context, type, and builder URL. Optionally filter by scene key or view type (e.g. form, grid, table, report, search, menu, rich_text, map, calendar).',
        {
            appKey: z.string().optional(),
            sceneKey: z
                .string()
                .optional()
                .describe('Filter to views belonging to a specific scene.'),
            viewType: z
                .string()
                .optional()
                .describe(
                    'Filter by view type, e.g. form, grid, table, report, search, menu, rich_text, map, calendar.',
                ),
            maxResults: z.number().int().min(1).max(5000).default(100),
        },
        async ({ appKey, sceneKey, viewType, maxResults }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_list_views',
                args: { appKey, sceneKey, viewType, maxResults },
            });

            const scenes = await getScenesForApp(app);

            if (!scenes.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No scene data available. Run knack_refresh_cache with warm: true to load runtime metadata.',
                });
            }

            const runtimeMetadata = await getRuntimeMetadata(app);
            const normSceneKey = sceneKey?.toLowerCase();
            const normViewType = viewType?.toLowerCase();
            const results: Array<Record<string, unknown>> = [];
            const viewTypeCounts = new Map<string, number>();

            for (const scene of scenes) {
                if (
                    normSceneKey &&
                    scene.sceneKey.toLowerCase() !== normSceneKey
                )
                    continue;

                for (const view of scene.views) {
                    const vType = view.viewType || 'unknown';
                    viewTypeCounts.set(
                        vType,
                        (viewTypeCounts.get(vType) || 0) + 1,
                    );

                    if (normViewType && vType.toLowerCase() !== normViewType)
                        continue;

                    if (results.length < maxResults) {
                        results.push({
                            viewKey: view.viewKey,
                            viewName: view.viewName,
                            viewType: view.viewType,
                            sceneKey: scene.sceneKey,
                            sceneName: scene.sceneName,
                            sceneSlug: scene.sceneSlug,
                            builderUrl: makeViewBuilderUrl(
                                app,
                                {
                                    sceneKey: scene.sceneKey,
                                    viewKey: view.viewKey,
                                    viewType: view.viewType,
                                },
                                runtimeMetadata,
                            ),
                        });
                    }
                }
            }

            const viewTypeSummary = [...viewTypeCounts.entries()]
                .map(([type, count]) => ({ type, count }))
                .sort((a, b) => b.count - a.count);

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                filters: {
                    sceneKey: sceneKey || null,
                    viewType: viewType || null,
                },
                totalViews: results.length,
                viewTypeSummary,
                views: results,
            });
        },
    );

    server.tool(
        'knack_analyze_data_model',
        'Analyse the app data model and return structured design feedback: field-count distribution, isolated objects, connection density, field type spread, and objects with potential design issues.',
        {
            appKey: z.string().optional(),
        },
        async ({ appKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_analyze_data_model',
                args: { appKey },
            });

            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available. Run knack_refresh_cache with warm: true or ensure schema.json is present.',
                });
            }

            const analysis = buildDataModelAnalysis(schema);

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                ...analysis,
            });
        },
    );

    server.tool(
        'knack_app_deep_dive',
        'One-call onboarding snapshot for an unfamiliar Knack app. Combines what would otherwise take several separate calls (knack_get_app_overview, knack_analyze_data_model, knack_list_scenes/knack_list_views) into a single response: the data model (objects, field types, connection graph), design-feedback observations, and a UI-structure summary (scene/view counts and view-type breakdown). Call this first when starting work on an app you have not explored yet, then use the more targeted tools for deeper detail on a specific object, scene, or view.',
        {
            appKey: z.string().optional(),
            includeFieldDetails: z
                .boolean()
                .default(false)
                .describe(
                    'When true, include every field name/type per object in the data model section (verbose).',
                ),
            includeScenes: z
                .boolean()
                .default(false)
                .describe(
                    'When true, include the per-scene list (key, name, slug, view count) under ui.scenes. Off by default; only scene/view totals and the view-type summary are included otherwise.',
                ),
            maxRelationshipsListed: z
                .number()
                .int()
                .min(0)
                .max(2000)
                .default(200)
                .describe(
                    'Cap on the number of connection relationships listed in full under dataModel.relationships. dataModel.relationshipCount always reflects the true total even when the list is capped.',
                ),
        },
        async ({
            appKey,
            includeFieldDetails,
            includeScenes,
            maxRelationshipsListed,
        }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_app_deep_dive',
                args: {
                    appKey: app.appKey,
                    includeFieldDetails,
                    includeScenes,
                },
            });

            const { schema, source } = await getSchemaForApp(app);
            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No schema available from runtime API or schema.json. Run knack_refresh_cache with warm: true, or ensure schema.json is present.',
                });
            }

            const overview = buildAppOverview(schema, includeFieldDetails);
            const analysis = buildDataModelAnalysis(schema);

            const relationshipsTruncated =
                overview.relationships.length > maxRelationshipsListed;
            const relationships = overview.relationships.slice(
                0,
                maxRelationshipsListed,
            );

            const scenes = await getScenesForApp(app);
            const viewTypeCounts = new Map<string, number>();
            let totalViewCount = 0;
            for (const scene of scenes) {
                for (const view of scene.views) {
                    totalViewCount += 1;
                    const vType = view.viewType || 'unknown';
                    viewTypeCounts.set(
                        vType,
                        (viewTypeCounts.get(vType) || 0) + 1,
                    );
                }
            }
            const viewTypeSummary = [...viewTypeCounts.entries()]
                .map(([type, count]) => ({ type, count }))
                .sort((a, b) => b.count - a.count);

            const ui: Record<string, unknown> = scenes.length
                ? {
                      available: true,
                      sceneCount: scenes.length,
                      totalViewCount,
                      viewTypeSummary,
                  }
                : {
                      available: false,
                      message:
                          'No scene/view metadata cached yet. Run knack_refresh_cache with warm: true to include UI structure here.',
                  };

            if (includeScenes && scenes.length) {
                ui.scenes = scenes.map((scene) => ({
                    sceneKey: scene.sceneKey,
                    sceneName: scene.sceneName,
                    sceneSlug: scene.sceneSlug,
                    viewCount: scene.views.length,
                }));
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                dataModel: {
                    objectCount: overview.objectCount,
                    totalFields: overview.totalFields,
                    relationshipCount: overview.relationshipCount,
                    relationshipsTruncated,
                    objects: overview.objects,
                    relationships,
                    analysisSummary: analysis.summary,
                    fieldTypeDistribution: analysis.fieldTypeDistribution,
                    isolatedObjects: analysis.isolatedObjects,
                    highFieldCountObjects: analysis.highFieldCountObjects,
                    lowFieldCountObjects: analysis.lowFieldCountObjects,
                    observations: analysis.observations,
                },
                ui,
                nextSteps: [
                    'knack_get_app_overview / knack_analyze_data_model for the full data-model detail behind this summary.',
                    'knack_list_scenes / knack_list_views to drill into specific pages once you know what you are looking for.',
                    'knack_get_object_connections on a specific object to trace its relationships in isolation.',
                ],
            });
        },
    );

    if (HAS_DIAGNOSTIC_TOOLS) {
        server.tool(
            'knack_get_raw_object',
            'Return the raw Knack API object definition, including full field payloads and format metadata.',
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
            },
            async ({ appKey, objectKey }) => {
                const app = getAppOrThrow(appKey);
                assertDiagnosticAccess(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_get_raw_object',
                    args: { appKey: app.appKey, objectKey },
                });

                const result = await knackRequest(
                    app,
                    apiKey,
                    `/objects/${objectKey}`,
                );
                const bodyDetail = getInlineDetail(result.body);
                return makeTextResponse({
                    appKey: app.appKey,
                    objectKey,
                    action: 'get_raw_object',
                    ok: result.ok,
                    status: result.status,
                    bodyIncluded: bodyDetail.included,
                    bodySizeBytes: bodyDetail.sizeBytes,
                    body: bodyDetail.value,
                    bodySummary: bodyDetail.summary,
                });
            },
        );
    }

    // -----------------------
    // Mutation tools (opt-in to keep the default tool catalogue smaller)
    // -----------------------

    server.tool(
        'knack_download_file',
        'Download an attachment from an approved Knack file or image field to a controlled local temporary path. Read-only.',
        {
            appKey: z.string().optional(),
            objectKey: z
                .string()
                .describe('Object containing the file or image field.'),
            recordId: z.string().describe('Record containing the attachment.'),
            fieldKey: z
                .string()
                .describe('File or image field containing the attachment.'),
        },
        async ({ appKey, objectKey, recordId, fieldKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_download_file',
                args: { appKey: app.appKey, objectKey, recordId, fieldKey },
            });
            const attachment = await getRecordAttachment(
                app,
                objectKey,
                recordId,
                fieldKey,
            );
            const download = await downloadRecordAttachment(
                app,
                recordId,
                attachment,
            );

            return makeTextResponse({
                ok: true,
                action: 'download_file',
                appKey: app.appKey,
                objectKey,
                recordId,
                fieldKey,
                filename: attachment.filename,
                mimeType: attachment.mimeType,
                sourceSizeBytes: attachment.sizeBytes,
                downloadedSizeBytes: download.sizeBytes,
                filePath: download.filePath,
            });
        },
    );

    server.tool(
        'knack_read_file',
        'Download an approved Knack attachment and extract bounded plain text for AI review. Supports PDF, DOCX, TXT, CSV, JSON, Markdown, and XML. Read-only.',
        {
            appKey: z.string().optional(),
            objectKey: z
                .string()
                .describe('Object containing the file or image field.'),
            recordId: z.string().describe('Record containing the attachment.'),
            fieldKey: z
                .string()
                .describe('File or image field containing the attachment.'),
        },
        async ({ appKey, objectKey, recordId, fieldKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', {
                tool: 'knack_read_file',
                args: { appKey: app.appKey, objectKey, recordId, fieldKey },
            });
            const attachment = await getRecordAttachment(
                app,
                objectKey,
                recordId,
                fieldKey,
            );
            const download = await downloadRecordAttachment(
                app,
                recordId,
                attachment,
            );
            const extraction = await extractAttachmentText(
                download.filePath,
                attachment.mimeType,
            );

            return makeTextResponse({
                ok: extraction.supported,
                action: 'read_file',
                appKey: app.appKey,
                objectKey,
                recordId,
                fieldKey,
                filename: attachment.filename,
                mimeType: attachment.mimeType,
                sourceSizeBytes: attachment.sizeBytes,
                downloadedSizeBytes: download.sizeBytes,
                filePath: download.filePath,
                text: extraction.text,
                truncated: extraction.truncated,
                message: extraction.supported
                    ? null
                    : 'The file was downloaded, but its format is not supported for text extraction.',
            });
        },
    );

    if (HAS_MUTATION_TOOLS) {
        server.tool(
            'knack_create_field',
            'Create a new field on a Knack object. Requires the app to have readonly: false in app.json. Pass dryRun: true to validate and preview the definition without creating it.',
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
                name: z.string().describe('Field name'),
                type: z
                    .string()
                    .describe(
                        'Field type, e.g. short_text, number, boolean, sum, connection, etc.',
                    ),
                required: z.boolean().optional().default(false),
                unique: z.boolean().optional().default(false),
                format: z
                    .string()
                    .optional()
                    .describe(
                        'Optional format object as JSON string (for sum, equation, connection, etc.). Call knack_describe_field_shape(type) first for the verified shape and gotchas — for equation and connection fields, this is the whole field definition.',
                    ),
                relationship: z
                    .string()
                    .optional()
                    .describe(
                        'Optional relationship object as JSON string for connection fields. Call knack_describe_field_shape("connection") first for the verified shape.',
                    ),
                description: z
                    .string()
                    .optional()
                    .describe(
                        "Note describing what this field is for. Stored as the field's description/help text in the Knack Builder (sent as meta.description, since Knack's API does not reliably persist a bare top-level description) — useful documentation for other developers or AI assistants reading the schema later. AI callers should populate this by default on every new field, unless the user explicitly asked for no description.",
                    ),
                dryRun: z
                    .boolean()
                    .optional()
                    .default(false)
                    .describe(
                        'Validate the payload (including equation token checks) and return the resulting field definition without creating it in the app.',
                    ),
            },
            async ({
                appKey,
                objectKey,
                name,
                type,
                required,
                unique,
                format,
                relationship,
                description,
                dryRun,
            }) => {
                const app = getAppOrThrow(appKey);
                assertWritable(app);
                debugLog('tool_call', {
                    tool: 'knack_create_field',
                    args: { appKey: app.appKey, objectKey, name, type },
                });

                const payload: Record<string, unknown> = {
                    name,
                    type,
                    required,
                    unique,
                };
                if (description !== undefined) {
                    payload.description = description;
                    normalizeFieldDescriptionForWrite(payload);
                }
                const validationErrors: string[] = [];
                let equationWarnings: string[] = [];
                if (format) {
                    const parsed = parseJsonObjectInput(format, 'format');
                    validationErrors.push(...parsed.errors);
                    if (parsed.payload) {
                        payload.format = parsed.payload;
                        const equation = parsed.payload.equation;
                        if (typeof equation === 'string' && equation.trim()) {
                            const { schema } = await getSchemaForApp(app);
                            if (schema) {
                                const check = validateEquationTokens(
                                    schema,
                                    objectKey,
                                    equation,
                                );
                                validationErrors.push(...check.errors);
                                equationWarnings = check.warnings;
                            } else {
                                equationWarnings = [
                                    'Could not validate equation tokens: no schema is available (neither runtime API nor schema.json) for this app, so this write is going out unchecked.',
                                ];
                            }
                        }
                    }
                }
                if (relationship) {
                    const parsed = parseJsonObjectInput(
                        relationship,
                        'relationship',
                    );
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
                        ...(equationWarnings.length
                            ? { equationWarnings }
                            : {}),
                    });
                }

                const apiKey = getApiKeyOrThrow(app.appKey);
                const result = await knackRequest(
                    app,
                    apiKey,
                    `/objects/${objectKey}/fields`,
                    {
                        method: 'POST',
                        body: JSON.stringify(payload),
                    },
                );

                if (result.ok) {
                    const bodyDetail = getInlineDetail(result.body);
                    if (!bodyDetail.included) {
                        // Knack returns the full application schema for connection-field
                        // writes (creating a connection also updates the cross-object
                        // relationship graph) — project it down to the created field.
                        const createdField = findFieldInFieldWriteResponse(
                            result.body,
                            objectKey,
                            { name, type },
                        );
                        return makeTextResponse({
                            appKey: app.appKey,
                            objectKey,
                            action: 'create_field',
                            ok: true,
                            status: result.status,
                            ...(equationWarnings.length
                                ? { equationWarnings }
                                : {}),
                            ...(createdField ? { field: createdField } : {}),
                            bodySizeBytes: bodyDetail.sizeBytes,
                            bodySummary: bodyDetail.summary,
                            note: createdField
                                ? "Knack's response for this write included the full application schema (expected for connection fields, since they update the cross-object relationship graph) — projected down to the created field above plus a structural summary. Call knack_get_field for the full raw field definition if needed."
                                : `Knack's response for this write included the full application schema. Could not unambiguously identify the created field in it (e.g. another field named "${name}" of type ${type} may already exist on ${objectKey} — Knack field names aren't unique — or the response may not have included this object at all) — call knack_get_object_fields on ${objectKey} to find the new field's key.`,
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
                    ...(result.ok
                        ? { cacheNote: SCHEMA_CACHE_STALE_NOTE }
                        : {}),
                });
            },
        );

        server.tool(
            'knack_update_field',
            'Update an existing field on a Knack object. Send only the properties to change. Requires readonly: false. Pass dryRun: true to validate and preview the merged definition without persisting it. If the field currently has a description, changing it requires care: Knack replaces the description outright, so any existing KTL keyword tokens (e.g. "_keyword") not carried over into the new text are blocked by default — see confirmRemoveKtlKeywords.',
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
                fieldKey: z.string().describe('The field key, e.g. field_123'),
                updates: z
                    .string()
                    .optional()
                    .describe(
                        'Partial field definition as JSON string with properties to update (name, format, rules, etc.). For format on equation/connection fields, call knack_describe_field_shape(type) first, or knack_get_field on a working example, rather than guessing the shape. Optional if you are only setting description via the dedicated description parameter.',
                    ),
                description: z
                    .string()
                    .optional()
                    .describe(
                        'Sets the field\'s description/help note, shown in the Knack Builder (sent as meta.description, since Knack\'s API does not reliably persist a bare top-level description) — useful documentation for other developers or AI assistants reading the schema later. Takes precedence over any "description" key already present in updates. Pass an empty string to clear an existing description. Do not drop the field\'s existing content or any KTL keyword tokens (e.g. "_keyword") when composing the new text — append to or edit around them instead of replacing wholesale, unless the user explicitly asked to remove something.',
                    ),
                confirmRemoveKtlKeywords: z
                    .boolean()
                    .optional()
                    .default(false)
                    .describe(
                        'The current description may contain KTL keyword tokens (e.g. "_keyword") that drive Knack Tools & Libraries behaviour on this field. By default, an update whose new description is missing one of them is blocked. Only set this to true after explicitly confirming the removal with the user — never on your own initiative.',
                    ),
                dryRun: z
                    .boolean()
                    .optional()
                    .default(false)
                    .describe(
                        'Validate the update (including equation token checks and the KTL-keyword safety check) and return the resulting merged field definition without persisting it.',
                    ),
            },
            async ({
                appKey,
                objectKey,
                fieldKey,
                updates,
                description,
                confirmRemoveKtlKeywords,
                dryRun,
            }) => {
                const app = getAppOrThrow(appKey);
                assertWritable(app);
                debugLog('tool_call', {
                    tool: 'knack_update_field',
                    args: { appKey: app.appKey, objectKey, fieldKey },
                });

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
                    // Plain top-level assignment (no HTML wrapping) so this stays
                    // consistent with knack_create_field, and so it actually takes
                    // precedence over a raw "description" key already in `updates` —
                    // normalizeFieldDescriptionForWrite below mirrors whichever value
                    // wins here into meta.description.
                    parsed.payload = { ...parsed.payload, description };
                }
                if (parsed.payload) {
                    // Covers description set via the dedicated parameter above, and via a
                    // raw {"description": "..."} key inside `updates` JSON.
                    normalizeFieldDescriptionForWrite(parsed.payload);
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
                    const { schema } = await getSchemaForApp(app);
                    if (schema) {
                        const check = validateEquationTokens(
                            schema,
                            objectKey,
                            equation,
                        );
                        validationErrors.push(...check.errors);
                        equationWarnings = check.warnings;
                    } else {
                        equationWarnings = [
                            'Could not validate equation tokens: no schema is available (neither runtime API nor schema.json) for this app, so this write is going out unchecked.',
                        ];
                    }
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

                const apiKey = getApiKeyOrThrow(app.appKey);

                const descriptionKeyPresent = Boolean(
                    parsed.payload &&
                    (Object.hasOwn(parsed.payload, 'description') ||
                        typeof asRecord(parsed.payload.meta)?.description ===
                            'string'),
                );

                let currentField: Record<string, unknown> | undefined;
                let currentFieldFetchOk = true;
                let currentFieldFetchStatus = 0;

                if (dryRun || descriptionKeyPresent) {
                    const objResult = (await knackRequest(
                        app,
                        apiKey,
                        `/objects/${objectKey}`,
                    )) as {
                        ok: boolean;
                        status: number;
                        body?: {
                            object?: {
                                fields?: Array<Record<string, unknown>>;
                            };
                        };
                    };
                    currentField = objResult.body?.object?.fields?.find(
                        (entry) => entry.key === fieldKey,
                    );
                    currentFieldFetchOk = objResult.ok && Boolean(currentField);
                    currentFieldFetchStatus = objResult.status;
                }

                const ktlKeywordWarnings: string[] = [];
                if (descriptionKeyPresent) {
                    if (currentField) {
                        const currentFieldMeta = asRecord(currentField.meta);
                        const currentDescription =
                            (typeof currentField.description === 'string'
                                ? currentField.description
                                : typeof currentFieldMeta?.description ===
                                    'string'
                                  ? currentFieldMeta.description
                                  : '') || '';
                        const newPayloadMeta = asRecord(parsed.payload?.meta);
                        const newDescription =
                            (typeof parsed.payload?.description === 'string'
                                ? parsed.payload.description
                                : typeof newPayloadMeta?.description ===
                                    'string'
                                  ? newPayloadMeta.description
                                  : '') || '';
                        const currentKeywords = [
                            ...new Set(
                                extractKtlKeywordsFromText(
                                    currentDescription,
                                ).map((hit) => hit.keyword),
                            ),
                        ];
                        const droppedKeywords = currentKeywords.filter(
                            (keyword) =>
                                !containsKtlKeywordToken(
                                    newDescription,
                                    keyword,
                                ),
                        );
                        if (
                            droppedKeywords.length &&
                            !confirmRemoveKtlKeywords
                        ) {
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
                        ktlKeywordWarnings.push(
                            'Could not fetch the current field to check for KTL keywords in its existing description, so this description change is going out without that safety check.',
                        );
                    }
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
                    const changedKeys = parsed.payload
                        ? Object.keys(parsed.payload)
                        : [];
                    const mergedPreview = parsed.payload
                        ? deepMergeRecords(currentField, parsed.payload)
                        : currentField;
                    const resolveCurrentValue = (key: string): unknown => {
                        // Knack's raw field payload sometimes nests description under
                        // meta.description rather than the top-level key; fall back to
                        // that so the diff doesn't show a false "from: undefined".
                        if (
                            key === 'description' &&
                            currentField.description === undefined
                        ) {
                            const meta = asRecord(currentField.meta);
                            if (typeof meta?.description === 'string') {
                                return meta.description;
                            }
                        }
                        return currentField[key];
                    };
                    const changes: Record<
                        string,
                        { from: unknown; to: unknown }
                    > = {};
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
                        currentField,
                        changes,
                        ...(equationWarnings.length
                            ? { equationWarnings }
                            : {}),
                        ...(ktlKeywordWarnings.length
                            ? { ktlKeywordWarnings }
                            : {}),
                        ...(touchesNestedPreview
                            ? { mergeNote: NESTED_MERGE_UNCERTAINTY_NOTE }
                            : {}),
                    });
                }

                const result = await knackRequest(
                    app,
                    apiKey,
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
                        // Same connection-field bloat as knack_create_field: Knack's
                        // response can include the full application schema — project it
                        // down to the updated field.
                        const updatedField = findFieldInFieldWriteResponse(
                            result.body,
                            objectKey,
                            { fieldKey },
                        );
                        return makeTextResponse({
                            appKey: app.appKey,
                            objectKey,
                            fieldKey,
                            action: 'update_field',
                            ok: true,
                            status: result.status,
                            ...(equationWarnings.length
                                ? { equationWarnings }
                                : {}),
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
                                ? {
                                      mergeNote: NESTED_MERGE_UNCERTAINTY_NOTE,
                                  }
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
                    ...(ktlKeywordWarnings.length
                        ? { ktlKeywordWarnings }
                        : {}),
                    ...result,
                    ...(result.ok
                        ? { cacheNote: SCHEMA_CACHE_STALE_NOTE }
                        : {}),
                    ...(result.ok && payloadTouchesNested
                        ? { mergeNote: NESTED_MERGE_UNCERTAINTY_NOTE }
                        : {}),
                });
            },
        );

        server.tool(
            'knack_delete_field',
            'Delete a field from a Knack object. This is destructive and cannot be undone. Requires readonly: false.',
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
                fieldKey: z
                    .string()
                    .describe('The field key to delete, e.g. field_123'),
            },
            async ({ appKey, objectKey, fieldKey }) => {
                const app = getAppOrThrow(appKey);
                assertDeletable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_delete_field',
                    args: { appKey: app.appKey, objectKey, fieldKey },
                });

                const result = await knackRequest(
                    app,
                    apiKey,
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
                    ...(result.ok
                        ? { cacheNote: SCHEMA_CACHE_STALE_NOTE }
                        : {}),
                });
            },
        );

        server.tool(
            'knack_duplicate_field',
            'Duplicate an existing field with a new name. Reads the source field definition, strips the key/_id, and creates a copy. Requires readonly: false.',
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
                sourceFieldKey: z
                    .string()
                    .describe('The field key to duplicate, e.g. field_3539'),
                newName: z.string().describe('Name for the new field'),
            },
            async ({ appKey, objectKey, sourceFieldKey, newName }) => {
                const app = getAppOrThrow(appKey);
                assertWritable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_duplicate_field',
                    args: {
                        appKey: app.appKey,
                        objectKey,
                        sourceFieldKey,
                        newName,
                    },
                });

                // Fetch the object to get the source field definition
                const objResult = (await knackRequest(
                    app,
                    apiKey,
                    `/objects/${objectKey}`,
                )) as {
                    ok: boolean;
                    body: {
                        object: { fields: Array<Record<string, unknown>> };
                    };
                };
                const fields = objResult.body?.object?.fields;
                if (!fields) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        message: 'Could not fetch object fields.',
                    });
                }

                const sourceField = fields.find(
                    (f: Record<string, unknown>) => f.key === sourceFieldKey,
                );
                if (!sourceField) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        message: `Source field ${sourceFieldKey} not found on ${objectKey}.`,
                    });
                }

                // Clone and strip identifiers
                const newField = { ...sourceField };
                delete newField.key;
                delete newField._id;
                newField.name = newName;
                // The source field may only have a top-level `description` (e.g. it
                // predates normalizeFieldDescriptionForWrite), which wouldn't reliably
                // persist on this new POST either — mirror it into meta.description too.
                normalizeFieldDescriptionForWrite(newField);

                const result = await knackRequest(
                    app,
                    apiKey,
                    `/objects/${objectKey}/fields`,
                    {
                        method: 'POST',
                        body: JSON.stringify(newField),
                    },
                );

                if (result.ok) {
                    const bodyDetail = getInlineDetail(result.body);
                    if (!bodyDetail.included) {
                        // Same connection-field bloat as knack_create_field: Knack
                        // returns the full application schema, not just the field.
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
                            ...(duplicatedField
                                ? { field: duplicatedField }
                                : {}),
                            bodySizeBytes: bodyDetail.sizeBytes,
                            bodySummary: bodyDetail.summary,
                            note: duplicatedField
                                ? "Knack's response for this write included the full application schema (expected for connection fields, since they update the cross-object relationship graph) — projected down to the duplicated field above plus a structural summary. Call knack_get_object_fields for the full raw field definition if needed."
                                : `Knack's response for this write included the full application schema. Could not unambiguously identify the duplicated field in it (e.g. another field named "${newName}" of the same type may already exist on ${objectKey} — Knack field names aren't unique — or the response may not have included this object at all) — call knack_get_object_fields on ${objectKey} to find the new field's key.`,
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
                    ...(result.ok
                        ? { cacheNote: SCHEMA_CACHE_STALE_NOTE }
                        : {}),
                });
            },
        );

        server.tool(
            'knack_create_record',
            'Create a new record in a Knack object. Requires readonly: false.',
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
                data: z
                    .string()
                    .describe(
                        'Record data as JSON string with field_key: value pairs',
                    ),
            },
            async ({ appKey, objectKey, data }) => {
                const app = getAppOrThrow(appKey);
                assertWritable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_create_record',
                    args: { appKey: app.appKey, objectKey },
                });

                const result = await knackRequest(
                    app,
                    apiKey,
                    `/objects/${objectKey}/records`,
                    {
                        method: 'POST',
                        body: data,
                    },
                );
                return makeTextResponse({
                    appKey: app.appKey,
                    objectKey,
                    action: 'create_record',
                    ...result,
                });
            },
        );

        server.tool(
            'knack_update_record',
            'Update an existing record in a Knack object. Requires readonly: false.',
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
                recordId: z.string().describe('The record ID to update'),
                data: z
                    .string()
                    .describe(
                        'Fields to update as JSON string with field_key: value pairs',
                    ),
            },
            async ({ appKey, objectKey, recordId, data }) => {
                const app = getAppOrThrow(appKey);
                assertWritable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_update_record',
                    args: { appKey: app.appKey, objectKey, recordId },
                });

                const result = await knackRequest(
                    app,
                    apiKey,
                    `/objects/${objectKey}/records/${recordId}`,
                    {
                        method: 'PUT',
                        body: data,
                    },
                );
                return makeTextResponse({
                    appKey: app.appKey,
                    objectKey,
                    recordId,
                    action: 'update_record',
                    ...result,
                });
            },
        );

        server.tool(
            'knack_delete_record',
            'Delete a record from a Knack object. This is destructive and cannot be undone. Requires readonly: false.',
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
                recordId: z.string().describe('The record ID to delete'),
            },
            async ({ appKey, objectKey, recordId }) => {
                const app = getAppOrThrow(appKey);
                assertDeletable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_delete_record',
                    args: { appKey: app.appKey, objectKey, recordId },
                });

                const result = await knackRequest(
                    app,
                    apiKey,
                    `/objects/${objectKey}/records/${recordId}`,
                    {
                        method: 'DELETE',
                    },
                );
                return makeTextResponse({
                    appKey: app.appKey,
                    objectKey,
                    recordId,
                    action: 'delete_record',
                    ...result,
                });
            },
        );

        server.tool(
            'knack_batch_create_records',
            "Create multiple records in a Knack object in one call. Each record is created with its own API request, run with limited concurrency and retry-on-429 (not 5xx, to avoid risking a duplicate create), so one failure does not abort the rest — per-record results are reported individually. Requires readonly: false. Pass dryRun: true to validate every record's JSON without creating anything.",
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
                records: z
                    .array(z.string())
                    .min(1)
                    .max(100)
                    .describe(
                        'Array of record data JSON strings, one per record (same shape as knack_create_record\'s data param, e.g. \'{"field_1":"value"}\'). Max 100 per call.',
                    ),
                dryRun: z
                    .boolean()
                    .optional()
                    .default(false)
                    .describe(
                        "Validate every record's JSON and return the count without creating anything.",
                    ),
            },
            async ({ appKey, objectKey, records, dryRun }) => {
                const app = getAppOrThrow(appKey);
                assertWritable(app);
                debugLog('tool_call', {
                    tool: 'knack_batch_create_records',
                    args: {
                        appKey: app.appKey,
                        objectKey,
                        count: records.length,
                        dryRun,
                    },
                });

                const parsedRecords = records.map((raw, index) => ({
                    index,
                    ...parseJsonObjectInput(raw, `records[${index}]`),
                }));
                const invalid = parsedRecords.filter(
                    (entry) => entry.errors.length,
                );
                if (invalid.length) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        action: 'batch_create_records_preflight',
                        errors: invalid.flatMap((entry) => entry.errors),
                    });
                }

                if (dryRun) {
                    return makeTextResponse({
                        ok: true,
                        appKey: app.appKey,
                        objectKey,
                        action: 'batch_create_records_dry_run',
                        dryRun: true,
                        wouldCreateCount: parsedRecords.length,
                        wouldCreate: parsedRecords.map(
                            (entry) => entry.payload,
                        ),
                    });
                }

                const apiKey = getApiKeyOrThrow(app.appKey);

                const itemResults = await runWithConcurrency(
                    parsedRecords,
                    BATCH_CONCURRENCY,
                    async (entry) => {
                        try {
                            const result = await knackRequestWithRetry(
                                app,
                                apiKey,
                                `/objects/${objectKey}/records`,
                                {
                                    method: 'POST',
                                    body: JSON.stringify(entry.payload),
                                },
                            );
                            return {
                                index: entry.index,
                                ok: result.ok,
                                status: result.status,
                                body: result.body,
                            };
                        } catch (error) {
                            return {
                                index: entry.index,
                                ok: false,
                                error:
                                    error instanceof Error
                                        ? error.message
                                        : String(error),
                            };
                        }
                    },
                );

                const successCount = itemResults.filter((r) => r.ok).length;
                const failureCount = itemResults.length - successCount;

                return makeTextResponse({
                    ok: failureCount === 0,
                    appKey: app.appKey,
                    objectKey,
                    action: 'batch_create_records',
                    requestedCount: records.length,
                    successCount,
                    failureCount,
                    results: itemResults,
                    note: `Records were created with up to ${BATCH_CONCURRENCY} requests in flight at once, retrying individual requests on a 429 with backoff (not on 5xx — a lost/delayed 5xx response after a create that actually succeeded would otherwise risk creating a duplicate record). Check each entry in results for its own ok/status rather than assuming the whole batch succeeded.`,
                });
            },
        );

        server.tool(
            'knack_batch_update_records',
            "Update multiple existing records in a Knack object in one call. Each record is updated with its own API request, run with limited concurrency and retry-on-429/5xx, so one failure does not abort the rest — per-record results are reported individually. Requires readonly: false. Pass dryRun: true to validate every record's JSON without persisting anything.",
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
                records: z
                    .array(
                        z.object({
                            recordId: z
                                .string()
                                .describe('The record ID to update'),
                            data: z
                                .string()
                                .describe(
                                    'Fields to update as a JSON string with field_key: value pairs',
                                ),
                        }),
                    )
                    .min(1)
                    .max(100)
                    .describe(
                        'Array of {recordId, data} pairs, one per record. Max 100 per call.',
                    ),
                dryRun: z
                    .boolean()
                    .optional()
                    .default(false)
                    .describe(
                        "Validate every record's JSON and return the count without persisting anything.",
                    ),
            },
            async ({ appKey, objectKey, records, dryRun }) => {
                const app = getAppOrThrow(appKey);
                assertWritable(app);
                debugLog('tool_call', {
                    tool: 'knack_batch_update_records',
                    args: {
                        appKey: app.appKey,
                        objectKey,
                        count: records.length,
                        dryRun,
                    },
                });

                const parsedRecords = records.map((record, index) => ({
                    index,
                    recordId: record.recordId,
                    ...parseJsonObjectInput(
                        record.data,
                        `records[${index}].data`,
                    ),
                }));
                const invalid = parsedRecords.filter(
                    (entry) => entry.errors.length,
                );
                if (invalid.length) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        action: 'batch_update_records_preflight',
                        errors: invalid.flatMap((entry) => entry.errors),
                    });
                }

                if (dryRun) {
                    return makeTextResponse({
                        ok: true,
                        appKey: app.appKey,
                        objectKey,
                        action: 'batch_update_records_dry_run',
                        dryRun: true,
                        wouldUpdateCount: parsedRecords.length,
                        wouldUpdate: parsedRecords.map((entry) => ({
                            recordId: entry.recordId,
                            data: entry.payload,
                        })),
                    });
                }

                const apiKey = getApiKeyOrThrow(app.appKey);

                const itemResults = await runWithConcurrency(
                    parsedRecords,
                    BATCH_CONCURRENCY,
                    async (entry) => {
                        try {
                            const result = await knackRequestWithRetry(
                                app,
                                apiKey,
                                `/objects/${objectKey}/records/${entry.recordId}`,
                                {
                                    method: 'PUT',
                                    body: JSON.stringify(entry.payload),
                                },
                            );
                            return {
                                index: entry.index,
                                recordId: entry.recordId,
                                ok: result.ok,
                                status: result.status,
                                body: result.body,
                            };
                        } catch (error) {
                            return {
                                index: entry.index,
                                recordId: entry.recordId,
                                ok: false,
                                error:
                                    error instanceof Error
                                        ? error.message
                                        : String(error),
                            };
                        }
                    },
                );

                const successCount = itemResults.filter((r) => r.ok).length;
                const failureCount = itemResults.length - successCount;

                return makeTextResponse({
                    ok: failureCount === 0,
                    appKey: app.appKey,
                    objectKey,
                    action: 'batch_update_records',
                    requestedCount: records.length,
                    successCount,
                    failureCount,
                    results: itemResults,
                    note: `Records were updated with up to ${BATCH_CONCURRENCY} requests in flight at once, retrying individual requests on a 429/5xx with backoff. Check each entry in results for its own ok/status rather than assuming the whole batch succeeded.`,
                });
            },
        );

        server.tool(
            'knack_batch_delete_records',
            'Delete multiple records from a Knack object in one call. This is destructive and cannot be undone. Blocked by default — pass confirm: true only after explicitly confirming the record count and object with the user. Requires readonly: false and allowDelete: true.',
            {
                appKey: z.string().optional(),
                objectKey: z.string().describe('The object key, e.g. object_2'),
                recordIds: z
                    .array(z.string())
                    .min(1)
                    .max(100)
                    .describe('Record IDs to delete. Max 100 per call.'),
                confirm: z
                    .boolean()
                    .optional()
                    .default(false)
                    .describe(
                        'Must be true to actually delete. When false, returns a preview of what would be deleted instead of deleting anything.',
                    ),
            },
            async ({ appKey, objectKey, recordIds, confirm }) => {
                const app = getAppOrThrow(appKey);
                assertDeletable(app);
                debugLog('tool_call', {
                    tool: 'knack_batch_delete_records',
                    args: {
                        appKey: app.appKey,
                        objectKey,
                        count: recordIds.length,
                        confirm,
                    },
                });

                if (!confirm) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        objectKey,
                        action: 'batch_delete_records_preflight',
                        message: `This would permanently delete ${recordIds.length} record(s) from ${objectKey}. This cannot be undone. Pass confirm: true only after explicitly confirming this with the user.`,
                        wouldDeleteCount: recordIds.length,
                        wouldDeleteRecordIds: recordIds,
                    });
                }

                const apiKey = getApiKeyOrThrow(app.appKey);

                const itemResults = await runWithConcurrency(
                    recordIds,
                    BATCH_CONCURRENCY,
                    async (recordId) => {
                        try {
                            const result = await knackRequestWithRetry(
                                app,
                                apiKey,
                                `/objects/${objectKey}/records/${recordId}`,
                                { method: 'DELETE' },
                            );
                            return {
                                recordId,
                                ok: result.ok,
                                status: result.status,
                                body: result.body,
                            };
                        } catch (error) {
                            return {
                                recordId,
                                ok: false,
                                error:
                                    error instanceof Error
                                        ? error.message
                                        : String(error),
                            };
                        }
                    },
                );

                const successCount = itemResults.filter((r) => r.ok).length;
                const failureCount = itemResults.length - successCount;

                return makeTextResponse({
                    ok: failureCount === 0,
                    appKey: app.appKey,
                    objectKey,
                    action: 'batch_delete_records',
                    requestedCount: recordIds.length,
                    successCount,
                    failureCount,
                    results: itemResults,
                    note: `Records were deleted with up to ${BATCH_CONCURRENCY} requests in flight at once, retrying individual requests on a 429/5xx with backoff. Check each entry in results for its own ok/status — a partial failure means some records were deleted and others were not.`,
                });
            },
        );

        server.tool(
            'knack_upload_asset',
            'Upload a local file to Knack as an asset (file or image). Returns the asset id, which can then be used as the value for a file/image field in knack_create_record or knack_update_record. Requires readonly: false.',
            {
                appKey: z.string().optional(),
                filePath: z
                    .string()
                    .describe('Absolute path to the local file to upload'),
                assetType: z
                    .enum(['file', 'image'])
                    .default('file')
                    .describe(
                        'Knack asset type: "file" for file fields, "image" for image fields',
                    ),
            },
            async ({ appKey, filePath, assetType }) => {
                const app = getAppOrThrow(appKey);
                assertWritable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_upload_asset',
                    args: { appKey: app.appKey, filePath, assetType },
                });

                if (!fs.existsSync(filePath)) {
                    return makeTextResponse({
                        ok: false,
                        status: 0,
                        body: { error: 'file_not_found', filePath },
                    });
                }
                const stat = fs.statSync(filePath);
                if (!stat.isFile()) {
                    return makeTextResponse({
                        ok: false,
                        status: 0,
                        body: { error: 'not_a_file', filePath },
                    });
                }

                const buffer = fs.readFileSync(filePath);
                const fileName = path.basename(filePath);
                const blob = new Blob([new Uint8Array(buffer)]);
                const form = new FormData();
                form.append('files', blob, fileName);

                const url = `${app.apiBase || DEFAULT_API_BASE}/applications/${encodeURIComponent(app.appId)}/assets/${assetType}/upload`;
                const result = await knackFetchJson(url, {
                    method: 'POST',
                    headers: {
                        'X-Knack-Application-Id': app.appId,
                        'X-Knack-REST-API-Key': apiKey,
                    },
                    body: form,
                });
                return makeTextResponse({
                    appKey: app.appKey,
                    action: 'upload_asset',
                    filePath,
                    fileName,
                    sizeBytes: stat.size,
                    assetType,
                    ...result,
                });
            },
        );
    }

    if (HAS_VIEW_MUTATION_TOOLS) {
        server.tool(
            'knack_get_view_payload_template',
            'Build a starter payload for a common Knack view type. Uses `table` as the canonical saved type for grid views. The source can be scoped to a connection (connectionKey + relationshipType), to the logged-in account (authenticatedUser), through a parent page hop (parentSourceObject + parentSourceConnection), and filtered (filters). The response carries the measured source shapes, so read those before hand-building one.',
            {
                appKey: z
                    .string()
                    .optional()
                    .describe(
                        'Required when you want the helper to derive fields automatically from object metadata.',
                    ),
                viewType: z
                    .enum(['grid', 'table', 'form', 'details', 'list'])
                    .describe(
                        "Common view type. `grid` is normalised to Knack's saved `table` type.",
                    ),
                objectKey: z
                    .string()
                    .optional()
                    .describe(
                        'Source object key. Required for grid/table, form, details, and list templates.',
                    ),
                sceneKey: z
                    .string()
                    .optional()
                    .describe(
                        'Optional scene/page key. When existingViewKeys are omitted, the helper derives them from this scene.',
                    ),
                name: z
                    .string()
                    .optional()
                    .describe('View name to use in the template.'),
                title: z
                    .string()
                    .optional()
                    .describe(
                        'Optional view title. Defaults to the name for record views.',
                    ),
                fieldKeys: z
                    .array(z.string())
                    .optional()
                    .describe(
                        'Field keys to include. Strongly recommended for all record-backed templates.',
                    ),
                maxFields: z
                    .number()
                    .int()
                    .min(1)
                    .max(100)
                    .optional()
                    .default(12)
                    .describe(
                        'When deriving fields from object metadata, limit the number of included fields. Ignored when fieldKeys are passed explicitly.',
                    ),
                existingViewKeys: z
                    .array(z.string())
                    .optional()
                    .describe(
                        'Existing view keys already on the page. The template appends the new view after these keys in pageGroups.',
                    ),
                connectionKey: z
                    .string()
                    .optional()
                    .describe(
                        'Scope the view to records connected through this connection field. Requires relationshipType.',
                    ),
                relationshipType: z
                    .enum(['foreign', 'local'])
                    .optional()
                    .describe(
                        'Which object owns the connection field: "foreign" when it lives on this view\'s own object and points outward, "local" when it lives on the other object and points back. Required with connectionKey, and the value to recompute when repointing a copied view at a different connection.',
                    ),
                authenticatedUser: z
                    .boolean()
                    .optional()
                    .describe(
                        "Scope to the logged-in account. Emitted only when true, since false was never observed in a real app. Works with or without connectionKey — without it, the view is scoped to the user's own record.",
                    ),
                parentSourceObject: z
                    .string()
                    .optional()
                    .describe(
                        'Object of the record context the page supplies, for a multi-hop source. Pair with parentSourceConnection.',
                    ),
                parentSourceConnection: z
                    .string()
                    .optional()
                    .describe(
                        'Connection field of that parent hop. Needed when the hop differs from connectionKey, which cannot be reconstructed from connectionKey alone.',
                    ),
                filters: z
                    .string()
                    .optional()
                    .describe(
                        'Source filter criteria as a JSON string: { match: "all"|"any", rules: [{field, operator, value}], groups: [[{field, operator, value}]] }. A group is an array of rules and carries no match of its own — its operator is the inverse of the top-level match, so with match "all" each group is an OR.',
                    ),
                sort: z
                    .string()
                    .optional()
                    .describe(
                        'Default sort as a JSON string: [{ "field": "field_1", "order": "asc" }]. Omitted stores an empty array, which is what Knack holds for a view with no sort chosen. Pass one when rebuilding an existing view — a real view\'s sort is part of its design and a rebuild that omits it looks correct while ordering differently.',
                    ),
                columnConnections: z
                    .string()
                    .optional()
                    .describe(
                        'JSON object mapping a field key to the connection field a column reaches through: { "field_10": "field_3" }. Use it for columns showing a field on a CONNECTED record rather than on this view\'s own object — Knack stores that as `connection: { key }` beside the column\'s own field. These are the references a repoint most often misses: changing the source leaves them pointing at the old relationship, and the columns still render values.',
                    ),
                noDataText: z
                    .string()
                    .optional()
                    .describe(
                        'Empty-state line for table and list views. Defaults to "No <object name> Records" when the schema was loaded, or "No records" when it was not — never left blank, since all 223 views that carry the key in a 738-view export hold a non-empty value. Ignored for view types that do not carry the key.',
                    ),
            },
            async ({
                appKey,
                viewType,
                objectKey,
                sceneKey,
                name,
                title,
                fieldKeys = [],
                maxFields = 12,
                existingViewKeys = [],
                connectionKey,
                relationshipType,
                authenticatedUser,
                parentSourceObject,
                parentSourceConnection,
                filters,
                sort,
                columnConnections,
                noDataText,
            }) => {
                const canonicalType = viewType === 'grid' ? 'table' : viewType;
                const displayName =
                    name ||
                    (viewType === 'grid'
                        ? 'Grid'
                        : canonicalType[0].toUpperCase() +
                          canonicalType.slice(1));
                const resolvedTitle = title ?? displayName;
                const notes: string[] = [];
                let schemaSource: CacheSource | null = null;
                let layoutViewKeys = existingViewKeys;
                let allObjectFields: CachedField[] = [];
                let sourceObjectName: string | null = null;

                const parsedSort = sort
                    ? parseJsonInput<ViewSourceSort[]>('sort', sort)
                    : undefined;

                let parsedColumnConnections: Record<string, string> | undefined;
                if (columnConnections) {
                    const raw = parseJsonInput<unknown>(
                        'columnConnections',
                        columnConnections,
                    );

                    // parseJsonInput casts rather than checks, so nothing above this
                    // line establishes the shape. Left unvalidated, an array was
                    // reported as a set of unmatched entries and `{field_2: 42}` was
                    // emitted as `connection: {key: 42}`.
                    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
                        throw new Error(
                            'columnConnections must be a JSON object mapping a field key to a connection field key, e.g. { "field_10": "field_3" }.',
                        );
                    }

                    for (const [fieldKey, connectionKey] of Object.entries(
                        raw,
                    )) {
                        if (
                            typeof connectionKey !== 'string' ||
                            connectionKey.trim() === ''
                        ) {
                            throw new Error(
                                `columnConnections["${fieldKey}"] must be a non-empty string naming the connection field to reach through, not ${JSON.stringify(connectionKey)}.`,
                            );
                        }
                    }

                    parsedColumnConnections = raw as Record<string, string>;
                }

                if (!objectKey) {
                    throw new Error(
                        'objectKey is required for common record-backed view templates.',
                    );
                }

                // The schema is loaded whenever an appKey is available, not only when
                // fields have to be derived. Passing fieldKeys used to skip this whole
                // branch, which had two visible costs: every column header fell back to
                // the raw field key, so a generated view read as `field_196` in the
                // builder where a hand-built one reads its label; and a sceneKey given
                // without existingViewKeys never derived them. Both were measured on a
                // live app on 2026-09-03.
                if (appKey) {
                    const app = getAppOrThrow(appKey);
                    const { schema, source } = await getSchemaForApp(app);
                    schemaSource = source;
                    const sourceObject = schema?.objects?.find(
                        (object) => object.key === objectKey,
                    );
                    allObjectFields = sourceObject?.fields || [];
                    sourceObjectName = sourceObject?.name ?? null;

                    if (sceneKey) {
                        const scenes = await getScenesForApp(app);
                        const sceneViewKeys = getSceneViewKeys(
                            scenes,
                            sceneKey,
                        );

                        if (layoutViewKeys.length === 0) {
                            layoutViewKeys = sceneViewKeys;
                            if (layoutViewKeys.length > 0) {
                                notes.push(
                                    `Derived ${layoutViewKeys.length} existing view key(s) from scene ${sceneKey}.`,
                                );
                            }
                        } else {
                            // The scene is read even when keys were passed, purely to
                            // catch a list that has gone stale between two creates.
                            const gap = describeLayoutKeyGap(
                                layoutViewKeys,
                                sceneViewKeys,
                            );
                            if (gap) notes.push(gap);
                        }
                    }
                }

                const resolved = resolveTemplateFields({
                    fieldKeys,
                    allObjectFields,
                    objectKey,
                    canonicalType,
                    maxFields,
                });
                const { derivedFromSchema } = resolved;
                notes.push(...resolved.notes);

                const fieldDescriptors = parsedColumnConnections
                    ? resolved.fieldDescriptors.map((descriptor) => {
                          const connectionKey =
                              parsedColumnConnections[descriptor.key];
                          return connectionKey
                              ? { ...descriptor, connectionKey }
                              : descriptor;
                      })
                    : resolved.fieldDescriptors;

                if (parsedColumnConnections && canonicalType === 'form') {
                    // A form input reaches a connected record through
                    // `source.connections[]`, an entirely different shape from a
                    // column's `connection: { key }`. Accepting the option here and
                    // dropping it is how the response came to report connections the
                    // payload did not carry.
                    throw new Error(
                        "columnConnections does not apply to a form template. A form input filters a connection through source.connections[], which is a different shape from a column's connection: { key } — build the form and set its input sources explicitly.",
                    );
                }

                if (parsedColumnConnections) {
                    const matched = fieldDescriptors.filter(
                        (descriptor) => descriptor.connectionKey,
                    );
                    const unmatched = Object.keys(
                        parsedColumnConnections,
                    ).filter(
                        (fieldKey) =>
                            !fieldDescriptors.some(
                                (descriptor) => descriptor.key === fieldKey,
                            ),
                    );

                    notes.push(
                        `${matched.length} of ${fieldDescriptors.length} column(s) will reach through a connection, emitted as connection: { key } beside the column's own field. This is a DISPLAY path out of the view's own object and is independent of the source's scoping: a rescope leaves it correct, and only changing source.object invalidates it.`,
                    );

                    if (unmatched.length > 0) {
                        notes.push(
                            `columnConnections named ${unmatched.length} field(s) that are not among this template's columns (${unmatched.join(', ')}); those entries did nothing. A connection on a column the view does not show is silently inert.`,
                        );
                    }
                }

                if (fieldKeys.length === 0 && !appKey) {
                    notes.push(
                        'No fieldKeys were supplied. Pass appKey to derive starter fields from object metadata, or provide fieldKeys explicitly.',
                    );
                    if (sceneKey && layoutViewKeys.length === 0) {
                        notes.push(
                            'sceneKey was provided, but appKey is required to derive existingViewKeys from scene metadata.',
                        );
                    }
                }

                const pageGroups = buildStarterPageGroups(layoutViewKeys);

                let parsedFilters: ViewSourceFilters | undefined;
                if (filters !== undefined) {
                    try {
                        parsedFilters = JSON.parse(
                            filters,
                        ) as ViewSourceFilters;
                    } catch (error) {
                        throw new Error(
                            `filters is not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
                            { cause: error },
                        );
                    }
                }

                if (
                    (parentSourceObject && !parentSourceConnection) ||
                    (parentSourceConnection && !parentSourceObject)
                ) {
                    throw new Error(
                        'parentSourceObject and parentSourceConnection must be passed together — a half-specified hop cannot be resolved.',
                    );
                }

                // Every branch below shares one source, so a connected or filtered
                // source is available on each view type rather than only on tables.
                const viewSource = buildViewSource({
                    objectKey,
                    connectionKey,
                    relationshipType,
                    authenticatedUser,
                    parentSource:
                        parentSourceObject && parentSourceConnection
                            ? {
                                  object: parentSourceObject,
                                  connection: parentSourceConnection,
                              }
                            : undefined,
                    filters: parsedFilters,
                    sort: parsedSort,
                });

                if (connectionKey) {
                    notes.push(
                        `Source is scoped through ${connectionKey} with relationship_type "${relationshipType}". That value follows which object owns the connection field, so recompute it rather than copying it when repointing this view at a different connection.`,
                    );
                }

                if (parsedFilters?.groups?.length) {
                    notes.push(
                        `Filter carries ${parsedFilters.groups.length} group(s). With match "${parsedFilters.match ?? 'all'}", each group combines internally as ${(parsedFilters.match ?? 'all') === 'all' ? 'OR' : 'AND'} — the inverse of the top-level match.`,
                    );
                }

                // Resolved for every type, applied only to the two that carry
                // the key. An explicit value wins; otherwise the object's own
                // name is used, and a bare fallback when no schema was loaded.
                const resolvedNoDataText =
                    noDataText ?? buildNoDataText(sourceObjectName);

                if (viewTypeCarriesNoDataText(canonicalType)) {
                    notes.push(
                        `no_data_text set to "${resolvedNoDataText}". Left unset, Knack stores an empty string and the view falls back to its stock empty-state line; all 223 views carrying the key in a 738-view export held a non-empty value. Knack does not template this string, so it cannot vary per record set — pass noDataText to override.`,
                    );
                } else if (noDataText !== undefined) {
                    notes.push(
                        `noDataText was ignored: a ${canonicalType} view does not carry no_data_text. Measured across a 738-view export, the key appears only on table and list views.`,
                    );
                }

                const payload = buildViewTemplatePayload({
                    canonicalType,
                    displayName,
                    resolvedTitle,
                    viewSource,
                    fieldDescriptors,
                    pageGroups,
                    noDataText: resolvedNoDataText,
                });

                if (canonicalType === 'table') {
                    notes.push('Knack stores grid views as type `table`.');
                } else if (canonicalType === 'form') {
                    notes.push(
                        'Review the generated inputs and rules before creating the form, especially when the object includes connection or special field types.',
                    );
                }

                const payloadDetail = getInlineDetail(payload);

                return makeTextResponse({
                    ok: true,
                    action: 'view_payload_template',
                    appKey: appKey || null,
                    objectKey,
                    sceneKey: sceneKey || null,
                    requestedViewType: viewType,
                    canonicalViewType: canonicalType,
                    derivedFromSchema,
                    schemaSource,
                    layoutDerivedFromScene:
                        layoutViewKeys.length > 0 &&
                        existingViewKeys.length === 0,
                    existingViewKeysUsed: layoutViewKeys,
                    fieldKeysUsed: fieldDescriptors.map((field) => field.key),
                    viewSourceShape: KNACK_VIEW_SOURCE_SHAPE,
                    payloadIncluded: payloadDetail.included,
                    payloadSizeBytes: payloadDetail.sizeBytes,
                    payload: payloadDetail.value,
                    payloadSummary: payloadDetail.summary,
                    notes,
                });
            },
        );

        server.tool(
            'knack_get_view_payload_template_from_view',

            'Build a starter create-view payload by cloning an existing view from runtime metadata or cached viewMap.json. Only details/list conversion is supported because their layout shapes are compatible; configured columns, including static title and divider elements, are preserved.',

            {
                appKey: z.string().optional(),

                sourceViewKey: z
                    .string()
                    .describe('Existing view key to clone from view metadata.'),

                targetViewType: z
                    .enum(['grid', 'table', 'form', 'details', 'list'])
                    .optional()
                    .describe(
                        "Optional type for the cloned view. Only a same-type clone or details/list conversion is allowed; `grid` is normalised to Knack's saved `table` type.",
                    ),

                sceneKey: z
                    .string()
                    .optional()
                    .describe(
                        'Optional target scene/page key. When existingViewKeys are omitted, the helper derives the destination layout from this scene.',
                    ),

                name: z
                    .string()
                    .optional()
                    .describe(
                        'Optional new view name. Defaults to the source view name with " Copy" appended.',
                    ),

                title: z
                    .string()
                    .optional()
                    .describe(
                        'Optional title override. When omitted, the source title is preserved.',
                    ),

                existingViewKeys: z
                    .array(z.string())
                    .optional()
                    .describe(
                        'Existing view keys already on the target page. If omitted, the helper uses the source scene view order when available.',
                    ),

                noDataText: z
                    .string()
                    .optional()
                    .describe(
                        "Empty-state line for a table or list target. When omitted the clone keeps the source view's value, and one is derived from the object name if the source had none — which is what a details-to-list conversion needs, since a details view carries no such key.",
                    ),
            },

            async ({
                appKey,
                sourceViewKey,
                targetViewType,
                sceneKey,
                name,
                title,
                existingViewKeys,
                noDataText,
            }) => {
                const app = getAppOrThrow(appKey);

                debugLog('tool_call', {
                    tool: 'knack_get_view_payload_template_from_view',
                    args: { appKey: app.appKey, sourceViewKey, targetViewType },
                });

                const { viewMap, source } = await getViewMapForApp(app);

                if (!viewMap) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        message:
                            'No view map available from runtime API or viewMap.json.',
                    });
                }

                const sourceAttributes = asRecord(viewMap[sourceViewKey]);
                if (!sourceAttributes) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        sourceViewKey,
                        source,
                        message: `View not found in view metadata: ${sourceViewKey}`,
                    });
                }

                const sourceViewType =
                    typeof sourceAttributes.type === 'string'
                        ? sourceAttributes.type
                        : null;

                const canonicalSourceViewType =
                    sourceViewType === 'grid' ? 'table' : sourceViewType;

                const canonicalTargetViewType =
                    targetViewType === 'grid' ? 'table' : targetViewType;

                const isDetailsListConversion =
                    (canonicalSourceViewType === 'details' &&
                        canonicalTargetViewType === 'list') ||
                    (canonicalSourceViewType === 'list' &&
                        canonicalTargetViewType === 'details');

                if (
                    canonicalTargetViewType &&
                    canonicalTargetViewType !== canonicalSourceViewType &&
                    !isDetailsListConversion
                ) {
                    return makeTextResponse({
                        ok: false,

                        appKey: app.appKey,

                        sourceViewKey,

                        sourceViewType,

                        requestedTargetViewType: targetViewType || null,

                        message:
                            'This helper only supports details/list conversion. Other view types require a type-specific payload rather than a cloned layout.',
                    });
                }

                const payload = cloneJsonValue(sourceAttributes) as Record<
                    string,
                    unknown
                >;

                delete payload._id;

                delete payload.key;

                if (canonicalTargetViewType) {
                    payload.type = canonicalTargetViewType;
                }

                const sourceName =
                    typeof sourceAttributes.name === 'string'
                        ? sourceAttributes.name
                        : sourceViewKey;
                payload.name = name || `${sourceName} Copy`;
                if (title !== undefined) {
                    payload.title = title;
                }

                const viewContextMap = await getViewContextMapForApp(app);
                const context = viewContextMap[sourceViewKey] || {};
                const scenes = await getScenesForApp(app);
                const sourceSceneKey = context.sceneKey;
                const derivedSceneKey = sceneKey || sourceSceneKey;
                const sceneViews = getSceneViewKeys(scenes, derivedSceneKey);
                const layoutViewKeys =
                    existingViewKeys && existingViewKeys.length > 0
                        ? existingViewKeys
                        : sceneViews;

                if (layoutViewKeys.length > 0) {
                    payload.pageGroups = buildStarterPageGroups(layoutViewKeys);
                }

                // A details view carries no `no_data_text`, so converting one to
                // a list would otherwise produce a list with no empty-state line
                // — the one case where cloning silently loses a setting the
                // target type expects.
                const effectiveTargetType =
                    canonicalTargetViewType || canonicalSourceViewType || '';
                const noDataTextNotes: string[] = [];

                if (viewTypeCarriesNoDataText(effectiveTargetType)) {
                    const clonedNoDataText =
                        typeof payload.no_data_text === 'string'
                            ? payload.no_data_text.trim()
                            : '';

                    if (noDataText !== undefined) {
                        payload.no_data_text = noDataText;
                        noDataTextNotes.push(
                            `no_data_text was overridden to "${noDataText}".`,
                        );
                    } else if (!clonedNoDataText) {
                        const sourceObjectKey = asRecord(
                            payload.source,
                        )?.object;
                        let objectName: string | null = null;

                        if (typeof sourceObjectKey === 'string') {
                            const { schema } = await getSchemaForApp(app);
                            objectName =
                                schema?.objects?.find(
                                    (object) => object.key === sourceObjectKey,
                                )?.name ?? null;
                        }

                        payload.no_data_text = buildNoDataText(objectName);
                        noDataTextNotes.push(
                            `The source view carried no no_data_text, so one was derived for the ${effectiveTargetType} target: "${String(payload.no_data_text)}". Pass noDataText to set it explicitly.`,
                        );
                    }
                } else if (noDataText !== undefined) {
                    noDataTextNotes.push(
                        `noDataText was ignored: a ${effectiveTargetType || 'clone of this'} view does not carry no_data_text.`,
                    );
                }

                const payloadDetail = getInlineDetail(payload);

                return makeTextResponse({
                    ok: true,
                    action: 'view_payload_template_from_view',
                    appKey: app.appKey,
                    source,
                    sourceViewKey,

                    sourceViewType,

                    requestedTargetViewType: targetViewType || null,

                    targetViewType:
                        canonicalTargetViewType ||
                        (typeof sourceAttributes.type === 'string'
                            ? sourceAttributes.type
                            : null),

                    sourceSceneKey: sourceSceneKey || null,

                    targetSceneKey: derivedSceneKey || null,

                    existingViewKeysUsed: layoutViewKeys,
                    payloadIncluded: payloadDetail.included,
                    payloadSizeBytes: payloadDetail.sizeBytes,
                    payload: payloadDetail.value,
                    payloadSummary: payloadDetail.summary,
                    notes: [
                        'The payload was cloned from existing view metadata with key/_id removed.',

                        canonicalTargetViewType
                            ? `The cloned view type was changed to ${canonicalTargetViewType}; configured columns, including static elements, were preserved.`
                            : 'The cloned view type was preserved from the source view.',

                        layoutViewKeys.length > 0
                            ? `pageGroups were rebuilt using ${layoutViewKeys.length} existing view key(s).`
                            : 'No pageGroups were derived automatically. Supply existingViewKeys if the target page layout matters.',

                        ...noDataTextNotes,
                    ],
                });
            },
        );

        server.tool(
            'knack_snapshot_app',
            'Write a timestamped restore point for this app: the full scene tree (routes, slugs, parent pages), a pointer to the object schema on disk, and optionally one view definition. Take one before any Knack builder change too — the server never sees builder-side edits, and this is the only record that can rebuild a cascade-deleted page tree.',
            {
                appKey: z.string().optional(),
                sceneKey: z
                    .string()
                    .optional()
                    .describe('Optional scene to name the snapshot after.'),
                viewKey: z
                    .string()
                    .optional()
                    .describe(
                        'Optional view to capture in full. Requires sceneKey.',
                    ),
            },
            async ({ appKey, sceneKey, viewKey }) => {
                const app = getAppOrThrow(appKey);
                debugLog('tool_call', {
                    tool: 'knack_snapshot_app',
                    args: { appKey: app.appKey, sceneKey, viewKey },
                });

                if (viewKey && !sceneKey) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        action: 'snapshot_app',
                        error: 'INVALID_INPUT',
                        message:
                            'viewKey requires sceneKey — a view can only be read through the scene that owns it. Supply both to capture the view, or neither to snapshot scenes and schema only. Refusing rather than writing a snapshot with no view in it and reporting success.',
                    });
                }

                let view: unknown;
                if (sceneKey && viewKey) {
                    // Read from runtime metadata, exactly as the guard's preflight
                    // does. Knack serves no read handler on
                    // /scenes/<scene>/views/<view> — every host answers with a
                    // web-server HTML 404 — so this tool asked for a route that does
                    // not exist and refused every view-inclusive snapshot with
                    // COULD_NOT_VERIFY_VIEW. The preflight was moved off that route;
                    // this call site was missed, which left the one tool whose whole
                    // job is capturing a restore point unable to capture a view.
                    //
                    // Uncached for the same reason the preflight is: a restore point
                    // describing the app as it stood up to five minutes ago is worse
                    // than an obvious failure.
                    runtimeMetadataCache.delete(app.appKey);
                    const metadata = await getRuntimeMetadata(app);
                    if (!metadata) {
                        return makeTextResponse({
                            ok: false,
                            appKey: app.appKey,
                            action: 'snapshot_app',
                            error: 'COULD_NOT_VERIFY_VIEW',
                            message: `Runtime metadata could not be fetched from Knack, so ${viewKey} could not be read and the snapshot would be incomplete. Retry, or omit viewKey to snapshot scenes and schema only.`,
                        });
                    }

                    const found = findRawViewInMetadata(
                        metadata,
                        sceneKey,
                        viewKey,
                    );
                    if (!found) {
                        return makeTextResponse({
                            ok: false,
                            appKey: app.appKey,
                            action: 'snapshot_app',
                            error: 'COULD_NOT_VERIFY_VIEW',
                            message: `${viewKey} was not found in ${sceneKey} in this app's metadata, so the snapshot would be incomplete. Check both keys, or omit viewKey to snapshot scenes and schema only.`,
                        });
                    }
                    view = found;
                }

                const result = await writeMutationSnapshot(app, {
                    action: 'manual',
                    sceneKey,
                    viewKey,
                    view,
                });

                if (!result.ok) {
                    return makeTextResponse({
                        ok: false,
                        appKey: app.appKey,
                        action: 'snapshot_app',
                        error: 'SNAPSHOT_FAILED',
                        message: `Could not write the snapshot: ${result.error}. Check KNACK_APPS_DIR and the app folder are writable.`,
                    });
                }

                return makeTextResponse({
                    ok: true,
                    appKey: app.appKey,
                    action: 'snapshot_app',
                    snapshotPath: result.path,
                    viewIncluded: Boolean(view),
                });
            },
        );

        server.tool(
            'knack_create_view',
            'Create a new view on a Knack scene/page. Requires "allowViewMutation": true in app.json.',
            {
                appKey: z.string().optional(),
                sceneKey: z
                    .string()
                    .describe('The scene/page key, e.g. scene_84'),
                payload: z
                    .string()
                    .describe(
                        'Full view definition as a JSON string. Include the type-specific properties and pageGroups.',
                    ),
            },
            async ({ appKey, sceneKey, payload }) => {
                const app = getAppOrThrow(appKey);
                assertViewWritable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_create_view',
                    args: { appKey: app.appKey, sceneKey },
                });

                return makeTextResponse(
                    await runViewMutationTool(
                        app,
                        apiKey,
                        {
                            action: 'create_view',
                            sceneKey,
                            updates: payload,
                        },
                        () =>
                            knackRequest(
                                app,
                                apiKey,
                                `/scenes/${sceneKey}/views`,
                                { method: 'POST', body: payload },
                            ),
                    ),
                );
            },
        );

        server.tool(
            'knack_update_view_order',
            'Update the order/layout of views on a Knack scene/page. Requires "allowViewMutation": true.',
            {
                appKey: z.string().optional(),
                sceneKey: z
                    .string()
                    .describe('The scene/page key, e.g. scene_84'),
                order: z
                    .string()
                    .describe(
                        'Order array as a JSON string, typically an array of view keys in the desired order.',
                    ),
                pageGroups: z
                    .string()
                    .describe('Page groups layout as a JSON string.'),
            },
            async ({ appKey, sceneKey, order, pageGroups }) => {
                const app = getAppOrThrow(appKey);
                assertViewWritable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_update_view_order',
                    args: { appKey: app.appKey, sceneKey },
                });

                const body = JSON.stringify({
                    order: parseJsonInput<unknown[]>('order', order),
                    pageGroups: parseJsonInput<unknown[]>(
                        'pageGroups',
                        pageGroups,
                    ),
                });

                return makeTextResponse(
                    await runViewMutationTool(
                        app,
                        apiKey,
                        {
                            action: 'update_view_order',
                            sceneKey,
                            // Passed so the payload gets the same depth and links
                            // inspection as any other caller-supplied JSON, rather
                            // than reaching the API unexamined.
                            updates: body,
                        },
                        () =>
                            knackRequest(
                                app,
                                apiKey,
                                `/scenes/${sceneKey}/views/sort`,
                                { method: 'POST', body },
                            ),
                    ),
                );
            },
        );

        server.tool(
            'knack_update_view',
            'Update an existing Knack view. Requires "allowViewMutation": true. Send only the properties you are changing: Knack\'s route replaces rather than patches, so this server reads the live definition, merges your changes into it, and sends the whole thing. Each property you do send REPLACES that whole property rather than merging into it, so {"source": {"sort": [...]}} discards the rest of `source`, filters included — read the current value and send it back whole. Works on every view type, menus included. What is guarded is losing a link: a payload that no longer carries a link to a child page destroys that page, unless another view still links to it, and that goes to the human operating the client for confirmation. The calling model cannot answer that prompt, and a client that cannot raise one cannot make the change.',
            {
                appKey: z.string().optional(),
                sceneKey: z
                    .string()
                    .describe('The scene/page key, e.g. scene_84'),
                viewKey: z.string().describe('The view key, e.g. view_230'),
                updates: z.string().describe('View updates as a JSON string.'),
                confirmDestructive: z
                    .boolean()
                    .optional()
                    .describe(
                        'Removed. Any use is refused on an update that would destroy child pages. Only the human operating this client can confirm that, and they are asked directly — no parameter can answer for them.',
                    ),
            },
            async ({
                appKey,
                sceneKey,
                viewKey,
                updates,
                confirmDestructive,
            }) => {
                const app = getAppOrThrow(appKey);
                assertViewWritable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_update_view',
                    args: { appKey: app.appKey, sceneKey, viewKey },
                });

                return makeTextResponse(
                    await runViewMutationTool(
                        app,
                        apiKey,
                        {
                            action: 'update_view',
                            sceneKey,
                            viewKey,
                            updates,
                            confirmDestructive,
                        },
                        async ({ outgoingBody }) => {
                            // The guard merged this from the live definition and the
                            // caller's patch, and every decision it made — which pages
                            // die, whether a human had to agree — was made against
                            // this exact object. Rebuilding it here would put two
                            // reasoners on one payload.
                            //
                            // It is built for linked views too, now that re-sending a
                            // link column is measured not to cascade: a complete
                            // definition carrying every link is what makes a scalar
                            // edit to a linked view possible at all.
                            const completeBody = outgoingBody;

                            const result = await knackRequest(
                                app,
                                apiKey,
                                `/scenes/${sceneKey}/views/${viewKey}`,
                                {
                                    method: 'PUT',
                                    body: completeBody
                                        ? JSON.stringify(completeBody)
                                        : updates,
                                },
                            );

                            return result;
                        },
                    ),
                );
            },
        );

        server.tool(
            'knack_copy_view',
            'Copy a view from one Knack scene/page to another. Requires "allowViewMutation": true.',
            {
                appKey: z.string().optional(),
                sourceSceneKey: z
                    .string()
                    .describe(
                        'The source scene/page key that currently owns the view.',
                    ),
                targetSceneKey: z
                    .string()
                    .describe('The destination scene/page key.'),
                viewKey: z.string().describe('The view key to copy.'),
                completeViewSchema: z
                    .boolean()
                    .optional()
                    .default(false)
                    .describe(
                        "Whether to request a full schema copy, matching Knack's copyView API flag.",
                    ),
            },
            async ({
                appKey,
                sourceSceneKey,
                targetSceneKey,
                viewKey,
                completeViewSchema,
            }) => {
                const app = getAppOrThrow(appKey);
                assertViewWritable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_copy_view',
                    args: {
                        appKey: app.appKey,
                        sourceSceneKey,
                        targetSceneKey,
                        viewKey,
                        completeViewSchema,
                    },
                });

                return makeTextResponse({
                    // `sceneKey` is what the guard reports, but this tool has always
                    // named its two scenes explicitly. Keep both so a caller written
                    // against the old response shape still finds sourceSceneKey.
                    sourceSceneKey,
                    targetSceneKey,
                    ...(await runViewMutationTool(
                        app,
                        apiKey,
                        {
                            action: 'copy_view',
                            sceneKey: sourceSceneKey,
                            viewKey,
                        },
                        () =>
                            knackRequest(
                                app,
                                apiKey,
                                `/scenes/${sourceSceneKey}/copyview`,
                                {
                                    method: 'POST',
                                    body: JSON.stringify({
                                        action: 'copy',
                                        target_scene_key: targetSceneKey,
                                        view_key: viewKey,
                                        completeViewSchema,
                                    }),
                                },
                            ),
                    )),
                });
            },
        );

        server.tool(
            'knack_move_view',
            'Move a view from one Knack scene/page to another. Requires "allowViewMutation": true. Moving a view takes its links with it, so any child page reached only through this view is destroyed — that goes to the human operating the client for confirmation, naming the pages.',
            {
                appKey: z.string().optional(),
                sourceSceneKey: z
                    .string()
                    .describe(
                        'The source scene/page key that currently owns the view.',
                    ),
                targetSceneKey: z
                    .string()
                    .describe('The destination scene/page key.'),
                viewKey: z.string().describe('The view key to move.'),
                completeViewSchema: z
                    .boolean()
                    .optional()
                    .default(false)
                    .describe(
                        "Whether to request a full schema move, matching Knack's moveView API flag.",
                    ),
            },
            async ({
                appKey,
                sourceSceneKey,
                targetSceneKey,
                viewKey,
                completeViewSchema,
            }) => {
                const app = getAppOrThrow(appKey);
                assertViewWritable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_move_view',
                    args: {
                        appKey: app.appKey,
                        sourceSceneKey,
                        targetSceneKey,
                        viewKey,
                        completeViewSchema,
                    },
                });

                return makeTextResponse({
                    // `sceneKey` is what the guard reports, but this tool has always
                    // named its two scenes explicitly. Keep both so a caller written
                    // against the old response shape still finds sourceSceneKey.
                    sourceSceneKey,
                    targetSceneKey,
                    ...(await runViewMutationTool(
                        app,
                        apiKey,
                        {
                            action: 'move_view',
                            sceneKey: sourceSceneKey,
                            viewKey,
                        },
                        () =>
                            knackRequest(
                                app,
                                apiKey,
                                `/scenes/${sourceSceneKey}/copyview`,
                                {
                                    method: 'POST',
                                    body: JSON.stringify({
                                        action: 'move',
                                        target_scene_key: targetSceneKey,
                                        view_key: viewKey,
                                        completeViewSchema,
                                    }),
                                },
                            ),
                    )),
                });
            },
        );

        server.tool(
            'knack_delete_view',
            'Delete a view from a Knack scene/page. This is destructive and cannot be undone. Requires "allowViewMutation": true and "allowDelete": true. Deleting a view removes every link it holds, so any child page reached only through it is destroyed too — that goes to the human operating the client for confirmation, naming the pages. A page another view still links to survives and moves under that view.',
            {
                appKey: z.string().optional(),
                sceneKey: z
                    .string()
                    .describe('The scene/page key, e.g. scene_84'),
                viewKey: z
                    .string()
                    .describe('The view key to delete, e.g. view_230'),
            },
            async ({ appKey, sceneKey, viewKey }) => {
                const app = getAppOrThrow(appKey);
                assertViewDeletable(app);
                const apiKey = getApiKeyOrThrow(app.appKey);
                debugLog('tool_call', {
                    tool: 'knack_delete_view',
                    args: { appKey: app.appKey, sceneKey, viewKey },
                });

                return makeTextResponse(
                    await runViewMutationTool(
                        app,
                        apiKey,
                        {
                            action: 'delete_view',
                            sceneKey,
                            viewKey,
                        },
                        () =>
                            knackRequest(
                                app,
                                apiKey,
                                `/scenes/${sceneKey}/views/${viewKey}`,
                                { method: 'DELETE' },
                            ),
                    ),
                );
            },
        );
    }

    // -----------------------
    // Resources: schema / fieldMap / viewMap (per app)
    // -----------------------

    // A generic pattern: knack://<AppKey>/schema, knack://<AppKey>/fieldMap, knack://<AppKey>/viewMap
    server.resource('knack_schema', 'knack://schema', async (uri: URL) => {
        debugLog('resource_call', {
            resource: 'knack_schema',
            uri: uri.toString(),
        });
        // uri format: knack://ARC/schema
        const parts = uri.toString().replace('knack://', '').split('/');
        const appKey = parts[0];
        const type = parts[1];

        const app = appsByKey.get(appKey);
        if (!app) {
            return {
                contents: [
                    {
                        uri: uri.toString(),
                        mimeType: 'application/json',
                        text: JSON.stringify({
                            ok: false,
                            message: 'Unknown appKey',
                        }),
                    },
                ],
            };
        }

        if (type === 'schema') {
            const schemaResult = await getSchemaForApp(app);
            const schema = schemaResult.schema || {
                ok: false,
                message: 'No schema available from runtime API or schema.json.',
            };
            return {
                contents: [
                    {
                        uri: uri.toString(),
                        mimeType: 'application/json',
                        text: JSON.stringify(schema, null, 2),
                    },
                ],
            };
        }

        if (type === 'fieldMap') {
            const fieldMapResult = await getFieldMapForApp(app);
            const fieldMap = fieldMapResult.fieldMap || {
                ok: false,
                message:
                    'No field map available from runtime API or fieldMap.json.',
            };
            return {
                contents: [
                    {
                        uri: uri.toString(),
                        mimeType: 'application/json',
                        text: JSON.stringify(fieldMap, null, 2),
                    },
                ],
            };
        }

        if (type === 'viewMap') {
            const viewMapResult = await getViewMapForApp(app);
            const viewMap = viewMapResult.viewMap || {
                ok: false,
                message:
                    'No view map available from runtime API or viewMap.json.',
            };
            return {
                contents: [
                    {
                        uri: uri.toString(),
                        mimeType: 'application/json',
                        text: JSON.stringify(viewMap, null, 2),
                    },
                ],
            };
        }

        return {
            contents: [
                {
                    uri: uri.toString(),
                    mimeType: 'application/json',
                    text: JSON.stringify({
                        ok: false,
                        message: 'Unknown resource type',
                    }),
                },
            ],
        };
    });

    return server;
}

export async function main(options: ServerOptions = {}) {
    // Stated before createServer, which throws on a missing KNACK_APPS_DIR or an
    // unreadable KnackApps folder. A server that fails to start is the case where
    // knowing which code is failing matters most, and it is also the one case that
    // never reaches a tool call — so logging this afterwards would print it exactly
    // when it is not needed and omit it exactly when it is.
    //
    // Unconditional rather than behind DEBUG: a stale or broken server is not a
    // situation anyone has switched debugging on for in advance. stdout stays
    // reserved for JSON-RPC; clients surface stderr in a server log pane.
    console.error(
        `[knack-mcp] ${summariseServerBuild(
            describeServerBuild(options.readOnly === true),
        )}`,
    );

    const server = createServer(options);

    const transport = new StdioServerTransport();

    await server.connect(transport);
}

const isDirectExecution = (() => {
    const entryPath = process.argv[1];
    return entryPath
        ? import.meta.url === pathToFileURL(entryPath).href
        : false;
})();

if (isDirectExecution) {
    main().catch((err) => {
        // Important: log to stderr for MCP clients; stdout is reserved for JSON-RPC.
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[knack-mcp] startup failed: ${message}`);

        if (err instanceof Error && err.stack) {
            console.error(err.stack);
        }
        process.exit(1);
    });
}
