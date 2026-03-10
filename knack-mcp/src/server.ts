import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

import { z } from 'zod';

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';

type AppConfig = {
    appKey: string;
    appName?: string;
    appId: string;
    apiBase?: string;
    notes?: string;
    appFolder: string;
};

type SecretsMap = Record<string, string>;

type CachedSchema = {
    objects?: Array<{
        key: string;
        name?: string;
        fields?: Array<{ key: string; name?: string; type?: string }>;
    }>;
};

type CachedFieldMapEntry = {
    fieldKey: string;
    fieldType?: string | null;
};

type CachedFieldMap = Record<string, CachedFieldMapEntry>;

type CachedViewMap = Record<string, Record<string, unknown>>;

type ViewContextMap = Record<string, { sceneKey?: string; sceneName?: string; sceneSlug?: string }>;

type CacheSource = 'runtime' | 'file';

type CacheEntry<T> = {
    value: T;
    source: CacheSource;
    loadedAt: number;
    expiresAt: number;
};

type RuntimeMetadata = Record<string, unknown>;

const DEFAULT_API_BASE = 'https://api.knack.com/v1';
const DEFAULT_CACHE_TTL_MS = 5 * 60 * 1000;

const ENV_KNACK_APPS_DIR = process.env.KNACK_APPS_DIR; // e.g. C:\Work\KnackApps
const ENV_SECRETS_PATH = process.env.KNACK_MCP_SECRETS_PATH; // e.g. C:\Users\you\.knack-mcp-secrets.json
const ENV_DEBUG = process.env.DEBUG;
const ENV_CACHE_TTL_MS = process.env.KNACK_CACHE_TTL_MS;
const ENV_MAX_RESPONSE_BYTES = process.env.KNACK_MAX_RESPONSE_BYTES;

const DEBUG_ENABLED = ['1', 'true', 'yes', 'on'].includes((ENV_DEBUG || '').trim().toLowerCase());
const CACHE_TTL_MS = (() => {
    if (!ENV_CACHE_TTL_MS) return DEFAULT_CACHE_TTL_MS;
    const ttl = Number(ENV_CACHE_TTL_MS);
    if (!Number.isFinite(ttl) || ttl <= 0) return DEFAULT_CACHE_TTL_MS;
    return Math.trunc(ttl);
})();

const DEFAULT_MAX_RESPONSE_BYTES = 20 * 1024 * 1024;
const MAX_RESPONSE_BYTES = (() => {
    if (!ENV_MAX_RESPONSE_BYTES) return DEFAULT_MAX_RESPONSE_BYTES;
    const size = Number(ENV_MAX_RESPONSE_BYTES);
    if (!Number.isFinite(size) || size <= 0) return DEFAULT_MAX_RESPONSE_BYTES;
    return Math.trunc(size);
})();

function makeTextResponse(data: unknown) {
    return {
        content: [
            {
                type: 'text' as const,
                text: JSON.stringify(data, null, 2),
            },
        ],
    };
}

function normalisePath(p: string): string {
    // Normalise for Windows/Mac comparisons
    return path.resolve(p).replaceAll('\\', '/').toLowerCase();
}

function readJsonFile<T>(filePath: string): T | null {
    try {
        const raw = fs.readFileSync(filePath, 'utf8');
        return JSON.parse(raw) as T;
    } catch {
        return null;
    }
}

function writeJsonFile(filePath: string, data: unknown): { ok: true } | { ok: false; error: string } {
    try {
        fs.mkdirSync(path.dirname(filePath), { recursive: true });
        fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, 'utf8');
        return { ok: true };
    } catch (error) {
        return {
            ok: false,
            error: error instanceof Error ? error.message : String(error),
        };
    }
}

function asRecord(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
    return value as Record<string, unknown>;
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

function getCacheEntry<T>(cache: Map<string, CacheEntry<T>>, key: string): CacheEntry<T> | null {
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
    return text.trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
}

function getFieldTypeByKey(schema: CachedSchema | null): Record<string, string | null> {
    const fieldTypeByKey: Record<string, string | null> = {};
    for (const obj of schema?.objects || []) {
        for (const field of obj.fields || []) {
            fieldTypeByKey[field.key] = field.type || null;
        }
    }
    return fieldTypeByKey;
}

function generateStrictFieldMapFromSchema(schema: CachedSchema): CachedFieldMap {
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

function coerceFieldMap(value: unknown, schema: CachedSchema | null): CachedFieldMap | null {
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
        const fieldType = typeof rec.fieldType === 'string'
            ? rec.fieldType
            : fieldTypeByKey[fieldKey] ?? null;

        map[alias] = {
            fieldKey,
            fieldType,
        };
    }

    return Object.keys(map).length ? map : null;
}

function resolveAliasToFieldKey(fieldMap: CachedFieldMap, alias: string): string | null {
    const entry = fieldMap[alias];
    if (!entry) return null;
    return entry.fieldKey;
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
        const fields: Array<{ key: string; name?: string; type?: string }> = [];

        for (const fieldItem of fieldsRaw) {
            const field = asRecord(fieldItem);
            if (!field) continue;
            const fieldKey = typeof field.key === 'string' ? field.key : null;
            if (!fieldKey) continue;

            fields.push({
                key: fieldKey,
                name: typeof field.name === 'string' ? field.name : undefined,
                type: typeof field.type === 'string' ? field.type : undefined,
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
        const sceneName = typeof scene.name === 'string' ? scene.name : undefined;
        const sceneSlug = typeof scene.slug === 'string' ? scene.slug : undefined;
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
        const candidates = ['value', 'text', 'email', 'to', 'message', 'subject', 'name'];
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

function extractKtlKeywordsFromText(text: string): Array<{ keyword: string; snippet: string }> {
    const regex = /(?:^|\s|>)(_[a-zA-Z0-9_]+)/g;
    const hits: Array<{ keyword: string; snippet: string }> = [];
    let match: RegExpExecArray | null = null;

    while ((match = regex.exec(text)) !== null) {
        const keyword = match[1];
        const start = Math.max(0, (match.index || 0) - 40);
        const end = Math.min(text.length, (match.index || 0) + 200);
        const snippet = text.slice(start, end).replace(/\s+/g, ' ').trim();
        hits.push({ keyword, snippet });
    }

    return hits;
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
    seen = new WeakSet<object>()
) {
    if (!node || typeof node !== 'object') return out;
    if (seen.has(node)) return out;
    seen.add(node);

    if (Array.isArray(node)) {
        node.forEach((item, index) => collectEmailNodes(item, [...pathParts, String(index)], out, seen));
        return out;
    }

    const rec = node as Record<string, unknown>;
    const action = typeof rec.action === 'string' ? rec.action : null;
    const to = getStringFromUnknown(rec.to ?? rec.to_email ?? rec.recipient ?? rec.recipients ?? rec.email);
    const cc = getStringFromUnknown(rec.cc);
    const bcc = getStringFromUnknown(rec.bcc);
    const subject = getStringFromUnknown(rec.subject ?? rec.email_subject ?? rec.title);
    const message = getStringFromUnknown(rec.message ?? rec.email_message ?? rec.body ?? rec.text);

    const hasRecipientKey = ['to', 'to_email', 'recipient', 'recipients', 'email', 'cc', 'bcc'].some((key) => key in rec);
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
            message: 'Secrets file not found/readable. API-key tools will fail until secrets are configured.',
            secretsPath,
        });
        return {};
    }
    return secrets;
}

function discoverApps(knackAppsDir: string): AppConfig[] {
    const entries = fs.readdirSync(knackAppsDir, { withFileTypes: true })
        .filter((e) => e.isDirectory())
        .map((e) => e.name);

    const apps: AppConfig[] = [];

    for (const dirName of entries) {
        const appFolder = path.join(knackAppsDir, dirName);
        const appJsonCandidates = [
            path.join(appFolder, 'schema', 'app.json'),
            path.join(appFolder, 'app.json'),
        ];
        const appJsonPath = appJsonCandidates.find((candidate) => fileExists(candidate));
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
    const bodyAny = res.body as unknown as { getReader?: () => ReadableStreamDefaultReader<Uint8Array> } | null;
    if (!bodyAny || typeof bodyAny.getReader !== 'function') {
        const text = await res.text();
        const sizeBytes = Buffer.byteLength(text, 'utf8');
        return { text: sizeBytes > maxBytes ? '' : text, sizeBytes, tooLarge: sizeBytes > maxBytes };
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
            try { await reader.cancel(); } catch { }
            return { text: '', sizeBytes, tooLarge: true };
        }

        text += decoder.decode(value, { stream: true });
    }

    text += decoder.decode();
    try { reader.releaseLock(); } catch { }

    return { text, sizeBytes, tooLarge: false };
}

async function knackFetchJson(url: string, init: RequestInit): Promise<{ ok: boolean; status: number; body: unknown }> {
    const res = await fetch(url, init);
    const contentLength = Number(res.headers.get('content-length') || 0);
    if (contentLength && contentLength > MAX_RESPONSE_BYTES) {
        if (DEBUG_ENABLED) {
            console.error('[knack-mcp] response_too_large', JSON.stringify({
                url,
                status: res.status,
                contentLength,
                maxResponseBytes: MAX_RESPONSE_BYTES,
                precheck: true,
            }));
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

    const { text, sizeBytes, tooLarge } = await readResponseTextWithLimit(res, MAX_RESPONSE_BYTES);
    if (tooLarge) {
        if (DEBUG_ENABLED) {
            console.error('[knack-mcp] response_too_large', JSON.stringify({
                url,
                status: res.status,
                sizeBytes,
                maxResponseBytes: MAX_RESPONSE_BYTES,
            }));
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

type SessionState = {
    activeAppKey: string | null;
    lastContextPath: string | null;
};

function createServer() {
    const knackAppsDir = ENV_KNACK_APPS_DIR;
    if (!knackAppsDir) {
        throw new Error('Missing env var KNACK_APPS_DIR (absolute path to your KnackApps folder).');
    }

    const apps = discoverApps(knackAppsDir);
    if (!apps.length) {
        throw new Error(`No apps discovered in ${knackAppsDir}. Ensure KnackApps/*/schema/app.json (or legacy KnackApps/*/app.json) exists.`);
    }

    const secrets = loadSecrets();

    const appsByKey = new Map<string, AppConfig>();
    for (const app of apps) appsByKey.set(app.appKey, app);

    const runtimeMetadataCache = new Map<string, CacheEntry<RuntimeMetadata>>();
    const schemaCache = new Map<string, CacheEntry<CachedSchema>>();
    const fieldMapCache = new Map<string, CacheEntry<CachedFieldMap>>();
    const viewMapCache = new Map<string, CacheEntry<CachedViewMap>>();

    // Simple in-memory session state (works well for local usage)
    const state: SessionState = {
        activeAppKey: null,
        lastContextPath: null,
    };

    function getAppOrThrow(appKey?: string): AppConfig {
        const key = appKey || state.activeAppKey;
        if (!key) {
            throw new Error('No app selected. Call knack_set_context or pass appKey.');
        }
        const app = appsByKey.get(key);
        if (!app) {
            throw new Error(`Unknown appKey: ${key}. Call knack_list_apps to see available apps.`);
        }
        return app;
    }

    function getApiKeyOrThrow(appKey: string): string {
        const apiKey = secrets[appKey];
        if (!apiKey) {
            throw new Error(`No API key found for appKey "${appKey}" in your secrets file.`);
        }
        return apiKey;
    }

    function inferAppKeyFromPath(contextPath: string): string | null {
        const nContext = normalisePath(contextPath);

        // If the file is inside KnackApps/<AppKey>/... we can infer directly
        // Example: .../KnackApps/ARC/somefile.js -> ARC
        for (const app of apps) {
            const nFolder = normalisePath(app.appFolder);
            if (nContext.startsWith(nFolder + '/')) {
                return app.appKey;
            }
        }

        // Otherwise, if you keep repos named similarly, you can add extra heuristics here.

        return null;
    }

    async function knackRequest(app: AppConfig, apiKey: string, apiPath: string, init?: RequestInit) {
        const url = `${app.apiBase || DEFAULT_API_BASE}${apiPath}`;
        debugLog('knack_request', { appKey: app.appKey, method: init?.method || 'GET', apiPath });
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

    async function knackRequestPublic(app: AppConfig, apiPath: string, init?: RequestInit) {
        const publicBase = getPublicApiBase(app.apiBase);
        const url = `${publicBase}${apiPath}`;
        debugLog('knack_request_public', { appKey: app.appKey, method: init?.method || 'GET', apiPath });
        const result = await knackFetchJson(url, {
            ...init,
            headers: {
                'Content-Type': 'application/json',
                ...(init?.headers || {}),
            },
        });
        return result;
    }

    function getMetadataFilePaths(app: AppConfig, fileName: string): string[] {
        return [
            path.join(app.appFolder, 'schema', fileName),
            path.join(app.appFolder, fileName),
        ];
    }

    function resolveMetadataFilePath(app: AppConfig, fileName: string): string {
        const candidates = getMetadataFilePaths(app, fileName);
        return candidates.find((candidate) => fileExists(candidate)) || candidates[0];
    }

    function metadataFileExists(app: AppConfig, fileName: string): boolean {
        return getMetadataFilePaths(app, fileName).some((candidate) => fileExists(candidate));
    }

    function readMetadataJson<T>(app: AppConfig, fileName: string): T | null {
        const candidates = getMetadataFilePaths(app, fileName);
        for (const candidate of candidates) {
            const parsed = readJsonFile<T>(candidate);
            if (parsed) return parsed;
        }
        return null;
    }

    function writeMetadataJson(app: AppConfig, fileName: string, data: unknown) {
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

    function readFieldMapFromDisk(app: AppConfig, schema: CachedSchema | null): CachedFieldMap | null {
        const raw = readMetadataJson<unknown>(app, 'fieldMap.json');
        return coerceFieldMap(raw, schema);
    }

    function readViewMapFromDisk(app: AppConfig): CachedViewMap | null {
        return readMetadataJson<CachedViewMap>(app, 'viewMap.json');
    }

    async function getRuntimeMetadata(app: AppConfig): Promise<RuntimeMetadata | null> {
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
                topLevelKeys: payload ? Object.keys(payload).slice(0, 30) : null,
            });
            return null;
        }

        runtimeMetadataCache.set(app.appKey, makeCacheEntry(payload, 'runtime'));
        return payload;
    }

    async function getSchemaForApp(app: AppConfig): Promise<{ schema: CachedSchema | null; source: CacheSource | null }> {
        const cached = getCacheEntry(schemaCache, app.appKey);
        if (cached) return { schema: cached.value, source: cached.source };

        const runtimeMetadata = await getRuntimeMetadata(app);
        const runtimeSchema = parseRuntimeSchema(runtimeMetadata);
        if (runtimeSchema?.objects?.length) {
            schemaCache.set(app.appKey, makeCacheEntry(runtimeSchema, 'runtime'));
            return { schema: runtimeSchema, source: 'runtime' };
        }

        const diskSchema = readSchemaFromDisk(app);
        if (diskSchema?.objects?.length) {
            schemaCache.set(app.appKey, makeCacheEntry(diskSchema, 'file'));
            return { schema: diskSchema, source: 'file' };
        }

        return { schema: null, source: null };
    }

    async function getFieldMapForApp(app: AppConfig): Promise<{ fieldMap: CachedFieldMap | null; source: CacheSource | null }> {
        const cached = getCacheEntry(fieldMapCache, app.appKey);
        if (cached) return { fieldMap: cached.value, source: cached.source };

        const runtimeMetadata = await getRuntimeMetadata(app);
        const runtimeFieldMap = parseRuntimeFieldMap(runtimeMetadata);
        if (runtimeFieldMap && Object.keys(runtimeFieldMap).length) {
            fieldMapCache.set(app.appKey, makeCacheEntry(runtimeFieldMap, 'runtime'));
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

    async function getViewMapForApp(app: AppConfig): Promise<{ viewMap: CachedViewMap | null; source: CacheSource | null }> {
        const cached = getCacheEntry(viewMapCache, app.appKey);
        if (cached) return { viewMap: cached.value, source: cached.source };

        const runtimeMetadata = await getRuntimeMetadata(app);
        const runtimeViewMap = parseRuntimeViewMap(runtimeMetadata);
        if (runtimeViewMap && Object.keys(runtimeViewMap).length) {
            viewMapCache.set(app.appKey, makeCacheEntry(runtimeViewMap, 'runtime'));
            return { viewMap: runtimeViewMap, source: 'runtime' };
        }

        const diskViewMap = readViewMapFromDisk(app);
        if (diskViewMap && Object.keys(diskViewMap).length) {
            viewMapCache.set(app.appKey, makeCacheEntry(diskViewMap, 'file'));
            return { viewMap: diskViewMap, source: 'file' };
        }

        return { viewMap: null, source: null };
    }

    async function getViewContextMapForApp(app: AppConfig): Promise<ViewContextMap> {
        const runtimeMetadata = await getRuntimeMetadata(app);
        return parseRuntimeViewContextMap(runtimeMetadata);
    }

    function buildRecordSearchParams({
        page,
        rowsPerPage,
        q,
        filters,
    }: {
        page: number;
        rowsPerPage: number;
        q?: string;
        filters?: string | Record<string, unknown>;
    }): URLSearchParams {
        const params = new URLSearchParams();
        params.set('page', String(page));
        params.set('rows_per_page', String(rowsPerPage));
        if (q) params.set('q', q);

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

    const server = new McpServer({
        name: 'knack-mcp-multi',
        version: '1.0.0',
    });

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
    //
    // View/search helpers:
    // - knack_get_view_context
    // - knack_get_view_attributes
    // - knack_search_ktl_keywords
    // - knack_search_emails

    // -----------------------
    // Tools: context + discovery
    // -----------------------

    server.tool(
        'knack_list_apps',
        'List all Knack apps discovered from the KnackApps folder.',
        {},
        async () => {
            debugLog('tool_call', { tool: 'knack_list_apps' });
            return makeTextResponse({
                ok: true,
                knackAppsDir,
                activeAppKey: state.activeAppKey,
                apps: apps.map((a) => ({
                    appKey: a.appKey,
                    appName: a.appName,
                    appId: a.appId,
                    appFolder: a.appFolder,
                    notes: a.notes,
                })),
            });
        }
    );

    server.tool(
        'knack_set_context',
        'Set the active Knack app based on a file/folder path. The server will infer which KnackApps/<AppKey>/... the path belongs to.',
        {
            contextPath: z.string().describe('A file path (preferred) or folder path within your workspace.'),
        },
        async (args: { contextPath: string }) => {
            debugLog('tool_call', { tool: 'knack_set_context', args });
            const { contextPath } = args;
            const inferred = inferAppKeyFromPath(contextPath);

            if (!inferred) {
                return makeTextResponse({
                    ok: false,
                    message: 'Could not infer appKey from the given contextPath.',
                    contextPath,
                    hint: 'Ensure your file path is inside KnackApps/<AppKey>/... or pass appKey explicitly to tools.',
                    availableApps: apps.map((a) => a.appKey),
                });
            }

            state.activeAppKey = inferred;
            state.lastContextPath = contextPath;

            return makeTextResponse({
                ok: true,
                activeAppKey: state.activeAppKey,
                contextPath,
            });
        }
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

            const schemaEntry = getCacheEntry(schemaCache, app.appKey);
            const fieldMapEntry = getCacheEntry(fieldMapCache, app.appKey);
            const viewMapEntry = getCacheEntry(viewMapCache, app.appKey);
            const metadataEntry = getCacheEntry(runtimeMetadataCache, app.appKey);

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                activeAppKey: state.activeAppKey,
                lastContextPath: state.lastContextPath,
                cacheTtlMs: CACHE_TTL_MS,
                files: {
                    schemaPath,
                    schemaExists: metadataFileExists(app, 'schema.json'),
                    schemaPathCandidates: getMetadataFilePaths(app, 'schema.json'),
                    fieldMapPath,
                    fieldMapExists: metadataFileExists(app, 'fieldMap.json'),
                    fieldMapPathCandidates: getMetadataFilePaths(app, 'fieldMap.json'),
                    viewMapPath,
                    viewMapExists: metadataFileExists(app, 'viewMap.json'),
                    viewMapPathCandidates: getMetadataFilePaths(app, 'viewMap.json'),
                },
                cache: {
                    schema: schemaEntry
                        ? {
                            cached: true,
                            source: schemaEntry.source,
                            loadedAt: new Date(schemaEntry.loadedAt).toISOString(),
                            expiresAt: new Date(schemaEntry.expiresAt).toISOString(),
                            expiresInMs: Math.max(0, schemaEntry.expiresAt - Date.now()),
                        }
                        : { cached: false },
                    fieldMap: fieldMapEntry
                        ? {
                            cached: true,
                            source: fieldMapEntry.source,
                            loadedAt: new Date(fieldMapEntry.loadedAt).toISOString(),
                            expiresAt: new Date(fieldMapEntry.expiresAt).toISOString(),
                            expiresInMs: Math.max(0, fieldMapEntry.expiresAt - Date.now()),
                        }
                        : { cached: false },
                    viewMap: viewMapEntry
                        ? {
                            cached: true,
                            source: viewMapEntry.source,
                            loadedAt: new Date(viewMapEntry.loadedAt).toISOString(),
                            expiresAt: new Date(viewMapEntry.expiresAt).toISOString(),
                            expiresInMs: Math.max(0, viewMapEntry.expiresAt - Date.now()),
                        }
                        : { cached: false },
                    runtimeMetadata: metadataEntry
                        ? {
                            cached: true,
                            loadedAt: new Date(metadataEntry.loadedAt).toISOString(),
                            expiresAt: new Date(metadataEntry.expiresAt).toISOString(),
                            expiresInMs: Math.max(0, metadataEntry.expiresAt - Date.now()),
                        }
                        : { cached: false },
                },
            });
        }
    );

    server.tool(
        'knack_refresh_cache',
        'Clear runtime/schema/fieldMap/viewMap caches for one app or all apps, optionally warming immediately and persisting runtime metadata to local files.',
        {
            appKey: z.string().optional(),
            warm: z.boolean().default(false),
            persistFiles: z.boolean().default(true),
        },
        async (args: { appKey?: string; warm: boolean; persistFiles: boolean }) => {
            debugLog('tool_call', { tool: 'knack_refresh_cache', args });

            const { appKey, warm, persistFiles } = args;
            const targetApps = appKey ? [getAppOrThrow(appKey)] : [...appsByKey.values()];

            const getSizes = () => ({
                runtimeMetadata: runtimeMetadataCache.size,
                schema: schemaCache.size,
                fieldMap: fieldMapCache.size,
                viewMap: viewMapCache.size,
            });

            const beforeSizes = getSizes();

            if (appKey) {
                runtimeMetadataCache.delete(appKey);
                schemaCache.delete(appKey);
                fieldMapCache.delete(appKey);
                viewMapCache.delete(appKey);
            } else {
                runtimeMetadataCache.clear();
                schemaCache.clear();
                fieldMapCache.clear();
                viewMapCache.clear();
            }

            const warmed: Array<Record<string, unknown>> = [];
            if (warm) {
                for (const app of targetApps) {
                    try {
                        const metadata = await getRuntimeMetadata(app);
                        const schemaResult = await getSchemaForApp(app);
                        const fieldMapResult = await getFieldMapForApp(app);
                        const viewMapResult = await getViewMapForApp(app);

                        const persisted: Record<string, unknown> = {
                            enabled: persistFiles,
                        };

                        if (persistFiles) {
                            if (schemaResult.source === 'runtime' && schemaResult.schema) {
                                persisted.schema = writeMetadataJson(app, 'schema.json', schemaResult.schema);
                            }
                            if (fieldMapResult.source === 'runtime' && fieldMapResult.fieldMap) {
                                persisted.fieldMap = writeMetadataJson(app, 'fieldMap.json', fieldMapResult.fieldMap);
                            }
                            if (viewMapResult.source === 'runtime' && viewMapResult.viewMap) {
                                persisted.viewMap = writeMetadataJson(app, 'viewMap.json', viewMapResult.viewMap);
                            }
                        }

                        warmed.push({
                            appKey: app.appKey,
                            ok: true,
                            runtimeMetadataLoaded: Boolean(metadata),
                            schemaSource: schemaResult.source,
                            fieldMapSource: fieldMapResult.source,
                            viewMapSource: viewMapResult.source,
                            persisted,
                        });
                    } catch (error) {
                        warmed.push({
                            appKey: app.appKey,
                            ok: false,
                            error: error instanceof Error ? error.message : String(error),
                        });
                    }
                }
            }

            return makeTextResponse({
                ok: true,
                target: appKey || 'all',
                warm,
                persistFiles,
                appCount: targetApps.length,
                beforeSizes,
                afterSizes: getSizes(),
                warmed,
            });
        }
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
        async (args: { appKey?: string; objectKey: string; recordId: string }) => {
            debugLog('tool_call', { tool: 'knack_get_record', args });
            const { appKey, objectKey, recordId } = args;
            const app = getAppOrThrow(appKey);
            const apiKey = getApiKeyOrThrow(app.appKey);
            const result = await knackRequest(app, apiKey, `/objects/${objectKey}/records/${recordId}`);
            return makeTextResponse({ appKey: app.appKey, ...result });
        }
    );

    server.tool(
        'knack_find_records',
        'Search Knack records (basic query + paging). Uses appKey if provided, otherwise the active app context.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
            page: z.number().int().min(1).default(1),
            rowsPerPage: z.number().int().min(1).max(1000).default(25),
            q: z.string().optional().describe('Free text search (q=)'),
            filters: z.union([z.string(), z.record(z.string(), z.unknown())]).optional().describe('Structured Knack filters object (recommended) or JSON string.'),
        },
        async ({ appKey, objectKey, page, rowsPerPage, q, filters }) => {
            debugLog('tool_call', { tool: 'knack_find_records', args: { appKey, objectKey, page, rowsPerPage, q, filters } });
            const app = getAppOrThrow(appKey);
            const apiKey = getApiKeyOrThrow(app.appKey);
            const params = buildRecordSearchParams({ page, rowsPerPage, q, filters });

            const result = await knackRequest(app, apiKey, `/objects/${objectKey}/records?${params.toString()}`);
            return makeTextResponse({ appKey: app.appKey, ...result });
        }
    );

    server.tool(
        'knack_get_object_records_with_schema',
        'Fetch records for an object and include that object schema in the same response. Defaults to ARC object_294.',
        {
            appKey: z.string().default('ARC'),
            objectKey: z.string().default('object_294'),
            page: z.number().int().min(1).default(1),
            rowsPerPage: z.number().int().min(1).max(1000).default(25),
            q: z.string().optional().describe('Free text search (q=)'),
            filters: z.union([z.string(), z.record(z.string(), z.unknown())]).optional().describe('Structured Knack filters object (recommended) or JSON string.'),
        },
        async ({ appKey, objectKey, page, rowsPerPage, q, filters }) => {
            debugLog('tool_call', { tool: 'knack_get_object_records_with_schema', args: { appKey, objectKey, page, rowsPerPage, q, filters } });
            const app = getAppOrThrow(appKey);
            const apiKey = getApiKeyOrThrow(app.appKey);
            const params = buildRecordSearchParams({ page, rowsPerPage, q, filters });

            const [schemaResult, recordsResult] = await Promise.all([
                getSchemaForApp(app),
                knackRequest(app, apiKey, `/objects/${objectKey}/records?${params.toString()}`),
            ]);

            const object = schemaResult.schema?.objects?.find((entry) => entry.key === objectKey) || null;

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
                        })),
                    }
                    : null,
                recordsResponse: recordsResult,
            });
        }
    );

    // -----------------------
    // Tools: schema helpers (local, fast)
    // -----------------------

    server.tool(
        'knack_get_object_fields',
        'Return fields for an object from the cached schema.json (recommended) for the selected app.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_get_object_fields', args: { appKey, objectKey } });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
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
                })),
            });
        }
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
            debugLog('tool_call', { tool: 'knack_resolve_field_alias', args: { appKey, alias } });
            const { fieldMap, source } = await getFieldMapForApp(app);

            if (!fieldMap) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No field map available from runtime API or fieldMap.json.',
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

            return makeTextResponse({ ok: true, appKey: app.appKey, source, alias, fieldKey });
        }
    );

    server.tool(
        'knack_get_object',
        'Return a Knack object definition (object metadata + fields) from cached schema data.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_get_object', args: { appKey, objectKey } });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
                });
            }

            const obj = schema.objects.find((entry) => entry.key === objectKey);
            if (!obj) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    source,
                    message: `Object not found in schema: ${objectKey}`,
                    availableObjectKeys: schema.objects.map((entry) => entry.key),
                });
            }

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
                    })),
                },
            });
        }
    );

    server.tool(
        'knack_list_fields',
        'List all fields for a Knack object (field key, name, type).',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_list_fields', args: { appKey, objectKey } });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
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
                })),
            });
        }
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
            debugLog('tool_call', { tool: 'validateFieldMapping', args: { appKey, mappingSize: Object.keys(mappingObject).length } });

            const schemaResult = await getSchemaForApp(app);
            const fieldMapResult = await getFieldMapForApp(app);
            const schema = schemaResult.schema;
            const fieldMap = fieldMapResult.fieldMap || {};

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
                });
            }

            const validFieldKeys = new Set(
                schema.objects.flatMap((obj) => (obj.fields || []).map((field) => field.key)).filter((key): key is string => Boolean(key))
            );

            const resolvedMapping: Record<string, string> = {};
            const invalidMappings: Array<{ mappingKey: string; input: string; reason: string }> = [];

            for (const [mappingKey, value] of Object.entries(mappingObject)) {
                const directFieldKey = /^field_\d+$/i.test(value) ? value : null;
                const resolvedFieldKey = directFieldKey || resolveAliasToFieldKey(fieldMap, value) || null;

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
                usageByField.set(fieldKey, [...(usageByField.get(fieldKey) || []), mappingKey]);
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
        }
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
            debugLog('tool_call', { tool: 'generateSnapshotStructure', args: { appKey, objectKey } });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
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
        }
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
                args: { appKey, mappingSize: mappingObject ? Object.keys(mappingObject).length : 0 },
            });

            const schemaResult = await getSchemaForApp(app);
            const fieldMapResult = await getFieldMapForApp(app);
            const schema = schemaResult.schema;
            const fieldMap = fieldMapResult.fieldMap || {};

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
                });
            }

            const validFieldKeys = new Set(
                schema.objects.flatMap((obj) => (obj.fields || []).map((field) => field.key)).filter((key): key is string => Boolean(key))
            );

            const aliasUsageByField = new Map<string, string[]>();
            for (const [alias, entry] of Object.entries(fieldMap)) {
                const fieldKey = entry.fieldKey;
                if (!validFieldKeys.has(fieldKey)) continue;
                aliasUsageByField.set(fieldKey, [...(aliasUsageByField.get(fieldKey) || []), alias]);
            }

            const fieldMapDuplicates = [...aliasUsageByField.entries()]
                .filter(([, aliases]) => aliases.length > 1)
                .map(([fieldKey, aliases]) => ({ fieldKey, aliases }));

            let mappingDuplicates: Array<{ fieldKey: string; mappingKeys: string[] }> = [];
            let mappingInvalidEntries: Array<{ mappingKey: string; input: string; reason: string }> = [];

            if (mappingObject) {
                const mappingUsageByField = new Map<string, string[]>();

                for (const [mappingKey, value] of Object.entries(mappingObject)) {
                    const directFieldKey = /^field_\d+$/i.test(value) ? value : null;
                    const resolvedFieldKey = directFieldKey || resolveAliasToFieldKey(fieldMap, value) || null;

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

                    mappingUsageByField.set(resolvedFieldKey, [...(mappingUsageByField.get(resolvedFieldKey) || []), mappingKey]);
                }

                mappingDuplicates = [...mappingUsageByField.entries()]
                    .filter(([, mappingKeys]) => mappingKeys.length > 1)
                    .map(([fieldKey, mappingKeys]) => ({ fieldKey, mappingKeys }));
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
        }
    );

    server.tool(
        'knack_get_field_type',
        'Return the field type for a field key or alias from schema data.',
        {
            appKey: z.string().optional(),
            fieldKey: z.string().optional().describe('Knack field key, e.g. field_1234'),
            alias: z.string().optional().describe('Alias from fieldMap.json, e.g. object_2.name'),
            objectKey: z.string().optional().describe('Optional object key to scope the lookup, e.g. object_2'),
        },
        async ({ appKey, fieldKey, alias, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_get_field_type', args: { appKey, fieldKey, alias, objectKey } });

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
                        message: 'No field map available from runtime API or fieldMap.json; cannot resolve alias.',
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
                    message: 'No schema available from runtime API or schema.json.',
                });
            }

            const matches: Array<{
                objectKey: string;
                objectName?: string;
                fieldKey: string;
                fieldName?: string;
                fieldType?: string;
            }> = [];

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
        }
    );

    server.tool(
        'knack_resolve_any',
        'Resolve an identifier (field key or alias) to field key + name + type + object key.',
        {
            appKey: z.string().optional(),
            identifier: z.string(),
            objectKey: z.string().optional().describe('Optional object key to narrow lookup.'),
        },
        async ({ appKey, identifier, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_resolve_any', args: { appKey, identifier, objectKey } });

            const schemaResult = await getSchemaForApp(app);
            const schema = schemaResult.schema;
            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
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

            let resolvedFieldKey: string | null = null;
            let resolvedBy: 'fieldKey' | 'alias' = 'alias';
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
                        message: 'No field map available from runtime API or fieldMap.json; cannot resolve alias identifier.',
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
            }> = [];

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
        }
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
            debugLog('tool_call', { tool: 'knack_list_field_types', args: { appKey, objectKey } });

            const schemaResult = await getSchemaForApp(app);
            const schema = schemaResult.schema;
            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
                });
            }

            const obj = schema.objects.find((entry) => entry.key === objectKey);
            if (!obj) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    schemaSource: schemaResult.source,
                    message: `Object not found in schema: ${objectKey}`,
                    availableObjectKeys: schema.objects.map((entry) => entry.key),
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
                .sort((a, b) => b.count - a.count || a.fieldType.localeCompare(b.fieldType));

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
        }
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
            debugLog('tool_call', { tool: 'knack_get_view_context', args: { appKey, viewKey } });

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

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                viewKey,
                context,
            });
        }
    );

    server.tool(
        'knack_get_view_attributes',
        'Return all attributes for a view key from runtime metadata or cached viewMap.json.',
        {
            appKey: z.string().optional(),
            viewKey: z.string(),
        },
        async ({ appKey, viewKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_get_view_attributes', args: { appKey, viewKey } });
            const { viewMap, source } = await getViewMapForApp(app);

            if (!viewMap) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No view map available from runtime API or viewMap.json.',
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

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                viewKey,
                attributes,
            });
        }
    );

    server.tool(
        'knack_search_ktl_keywords',
        'Search KTL-style underscore keywords in view title/description across the selected app.',
        {
            appKey: z.string().optional(),
            keyword: z.string().optional().describe('Optional keyword filter (e.g. _sth).'),
            maxResults: z.number().int().min(1).max(5000).default(500),
        },
        async ({ appKey, keyword, maxResults }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_search_ktl_keywords', args: { appKey, keyword, maxResults } });

            const { viewMap, source } = await getViewMapForApp(app);
            if (!viewMap) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No view map available from runtime API or viewMap.json.',
                });
            }

            const keywordFilter = keyword ? keyword.trim().toLowerCase() : null;
            const viewContextMap = await getViewContextMapForApp(app);
            const matches: Array<Record<string, unknown>> = [];
            const keywordCounts = new Map<string, number>();

            for (const [viewKey, viewAttrs] of Object.entries(viewMap)) {
                const title = typeof viewAttrs.title === 'string' ? viewAttrs.title : '';
                const description = typeof viewAttrs.description === 'string' ? viewAttrs.description : '';
                const viewName = typeof viewAttrs.name === 'string' ? viewAttrs.name : undefined;
                const viewType = typeof viewAttrs.type === 'string' ? viewAttrs.type : undefined;

                const titleHits = extractKtlKeywordsFromText(title).map((entry) => ({ ...entry, source: 'title' }));
                const descriptionHits = extractKtlKeywordsFromText(description).map((entry) => ({ ...entry, source: 'description' }));
                const allHits = [...titleHits, ...descriptionHits];
                if (!allHits.length) continue;

                const filteredHits = keywordFilter
                    ? allHits.filter((hit) => hit.keyword.toLowerCase() === keywordFilter || hit.keyword.toLowerCase().includes(keywordFilter))
                    : allHits;

                if (!filteredHits.length) continue;

                const uniqueKeywords = [...new Set(filteredHits.map((hit) => hit.keyword))];
                uniqueKeywords.forEach((kw) => keywordCounts.set(kw, (keywordCounts.get(kw) || 0) + 1));

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
        }
    );

    server.tool(
        'knack_search_emails',
        'Search views for email-related rules/actions and return recipient (to) plus subject/message context.',
        {
            appKey: z.string().optional(),
            query: z.string().optional().describe('Optional text filter applied to to/cc/bcc/subject/message/path.'),
            includeMessage: z.boolean().default(true),
            maxResults: z.number().int().min(1).max(5000).default(500),
        },
        async ({ appKey, query, includeMessage, maxResults }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_search_emails', args: { appKey, query, includeMessage, maxResults } });

            const { viewMap, source } = await getViewMapForApp(app);
            if (!viewMap) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No view map available from runtime API or viewMap.json.',
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
                    const searchable = [node.path, node.to, node.cc, node.bcc, node.subject, node.message, node.action]
                        .filter((part): part is string => Boolean(part))
                        .join(' || ')
                        .toLowerCase();

                    if (filter && !searchable.includes(filter)) continue;

                    matches.push({
                        viewKey,
                        viewName: typeof viewAttrs.name === 'string' ? viewAttrs.name : undefined,
                        viewType: typeof viewAttrs.type === 'string' ? viewAttrs.type : undefined,
                        sceneKey: sceneContext.sceneKey,
                        sceneName: sceneContext.sceneName,
                        sceneSlug: sceneContext.sceneSlug,
                        path: node.path,
                        action: node.action,
                        to: node.to,
                        cc: node.cc,
                        bcc: node.bcc,
                        subject: truncateText(node.subject, 2000),
                        message: includeMessage ? truncateText(node.message, 4000) : undefined,
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
        }
    );

    // -----------------------
    // Resources: schema / fieldMap / viewMap (per app)
    // -----------------------

    // A generic pattern: knack://<AppKey>/schema, knack://<AppKey>/fieldMap, knack://<AppKey>/viewMap
    server.resource(
        'knack_schema',
        'knack://schema',
        async (uri: URL) => {
            debugLog('resource_call', { resource: 'knack_schema', uri: uri.toString() });
            // uri format: knack://ARC/schema
            const parts = uri.toString().replace('knack://', '').split('/');
            const appKey = parts[0];
            const type = parts[1];

            const app = appsByKey.get(appKey);
            if (!app) {
                return {
                    contents: [{ uri: uri.toString(), mimeType: 'application/json', text: JSON.stringify({ ok: false, message: 'Unknown appKey' }) }],
                };
            }

            if (type === 'schema') {
                const schemaResult = await getSchemaForApp(app);
                const schema = schemaResult.schema || { ok: false, message: 'No schema available from runtime API or schema.json.' };
                return {
                    contents: [{ uri: uri.toString(), mimeType: 'application/json', text: JSON.stringify(schema, null, 2) }],
                };
            }

            if (type === 'fieldMap') {
                const fieldMapResult = await getFieldMapForApp(app);
                const fieldMap = fieldMapResult.fieldMap || { ok: false, message: 'No field map available from runtime API or fieldMap.json.' };
                return {
                    contents: [{ uri: uri.toString(), mimeType: 'application/json', text: JSON.stringify(fieldMap, null, 2) }],
                };
            }

            if (type === 'viewMap') {
                const viewMapResult = await getViewMapForApp(app);
                const viewMap = viewMapResult.viewMap || { ok: false, message: 'No view map available from runtime API or viewMap.json.' };
                return {
                    contents: [{ uri: uri.toString(), mimeType: 'application/json', text: JSON.stringify(viewMap, null, 2) }],
                };
            }

            return {
                contents: [{ uri: uri.toString(), mimeType: 'application/json', text: JSON.stringify({ ok: false, message: 'Unknown resource type' }) }],
            };
        }
    );

    return server;
}

async function main() {
    const server = createServer();
    const transport = new StdioServerTransport();
    await server.connect(transport);
}

main().catch((err) => {
    // Important: log to stderr for MCP clients
    console.error(err);
    process.exit(1);
});