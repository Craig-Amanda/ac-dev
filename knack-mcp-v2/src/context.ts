/**
 * Everything a tool needs, passed explicitly.
 *
 * The legacy server held all of this in one closure, which is why none of its tools
 * could be tested. Here it is one object: apps and secrets, the session's selected
 * app, the metadata caches, the Knack HTTP layer and the MCP server handle used for
 * human confirmation. A test builds one from plain values; production builds one from
 * the environment.
 */
import path from 'node:path';

import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';

import {
    type AppConfig,
    DEFAULT_API_BASE,
    ENV_KNACK_APPS_DIR,
    type SecretsMap,
    type ServerOptions,
    discoverApps,
    loadSecrets,
} from './config.js';
import { type KnackApiResult, knackFetchJson } from './http.js';
import { getCacheEntry, makeCacheEntry } from './lib/cache.js';
import {
    makeBuilderBaseUrl,
    makeFieldBuilderUrl,
    makeSceneBuilderUrl,
    makeViewBuilderUrl,
    getPublicApiBase,
} from './lib/builder-urls.js';
import { coerceFieldMap } from './lib/field-map.js';
import { buildFieldReferenceIndex } from './lib/field-references.js';
import { debugLog } from './lib/log.js';
import {
    isRuntimeMetadataPayload,
    parseRuntimeFieldMap,
    parseRuntimeScenes,
    parseRuntimeSchema,
    parseRuntimeViewContextMap,
    parseRuntimeViewMap,
} from './lib/metadata.js';
import {
    asRecord,
    fileExists,
    normaliseAppIdentity,
    normalisePath,
    readJsonFile,
    sleep,
    writeJsonFile,
} from './lib/util.js';
import type {
    CacheEntry,
    CacheSource,
    CachedFieldMap,
    CachedFieldReferenceIndex,
    CachedSchema,
    CachedViewMap,
    RuntimeMetadata,
    SceneInfo,
    ViewContextMap,
} from './types.js';

export type SessionState = {
    activeAppKey: string | null;
    lastContextPath: string | null;
};

export type AppInferenceResult = {
    appKey: string | null;
    inferenceMode: 'direct-folder' | 'segment-alias' | 'basename-alias' | null;
    candidateAppKeys: string[];
};

export type MetadataFileName =
    | 'schema.json'
    | 'fieldMap.json'
    | 'viewMap.json'
    | 'fieldReferenceIndex.json';

export class KnackContext {
    readonly options: ServerOptions;
    readonly knackAppsDir: string;
    readonly state: SessionState = {
        activeAppKey: null,
        lastContextPath: null,
    };
    readonly caches = {
        runtimeMetadata: new Map<string, CacheEntry<RuntimeMetadata>>(),
        schema: new Map<string, CacheEntry<CachedSchema>>(),
        fieldMap: new Map<string, CacheEntry<CachedFieldMap>>(),
        viewMap: new Map<string, CacheEntry<CachedViewMap>>(),
        fieldReference: new Map<
            string,
            CacheEntry<CachedFieldReferenceIndex>
        >(),
    };
    /** Set once the MCP server exists; used for elicitation and client capabilities. */
    server: McpServer | null = null;

    private appsByKey = new Map<string, AppConfig>();
    private secrets: SecretsMap;
    private readonly discover: () => AppConfig[];
    private readonly readSecrets: () => SecretsMap;

    constructor(input: {
        knackAppsDir: string;
        apps: AppConfig[];
        secrets: SecretsMap;
        options?: ServerOptions;
        /** How to re-read apps on demand; defaults to the folder scan. */
        discover?: () => AppConfig[];
        readSecrets?: () => SecretsMap;
    }) {
        this.knackAppsDir = input.knackAppsDir;
        this.options = input.options ?? {};
        this.secrets = input.secrets;
        this.discover =
            input.discover ?? (() => discoverApps(this.knackAppsDir));
        this.readSecrets = input.readSecrets ?? loadSecrets;
        for (const app of input.apps) this.appsByKey.set(app.appKey, app);
    }

    /** Production construction: the same startup failures the legacy server reported. */
    static fromEnvironment(options: ServerOptions = {}): KnackContext {
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
        return new KnackContext({
            knackAppsDir,
            apps,
            secrets: loadSecrets(),
            options,
        });
    }

    // -----------------------
    // Apps and secrets
    // -----------------------

    get apps(): AppConfig[] {
        return [...this.appsByKey.values()];
    }

    /** Re-read the folder and the secrets file, so a newly added app is visible. */
    rescanApps(): AppConfig[] {
        const fresh = this.discover();
        this.appsByKey.clear();
        for (const app of fresh) this.appsByKey.set(app.appKey, app);
        this.secrets = this.readSecrets();
        return fresh;
    }

    getApp(appKey?: string): AppConfig {
        const key = appKey || this.state.activeAppKey;
        if (!key) {
            throw new Error(
                'No app selected. Call knack_set_context or pass appKey.',
            );
        }
        const app = this.appsByKey.get(key);
        if (!app) {
            throw new Error(
                `Unknown appKey: ${key}. Call knack_list_apps to see available apps.`,
            );
        }
        return app;
    }

    findApp(appKey: string): AppConfig | undefined {
        return this.appsByKey.get(appKey);
    }

    getApiKey(appKey: string): string {
        const apiKey = this.secrets[appKey];
        if (!apiKey) {
            throw new Error(
                `No API key found for appKey "${appKey}" in your secrets file.`,
            );
        }
        return apiKey;
    }

    hasApiKey(appKey: string): boolean {
        return Boolean(this.secrets[appKey]);
    }

    getAppAliases(app: AppConfig): string[] {
        const aliases = new Set<string>();
        for (const candidate of [
            app.appKey,
            app.appName,
            path.basename(app.appFolder),
        ]) {
            if (!candidate) continue;
            const normalised = normaliseAppIdentity(candidate);
            if (normalised) aliases.add(normalised);
        }
        return [...aliases];
    }

    /** Which app a file path belongs to: folder containment, then a path segment, then the basename. */
    inferAppKeyFromPath(contextPath: string): AppInferenceResult {
        const apps = this.apps;
        const nContext = normalisePath(contextPath);

        for (const app of apps) {
            if (nContext.startsWith(normalisePath(app.appFolder) + '/')) {
                return {
                    appKey: app.appKey,
                    inferenceMode: 'direct-folder',
                    candidateAppKeys: [app.appKey],
                };
            }
        }

        const segments = nContext
            .split('/')
            .filter(Boolean)
            .map((segment) => normaliseAppIdentity(segment))
            .filter(Boolean);
        const segmentMatches = apps.filter((app) =>
            this.getAppAliases(app).some((alias) => segments.includes(alias)),
        );
        if (segmentMatches.length === 1) {
            return {
                appKey: segmentMatches[0].appKey,
                inferenceMode: 'segment-alias',
                candidateAppKeys: [segmentMatches[0].appKey],
            };
        }

        const basenameAlias = normaliseAppIdentity(
            path.basename(contextPath, path.extname(contextPath)),
        );
        if (basenameAlias) {
            const basenameMatches = apps.filter((app) =>
                this.getAppAliases(app).includes(basenameAlias),
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

    // -----------------------
    // Knack REST API
    // -----------------------

    /** One authenticated call against the app's REST base. */
    async request(
        app: AppConfig,
        apiPath: string,
        init?: RequestInit,
    ): Promise<KnackApiResult> {
        const apiKey = this.getApiKey(app.appKey);
        debugLog('knack_request', {
            appKey: app.appKey,
            method: init?.method || 'GET',
            apiPath,
        });
        return knackFetchJson(`${app.apiBase || DEFAULT_API_BASE}${apiPath}`, {
            ...init,
            headers: {
                'X-Knack-Application-Id': app.appId,
                'X-Knack-REST-API-Key': apiKey,
                'Content-Type': 'application/json',
                ...(init?.headers || {}),
            },
        });
    }

    /**
     * Like `request`, with exponential backoff on 429 and, for idempotent methods, 5xx.
     *
     * Knack has no idempotency key, so a 5xx on a POST is ambiguous: the create may have
     * applied and only the response been lost. POST therefore retries on 429 only. PUT and
     * DELETE re-apply harmlessly. A DELETE that returns 404 right after a 5xx retry almost
     * certainly succeeded the first time, and is reported as success.
     */
    async requestWithRetry(
        app: AppConfig,
        apiPath: string,
        init?: RequestInit,
        maxAttempts = 4,
    ): Promise<KnackApiResult> {
        const method = (init?.method || 'GET').toUpperCase();
        const canRetryOn5xx = method !== 'POST';
        let last = await this.request(app, apiPath, init);

        for (let attempt = 2; attempt <= maxAttempts; attempt++) {
            const after5xx = last.status >= 500;
            if (!(last.status === 429 || (canRetryOn5xx && after5xx))) break;
            await sleep(500 * 2 ** (attempt - 2));
            last = await this.request(app, apiPath, init);
            if (method === 'DELETE' && after5xx && last.status === 404) {
                return {
                    ok: true,
                    status: 200,
                    body: {
                        inferredSuccess: true,
                        message:
                            'Treated as a successful delete: a 5xx on the first attempt was retried and came back 404, which almost certainly means the delete already applied and only its response was lost — not that the record never existed.',
                        upstreamStatus: last.status,
                        upstreamBody: last.body,
                    },
                };
            }
        }
        return last;
    }

    // -----------------------
    // Metadata files on disk (KnackApps/<App>/schema/*.json, legacy KnackApps/<App>/*.json)
    // -----------------------

    metadataFilePaths(app: AppConfig, fileName: string): string[] {
        return [
            path.join(app.appFolder, 'schema', fileName),
            path.join(app.appFolder, fileName),
        ];
    }

    resolveMetadataFilePath(app: AppConfig, fileName: string): string {
        const candidates = this.metadataFilePaths(app, fileName);
        return candidates.find(fileExists) || candidates[0];
    }

    metadataFileExists(app: AppConfig, fileName: string): boolean {
        return this.metadataFilePaths(app, fileName).some(fileExists);
    }

    readMetadataJson<T>(app: AppConfig, fileName: string): T | null {
        for (const candidate of this.metadataFilePaths(app, fileName)) {
            const parsed = readJsonFile<T>(candidate);
            if (parsed) return parsed;
        }
        return null;
    }

    writeMetadataJson(
        app: AppConfig,
        fileName: string,
        data: unknown,
    ): { ok: true; path: string } | { ok: false; path: string; error: string } {
        const targetPath = this.resolveMetadataFilePath(app, fileName);
        const result = writeJsonFile(targetPath, data);
        return result.ok
            ? { ok: true, path: targetPath }
            : { ok: false, path: targetPath, error: result.error };
    }

    // -----------------------
    // Runtime metadata and its derived caches
    // -----------------------

    /** The public application payload: objects, fields, scenes and views in one document. */
    async getRuntimeMetadata(app: AppConfig): Promise<RuntimeMetadata | null> {
        const cached = getCacheEntry(this.caches.runtimeMetadata, app.appKey);
        if (cached) return cached.value;

        const url = `${getPublicApiBase(app.apiBase)}/v1/applications/${encodeURIComponent(app.appId)}`;
        debugLog('runtime_metadata_attempt', { appKey: app.appKey, url });
        const result = await knackFetchJson(url, { method: 'GET' });
        if (!result.ok) return null;

        const payload = asRecord(result.body);
        if (!payload || !isRuntimeMetadataPayload(payload)) {
            debugLog('runtime_metadata_invalid_shape', {
                appKey: app.appKey,
                url,
                topLevelKeys: payload
                    ? Object.keys(payload).slice(0, 30)
                    : null,
            });
            return null;
        }
        this.caches.runtimeMetadata.set(
            app.appKey,
            makeCacheEntry(payload, 'runtime'),
        );
        return payload;
    }

    /** Drop every cached view of one app, or of all apps. */
    invalidate(appKey?: string): void {
        for (const cache of Object.values(this.caches)) {
            if (appKey) cache.delete(appKey);
            else cache.clear();
        }
    }

    async getSchema(
        app: AppConfig,
    ): Promise<{ schema: CachedSchema | null; source: CacheSource | null }> {
        const cached = getCacheEntry(this.caches.schema, app.appKey);
        if (cached) return { schema: cached.value, source: cached.source };

        const runtimeSchema = parseRuntimeSchema(
            await this.getRuntimeMetadata(app),
        );
        if (runtimeSchema?.objects?.length) {
            this.caches.schema.set(
                app.appKey,
                makeCacheEntry(runtimeSchema, 'runtime'),
            );
            return { schema: runtimeSchema, source: 'runtime' };
        }
        const diskSchema = this.readMetadataJson<CachedSchema>(
            app,
            'schema.json',
        );
        if (diskSchema?.objects?.length) {
            this.caches.schema.set(
                app.appKey,
                makeCacheEntry(diskSchema, 'file'),
            );
            return { schema: diskSchema, source: 'file' };
        }
        return { schema: null, source: null };
    }

    /** The schema, or an error naming the app: most schema tools cannot do anything without one. */
    async requireSchema(
        app: AppConfig,
    ): Promise<{ schema: CachedSchema; source: CacheSource }> {
        const { schema, source } = await this.getSchema(app);
        if (!schema || !source) {
            throw new Error(
                `No schema available for "${app.appKey}" from the runtime API or schema.json.`,
            );
        }
        return { schema, source };
    }

    async getFieldMap(
        app: AppConfig,
    ): Promise<{
        fieldMap: CachedFieldMap | null;
        source: CacheSource | null;
    }> {
        const cached = getCacheEntry(this.caches.fieldMap, app.appKey);
        if (cached) return { fieldMap: cached.value, source: cached.source };

        const runtimeFieldMap = parseRuntimeFieldMap(
            await this.getRuntimeMetadata(app),
        );
        if (runtimeFieldMap && Object.keys(runtimeFieldMap).length) {
            this.caches.fieldMap.set(
                app.appKey,
                makeCacheEntry(runtimeFieldMap, 'runtime'),
            );
            return { fieldMap: runtimeFieldMap, source: 'runtime' };
        }
        const { schema } = await this.getSchema(app);
        const diskFieldMap = coerceFieldMap(
            this.readMetadataJson<unknown>(app, 'fieldMap.json'),
            schema,
        );
        if (diskFieldMap && Object.keys(diskFieldMap).length) {
            this.caches.fieldMap.set(
                app.appKey,
                makeCacheEntry(diskFieldMap, 'file'),
            );
            return { fieldMap: diskFieldMap, source: 'file' };
        }
        return { fieldMap: null, source: null };
    }

    async getViewMap(
        app: AppConfig,
    ): Promise<{ viewMap: CachedViewMap | null; source: CacheSource | null }> {
        const cached = getCacheEntry(this.caches.viewMap, app.appKey);
        if (cached) return { viewMap: cached.value, source: cached.source };

        const runtimeViewMap = parseRuntimeViewMap(
            await this.getRuntimeMetadata(app),
        );
        if (runtimeViewMap && Object.keys(runtimeViewMap).length) {
            this.caches.viewMap.set(
                app.appKey,
                makeCacheEntry(runtimeViewMap, 'runtime'),
            );
            return { viewMap: runtimeViewMap, source: 'runtime' };
        }
        const diskViewMap = this.readMetadataJson<CachedViewMap>(
            app,
            'viewMap.json',
        );
        if (diskViewMap && Object.keys(diskViewMap).length) {
            this.caches.viewMap.set(
                app.appKey,
                makeCacheEntry(diskViewMap, 'file'),
            );
            return { viewMap: diskViewMap, source: 'file' };
        }
        return { viewMap: null, source: null };
    }

    async getViewContextMap(app: AppConfig): Promise<ViewContextMap> {
        return parseRuntimeViewContextMap(await this.getRuntimeMetadata(app));
    }

    async getScenes(app: AppConfig): Promise<SceneInfo[]> {
        return parseRuntimeScenes(await this.getRuntimeMetadata(app));
    }

    async getFieldReferenceIndex(app: AppConfig): Promise<{
        index: CachedFieldReferenceIndex | null;
        source: CacheSource | null;
    }> {
        const cached = getCacheEntry(this.caches.fieldReference, app.appKey);
        if (cached) return { index: cached.value, source: cached.source };

        const [schemaResult, fieldMapResult, viewMapResult, viewContextMap] =
            await Promise.all([
                this.getSchema(app),
                this.getFieldMap(app),
                this.getViewMap(app),
                this.getViewContextMap(app),
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
                this.caches.fieldReference.set(
                    app.appKey,
                    makeCacheEntry(index, source),
                );
                return { index, source };
            }
        }

        const diskIndex = this.readMetadataJson<CachedFieldReferenceIndex>(
            app,
            'fieldReferenceIndex.json',
        );
        if (diskIndex && Object.keys(diskIndex).length) {
            this.caches.fieldReference.set(
                app.appKey,
                makeCacheEntry(diskIndex, 'file'),
            );
            return { index: diskIndex, source: 'file' };
        }
        return { index: null, source: null };
    }

    async getBuilderLinks(
        app: AppConfig,
        params: {
            sceneKey?: string;
            viewKey?: string;
            viewType?: string;
            objectKey?: string;
            fieldKey?: string;
        },
    ) {
        const runtimeMetadata = await this.getRuntimeMetadata(app);
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
                { objectKey: params.objectKey, fieldKey: params.fieldKey },
                runtimeMetadata,
            ),
        };
    }

    async findFieldOwner(
        app: AppConfig,
        fieldKey: string,
    ): Promise<{
        objectKey: string;
        objectName?: string;
        fieldName?: string;
    } | null> {
        const { schema } = await this.getSchema(app);
        for (const object of schema?.objects ?? []) {
            const field = (object.fields || []).find(
                (entry) => entry.key === fieldKey,
            );
            if (field) {
                return {
                    objectKey: object.key,
                    objectName: object.name,
                    fieldName: field.name,
                };
            }
        }
        return null;
    }

    // -----------------------
    // The connected client
    // -----------------------

    /** Whether this client advertised elicitation, so a person can be asked to confirm. */
    clientCanPromptHuman(): boolean {
        return Boolean(
            this.server?.server.getClientCapabilities()?.elicitation,
        );
    }

    describeClient(): string | null {
        const client = this.server?.server.getClientVersion();
        return client
            ? `${client.name}${client.version ? ` ${client.version}` : ''}`
            : null;
    }
}
