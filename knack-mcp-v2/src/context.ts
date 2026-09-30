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
import type { McpServer } from '@modelcontextprotocol/server';
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
import {
    type FieldExclusions,
    buildFieldExclusions,
} from './lib/field-exclusion.js';
import { buildFieldReferenceIndex } from './lib/field-references.js';
import { debugLog } from './lib/log.js';
import {
    getRuntimeArray,
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
    /** Per cached schema: its exclusions, rebuilt when either input changes. */
    private exclusionMemo = new WeakMap<
        CachedSchema,
        {
            dataAccess: AppConfig['dataAccess'];
            exclusions: FieldExclusions;
        }
    >();
    private secrets: SecretsMap;
    private readonly discover: () => AppConfig[];
    private readonly readSecrets: () => SecretsMap;
    /**
     * One fetch per app in flight at a time. getFieldReferenceIndex alone awaits four
     * loaders in parallel that all call getRuntimeMetadata on a cold cache — without
     * this, each one independently downloads and parses the same multi-megabyte
     * application payload.
     */
    private runtimeMetadataInFlight = new Map<
        string,
        Promise<RuntimeMetadata | null>
    >();
    /** Per app, the key value Knack last accepted, and the one it last rejected. */
    private verifiedApiKeys = new Map<string, string>();
    private rejectedApiKeys = new Map<string, { key: string; at: number }>();
    private unconfirmedApiKeys = new Map<string, { key: string; at: number }>();
    /** Per app, the key (or its absence) the cached views were built under. */
    private cachesBuiltWithKey = new Map<string, string | null>();
    /** Per app, why the runtime metadata was last withheld, for error messages. */
    private metadataRefusals = new Map<string, string>();

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
        const headers: Record<string, string> = {
            'X-Knack-Application-Id': app.appId,
            'X-Knack-REST-API-Key': apiKey,
            'Content-Type': 'application/json',
            ...((init?.headers as Record<string, string> | undefined) || {}),
        };
        const result = await knackFetchJson(
            `${app.apiBase || DEFAULT_API_BASE}${apiPath}`,
            { ...init, headers },
        );
        // Any call Knack accepts proves the key it carried, so a rejection of that key
        // (and the note it puts on responses) does not outlive it.
        const sentKey = headers['X-Knack-REST-API-Key'];
        if (
            result.ok &&
            this.rejectedApiKeys.get(app.appKey)?.key === sentKey
        ) {
            this.rejectedApiKeys.delete(app.appKey);
            this.metadataRefusals.delete(app.appKey);
        }
        return result;
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

    metadataFilePaths(app: AppConfig, fileName: MetadataFileName): string[] {
        return [
            path.join(app.appFolder, 'schema', fileName),
            path.join(app.appFolder, fileName),
        ];
    }

    resolveMetadataFilePath(
        app: AppConfig,
        fileName: MetadataFileName,
    ): string {
        const candidates = this.metadataFilePaths(app, fileName);
        return candidates.find(fileExists) || candidates[0];
    }

    metadataFileExists(app: AppConfig, fileName: MetadataFileName): boolean {
        return this.metadataFilePaths(app, fileName).some(fileExists);
    }

    readMetadataJson<T>(app: AppConfig, fileName: MetadataFileName): T | null {
        for (const candidate of this.metadataFilePaths(app, fileName)) {
            const parsed = readJsonFile<T>(candidate);
            if (parsed) return parsed;
        }
        return null;
    }

    writeMetadataJson(
        app: AppConfig,
        fileName: MetadataFileName,
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

    /**
     * The application payload: objects, fields, scenes and views in one document.
     *
     * Knack serves this document to anyone who has the application ID, because the live
     * app loads itself from it. It can carry personal data (addresses and text in email
     * rules, values typed into view filters) and maps every view a request could target,
     * so this server hands it out only for an app whose REST API key it holds and Knack
     * has not rejected. Without one it returns null, as for a failed fetch, so every
     * caller keeps its disk fallback; `metadataRefusal` says why.
     */
    async getRuntimeMetadata(app: AppConfig): Promise<RuntimeMetadata | null> {
        this.syncKeyState(app);
        const apiKey = this.secrets[app.appKey];
        if (!apiKey) {
            return this.refuseMetadata(
                app,
                `No API key found for appKey "${app.appKey}" in your secrets file. The app's metadata is read from Knack only with its REST API key.`,
            );
        }
        const rejected = this.rejectedApiKeys.get(app.appKey);
        if (
            rejected?.key === apiKey &&
            Date.now() - rejected.at < REJECTED_KEY_RECHECK_MS
        ) {
            return this.refuseMetadata(app, rejectedKeyMessage(app.appKey));
        }

        const cached = getCacheEntry(this.caches.runtimeMetadata, app.appKey);
        if (cached && this.verifiedApiKeys.get(app.appKey) === apiKey) {
            return cached.value;
        }
        // After a 429 or 5xx, serve the cached payload for a short while before asking
        // again, rather than paying the retry backoff on every read.
        const unconfirmed = this.unconfirmedApiKeys.get(app.appKey);
        if (
            cached &&
            unconfirmed?.key === apiKey &&
            Date.now() - unconfirmed.at < UNCONFIRMED_KEY_RECHECK_MS
        ) {
            return cached.value;
        }

        const inFlight = this.runtimeMetadataInFlight.get(app.appKey);
        if (inFlight) return inFlight;

        const fetchPromise = (async () => {
            // A payload cached while the key could not be confirmed is checked again
            // here rather than downloaded again.
            const payload =
                cached?.value ?? (await this.fetchRuntimeMetadata(app));
            if (!payload) return null;

            const verdict = await this.verifyApiKey(app, apiKey, payload);
            if (verdict === 'rejected') {
                // Views built from a payload served while the key was unconfirmed go
                // too, not only the payload.
                this.invalidate(app.appKey);
                return this.refuseMetadata(app, rejectedKeyMessage(app.appKey));
            }
            if (verdict === 'unusable') {
                return this.refuseMetadata(
                    app,
                    `The REST API key for appKey "${app.appKey}" could not be checked: the app's metadata lists no object with a key.`,
                );
            }
            this.metadataRefusals.delete(app.appKey);
            if (!cached) {
                this.caches.runtimeMetadata.set(
                    app.appKey,
                    makeCacheEntry(payload, 'runtime'),
                );
            }
            return payload;
        })();

        this.runtimeMetadataInFlight.set(app.appKey, fetchPromise);
        try {
            return await fetchPromise;
        } finally {
            this.runtimeMetadataInFlight.delete(app.appKey);
        }
    }

    /** Why the runtime metadata was last withheld for this app, if it was. */
    metadataRefusal(app: AppConfig): string | null {
        this.syncKeyState(app);
        return this.metadataRefusals.get(app.appKey) ?? null;
    }

    /** What is known about the app's key, for knack_list_apps. */
    apiKeyStatus(
        appKey: string,
    ): 'missing' | 'accepted' | 'rejected' | 'unchecked' {
        const apiKey = this.secrets[appKey];
        if (!apiKey) return 'missing';
        if (this.verifiedApiKeys.get(appKey) === apiKey) return 'accepted';
        if (this.rejectedApiKeys.get(appKey)?.key === apiKey) return 'rejected';
        return 'unchecked';
    }

    private async fetchRuntimeMetadata(
        app: AppConfig,
    ): Promise<RuntimeMetadata | null> {
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
        return payload;
    }

    /**
     * Prove the key with one authenticated read of an object named in the payload, so a
     * placeholder in the secrets file does not unlock it. The object key has to come
     * from the payload, so a key's first check downloads it; a rejection is remembered
     * per key value for REJECTED_KEY_RECHECK_MS, so a bad key costs that once every few
     * minutes and a one-off 401 or 403 does not lock out a good key. Only 401 and 403 count as a
     * rejection: a 429 or 5xx leaves the key unconfirmed, the payload is served and the
     * check runs again on the next read. The key sent is the one captured by the caller,
     * so the value recorded is the value Knack saw. An app with no objects has nothing
     * to check the key against and nothing it could protect.
     */
    private async verifyApiKey(
        app: AppConfig,
        apiKey: string,
        metadata: RuntimeMetadata,
    ): Promise<'verified' | 'rejected' | 'unconfirmed' | 'unusable'> {
        if (this.verifiedApiKeys.get(app.appKey) === apiKey) return 'verified';
        const objects = getRuntimeArray(metadata, 'objects') ?? [];
        if (!objects.length) {
            this.verifiedApiKeys.set(app.appKey, apiKey);
            return 'verified';
        }
        const objectKey = objects
            .map((entry) => asRecord(entry)?.key)
            .find(
                (key): key is string => typeof key === 'string' && key !== '',
            );
        if (!objectKey) return 'unusable';

        let result: KnackApiResult;
        try {
            result = await this.requestWithRetry(
                app,
                `/objects/${encodeURIComponent(objectKey)}`,
                { headers: { 'X-Knack-REST-API-Key': apiKey } },
            );
        } catch (error) {
            debugLog('api_key_check_failed', {
                appKey: app.appKey,
                error: error instanceof Error ? error.message : String(error),
            });
            this.unconfirmedApiKeys.set(app.appKey, {
                key: apiKey,
                at: Date.now(),
            });
            return 'unconfirmed';
        }
        this.unconfirmedApiKeys.delete(app.appKey);
        if (result.ok) {
            this.verifiedApiKeys.set(app.appKey, apiKey);
            this.rejectedApiKeys.delete(app.appKey);
            return 'verified';
        }
        if (result.status === 401 || result.status === 403) {
            this.rejectedApiKeys.set(app.appKey, {
                key: apiKey,
                at: Date.now(),
            });
            this.verifiedApiKeys.delete(app.appKey);
            return 'rejected';
        }
        debugLog('api_key_unconfirmed', {
            appKey: app.appKey,
            status: result.status,
        });
        this.unconfirmedApiKeys.set(app.appKey, {
            key: apiKey,
            at: Date.now(),
        });
        return 'unconfirmed';
    }

    private refuseMetadata(app: AppConfig, reason: string): null {
        this.metadataRefusals.set(app.appKey, reason);
        debugLog('runtime_metadata_withheld', { appKey: app.appKey, reason });
        return null;
    }

    /**
     * Drop the app's cached views when its key has changed since they were built, so a
     * view built with a key is not served once the key is gone, and one built from disk
     * during a key failure is not kept once the key is fixed.
     */
    private syncKeyState(app: AppConfig): void {
        const apiKey = this.secrets[app.appKey] ?? null;
        if (
            this.cachesBuiltWithKey.has(app.appKey) &&
            this.cachesBuiltWithKey.get(app.appKey) !== apiKey
        ) {
            this.invalidate(app.appKey);
            this.metadataRefusals.delete(app.appKey);
        }
        this.cachesBuiltWithKey.set(app.appKey, apiKey);
    }

    /** Drop every cached view of one app, or of all apps. */
    invalidate(appKey?: string): void {
        for (const cache of Object.values(this.caches)) {
            if (appKey) cache.delete(appKey);
            else cache.clear();
        }
    }

    /**
     * Runtime metadata first, then the on-disk JSON fallback, caching whichever source
     * actually produced a non-empty value — the shape behind getSchema, getFieldMap and
     * getViewMap. Kept in one place so the precedence rule and the "an empty result
     * falls through to disk" rule can't drift between the three. Metadata withheld for
     * a missing or rejected key falls through to disk too, since those files are the
     * user's own.
     */
    private async loadCached<T>(
        cache: Map<string, CacheEntry<T>>,
        app: AppConfig,
        fromRuntime: (metadata: RuntimeMetadata | null) => T | null,
        fromDisk: () => Promise<T | null> | T | null,
        isEmpty: (value: T) => boolean,
    ): Promise<{ value: T | null; source: CacheSource | null }> {
        this.syncKeyState(app);
        const cached = getCacheEntry(cache, app.appKey);
        if (cached) return { value: cached.value, source: cached.source };

        const runtimeValue = fromRuntime(await this.getRuntimeMetadata(app));
        if (runtimeValue !== null && !isEmpty(runtimeValue)) {
            cache.set(app.appKey, makeCacheEntry(runtimeValue, 'runtime'));
            return { value: runtimeValue, source: 'runtime' };
        }

        const diskValue = await fromDisk();
        if (diskValue !== null && !isEmpty(diskValue)) {
            if (this.apiKeyStatus(app.appKey) === 'rejected') {
                debugLog('schema_from_disk_after_rejected_key', {
                    appKey: app.appKey,
                });
            }
            cache.set(app.appKey, makeCacheEntry(diskValue, 'file'));
            return { value: diskValue, source: 'file' };
        }

        return { value: null, source: null };
    }

    /**
     * Every field. Since no keyword hides a field any more this is the same schema as
     * getSchema; the two names are kept so the exclusion policy and the guards read the
     * one that is guaranteed never to be filtered.
     */
    async getFullSchema(
        app: AppConfig,
    ): Promise<{ schema: CachedSchema | null; source: CacheSource | null }> {
        const { value: schema, source } = await this.loadCached<CachedSchema>(
            this.caches.schema,
            app,
            (metadata) => parseRuntimeSchema(metadata),
            () => this.readMetadataJson<CachedSchema>(app, 'schema.json'),
            (value) => !value.objects?.length,
        );
        return { schema, source };
    }

    /**
     * The schema as the model sees it. Every field is included: the `_mcp_*` keywords
     * limit a field's data and definition, not whether the model knows it exists.
     */
    async getSchema(
        app: AppConfig,
    ): Promise<{ schema: CachedSchema | null; source: CacheSource | null }> {
        return this.getFullSchema(app);
    }

    /** The app's field exclusions: dataAccess.redactedFieldKeys plus the `_mcp_*` keywords. */
    async getFieldExclusions(app: AppConfig): Promise<FieldExclusions> {
        const { schema } = await this.getFullSchema(app);
        if (!schema) return buildFieldExclusions(null, app.dataAccess);
        const memo = this.exclusionMemo.get(schema);
        if (memo && memo.dataAccess === app.dataAccess) return memo.exclusions;
        const exclusions = buildFieldExclusions(schema, app.dataAccess);
        this.exclusionMemo.set(schema, {
            dataAccess: app.dataAccess,
            exclusions,
        });
        return exclusions;
    }

    /** The schema, or an error naming the app: most schema tools cannot do anything without one. */
    async requireSchema(
        app: AppConfig,
    ): Promise<{ schema: CachedSchema; source: CacheSource }> {
        const { schema, source } = await this.getSchema(app);
        if (!schema || !source) {
            const refusal = this.metadataRefusal(app);
            throw new Error(
                `No schema available for "${app.appKey}" from the runtime API or schema.json.${refusal ? ` ${refusal}` : ''}`,
            );
        }
        return { schema, source };
    }

    async getFieldMap(app: AppConfig): Promise<{
        fieldMap: CachedFieldMap | null;
        source: CacheSource | null;
    }> {
        const { value: fieldMap, source } =
            await this.loadCached<CachedFieldMap>(
                this.caches.fieldMap,
                app,
                (metadata) => parseRuntimeFieldMap(metadata),
                async () => {
                    const { schema } = await this.getSchema(app);
                    return coerceFieldMap(
                        this.readMetadataJson<unknown>(app, 'fieldMap.json'),
                        schema,
                    );
                },
                (value) => Object.keys(value).length === 0,
            );
        return { fieldMap, source };
    }

    async getViewMap(
        app: AppConfig,
    ): Promise<{ viewMap: CachedViewMap | null; source: CacheSource | null }> {
        const { value: viewMap, source } = await this.loadCached<CachedViewMap>(
            this.caches.viewMap,
            app,
            (metadata) => parseRuntimeViewMap(metadata),
            () => this.readMetadataJson<CachedViewMap>(app, 'viewMap.json'),
            (value) => Object.keys(value).length === 0,
        );
        return { viewMap, source };
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
        this.syncKeyState(app);
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

    /**
     * Whether this client advertised elicitation, so a person can be asked to confirm.
     *
     * v2 splits the capability into `form`/`url` sub-modes; `elicitInput` below is always
     * called with a `requestedSchema` (form mode), never a `mode: "url"` request, so a
     * client that supports only url-mode elicitation must not be reported as promptable
     * here — it would pass this check and then fail `elicitInput` itself with a
     * `CAPABILITY_NOT_SUPPORTED` error, which `isRequestTimeout` correctly does not treat
     * as a timeout, misreporting a human-promptable client as one that cannot be asked.
     */
    clientCanPromptHuman(): boolean {
        return Boolean(
            this.server?.server.getClientCapabilities()?.elicitation?.form,
        );
    }

    describeClient(): string | null {
        const client = this.server?.server.getClientVersion();
        return client
            ? `${client.name}${client.version ? ` ${client.version}` : ''}`
            : null;
    }
}

/** How long a key left unconfirmed by a 429 or 5xx is served before it is checked again. */
export const UNCONFIRMED_KEY_RECHECK_MS = 60 * 1000;

/** How long a rejected key is trusted to stay rejected before Knack is asked again. */
export const REJECTED_KEY_RECHECK_MS = 5 * 60 * 1000;

function rejectedKeyMessage(appKey: string): string {
    return `Knack rejected the REST API key for appKey "${appKey}". Check the key in your secrets file.`;
}
