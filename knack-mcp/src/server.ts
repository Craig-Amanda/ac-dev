import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { pathToFileURL } from 'node:url';

import { z } from 'zod';

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';

type AppConfig = {
    appKey: string;
    appName?: string;
    appId: string;
    apiBase?: string;
    notes?: string;
    builderAccountSlug?: string;
    builderAppSlug?: string;
    readonly?: boolean;
    allowDelete?: boolean;
    appFolder: string;
};

type SecretsMap = Record<string, string>;

type CachedField = {
    key: string;
    name?: string;
    type?: string;
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

type ViewContextMap = Record<string, { sceneKey?: string; sceneName?: string; sceneSlug?: string }>;

type SceneViewInfo = {
    viewKey: string;
    viewName: string | undefined;
    viewType: string | undefined;
};

type SceneInfo = {
    sceneKey: string;
    sceneName: string | undefined;
    sceneSlug: string | undefined;
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

function slugifyForBuilder(value: string): string {
    return value
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
}

function getBuilderSlugs(app: AppConfig, runtimeMetadata?: RuntimeMetadata | null): { accountSlug: string; appSlug: string } {
    const runtimeApplication = asRecord(getObjectAtPath(runtimeMetadata, 'application'));
    const runtimeAccount = asRecord(runtimeApplication?.account);

    const runtimeAppSlug = typeof runtimeApplication?.slug === 'string'
        ? runtimeApplication.slug
        : typeof runtimeApplication?.name === 'string'
            ? slugifyForBuilder(runtimeApplication.name)
            : null;

    const runtimeAccountSlug = typeof runtimeAccount?.slug === 'string'
        ? runtimeAccount.slug
        : typeof runtimeApplication?.account_slug === 'string'
            ? runtimeApplication.account_slug
            : null;

    const fallbackSlug = slugifyForBuilder(app.appName || app.appKey);

    return {
        accountSlug: app.builderAccountSlug || runtimeAccountSlug || fallbackSlug,
        appSlug: app.builderAppSlug || runtimeAppSlug || fallbackSlug,
    };
}

function makeBuilderBaseUrl(app: AppConfig, runtimeMetadata?: RuntimeMetadata | null): string {
    const { accountSlug, appSlug } = getBuilderSlugs(app, runtimeMetadata);
    return `https://builder.knack.com/${accountSlug}/${appSlug}`;
}

function makeSceneBuilderUrl(app: AppConfig, sceneKey?: string, runtimeMetadata?: RuntimeMetadata | null): string | null {
    if (!sceneKey) return null;
    return `${makeBuilderBaseUrl(app, runtimeMetadata)}/pages/${sceneKey}`;
}

function makeViewBuilderUrl(app: AppConfig, params: { sceneKey?: string; viewKey?: string; viewType?: string }, runtimeMetadata?: RuntimeMetadata | null): string | null {
    if (!params.sceneKey || !params.viewKey) return null;
    const viewTypeSegment = (params.viewType || 'view').trim().toLowerCase();
    return `${makeBuilderBaseUrl(app, runtimeMetadata)}/pages/${params.sceneKey}/views/${params.viewKey}/${viewTypeSegment}`;
}

function makeFieldBuilderUrl(app: AppConfig, params: { objectKey?: string; fieldKey?: string }, runtimeMetadata?: RuntimeMetadata | null): string | null {
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

function getTrimmedString(value: unknown): string | null {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
}

function getOptionLabel(value: unknown): string | null {
    const direct = getTrimmedString(value);
    if (direct) return direct;

    const rec = asRecord(value);
    if (!rec) return null;

    const candidates = [rec.label, rec.name, rec.text, rec.value, rec.identifier];
    for (const candidate of candidates) {
        const label = getTrimmedString(candidate);
        if (label) return label;
    }

    return null;
}

function collectOptionLabels(value: unknown, output: string[], seen: Set<string>): void {
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
            const fieldDescription = typeof field.description === 'string'
                ? field.description
                : typeof fieldMeta?.description === 'string'
                    ? fieldMeta.description
                    : undefined;

            const fieldFormat = asRecord(field.format);
            const fieldRelationship = asRecord(field.relationship);
            const connectedObject =
                (typeof fieldFormat?.object === 'string' ? fieldFormat.object : undefined) ||
                (typeof fieldRelationship?.object === 'string' ? fieldRelationship.object : undefined);
            const choiceOptions = extractChoiceOptions(
                field.options,
                fieldFormat?.options,
                fieldFormat?.choices,
                fieldMeta?.options,
                fieldMeta?.choices
            );
            const allowsMultiple = extractBoolean(
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
                fieldRelationship?.many
            );

            fields.push({
                key: fieldKey,
                name: typeof field.name === 'string' ? field.name : undefined,
                type: typeof field.type === 'string' ? field.type : undefined,
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

        const sceneName = typeof scene.name === 'string' ? scene.name : undefined;
        const sceneSlug = typeof scene.slug === 'string' ? scene.slug : undefined;
        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];

        const views: SceneViewInfo[] = [];
        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (!view) continue;
            const viewKey = typeof view.key === 'string' ? view.key : null;
            if (!viewKey) continue;
            const attributes = asRecord(view.attributes) || view;
            const viewName = typeof attributes.name === 'string' ? attributes.name : undefined;
            const viewType = typeof attributes.type === 'string' ? attributes.type : undefined;
            views.push({ viewKey, viewName, viewType });
        }

        scenes.push({ sceneKey, sceneName, sceneSlug, views });
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

function extractFieldKeysFromString(text: string): string[] {
    const matches = text.match(/field_\d+/gi) || [];
    return [...new Set(matches.map((match) => match.toLowerCase()))];
}

function truncateReferenceText(text: string, maxLength = 300): string {
    const normalised = text.replace(/\s+/g, ' ').trim();
    if (normalised.length <= maxLength) return normalised;
    return `${normalised.slice(0, maxLength)}...`;
}

function classifyFieldReference(sourceType: FieldReference['sourceType'], pathParts: string[]): string[] {
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

    if (/(rule|rules|filter|filters|criteria|condition|conditions)/.test(joined)) {
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
                classification: classifyFieldReference(context.sourceType, context.pathParts),
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

    for (const [key, value] of Object.entries(node as Record<string, unknown>)) {
        const nextPathParts = [...context.pathParts, key];

        if (/^field_\d+$/i.test(key)) {
            const fieldKey = key.toLowerCase();
            addFieldReference(context.index, context.dedupe, {
                fieldKey,
                sourceType: context.sourceType,
                matchType: 'propertyKey',
                path: nextPathParts.join('.'),
                classification: [...classifyFieldReference(context.sourceType, nextPathParts), 'propertyKey'],
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
        const viewName = typeof viewAttrs.name === 'string' ? viewAttrs.name : undefined;
        const viewType = typeof viewAttrs.type === 'string' ? viewAttrs.type : undefined;

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

type FieldShapeInfo = {
    summary: string;
    formattedShape: unknown;
    rawShape: unknown;
    notes?: string;
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
        formattedShape: '"<a href=\"mailto:user@example.com\">user@example.com</a>"',
        rawShape: '{ "email": "user@example.com", "label": "Work" }',
        notes: 'Formatted output is typically a mailto anchor. Raw is an object with email and label.',
    },
    phone: {
        summary: 'Phone value with structured number parts.',
        formattedShape: '"<a href=\"tel:07543423538\">07543423538</a>"',
        rawShape: '{ "area": null, "number": "07543423538", "ext": null, "full": "07543423538", "country": null, "formatted": "07543423538" }',
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
        summary: 'Computed equation result whose shape depends on the configured return type.',
        formattedShape: '"(-42.00)" | "05/01/2026"',
        rawShape: '42 | "2026-01-05" | { "date": "01/05/2026", "date_formatted": "05/01/2026", "unix_timestamp": 1767571200000 }',
        notes: 'Equation fields can return numbers, plain strings, or date-like values depending on configuration. For date-returning equations, raw may be a scalar date string or a structured date object, while formatted applies the field display format.',
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
        rawShape: '{ "title": "Mr", "first": "John", "middle": "A", "last": "Smith", "full": "John A. Smith" }',
        notes: 'Raw is an object with individual name parts. Optional keys such as middle or suffix may be omitted or blank.',
    },
    address: {
        summary: 'Postal address with geocoordinates.',
        formattedShape: '"123 Main St<br />Springfield, IL 62701"',
        rawShape: '{ "street": "123 Main St", "street2": null, "city": "Springfield", "state": "IL", "zip": "62701", "country": null, "longitude": null, "latitude": null, "full": "123 Main St Springfield, IL 62701" }',
        notes: 'Formatted output can contain HTML line breaks. Raw includes address components plus a full string; geo fields are often null.',
    },
    date_time: {
        summary: 'Date and/or time value.',
        formattedShape: '"01/15/2024 10:30 am"',
        rawShape: '{ "date": "01/15/2024", "date_formatted": "January 15, 2024", "hours": "10", "minutes": "30", "am_pm": "AM", "unix_timestamp": 1705316400000, "iso_timestamp": "2024-01-15T10:30:00.000Z", "timestamp": "01/15/2024 10:30 am" }',
        notes: 'Formatted output depends on the field configuration and may be date-only, time-only, or a range. Raw for native date/time fields is typically a structured object with date/time parts, proper_* timestamp keys, and an optional to object for ranges rather than a scalar string.',
    },
    timer: {
        summary: 'Time tracking timer with start/stop times.',
        formattedShape: '"2:30:00"',
        rawShape: '{ "times": [{ "from": { "date": "01/15/2024", "hours": "10", "minutes": "00", "am_pm": "AM" }, "to": { "date": "01/15/2024", "hours": "12", "minutes": "30", "am_pm": "PM" } }], "running": false, "hours": 2.5, "minutes": 150, "seconds": 9000 }',
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
        formattedShape: '"<span class=\"abc123def456\" data-kn=\"connection-value\">Record Label A</span>"',
        rawShape: '[{ "id": "abc123def456", "identifier": "Record Label A" }, { "id": "789xyz", "identifier": "Record Label B" }]',
        notes: 'Raw is an array of objects with id and identifier. Formatted output is HTML, usually one span per connected record, not a plain comma-joined string.',
    },
    file: {
        summary: 'Uploaded file attachment.',
        formattedShape: '"document.pdf"',
        rawShape: '{ "id": "abc123", "filename": "document.pdf", "url": "https://...", "thumb_url": null, "size": 204800, "mime_type": "application/pdf" }',
        notes: 'Raw includes the download URL and file metadata.',
    },
    image: {
        summary: 'Uploaded image attachment.',
        formattedShape: '"<img src=\'...\' />"',
        rawShape: '{ "id": "abc123", "filename": "photo.jpg", "url": "https://...photo.jpg", "thumb_url": "https://...photo_thumb.jpg", "size": 102400, "mime_type": "image/jpeg" }',
        notes: 'Raw includes full-size and thumbnail URLs. Formatted is an HTML img tag.',
    },
    signature: {
        summary: 'Captured signature.',
        formattedShape: '"<img src=\"data:image/svg+xml;base64,...\" />"',
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
    importOrder: Array<{ objectKey: string; objectName: string; suggestedUniqueImportKey: string }>;
    objects: SeedCsvObject[];
};

type ExternalConnectionLookup = {
    objectKey: string;
    objectName?: string;
    values: string[];
    source: 'api';
    lookupField: 'identifier';
};

const CONNECTION_DISPLAY_VALUE_PRIORITY = ['identifier', 'display', 'name', 'label', 'id'] as const;

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

const SAMPLE_FIRST_NAMES = ['Avery', 'Jordan', 'Casey', 'Morgan', 'Riley', 'Taylor'];
const SAMPLE_LAST_NAMES = ['Bennett', 'Carter', 'Diaz', 'Foster', 'Hayes', 'Morgan'];
const SAMPLE_COMPANY_PREFIXES = ['Acme', 'Bluebird', 'Cedar', 'Northwind', 'Summit', 'Harbor'];
const SAMPLE_COMPANY_SUFFIXES = ['Logistics', 'Health', 'Supply', 'Advisory', 'Labs', 'Services'];
const SAMPLE_STREETS = ['100 Main St', '245 Oak Ave', '18 Market St', '77 River Rd', '910 Sunset Blvd', '62 Cedar Ln'];
const SAMPLE_CITIES = ['Austin', 'Denver', 'Madison', 'Phoenix', 'Raleigh', 'Seattle'];
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
    if (trimmed.endsWith('ies') && trimmed.length > 3) return `${trimmed.slice(0, -3)}y`;
    if (trimmed.endsWith('ses') && trimmed.length > 3) return trimmed.slice(0, -2);
    if (trimmed.endsWith('s') && !trimmed.endsWith('ss') && trimmed.length > 1) return trimmed.slice(0, -1);
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
    const parts = toSnakeCase(singularize(objectName)).split('_').filter(Boolean);
    const base = parts.length > 1
        ? parts.map((part) => part[0]).join('')
        : (parts[0] || 'rec').slice(0, 4);
    return base.toUpperCase();
}

function makeUniqueValue(objectName: string, index: number): string {
    return `${makeKeyPrefix(objectName)}-${String(index + 1).padStart(3, '0')}`;
}

function inferLabelValue(objectName: string, rowIndex: number): string {
    const lowerName = objectName.toLowerCase();
    if (/(company|client|customer|vendor|supplier|partner|agency|business|organization|organisation)/.test(lowerName)) {
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

function buildCsv(headers: string[], rows: Array<Record<string, string>>): string {
    const headerLine = headers.map(escapeCsvCell).join(',');
    const dataLines = rows.map((row) => headers.map((header) => escapeCsvCell(row[header] || '')).join(','));
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
            return [`${header} Title`, `${header} First`, `${header} Middle`, `${header} Last`, `${header} Suffix`];
        case 'address':
            return [`${header} Street`, `${header} Street 2`, `${header} City`, `${header} State`, `${header} Zip`, `${header} Country`];
        default:
            return [header];
    }
}

function findFieldByName(fields: CachedField[], target: string): CachedField | undefined {
    const normalizedTarget = target.trim().toLowerCase();
    return fields.find((field) => getFieldHeader(field).trim().toLowerCase() === normalizedTarget);
}

function chooseUniqueImportField(fields: CachedField[]): CachedField | undefined {
    const pattern = /\b(code|sku|external id|external_id|import key|import_key|unique key|unique_key|email|record key|record_key|id)\b/i;
    return fields.find((field) => {
        const type = (field.type || '').toLowerCase();
        return !['connection', 'multiple_choice', 'address', 'name'].includes(type) && pattern.test(getFieldHeader(field));
    });
}

function chooseLabelField(fields: CachedField[]): CachedField | undefined {
    const preferred = fields.find((field) => /\b(name|title|label)\b/i.test(getFieldHeader(field)));
    if (preferred) return preferred;
    return fields.find((field) => ['short_text', 'paragraph_text', 'email', 'name'].includes((field.type || '').toLowerCase()));
}

function getDefaultChoiceOptions(field: CachedField): string[] {
    if ((field.type || '').toLowerCase() === 'user_roles') {
        return ['Admin', 'Manager', 'Viewer'];
    }
    return ['Option A', 'Option B', 'Option C'];
}

function getSeedRowCount(fields: CachedField[], minimumRows: number): number {
    const optionCount = fields.reduce((max, field) => Math.max(max, field.choiceOptions?.length || 0), 0);
    return Math.max(minimumRows, Math.min(optionCount || minimumRows, 6));
}

function buildSeedObjectMeta(object: CachedObject, minimumRows: number): SeedObjectMeta {
    const objectName = object.name || object.key;
    const importableFields = (object.fields || []).filter(isImportableField);
    const uniqueImportField = chooseUniqueImportField(importableFields);
    const labelField = chooseLabelField(importableFields);
    const uniqueImportKey = uniqueImportField ? getFieldHeader(uniqueImportField) : makeSyntheticImportKey(objectName);
    const syntheticLabelField = labelField ? undefined : makeSyntheticLabelField(objectName);
    const rowCount = getSeedRowCount(importableFields, minimumRows);

    return {
        object,
        objectName,
        uniqueImportKey,
        uniqueImportField,
        labelField,
        syntheticLabelField,
        rowCount,
        uniqueValues: Array.from({ length: rowCount }, (_, index) => makeUniqueValue(objectName, index)),
        usedPlaceholderChoiceFields: [],
        skippedFields: (object.fields || [])
            .filter((field) => !isImportableField(field))
            .map((field) => getFieldHeader(field)),
    };
}

function topologicallySortObjects(objects: CachedObject[]): CachedObject[] {
    const objectsByKey = new Map(objects.map((object) => [object.key, object]));
    const dependents = new Map<string, Set<string>>();
    const indegree = new Map<string, number>(objects.map((object) => [object.key, 0]));

    for (const object of objects) {
        for (const field of object.fields || []) {
            if ((field.type || '').toLowerCase() !== 'connection' || !field.connectedObject || !objectsByKey.has(field.connectedObject)) continue;
            if (!dependents.has(field.connectedObject)) dependents.set(field.connectedObject, new Set());
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
                    queue.sort((a, b) => (a.name || a.key).localeCompare(b.name || b.key));
                }
            }
        }
    }

    if (ordered.length === objects.length) return ordered;

    const seen = new Set(ordered.map((object) => object.key));
    const remaining = objects.filter((object) => !seen.has(object.key)).sort((a, b) => (a.name || a.key).localeCompare(b.name || b.key));
    return [...ordered, ...remaining];
}

function populateMultipartField(row: Record<string, string>, headers: string[], rowIndex: number): void {
    if (headers.length === 5) {
        row[headers[0]] = rowIndex % 2 === 0 ? 'Ms' : 'Mr';
        row[headers[1]] = SAMPLE_FIRST_NAMES[rowIndex % SAMPLE_FIRST_NAMES.length];
        row[headers[2]] = '';
        row[headers[3]] = SAMPLE_LAST_NAMES[rowIndex % SAMPLE_LAST_NAMES.length];
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
    rowIndex: number
): void {
    const header = getFieldHeader(field);
    const fieldType = (field.type || '').toLowerCase();
    const lowerHeader = header.toLowerCase();
    const shouldUseMultipleValuesOnAlternatingRows = Boolean(field.allowsMultiple && rowIndex % 2 === 1);

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
            const connectedMeta = field.connectedObject ? metasByKey.get(field.connectedObject) : undefined;
            const externalLookup = field.connectedObject ? externalConnectionLookups.get(field.connectedObject) : undefined;
            if (!connectedMeta && externalLookup?.values.length) {
                const selectedValues = [externalLookup.values[rowIndex % externalLookup.values.length]];
                if (shouldUseMultipleValuesOnAlternatingRows && externalLookup.values.length > 1) {
                    selectedValues.push(externalLookup.values[(rowIndex + 1) % externalLookup.values.length]);
                }
                row[header] = selectedValues.join(',');
                return;
            }

            if (!connectedMeta) {
                row[header] = makeUniqueValue(field.connectedObject || header, 0);
                return;
            }

            const selectedValues = [connectedMeta.uniqueValues[rowIndex % connectedMeta.uniqueValues.length]];
            if (shouldUseMultipleValuesOnAlternatingRows && connectedMeta.uniqueValues.length > 1) {
                selectedValues.push(connectedMeta.uniqueValues[(rowIndex + 1) % connectedMeta.uniqueValues.length]);
            }
            row[header] = selectedValues.join(',');
            return;
        }
        case 'multiple_choice':
        case 'user_roles': {
            const options = field.choiceOptions?.length ? field.choiceOptions : getDefaultChoiceOptions(field);
            if (!field.choiceOptions?.length) {
                meta.usedPlaceholderChoiceFields.push(header);
            }
            const selectedValues = [options[rowIndex % options.length]];
            if (shouldUseMultipleValuesOnAlternatingRows && options.length > 1) {
                selectedValues.push(options[(rowIndex + 1) % options.length]);
            }
            row[header] = selectedValues.join(',');
            return;
        }
        case 'email':
            row[header] = `${toSnakeCase(singularize(meta.objectName)) || 'record'}${rowIndex + 1}@example.com`;
            return;
        case 'phone':
            row[header] = `555010${String(rowIndex + 1).padStart(3, '0')}`;
            return;
        case 'number':
        case 'currency':
            row[header] = ((rowIndex + 1) * 1250).toFixed(fieldType === 'currency' ? 2 : 0);
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
            row[header] = `Sample ${humanizeObjectName(meta.objectName).toLowerCase()} notes for workflow testing row ${rowIndex + 1}.`;
            return;
        case 'link':
            row[header] = `https://example.com/${toSnakeCase(singularize(meta.objectName)) || 'record'}/${rowIndex + 1}`;
            return;
        case 'short_text':
        default:
            row[header] = lowerHeader.includes('status')
                ? `Active ${rowIndex + 1}`
                : lowerHeader.includes('code') || lowerHeader.includes('sku') || lowerHeader.includes('id')
                    ? meta.uniqueValues[rowIndex]
                    : `${inferLabelValue(meta.objectName, rowIndex)} ${header}`;
            return;
    }
}

export function generateSeedCsvWorkbook(
    schema: CachedSchema,
    options?: { objectKeys?: string[]; rowsPerObject?: number; externalConnectionLookups?: Record<string, ExternalConnectionLookup> }
): SeedCsvWorkbook {
    const requestedKeys = options?.objectKeys?.length ? new Set(options.objectKeys) : null;
    const selectedObjects = (schema.objects || []).filter((object) => !requestedKeys || requestedKeys.has(object.key));
    const orderedObjects = topologicallySortObjects(selectedObjects);
    const metas = orderedObjects.map((object) => buildSeedObjectMeta(object, Math.max(options?.rowsPerObject || 4, 2)));
    const metasByKey = new Map(metas.map((meta) => [meta.object.key, meta]));
    const externalConnectionLookups = new Map(Object.entries(options?.externalConnectionLookups || {}));

    const objects: SeedCsvObject[] = metas.map((meta) => {
        const headers: string[] = [];
        const rows = Array.from({ length: meta.rowCount }, () => ({} as Record<string, string>));
        const importableFields = (meta.object.fields || []).filter(isImportableField);

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
                row[meta.syntheticLabelField] = inferLabelValue(meta.objectName, rowIndex);
            }

            for (const field of importableFields) {
                const multipartHeaders = getMultipartHeaders(field);
                if (multipartHeaders.length > 1) {
                    populateMultipartField(row, multipartHeaders, rowIndex);
                } else {
                    populateScalarField(row, field, meta, metasByKey, externalConnectionLookups, rowIndex);
                }
            }
        });

        const notes: string[] = [];
        if (!meta.uniqueImportField) {
            notes.push(`Suggested unique import key "${meta.uniqueImportKey}" is synthetic so child CSVs have a stable lookup value.`);
        }
        for (const field of importableFields.filter((entry) => (entry.type || '').toLowerCase() === 'connection')) {
            const connectedMeta = field.connectedObject ? metasByKey.get(field.connectedObject) : undefined;
            const externalLookup = field.connectedObject ? externalConnectionLookups.get(field.connectedObject) : undefined;
            if (connectedMeta) {
                notes.push(`Connection field "${getFieldHeader(field)}" uses ${connectedMeta.objectName}.${connectedMeta.uniqueImportKey} as the import lookup value.`);
                continue;
            }
            if (externalLookup) {
                notes.push(`Connection field "${getFieldHeader(field)}" uses existing ${externalLookup.objectName || field.connectedObject || 'connected object'} display values fetched from the API (${externalLookup.lookupField}).`);
                continue;
            }
            notes.push(`Connection field "${getFieldHeader(field)}" uses ${field.connectedObject || 'the connected object'} via an existing unique lookup field as the import lookup value.`);
        }
        if (meta.usedPlaceholderChoiceFields.length) {
            const uniquePlaceholderFields = Array.from(new Set(meta.usedPlaceholderChoiceFields));
            notes.push(`Schema metadata did not expose exact option labels for ${uniquePlaceholderFields.join(', ')}; placeholder option labels were used and should be replaced before import if needed.`);
        }
        if (meta.skippedFields.length) {
            notes.push(`Skipped non-importable/system fields: ${meta.skippedFields.join(', ')}.`);
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
    return value === null || value === undefined || value === '' || (Array.isArray(value) && value.length === 0);
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
    return Array.isArray(value) && value.every((entry) => typeof entry === 'string');
}

function extractRecordList(body: unknown): Record<string, unknown>[] {
    if (Array.isArray(body)) {
        return body.map((entry) => asRecord(entry)).filter((entry): entry is Record<string, unknown> => Boolean(entry));
    }

    const rec = asRecord(body);
    const records = rec?.records;
    if (!Array.isArray(records)) return [];
    return records.map((entry) => asRecord(entry)).filter((entry): entry is Record<string, unknown> => Boolean(entry));
}

function extractConnectionDisplayValues(body: unknown): string[] {
    const values: string[] = [];
    const seen = new Set<string>();

    for (const record of extractRecordList(body)) {
        const value = CONNECTION_DISPLAY_VALUE_PRIORITY
            .map((key) => getStringFromUnknown(record[key]))
            .find((candidate): candidate is string => Boolean(candidate));
        if (!value) continue;
        const dedupeKey = value.toLowerCase();
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);
        values.push(value);
    }

    return values;
}

function validateFieldShape(fieldType: string, formatted: unknown, raw: unknown): ShapeValidationResult {
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
            addFinding(typeof formatted === 'string', 'Formatted value should be a string.');
            addFinding(typeof raw === 'string', 'Raw value should be a string.');
            break;
        case 'email':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string or HTML anchor.');
            addFinding(rawHasKeys(raw, ['email']), 'Raw value should be an object containing an email key.');
            break;
        case 'phone':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string or HTML anchor.');
            addFinding(rawHasKeys(raw, ['number', 'full', 'formatted']), 'Raw value should be a phone object with number/full/formatted keys.');
            break;
        case 'number':
            addFinding(typeof formatted === 'string' || typeof formatted === 'number', 'Formatted value should be a string or number.');
            addFinding(typeof raw === 'number' || typeof raw === 'string', 'Raw value should be a number or numeric string.');
            break;
        case 'currency':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string.');
            addFinding(typeof raw === 'number' || typeof raw === 'string', 'Raw value should be a number or numeric string.');
            break;
        case 'auto_increment':
        case 'rating':
        case 'sum':
        case 'count':
        case 'average':
        case 'min':
        case 'max':
            addFinding(typeof formatted === 'string' || typeof formatted === 'number', 'Formatted value should be numeric-like.');
            addFinding(typeof raw === 'number', 'Raw value should be a number.');
            break;
        case 'boolean':
        case 'yes_no':
            addFinding(typeof formatted === 'string', 'Formatted value should be a display string such as Yes/No.');
            addFinding(typeof raw === 'boolean', 'Raw value should be a boolean.');
            break;
        case 'equation':
            addFinding(typeof formatted === 'string' || typeof formatted === 'number', 'Formatted value should be a string or number.');
            addFinding(typeof raw === 'number' || typeof raw === 'string' || asRecord(raw) !== null, 'Raw value should be a number, string, or structured date-like object.');
            break;
        case 'name':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string.');
            addFinding(rawHasKeys(raw, ['first', 'last', 'full', 'title', 'middle', 'suffix']), 'Raw value should be an object containing name parts.');
            break;
        case 'address':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string, often with HTML line breaks.');
            addFinding(rawHasKeys(raw, ['street', 'city', 'zip', 'full']), 'Raw value should be an object containing address components.');
            break;
        case 'date_time':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string.');
            addFinding(rawHasKeys(raw, ['date', 'timestamp', 'unix_timestamp', 'iso_timestamp', 'to']), 'Raw value should be a structured date/time object.');
            break;
        case 'timer':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string.');
            addFinding(rawHasKeys(raw, ['times', 'hours', 'minutes', 'seconds']), 'Raw value should be a timer object containing time segments or totals.');
            break;
        case 'multiple_choice':
            addFinding(typeof formatted === 'string', 'Formatted value should be a display string.');
            addFinding(typeof raw === 'string' || Array.isArray(raw), 'Raw value should be a string or an array of strings.');
            if (Array.isArray(raw)) {
                addFinding(rawIsStringArray(raw), 'Raw multiple choice arrays should contain strings.');
            }
            break;
        case 'connection':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string, usually HTML.');
            addFinding(rawIsConnectionArray(raw), 'Raw value should be an array of connection objects with id and/or identifier.');
            break;
        case 'file':
        case 'image':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string.');
            addFinding(rawHasKeys(raw, ['id', 'filename', 'url', 'thumb_url', 'mime_type']), 'Raw value should be an attachment object with file metadata.');
            break;
        case 'signature':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string.');
            addFinding(rawHasKeys(raw, ['svg', 'base30', 'base64', 'url', 'thumb_url', 'timestamp', 'date']), 'Raw value should be a signature object with stroke/image metadata.');
            break;
        case 'link':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string, often HTML.');
            addFinding(rawHasKeys(raw, ['url', 'label']), 'Raw value should be an object containing url/label.');
            break;
        case 'user_roles':
            addFinding(typeof formatted === 'string', 'Formatted value should be a display string.');
            addFinding(rawIsStringArray(raw), 'Raw value should be an array of role name strings.');
            break;
        case 'password':
            addFinding(typeof formatted === 'string', 'Formatted value should be a string.');
            addFinding(rawHasKeys(raw, ['validation']), 'Raw value should be an object containing password validation metadata.');
            break;
        default:
            return {
                status: 'unknown',
                observedFormattedShape,
                observedRawShape,
                findings: [`No automated verifier is defined for field type ${fieldType}.`],
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

function createServer() {
    const knackAppsDir = ENV_KNACK_APPS_DIR;
    if (!knackAppsDir) {
        throw new Error('Missing env var KNACK_APPS_DIR (absolute path to your KnackApps folder).');
    }

    const apps = discoverApps(knackAppsDir);
    if (!apps.length) {
        throw new Error(`No apps discovered in ${knackAppsDir}. Ensure KnackApps/*/schema/app.json (or legacy KnackApps/*/app.json) exists.`);
    }

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
    const fieldReferenceCache = new Map<string, CacheEntry<CachedFieldReferenceIndex>>();

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

    function assertWritable(app: AppConfig): void {
        if (app.readonly !== false) {
            throw new Error(`App "${app.appKey}" is readonly. Set "readonly": false in app.json to enable writes.`);
        }
    }

    function assertDeletable(app: AppConfig): void {
        assertWritable(app);
        if (app.allowDelete !== true) {
            throw new Error(`App "${app.appKey}" does not allow deletions. Set "allowDelete": true in app.json to enable delete operations.`);
        }
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

    function readFieldReferenceIndexFromDisk(app: AppConfig): CachedFieldReferenceIndex | null {
        return readMetadataJson<CachedFieldReferenceIndex>(app, 'fieldReferenceIndex.json');
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

    function getExternalSeedConnectionTargets(schema: CachedSchema, objectKeys?: string[]): CachedObject[] {
        const selectedKeys = new Set(objectKeys?.length ? objectKeys : (schema.objects || []).map((object) => object.key));
        const objectsByKey = new Map((schema.objects || []).map((object) => [object.key, object]));
        const targets = new Map<string, CachedObject>();

        for (const object of schema.objects || []) {
            if (!selectedKeys.has(object.key)) continue;
            for (const field of object.fields || []) {
                if ((field.type || '').toLowerCase() !== 'connection' || !field.connectedObject || selectedKeys.has(field.connectedObject)) continue;
                const target = objectsByKey.get(field.connectedObject);
                if (target) {
                    targets.set(target.key, target);
                }
            }
        }

        return [...targets.values()].sort((left, right) => (left.name || left.key).localeCompare(right.name || right.key));
    }

    async function fetchExternalSeedConnectionLookups(
        app: AppConfig,
        targets: CachedObject[],
        rowsPerObject: number
    ): Promise<{
        lookups: Record<string, ExternalConnectionLookup>;
        fetches: Array<{ objectKey: string; objectName?: string; apiPath: string; fetchedValues: number; ok: boolean; message?: string }>;
    }> {
        const apiKey = getApiKeyOrThrow(app.appKey);
        const lookups: Record<string, ExternalConnectionLookup> = {};
        const fetches: Array<{ objectKey: string; objectName?: string; apiPath: string; fetchedValues: number; ok: boolean; message?: string }> = [];

        for (const target of targets) {
            const params = new URLSearchParams();
            params.set('page', '1');
            params.set('rows_per_page', String(Math.max(rowsPerObject, 2)));
            const apiPath = `/objects/${target.key}/records?${params.toString()}`;
            const result = await knackRequest(app, apiKey, apiPath);
            const values = result.ok ? extractConnectionDisplayValues(result.body) : [];

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

    async function getScenesForApp(app: AppConfig): Promise<SceneInfo[]> {
        const runtimeMetadata = await getRuntimeMetadata(app);
        return parseRuntimeScenes(runtimeMetadata);
    }

    async function getFieldReferenceIndexForApp(app: AppConfig): Promise<{ index: CachedFieldReferenceIndex | null; source: CacheSource | null }> {
        const cached = getCacheEntry(fieldReferenceCache, app.appKey);
        if (cached) return { index: cached.value, source: cached.source };

        const [schemaResult, fieldMapResult, viewMapResult, viewContextMap] = await Promise.all([
            getSchemaForApp(app),
            getFieldMapForApp(app),
            getViewMapForApp(app),
            getViewContextMapForApp(app),
        ]);

        if (schemaResult.schema || fieldMapResult.fieldMap || viewMapResult.viewMap) {
            const index = buildFieldReferenceIndex({
                schema: schemaResult.schema,
                fieldMap: fieldMapResult.fieldMap,
                viewMap: viewMapResult.viewMap,
                viewContextMap,
            });

            if (Object.keys(index).length) {
                const source: CacheSource = [schemaResult.source, fieldMapResult.source, viewMapResult.source]
                    .every((entry) => entry === 'runtime')
                    ? 'runtime'
                    : 'file';
                fieldReferenceCache.set(app.appKey, makeCacheEntry(index, source));
                return { index, source };
            }
        }

        const diskIndex = readFieldReferenceIndexFromDisk(app);
        if (diskIndex && Object.keys(diskIndex).length) {
            fieldReferenceCache.set(app.appKey, makeCacheEntry(diskIndex, 'file'));
            return { index: diskIndex, source: 'file' };
        }

        return { index: null, source: null };
    }

    async function getBuilderLinksForApp(app: AppConfig, params: { sceneKey?: string; viewKey?: string; viewType?: string; objectKey?: string; fieldKey?: string }) {
        const runtimeMetadata = await getRuntimeMetadata(app);
        return {
            base: makeBuilderBaseUrl(app, runtimeMetadata),
            scene: makeSceneBuilderUrl(app, params.sceneKey, runtimeMetadata),
            view: makeViewBuilderUrl(app, {
                sceneKey: params.sceneKey,
                viewKey: params.viewKey,
                viewType: params.viewType,
            }, runtimeMetadata),
            field: makeFieldBuilderUrl(app, {
                objectKey: params.objectKey,
                fieldKey: params.fieldKey,
            }, runtimeMetadata),
        };
    }

    async function findFieldOwnerForApp(app: AppConfig, fieldKey: string): Promise<{ objectKey?: string; objectName?: string; fieldName?: string } | null> {
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
            return makeTextResponse({
                ok: true,
                knackAppsDir,
                activeAppKey: state.activeAppKey,
                apps: freshApps.map((a) => ({
                    appKey: a.appKey,
                    appName: a.appName,
                    appId: a.appId,
                    appFolder: a.appFolder,
                    readonly: a.readonly !== false,
                    allowDelete: a.allowDelete === true,
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
            const fieldReferenceIndexPath = resolveMetadataFilePath(app, 'fieldReferenceIndex.json');

            const schemaEntry = getCacheEntry(schemaCache, app.appKey);
            const fieldMapEntry = getCacheEntry(fieldMapCache, app.appKey);
            const viewMapEntry = getCacheEntry(viewMapCache, app.appKey);
            const metadataEntry = getCacheEntry(runtimeMetadataCache, app.appKey);
            const fieldReferenceEntry = getCacheEntry(fieldReferenceCache, app.appKey);

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
                    fieldReferenceIndexPath,
                    fieldReferenceIndexExists: metadataFileExists(app, 'fieldReferenceIndex.json'),
                    fieldReferenceIndexPathCandidates: getMetadataFilePaths(app, 'fieldReferenceIndex.json'),
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
                    fieldReferences: fieldReferenceEntry
                        ? {
                            cached: true,
                            source: fieldReferenceEntry.source,
                            loadedAt: new Date(fieldReferenceEntry.loadedAt).toISOString(),
                            expiresAt: new Date(fieldReferenceEntry.expiresAt).toISOString(),
                            expiresInMs: Math.max(0, fieldReferenceEntry.expiresAt - Date.now()),
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
                        const fieldReferenceResult = await getFieldReferenceIndexForApp(app);

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
                            if (fieldReferenceResult.index) {
                                persisted.fieldReferenceIndex = writeMetadataJson(app, 'fieldReferenceIndex.json', fieldReferenceResult.index);
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
                            description: field.description,
                        })),
                    }
                    : null,
                recordsResponse: recordsResult,
            });
        }
    );

    server.tool(
        'knack_get_raw_object_metadata',
        'Return the raw runtime metadata object payload for a Knack object before schema normalization. Useful for diagnosing fields that may not survive parser transforms.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_get_raw_object_metadata', args: { appKey, objectKey } });

            const runtimeMetadata = await getRuntimeMetadata(app);
            if (!runtimeMetadata) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No runtime metadata available from Knack application metadata endpoint.',
                });
            }

            const directObjects = getObjectAtPath(runtimeMetadata, 'objects');
            const nestedObjects = getObjectAtPath(runtimeMetadata, 'application', 'objects');
            const objectsRaw = Array.isArray(directObjects)
                ? directObjects
                : Array.isArray(nestedObjects)
                    ? nestedObjects
                    : null;

            if (!objectsRaw) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'Runtime metadata did not contain an objects array.',
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
                            return typeof obj?.key === 'string' ? obj.key : null;
                        })
                        .filter((key): key is string => Boolean(key)),
                });
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source: 'runtime',
                objectKey,
                rawObject,
            });
        }
    );

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
                    description: f.description,
                    builderUrl: makeFieldBuilderUrl(app, { objectKey: obj.key, fieldKey: f.key }, runtimeMetadata),
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
        'Return a Knack object definition (object metadata + fields) from cached schema data, including field descriptions when available.',
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
                        description: field.description,
                        builderUrl: makeFieldBuilderUrl(app, { objectKey: obj.key, fieldKey: field.key }, runtimeMetadata),
                    })),
                },
            });
        }
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
                    description: field.description,
                    builderUrl: makeFieldBuilderUrl(app, { objectKey: obj.key, fieldKey: field.key }, runtimeMetadata),
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
                        builderUrl: makeFieldBuilderUrl(app, { objectKey: obj.key, fieldKey: field.key }, runtimeMetadata),
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
                        builderUrl: makeFieldBuilderUrl(app, { objectKey: obj.key, fieldKey: field.key }, runtimeMetadata),
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

            const viewMapResult = await getViewMapForApp(app);
            const viewType = typeof viewMapResult.viewMap?.[viewKey]?.type === 'string'
                ? viewMapResult.viewMap[viewKey].type as string
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

            const viewContextMap = await getViewContextMapForApp(app);
            const context = viewContextMap[viewKey] || {};

            const builderUrls = await getBuilderLinksForApp(app, {
                sceneKey: context.sceneKey,
                viewKey,
                viewType: typeof attributes.type === 'string' ? attributes.type : undefined,
            });

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                viewKey,
                attributes,
                builderUrls,
            });
        }
    );

    server.tool(
        'knack_find_views_with_record_rule_field',
        'Find all views whose record-rule-related metadata references a specific field id.',
        {
            appKey: z.string().optional(),
            fieldKey: z.string().regex(/^field_\d+$/i),
            maxResults: z.number().int().min(1).max(5000).default(500),
        },
        async ({ appKey, fieldKey, maxResults }) => {
            const app = getAppOrThrow(appKey);
            const normalisedFieldKey = fieldKey.toLowerCase();
            debugLog('tool_call', { tool: 'knack_find_views_with_record_rule_field', args: { appKey, fieldKey: normalisedFieldKey, maxResults } });

            const fieldReferenceResult = await getFieldReferenceIndexForApp(app);
            const references = fieldReferenceResult.index?.[normalisedFieldKey] || [];
            const recordRuleRefs = references
                .filter((reference) => reference.viewKey && reference.classification.includes('viewRecordRule'))
                .slice(0, maxResults);

            const viewsByKey = new Map<string, {
                viewKey: string;
                viewName?: string;
                viewType?: string;
                sceneKey?: string;
                sceneName?: string;
                sceneSlug?: string;
                matchedPaths: string[];
                matches: FieldReference[];
            }>();

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
            const fieldOwner = await findFieldOwnerForApp(app, normalisedFieldKey);

            const results = [...viewsByKey.values()].map((entry) => ({
                ...entry,
                matchedPaths: [...new Set(entry.matchedPaths)].sort((left, right) => left.localeCompare(right)),
                matchCount: entry.matches.length,
                builderUrls: {
                    scene: makeSceneBuilderUrl(app, entry.sceneKey, runtimeMetadata),
                    view: makeViewBuilderUrl(app, {
                        sceneKey: entry.sceneKey,
                        viewKey: entry.viewKey,
                        viewType: entry.viewType,
                    }, runtimeMetadata),
                },
            }));

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source: fieldReferenceResult.source,
                fieldKey: normalisedFieldKey,
                builderUrls: {
                    field: makeFieldBuilderUrl(app, {
                        objectKey: fieldOwner?.objectKey,
                        fieldKey: normalisedFieldKey,
                    }, runtimeMetadata),
                },
                totalMatches: recordRuleRefs.length,
                totalViews: results.length,
                results,
            });
        }
    );

    server.tool(
        'knack_list_field_references',
        'List all cached schema, alias, and view references for a specific field id.',
        {
            appKey: z.string().optional(),
            fieldKey: z.string().regex(/^field_\d+$/i),
            maxResults: z.number().int().min(1).max(10000).default(1000),
        },
        async ({ appKey, fieldKey, maxResults }) => {
            const app = getAppOrThrow(appKey);
            const normalisedFieldKey = fieldKey.toLowerCase();
            debugLog('tool_call', { tool: 'knack_list_field_references', args: { appKey, fieldKey: normalisedFieldKey, maxResults } });

            const fieldReferenceResult = await getFieldReferenceIndexForApp(app);
            const references = (fieldReferenceResult.index?.[normalisedFieldKey] || []).slice(0, maxResults);
            const runtimeMetadata = await getRuntimeMetadata(app);
            const fieldOwner = await findFieldOwnerForApp(app, normalisedFieldKey);

            const countsBySource = new Map<string, number>();
            const countsByClassification = new Map<string, number>();

            for (const reference of references) {
                countsBySource.set(reference.sourceType, (countsBySource.get(reference.sourceType) || 0) + 1);
                for (const classification of reference.classification) {
                    countsByClassification.set(classification, (countsByClassification.get(classification) || 0) + 1);
                }
            }

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source: fieldReferenceResult.source,
                fieldKey: normalisedFieldKey,
                builderUrls: {
                    field: makeFieldBuilderUrl(app, {
                        objectKey: fieldOwner?.objectKey,
                        fieldKey: normalisedFieldKey,
                    }, runtimeMetadata),
                },
                totalReferences: fieldReferenceResult.index?.[normalisedFieldKey]?.length || 0,
                returnedReferences: references.length,
                countsBySource: [...countsBySource.entries()]
                    .map(([sourceType, count]) => ({ sourceType, count }))
                    .sort((left, right) => right.count - left.count || left.sourceType.localeCompare(right.sourceType)),
                countsByClassification: [...countsByClassification.entries()]
                    .map(([classification, count]) => ({ classification, count }))
                    .sort((left, right) => right.count - left.count || left.classification.localeCompare(right.classification)),
                references: references.map((reference) => ({
                    ...reference,
                    builderUrls: {
                        scene: makeSceneBuilderUrl(app, reference.sceneKey, runtimeMetadata),
                        view: makeViewBuilderUrl(app, {
                            sceneKey: reference.sceneKey,
                            viewKey: reference.viewKey,
                            viewType: reference.viewType,
                        }, runtimeMetadata),
                        field: makeFieldBuilderUrl(app, {
                            objectKey: reference.objectKey,
                            fieldKey: reference.fieldKey,
                        }, runtimeMetadata),
                    },
                })),
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
            debugLog('tool_call', { tool: 'knack_list_objects', args: { appKey } });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
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
        }
    );

    server.tool(
        'knack_describe_field_shape',
        'Return the expected API response shape (formatted and raw) for a Knack field type. Use this to understand what data structure to expect when reading records of a given field type.',
        {
            fieldType: z.string().describe('Knack field type, e.g. connection, date_time, name, address, multiple_choice.'),
        },
        async ({ fieldType }) => {
            debugLog('tool_call', { tool: 'knack_describe_field_shape', args: { fieldType } });
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
                formattedShape: info.formattedShape,
                rawShape: info.rawShape,
                notes: info.notes || null,
                tip: 'Knack returns both field_xxx (formatted) and field_xxx_raw (raw) for every field. Prefer raw values when you need machine-readable data (numbers, IDs, arrays).',
            });
        }
    );

    server.tool(
        'knack_verify_record_field_shapes',
        'Fetch a live Knack record and compare each field\'s observed formatted/raw values against the documented field shape heuristics. Use this to validate or refine KNACK_FIELD_SHAPES with real data.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
            recordId: z.string(),
            includeBlankFields: z.boolean().optional().describe('Include fields whose formatted and raw values are both blank. Defaults to false.'),
        },
        async ({ appKey, objectKey, recordId, includeBlankFields = false }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_verify_record_field_shapes', args: { appKey, objectKey, recordId, includeBlankFields } });
            const apiKey = getApiKeyOrThrow(app.appKey);

            const [schemaResult, recordResult] = await Promise.all([
                getSchemaForApp(app),
                knackRequest(app, apiKey, `/objects/${objectKey}/records/${recordId}`),
            ]);

            const schema = schemaResult.schema;
            const obj = schema?.objects?.find((entry) => entry.key === objectKey) || null;
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
                    message: 'Object was not found in the available schema, so field types could not be verified.',
                });
            }

            const results = (obj.fields || []).map((field) => {
                const formatted = record[field.key];
                const raw = record[`${field.key}_raw`];
                const validation = validateFieldShape(field.type || '', formatted, raw);
                const shapeInfo = field.type ? getFieldShapeInfo(field.type) : null;

                return {
                    fieldKey: field.key,
                    fieldName: field.name || null,
                    fieldType: field.type || null,
                    status: validation.status,
                    observedFormattedShape: validation.observedFormattedShape,
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
                matchCount: filteredResults.filter((entry) => entry.status === 'match').length,
                mismatchCount: filteredResults.filter((entry) => entry.status === 'mismatch').length,
                skippedCount: results.filter((entry) => entry.status === 'skipped').length,
                unknownCount: filteredResults.filter((entry) => entry.status === 'unknown').length,
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
        }
    );

    server.tool(
        'knack_get_object_connections',
        'Return all connection fields for a Knack object showing which other objects they link to. Essential for understanding relationships between objects when designing or coding against the data model.',
        {
            appKey: z.string().optional(),
            objectKey: z.string(),
        },
        async ({ appKey, objectKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_get_object_connections', args: { appKey, objectKey } });
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

            const connectionFields = (obj.fields || [])
                .filter((field) => field.type === 'connection')
                .map((field) => {
                    const connectedObjectKey = field.connectedObject || null;
                    const connectedObject = connectedObjectKey
                        ? schema.objects?.find((o) => o.key === connectedObjectKey) || null
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
        }
    );

    server.tool(
        'knack_get_app_overview',
        'Return a full overview of the app schema: all objects with field counts, field type summaries, and cross-object connection relationships. Use this to understand the data model at a glance and get database design advice.',
        {
            appKey: z.string().optional(),
            includeFieldDetails: z.boolean().default(false).describe('When true, include all field names and types for each object (verbose).'),
        },
        async ({ appKey, includeFieldDetails }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_get_app_overview', args: { appKey, includeFieldDetails } });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
                });
            }

            const objectKeyToName = new Map<string, string>(
                schema.objects.map((obj) => [obj.key, obj.name || obj.key])
            );

            const relationships: Array<{
                fromObjectKey: string;
                fromObjectName: string | undefined;
                fieldKey: string;
                fieldName: string | undefined;
                toObjectKey: string;
                toObjectName: string;
            }> = [];

            const objectSummaries = schema.objects.map((obj) => {
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
                            toObjectName: objectKeyToName.get(cf.connectedObject) || cf.connectedObject,
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

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                objectCount: schema.objects.length,
                totalFields: schema.objects.reduce((sum, obj) => sum + (obj.fields || []).length, 0),
                relationshipCount: relationships.length,
                objects: objectSummaries,
                relationships,
            });
        }
    );

    server.tool(
        'knack_generate_seed_csvs',
        'Generate Knack import-ready seed CSV content for new-object imports. Produces one CSV per object with headers, realistic example rows, connection lookup values that match generated parent rows, and comma-separated multi-select values. If you opt into API-backed existing parent lookups, the tool first returns a rough API-call estimate and requires explicit confirmation before using the API key.',
        {
            appKey: z.string().optional(),
            objectKeys: z.array(z.string()).optional().describe('Optional subset of object keys to include. Defaults to all objects in the schema.'),
            rowsPerObject: z.number().int().min(2).max(10).default(4).describe('Minimum number of example rows to generate per object.'),
            useExistingConnectionValues: z.boolean().default(false).describe('When true, fetch first-page display values from existing connected parent objects that are not included in objectKeys.'),
            confirmExistingConnectionValueFetch: z.boolean().default(false).describe('Required before any API-key-backed parent lookup fetches are executed.'),
        },
        async ({ appKey, objectKeys, rowsPerObject, useExistingConnectionValues, confirmExistingConnectionValueFetch }) => {
            const app = getAppOrThrow(appKey);
            const effectiveRowsPerObject = Math.max(rowsPerObject, 2);
            debugLog('tool_call', {
                tool: 'knack_generate_seed_csvs',
                args: { appKey, objectKeys, rowsPerObject, useExistingConnectionValues, confirmExistingConnectionValueFetch },
            });
            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available from runtime API or schema.json.',
                });
            }

            const externalTargets = useExistingConnectionValues ? getExternalSeedConnectionTargets(schema, objectKeys) : [];
            const apiCallEstimate = {
                requiresApiKey: useExistingConnectionValues && externalTargets.length > 0,
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

            if (apiCallEstimate.requiresApiKey && !confirmExistingConnectionValueFetch) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    source,
                    confirmationRequired: true,
                    message: 'Authenticated API fetches for existing parent connection values were requested. Review the estimated call count and re-run with confirmExistingConnectionValueFetch:true to proceed.',
                    apiCallEstimate,
                });
            }

            const externalLookupResult = apiCallEstimate.requiresApiKey
                ? await fetchExternalSeedConnectionLookups(app, externalTargets, effectiveRowsPerObject)
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
        }
    );

    server.tool(
        'knack_list_scenes',
        'List all scenes (pages) in the app with their key, name, slug, and views. Use this to explore the UI structure of a Knack application.',
        {
            appKey: z.string().optional(),
            includeViews: z.boolean().default(true).describe('When true, include the list of views for each scene with their key, name, and type (default: true).'),
        },
        async ({ appKey, includeViews }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_list_scenes', args: { appKey, includeViews } });

            const scenes = await getScenesForApp(app);

            if (!scenes.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No scene data available. Run knack_refresh_cache with warm: true to load runtime metadata.',
                });
            }

            const runtimeMetadata = await getRuntimeMetadata(app);
            const sceneSummaries = scenes.map((scene) => {
                const summary: Record<string, unknown> = {
                    sceneKey: scene.sceneKey,
                    sceneName: scene.sceneName,
                    sceneSlug: scene.sceneSlug,
                    viewCount: scene.views.length,
                    builderUrl: makeSceneBuilderUrl(app, scene.sceneKey, runtimeMetadata),
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
                totalViewCount: scenes.reduce((sum, s) => sum + s.views.length, 0),
                scenes: sceneSummaries,
            });
        }
    );

    server.tool(
        'knack_list_views',
        'List views across the app with scene context, type, and builder URL. Optionally filter by scene key or view type (e.g. form, grid, table, report, search, menu, rich_text, map, calendar).',
        {
            appKey: z.string().optional(),
            sceneKey: z.string().optional().describe('Filter to views belonging to a specific scene.'),
            viewType: z.string().optional().describe('Filter by view type, e.g. form, grid, table, report, search, menu, rich_text, map, calendar.'),
            maxResults: z.number().int().min(1).max(5000).default(500),
        },
        async ({ appKey, sceneKey, viewType, maxResults }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_list_views', args: { appKey, sceneKey, viewType, maxResults } });

            const scenes = await getScenesForApp(app);

            if (!scenes.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No scene data available. Run knack_refresh_cache with warm: true to load runtime metadata.',
                });
            }

            const runtimeMetadata = await getRuntimeMetadata(app);
            const normSceneKey = sceneKey?.toLowerCase();
            const normViewType = viewType?.toLowerCase();
            const results: Array<Record<string, unknown>> = [];
            const viewTypeCounts = new Map<string, number>();

            for (const scene of scenes) {
                if (normSceneKey && scene.sceneKey.toLowerCase() !== normSceneKey) continue;

                for (const view of scene.views) {
                    const vType = view.viewType || 'unknown';
                    viewTypeCounts.set(vType, (viewTypeCounts.get(vType) || 0) + 1);

                    if (normViewType && vType.toLowerCase() !== normViewType) continue;

                    if (results.length < maxResults) {
                        results.push({
                            viewKey: view.viewKey,
                            viewName: view.viewName,
                            viewType: view.viewType,
                            sceneKey: scene.sceneKey,
                            sceneName: scene.sceneName,
                            sceneSlug: scene.sceneSlug,
                            builderUrl: makeViewBuilderUrl(app, {
                                sceneKey: scene.sceneKey,
                                viewKey: view.viewKey,
                                viewType: view.viewType,
                            }, runtimeMetadata),
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
        }
    );

    server.tool(
        'knack_analyze_data_model',
        'Analyse the app data model and return structured design feedback: field-count distribution, isolated objects, connection density, field type spread, and objects with potential design issues.',
        {
            appKey: z.string().optional(),
        },
        async ({ appKey }) => {
            const app = getAppOrThrow(appKey);
            debugLog('tool_call', { tool: 'knack_analyze_data_model', args: { appKey } });

            const { schema, source } = await getSchemaForApp(app);

            if (!schema?.objects?.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: 'No schema available. Run knack_refresh_cache with warm: true or ensure schema.json is present.',
                });
            }

            const objects = schema.objects;
            const totalObjects = objects.length;
            const totalFields = objects.reduce((sum, obj) => sum + (obj.fields || []).length, 0);

            const globalTypeCounts = new Map<string, number>();
            const objectMetrics = objects.map((obj) => {
                const fields = obj.fields || [];
                const typeCounts: Record<string, number> = {};
                for (const field of fields) {
                    const t = field.type || 'unknown';
                    typeCounts[t] = (typeCounts[t] || 0) + 1;
                    globalTypeCounts.set(t, (globalTypeCounts.get(t) || 0) + 1);
                }
                const connectionCount = fields.filter((f) => f.type === 'connection').length;
                return { objectKey: obj.key, objectName: obj.name, fieldCount: fields.length, connectionCount, typeCounts };
            });

            const avgFieldCount = totalObjects ? Math.round(totalFields / totalObjects) : 0;
            const maxFieldCount = objectMetrics.reduce((max, m) => Math.max(max, m.fieldCount), 0);
            const minFieldCount = objectMetrics.reduce((min, m) => Math.min(min, m.fieldCount), Infinity) === Infinity ? 0 : objectMetrics.reduce((min, m) => Math.min(min, m.fieldCount), Infinity);

            const connectedObjectKeys = new Set<string>(
                objects.flatMap((obj) =>
                    (obj.fields || [])
                        .filter((f) => f.type === 'connection' && f.connectedObject)
                        .flatMap((f) => [obj.key, f.connectedObject as string])
                )
            );

            const isolatedObjects = objectMetrics
                .filter((m) => m.connectionCount === 0)
                .map((m) => ({ objectKey: m.objectKey, objectName: m.objectName, fieldCount: m.fieldCount }));

            // Objects are flagged as high-field when they exceed twice the app average or the absolute
            // minimum of 30 fields, whichever is larger. 30 is chosen as a practical Knack threshold
            // above which a single object often becomes hard to maintain.
            const MIN_HIGH_FIELD_THRESHOLD = 30;
            const highFieldThreshold = Math.max(avgFieldCount * 2, MIN_HIGH_FIELD_THRESHOLD);
            const highFieldCountObjects = objectMetrics
                .filter((m) => m.fieldCount >= highFieldThreshold)
                .map((m) => ({ objectKey: m.objectKey, objectName: m.objectName, fieldCount: m.fieldCount }))
                .sort((a, b) => b.fieldCount - a.fieldCount);

            // Objects with 2 or fewer fields are flagged as potentially stub/lookup tables.
            // Knack auto-creates a primary text field for every object, so ≤ 2 means only
            // that auto-field plus at most one user-added field — a likely placeholder or lookup list.
            const LOW_FIELD_COUNT_THRESHOLD = 2;
            const lowFieldCountObjects = objectMetrics
                .filter((m) => m.fieldCount <= LOW_FIELD_COUNT_THRESHOLD)
                .map((m) => ({ objectKey: m.objectKey, objectName: m.objectName, fieldCount: m.fieldCount }));

            const fieldTypeDistribution = [...globalTypeCounts.entries()]
                .map(([type, count]) => ({ type, count, percentage: Math.round((count / totalFields) * 100) }))
                .sort((a, b) => b.count - a.count);

            const connectionPct = totalObjects ? Math.round((connectedObjectKeys.size / totalObjects) * 100) : 0;
            const observations: string[] = [];
            if (isolatedObjects.length > 0) {
                observations.push(`${isolatedObjects.length} object(s) have no connection fields — they may be standalone lookup tables or unused.`);
            }
            if (highFieldCountObjects.length > 0) {
                observations.push(`${highFieldCountObjects.length} object(s) exceed ${highFieldThreshold} fields — consider whether any could be split into related objects.`);
            }
            if (lowFieldCountObjects.length > 0) {
                observations.push(`${lowFieldCountObjects.length} object(s) have ≤ ${LOW_FIELD_COUNT_THRESHOLD} fields — these may be stub/placeholder tables or simple lookup lists.`);
            }
            observations.push(`${connectionPct}% of objects participate in at least one connection relationship.`);

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
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
            });
        }
    );

    // -----------------------
    // Write Tools (guarded by readonly flag in app.json)
    // -----------------------

    server.tool(
        'knack_create_field',
        'Create a new field on a Knack object. Requires the app to have readonly: false in app.json.',
        {
            appKey: z.string().optional(),
            objectKey: z.string().describe('The object key, e.g. object_2'),
            name: z.string().describe('Field name'),
            type: z.string().describe('Field type, e.g. short_text, number, boolean, sum, connection, etc.'),
            required: z.boolean().optional().default(false),
            unique: z.boolean().optional().default(false),
            format: z.string().optional().describe('Optional format object as JSON string (for sum, equation, etc.)'),
        },
        async ({ appKey, objectKey, name, type, required, unique, format }) => {
            const app = getAppOrThrow(appKey);
            assertWritable(app);
            const apiKey = getApiKeyOrThrow(app.appKey);
            debugLog('tool_call', { tool: 'knack_create_field', args: { appKey: app.appKey, objectKey, name, type } });

            const payload: Record<string, unknown> = { name, type, required, unique };
            if (format) payload.format = JSON.parse(format);

            const result = await knackRequest(app, apiKey, `/objects/${objectKey}/fields`, {
                method: 'POST',
                body: JSON.stringify(payload),
            });
            return makeTextResponse({ appKey: app.appKey, objectKey, action: 'create_field', ...result });
        }
    );

    server.tool(
        'knack_update_field',
        'Update an existing field on a Knack object. Send only the properties to change. Requires readonly: false.',
        {
            appKey: z.string().optional(),
            objectKey: z.string().describe('The object key, e.g. object_2'),
            fieldKey: z.string().describe('The field key, e.g. field_123'),
            updates: z.string().describe('Partial field definition as JSON string with properties to update (name, format, etc.)'),
        },
        async ({ appKey, objectKey, fieldKey, updates }) => {
            const app = getAppOrThrow(appKey);
            assertWritable(app);
            const apiKey = getApiKeyOrThrow(app.appKey);
            debugLog('tool_call', { tool: 'knack_update_field', args: { appKey: app.appKey, objectKey, fieldKey } });

            const result = await knackRequest(app, apiKey, `/objects/${objectKey}/fields/${fieldKey}`, {
                method: 'PUT',
                body: updates,
            });
            return makeTextResponse({ appKey: app.appKey, objectKey, fieldKey, action: 'update_field', ...result });
        }
    );

    server.tool(
        'knack_delete_field',
        'Delete a field from a Knack object. This is destructive and cannot be undone. Requires readonly: false.',
        {
            appKey: z.string().optional(),
            objectKey: z.string().describe('The object key, e.g. object_2'),
            fieldKey: z.string().describe('The field key to delete, e.g. field_123'),
        },
        async ({ appKey, objectKey, fieldKey }) => {
            const app = getAppOrThrow(appKey);
            assertDeletable(app);
            const apiKey = getApiKeyOrThrow(app.appKey);
            debugLog('tool_call', { tool: 'knack_delete_field', args: { appKey: app.appKey, objectKey, fieldKey } });

            const result = await knackRequest(app, apiKey, `/objects/${objectKey}/fields/${fieldKey}`, {
                method: 'DELETE',
            });
            return makeTextResponse({ appKey: app.appKey, objectKey, fieldKey, action: 'delete_field', ...result });
        }
    );

    server.tool(
        'knack_duplicate_field',
        'Duplicate an existing field with a new name. Reads the source field definition, strips the key/_id, and creates a copy. Requires readonly: false.',
        {
            appKey: z.string().optional(),
            objectKey: z.string().describe('The object key, e.g. object_2'),
            sourceFieldKey: z.string().describe('The field key to duplicate, e.g. field_3539'),
            newName: z.string().describe('Name for the new field'),
        },
        async ({ appKey, objectKey, sourceFieldKey, newName }) => {
            const app = getAppOrThrow(appKey);
            assertWritable(app);
            const apiKey = getApiKeyOrThrow(app.appKey);
            debugLog('tool_call', { tool: 'knack_duplicate_field', args: { appKey: app.appKey, objectKey, sourceFieldKey, newName } });

            // Fetch the object to get the source field definition
            const objResult = await knackRequest(app, apiKey, `/objects/${objectKey}`) as { ok: boolean; body: { object: { fields: Array<Record<string, unknown>> } } };
            const fields = objResult.body?.object?.fields;
            if (!fields) {
                return makeTextResponse({ ok: false, appKey: app.appKey, message: 'Could not fetch object fields.' });
            }

            const sourceField = fields.find((f: Record<string, unknown>) => f.key === sourceFieldKey);
            if (!sourceField) {
                return makeTextResponse({ ok: false, appKey: app.appKey, message: `Source field ${sourceFieldKey} not found on ${objectKey}.` });
            }

            // Clone and strip identifiers
            const newField = { ...sourceField };
            delete newField.key;
            delete newField._id;
            newField.name = newName;

            const result = await knackRequest(app, apiKey, `/objects/${objectKey}/fields`, {
                method: 'POST',
                body: JSON.stringify(newField),
            });
            return makeTextResponse({ appKey: app.appKey, objectKey, action: 'duplicate_field', sourceFieldKey, newName, ...result });
        }
    );

    server.tool(
        'knack_create_record',
        'Create a new record in a Knack object. Requires readonly: false.',
        {
            appKey: z.string().optional(),
            objectKey: z.string().describe('The object key, e.g. object_2'),
            data: z.string().describe('Record data as JSON string with field_key: value pairs'),
        },
        async ({ appKey, objectKey, data }) => {
            const app = getAppOrThrow(appKey);
            assertWritable(app);
            const apiKey = getApiKeyOrThrow(app.appKey);
            debugLog('tool_call', { tool: 'knack_create_record', args: { appKey: app.appKey, objectKey } });

            const result = await knackRequest(app, apiKey, `/objects/${objectKey}/records`, {
                method: 'POST',
                body: data,
            });
            return makeTextResponse({ appKey: app.appKey, objectKey, action: 'create_record', ...result });
        }
    );

    server.tool(
        'knack_update_record',
        'Update an existing record in a Knack object. Requires readonly: false.',
        {
            appKey: z.string().optional(),
            objectKey: z.string().describe('The object key, e.g. object_2'),
            recordId: z.string().describe('The record ID to update'),
            data: z.string().describe('Fields to update as JSON string with field_key: value pairs'),
        },
        async ({ appKey, objectKey, recordId, data }) => {
            const app = getAppOrThrow(appKey);
            assertWritable(app);
            const apiKey = getApiKeyOrThrow(app.appKey);
            debugLog('tool_call', { tool: 'knack_update_record', args: { appKey: app.appKey, objectKey, recordId } });

            const result = await knackRequest(app, apiKey, `/objects/${objectKey}/records/${recordId}`, {
                method: 'PUT',
                body: data,
            });
            return makeTextResponse({ appKey: app.appKey, objectKey, recordId, action: 'update_record', ...result });
        }
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
            debugLog('tool_call', { tool: 'knack_delete_record', args: { appKey: app.appKey, objectKey, recordId } });

            const result = await knackRequest(app, apiKey, `/objects/${objectKey}/records/${recordId}`, {
                method: 'DELETE',
            });
            return makeTextResponse({ appKey: app.appKey, objectKey, recordId, action: 'delete_record', ...result });
        }
    );

    server.tool(
        'knack_upload_asset',
        'Upload a local file to Knack as an asset (file or image). Returns the asset id, which can then be used as the value for a file/image field in knack_create_record or knack_update_record. Requires readonly: false.',
        {
            appKey: z.string().optional(),
            filePath: z.string().describe('Absolute path to the local file to upload'),
            assetType: z.enum(['file', 'image']).default('file').describe('Knack asset type: "file" for file fields, "image" for image fields'),
        },
        async ({ appKey, filePath, assetType }) => {
            const app = getAppOrThrow(appKey);
            assertWritable(app);
            const apiKey = getApiKeyOrThrow(app.appKey);
            debugLog('tool_call', { tool: 'knack_upload_asset', args: { appKey: app.appKey, filePath, assetType } });

            if (!fs.existsSync(filePath)) {
                return makeTextResponse({ ok: false, status: 0, body: { error: 'file_not_found', filePath } });
            }
            const stat = fs.statSync(filePath);
            if (!stat.isFile()) {
                return makeTextResponse({ ok: false, status: 0, body: { error: 'not_a_file', filePath } });
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
        }
    );

    // -----------------------
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

const isDirectExecution = (() => {
    const entryPath = process.argv[1];
    return entryPath ? import.meta.url === pathToFileURL(entryPath).href : false;
})();

if (isDirectExecution) {
    main().catch((err) => {
        // Important: log to stderr for MCP clients
        console.error(err);
        process.exit(1);
    });
}
