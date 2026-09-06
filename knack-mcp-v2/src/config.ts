/**
 * Configuration: environment, app discovery and secrets.
 *
 * Everything the server reads from outside itself is read here, once, so a tool never
 * touches process.env and a test can construct an AppConfig without a file system.
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
    fileExists,
    getPositiveIntEnv,
    isEnabledEnv,
    readJsonFile,
} from './lib/util.js';

export type AppConfig = {
    appKey: string;
    appName?: string;
    appId: string;
    apiBase?: string;
    notes?: string;
    builderAccountSlug?: string;
    builderAppSlug?: string;
    /** Writes are refused unless this is explicitly false. */
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

export type ServerOptions = {
    /** A hard boundary: no write, view or diagnostic tool is advertised or runs. */
    readOnly?: boolean;
};

export type SecretsMap = Record<string, string>;

export const DEFAULT_API_BASE = 'https://api.knack.com/v1';

const env = process.env;

export const ENV_KNACK_APPS_DIR = env.KNACK_APPS_DIR;
export const ENV_SECRETS_PATH = env.KNACK_MCP_SECRETS_PATH;
export const DEBUG_ENABLED = isEnabledEnv(env.DEBUG, false);
export const CACHE_TTL_MS = getPositiveIntEnv(
    env.KNACK_CACHE_TTL_MS,
    5 * 60 * 1000,
);
/** Largest upstream body the server will read. */
export const MAX_RESPONSE_BYTES = getPositiveIntEnv(
    env.KNACK_MAX_RESPONSE_BYTES,
    20 * 1024 * 1024,
);
export const MAX_ATTACHMENT_REDIRECTS = 5;
/** Largest serialised tool response; beyond it a structural summary is returned. */
export const MAX_TOOL_TEXT_BYTES = getPositiveIntEnv(
    env.KNACK_MCP_MAX_TOOL_TEXT_BYTES,
    256 * 1024,
);
/** Largest raw payload inlined inside a normal response. */
export const MAX_INLINE_DETAIL_BYTES = getPositiveIntEnv(
    env.KNACK_MCP_MAX_INLINE_DETAIL_BYTES,
    48 * 1024,
);
export const MAX_EXTRACTED_TEXT_BYTES = getPositiveIntEnv(
    env.KNACK_MCP_MAX_EXTRACTED_TEXT_BYTES,
    192 * 1024,
);
export const PRETTY_TOOL_JSON = isEnabledEnv(
    env.KNACK_MCP_PRETTY_TOOL_JSON,
    false,
);
export const BATCH_CONCURRENCY = Math.min(
    10,
    getPositiveIntEnv(env.KNACK_MCP_BATCH_CONCURRENCY, 5),
);

export function getDefaultSecretsPath(): string {
    return path.join(os.homedir(), '.knack-mcp-secrets.json');
}

export function getSecretsPath(): string {
    return ENV_SECRETS_PATH || getDefaultSecretsPath();
}

/** Missing or unreadable secrets are not fatal: read-only file-backed tools still work. */
export function loadSecrets(): SecretsMap {
    return readJsonFile<SecretsMap>(getSecretsPath()) ?? {};
}

/** Every `<dir>/<App>/schema/app.json` (or legacy `<dir>/<App>/app.json`) with a key and id. */
export function discoverApps(knackAppsDir: string): AppConfig[] {
    const apps: AppConfig[] = [];
    for (const entry of fs.readdirSync(knackAppsDir, { withFileTypes: true })) {
        if (!entry.isDirectory()) continue;
        const appFolder = path.join(knackAppsDir, entry.name);
        const appJsonPath = [
            path.join(appFolder, 'schema', 'app.json'),
            path.join(appFolder, 'app.json'),
        ].find(fileExists);
        const config = appJsonPath
            ? readJsonFile<Omit<AppConfig, 'appFolder'>>(appJsonPath)
            : null;
        if (!config?.appKey || !config?.appId) continue;
        apps.push({
            ...config,
            apiBase: config.apiBase || DEFAULT_API_BASE,
            appFolder,
        });
    }
    return apps;
}
