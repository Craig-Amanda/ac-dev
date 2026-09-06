import { type AppConfig, DEFAULT_API_BASE } from '../config.js';
import { type RuntimeMetadata } from '../types.js';
import { asRecord } from './util.js';
import { getObjectAtPath } from './metadata.js';

export function getPublicApiBase(apiBase?: string): string {
    const base = (apiBase || DEFAULT_API_BASE).trim().replace(/\/+$/, '');
    return base.replace(/\/v1$/i, '');
}

export function slugifyForBuilder(value: string): string {
    return value
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
}

export function getBuilderSlugs(
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

export function makeBuilderBaseUrl(
    app: AppConfig,
    runtimeMetadata?: RuntimeMetadata | null,
): string {
    const { accountSlug, appSlug } = getBuilderSlugs(app, runtimeMetadata);
    return `https://builder.knack.com/${accountSlug}/${appSlug}`;
}

export function makeSceneBuilderUrl(
    app: AppConfig,
    sceneKey?: string,
    runtimeMetadata?: RuntimeMetadata | null,
): string | null {
    if (!sceneKey) return null;
    return `${makeBuilderBaseUrl(app, runtimeMetadata)}/pages/${sceneKey}`;
}

export function makeViewBuilderUrl(
    app: AppConfig,
    params: { sceneKey?: string; viewKey?: string; viewType?: string },
    runtimeMetadata?: RuntimeMetadata | null,
): string | null {
    if (!params.sceneKey || !params.viewKey) return null;
    const viewTypeSegment = (params.viewType || 'view').trim().toLowerCase();
    return `${makeBuilderBaseUrl(app, runtimeMetadata)}/pages/${params.sceneKey}/views/${params.viewKey}/${viewTypeSegment}`;
}

export function makeFieldBuilderUrl(
    app: AppConfig,
    params: { objectKey?: string; fieldKey?: string },
    runtimeMetadata?: RuntimeMetadata | null,
): string | null {
    if (!params.objectKey || !params.fieldKey) return null;
    return `${makeBuilderBaseUrl(app, runtimeMetadata)}/schema/list/objects/${params.objectKey}/fields/${params.fieldKey}/settings`;
}
