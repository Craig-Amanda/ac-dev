/**
 * Per-app permission model. Every level above `read` is an explicit opt-in in the app's
 * app.json, and the whole server can be pinned read-only at launch.
 */
import type { AppConfig, ServerOptions } from './config.js';

export type ToolAccess =
    | 'read'
    | 'write'
    | 'delete'
    | 'view'
    | 'view-delete'
    | 'diagnostic'
    | 'audit';

export function assertWritable(app: AppConfig, options: ServerOptions): void {
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

export function assertDeletable(app: AppConfig, options: ServerOptions): void {
    assertWritable(app, options);
    if (app.allowDelete !== true) {
        throw new Error(
            `App "${app.appKey}" does not allow deletions. Set "allowDelete": true in app.json to enable delete operations.`,
        );
    }
}

export function assertViewWritable(
    app: AppConfig,
    options: ServerOptions,
): void {
    assertWritable(app, options);
    if (app.allowViewMutation !== true) {
        throw new Error(
            `App "${app.appKey}" does not allow view mutations. Set "allowViewMutation": true in app.json to enable create/update view operations.`,
        );
    }
}

export function assertViewDeletable(
    app: AppConfig,
    options: ServerOptions,
): void {
    assertViewWritable(app, options);
    if (app.allowDelete !== true) {
        throw new Error(
            `App "${app.appKey}" does not allow deletions. Set "allowDelete": true in app.json to enable delete operations.`,
        );
    }
}

export function assertDiagnosticAccess(
    app: AppConfig,
    options: ServerOptions,
): void {
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

/**
 * The audit only reads, so enforced read-only mode does not withhold it; it needs its
 * own opt-in because what it reports is where personal data sits.
 */
export function assertAuditAccess(app: AppConfig): void {
    if (app.allowAudit !== true) {
        throw new Error(
            `App "${app.appKey}" does not allow the exposure audit. Set "allowAudit": true in app.json to enable knack_audit_exposure.`,
        );
    }
}

/** Enforce one access level for one app. `read` always passes. */
export function assertAccess(
    app: AppConfig,
    access: ToolAccess,
    options: ServerOptions,
): void {
    switch (access) {
        case 'read':
            return;
        case 'write':
            return assertWritable(app, options);
        case 'delete':
            return assertDeletable(app, options);
        case 'view':
            return assertViewWritable(app, options);
        case 'view-delete':
            return assertViewDeletable(app, options);
        case 'diagnostic':
            return assertDiagnosticAccess(app, options);
        case 'audit':
            return assertAuditAccess(app);
    }
}

/**
 * Whether a tool at this level is advertised at all. A level is advertised when at
 * least one app opts in; each call still enforces the selected app's own toggles.
 */
export function isAdvertised(
    access: ToolAccess,
    apps: AppConfig[],
    options: ServerOptions,
): boolean {
    if (access === 'read') return true;
    if (access === 'audit') return apps.some((app) => app.allowAudit === true);
    if (options.readOnly) return false;
    switch (access) {
        case 'write':
        case 'delete':
            return apps.some((app) => app.readonly === false);
        case 'view':
        case 'view-delete':
            return apps.some((app) => app.allowViewMutation === true);
        case 'diagnostic':
            return apps.some((app) => app.allowDiagnostics === true);
    }
}
