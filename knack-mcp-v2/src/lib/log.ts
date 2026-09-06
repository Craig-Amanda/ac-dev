import { DEBUG_ENABLED } from '../config.js';

/** stderr only: stdout is reserved for JSON-RPC. */
export function debugLog(message: string, payload?: unknown): void {
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
