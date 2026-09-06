import { DEBUG_ENABLED, MAX_RESPONSE_BYTES } from './config.js';

export async function readResponseTextWithLimit(
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

export type KnackApiResult = {
    ok: boolean;
    status: number;
    body: unknown;
};

export async function knackFetchJson(
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
