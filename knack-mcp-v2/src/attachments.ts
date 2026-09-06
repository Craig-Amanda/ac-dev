/**
 * File and image attachments: locate on a record, download with a byte cap, extract text.
 */
import fs from 'node:fs';
import * as https from 'node:https';
import os from 'node:os';
import path from 'node:path';

import mammoth from 'mammoth';
import pdf from 'pdf-parse';

import {
    type AppConfig,
    MAX_ATTACHMENT_REDIRECTS,
    MAX_EXTRACTED_TEXT_BYTES,
    MAX_RESPONSE_BYTES,
} from './config.js';
import type { KnackContext } from './context.js';
import { asRecord } from './lib/util.js';
import { getPermittedReadFields } from './records.js';

export type Attachment = {
    url: string;
    filename: string;
    mimeType: string;
    sizeBytes: number | null;
};

/** Resolve a file or image attachment from an approved record field. */
export async function getRecordAttachment(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    recordId: string,
    fieldKey: string,
): Promise<Attachment> {
    const { object } = await getPermittedReadFields(ctx, app, objectKey, [
        fieldKey,
    ]);
    const field = (object.fields || []).find((entry) => entry.key === fieldKey);
    if (!field || !['file', 'image'].includes(field.type || '')) {
        throw new Error(
            `Field ${fieldKey} is not a file or image field on ${objectKey}.`,
        );
    }

    const result = await ctx.request(
        app,
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
        typeof attachment?.filename === 'string' ? attachment.filename : null;
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
        sizeBytes: typeof attachment.size === 'number' ? attachment.size : null,
    };
}

const safeComponent = (value: string, fallback: string) =>
    value.replace(/[^a-zA-Z0-9._-]/g, '_') || fallback;

/** Download an attachment to a per-app temp directory with a hard byte limit. */
export async function downloadRecordAttachment(
    app: AppConfig,
    recordId: string,
    attachment: Attachment,
): Promise<{ filePath: string; sizeBytes: number }> {
    const attachmentUrl = new URL(attachment.url);
    if (attachmentUrl.protocol !== 'https:') {
        throw new Error('Knack attachment URLs must use HTTPS.');
    }

    const downloadDirectory = path.join(
        os.tmpdir(),
        'knack-mcp-downloads',
        safeComponent(app.appKey, 'app'),
        safeComponent(recordId, 'record'),
    );
    const filePath = path.join(
        downloadDirectory,
        safeComponent(path.basename(attachment.filename), 'attachment'),
    );
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
                            new Error('Knack attachment URLs must use HTTPS.'),
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

                const output = fs.createWriteStream(filePath, { flags: 'w' });
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

/** Bounded plain text from a downloaded attachment (PDF, DOCX, text-like). */
export async function extractAttachmentText(
    filePath: string,
    mimeType: string,
): Promise<{ text: string; truncated: boolean; supported: boolean }> {
    const extension = path.extname(filePath).toLowerCase();
    let text: string;

    if (mimeType === 'application/pdf' || extension === '.pdf') {
        text = (await pdf(fs.readFileSync(filePath))).text;
    } else if (
        mimeType ===
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ||
        extension === '.docx'
    ) {
        text = (await mammoth.extractRawText({ path: filePath })).value;
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

    if (Buffer.byteLength(text, 'utf8') <= MAX_EXTRACTED_TEXT_BYTES) {
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
