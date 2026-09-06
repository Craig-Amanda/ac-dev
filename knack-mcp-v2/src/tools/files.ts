/**
 * File tools: download an approved attachment to a controlled temp path, or download
 * and extract bounded plain text from it.
 */
import { z } from 'zod';

import {
    downloadRecordAttachment,
    extractAttachmentText,
    getRecordAttachment,
} from '../attachments.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { makeTextResponse } from '../response.js';

export const downloadFile = defineTool({
    name: 'knack_download_file',
    description:
        'Download an attachment from a file or image field to a local temporary path.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        recordId: z.string(),
        fieldKey: z
            .string()
            .describe('File or image field holding the attachment'),
    },
    handler: async ({ appKey, objectKey, recordId, fieldKey }, ctx) => {
        const app = ctx.getApp(appKey);
        const attachment = await getRecordAttachment(
            ctx,
            app,
            objectKey,
            recordId,
            fieldKey,
        );
        const download = await downloadRecordAttachment(
            app,
            recordId,
            attachment,
        );

        return makeTextResponse({
            ok: true,
            action: 'download_file',
            appKey: app.appKey,
            objectKey,
            recordId,
            fieldKey,
            filename: attachment.filename,
            mimeType: attachment.mimeType,
            sourceSizeBytes: attachment.sizeBytes,
            downloadedSizeBytes: download.sizeBytes,
            filePath: download.filePath,
        });
    },
});

export const readFile = defineTool({
    name: 'knack_read_file',
    description:
        'Download an attachment and extract bounded plain text (PDF, DOCX, TXT, CSV, JSON, MD, XML).',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        recordId: z.string(),
        fieldKey: z
            .string()
            .describe('File or image field holding the attachment'),
    },
    handler: async ({ appKey, objectKey, recordId, fieldKey }, ctx) => {
        const app = ctx.getApp(appKey);
        const attachment = await getRecordAttachment(
            ctx,
            app,
            objectKey,
            recordId,
            fieldKey,
        );
        const download = await downloadRecordAttachment(
            app,
            recordId,
            attachment,
        );
        const extraction = await extractAttachmentText(
            download.filePath,
            attachment.mimeType,
        );

        return makeTextResponse({
            ok: extraction.supported,
            action: 'read_file',
            appKey: app.appKey,
            objectKey,
            recordId,
            fieldKey,
            filename: attachment.filename,
            mimeType: attachment.mimeType,
            sourceSizeBytes: attachment.sizeBytes,
            downloadedSizeBytes: download.sizeBytes,
            filePath: download.filePath,
            text: extraction.text,
            truncated: extraction.truncated,
            message: extraction.supported
                ? null
                : 'The file was downloaded, but its format is not supported for text extraction.',
        });
    },
});

export const fileTools: AnyToolDef[] = [downloadFile, readFile];
