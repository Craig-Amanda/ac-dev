import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, it } from 'node:test';

import { z } from 'zod';

import { extractAttachmentText } from '../attachments.js';
import type { AppConfig } from '../config.js';
import type { KnackApiResult } from '../http.js';
import type { AnyToolDef } from '../registry.js';
import { makeApp, makeFakeContext } from '../testing/fake-context.js';
import type { RuntimeMetadata } from '../types.js';
import { downloadFile, fileTools, readFile } from './files.js';

const parseArgs = (tool: AnyToolDef, raw: Record<string, unknown>) =>
    z.object(tool.input).parse(raw);

const RUNTIME_METADATA: RuntimeMetadata = {
    objects: [
        {
            key: 'object_1',
            name: 'Documents',
            fields: [
                { key: 'field_1', name: 'Title', type: 'short_text' },
                { key: 'field_6', name: 'Attachment', type: 'file' },
                { key: 'field_7', name: 'Photo', type: 'image' },
            ],
        },
    ],
};

function setup(
    input: {
        app?: Partial<AppConfig>;
        responses?: Record<string, KnackApiResult>;
    } = {},
) {
    const app = makeApp(input.app);
    const fake = makeFakeContext({
        apps: [app],
        runtimeMetadata: { [app.appKey]: RUNTIME_METADATA },
        responses: input.responses,
    });
    fake.ctx.state.activeAppKey = app.appKey;
    return fake;
}

const RECORD_WITHOUT_UPLOAD: KnackApiResult = {
    ok: true,
    status: 200,
    body: {
        id: 'rec1',
        field_1: 'Spec',
        field_6: '',
        field_6_raw: null,
        field_7: '',
    },
};

describe('fileTools catalogue', () => {
    it('lists the file tools in order with read access', () => {
        assert.deepEqual(
            fileTools.map((tool) => tool.name),
            ['knack_download_file', 'knack_read_file'],
        );
        assert.ok(fileTools.every((tool) => tool.access === 'read'));
    });
});

// Both tools share attachment resolution; a real download would hit the network, so
// each is exercised up to the point where resolution refuses.
for (const tool of [downloadFile, readFile]) {
    describe(tool.name, () => {
        it('refuses a field that is not a file or image field before any request', async () => {
            const { ctx, requests } = setup();
            await assert.rejects(
                tool.handler(
                    parseArgs(tool, {
                        objectKey: 'object_1',
                        recordId: 'rec1',
                        fieldKey: 'field_1',
                    }),
                    ctx,
                ),
                /Field field_1 is not a file or image field on object_1\./,
            );
            assert.equal(requests.length, 0);
        });

        it('refuses a record whose file field holds no upload', async () => {
            const { ctx, requests } = setup({
                responses: {
                    'GET /objects/object_1/records/rec1': RECORD_WITHOUT_UPLOAD,
                },
            });
            await assert.rejects(
                tool.handler(
                    parseArgs(tool, {
                        objectKey: 'object_1',
                        recordId: 'rec1',
                        fieldKey: 'field_6',
                    }),
                    ctx,
                ),
                /Field field_6 does not contain an uploaded attachment\./,
            );
            assert.deepEqual(requests, [
                {
                    apiPath: '/objects/object_1/records/rec1',
                    method: 'GET',
                    body: null,
                },
            ]);
        });

        it('accepts image fields and refuses when the record cannot be fetched', async () => {
            const { ctx, requests } = setup({
                responses: {
                    'GET /objects/object_1/records/rec9': {
                        ok: false,
                        status: 404,
                        body: null,
                    },
                },
            });
            await assert.rejects(
                tool.handler(
                    parseArgs(tool, {
                        objectKey: 'object_1',
                        recordId: 'rec9',
                        fieldKey: 'field_7',
                    }),
                    ctx,
                ),
                /Unable to fetch record rec9 from object_1\./,
            );
            assert.equal(requests.length, 1);
        });

        it('refuses fields the dataAccess policy redacts', async () => {
            const { ctx, requests } = setup({
                app: { dataAccess: { redactedFieldKeys: ['field_6'] } },
            });
            await assert.rejects(
                tool.handler(
                    parseArgs(tool, {
                        objectKey: 'object_1',
                        recordId: 'rec1',
                        fieldKey: 'field_6',
                    }),
                    ctx,
                ),
                /Field field_6 is redacted by this app's dataAccess policy\./,
            );
            assert.equal(requests.length, 0);
        });

        it('refuses objects outside the dataAccess policy', async () => {
            const { ctx, requests } = setup({
                app: { dataAccess: { allowedObjectKeys: ['object_2'] } },
            });
            await assert.rejects(
                tool.handler(
                    parseArgs(tool, {
                        objectKey: 'object_1',
                        recordId: 'rec1',
                        fieldKey: 'field_6',
                    }),
                    ctx,
                ),
                /Read access to object_1 is not allowed by this app's dataAccess policy\./,
            );
            assert.equal(requests.length, 0);
        });
    });
}

describe('extractAttachmentText', () => {
    const tempDirs: string[] = [];
    afterEach(() => {
        for (const dir of tempDirs.splice(0))
            fs.rmSync(dir, { recursive: true, force: true });
    });
    const makeTempDir = () => {
        const dir = fs.mkdtempSync(
            path.join(os.tmpdir(), 'knack-mcp-v2-files-'),
        );
        tempDirs.push(dir);
        return dir;
    };

    it('reads a text attachment in full', async () => {
        const filePath = path.join(makeTempDir(), 'notes.txt');
        fs.writeFileSync(filePath, 'hello\nworld', 'utf8');
        assert.deepEqual(await extractAttachmentText(filePath, 'text/plain'), {
            text: 'hello\nworld',
            truncated: false,
            supported: true,
        });
    });

    it('falls back to the extension when the mime type is generic', async () => {
        const filePath = path.join(makeTempDir(), 'data.json');
        fs.writeFileSync(filePath, '{"a":1}', 'utf8');
        const result = await extractAttachmentText(
            filePath,
            'application/octet-stream',
        );
        assert.equal(result.supported, true);
        assert.equal(result.text, '{"a":1}');
    });

    it('reports unsupported formats without reading them', async () => {
        const filePath = path.join(makeTempDir(), 'blob.bin');
        fs.writeFileSync(filePath, Buffer.from([0, 1, 2]));
        assert.deepEqual(
            await extractAttachmentText(filePath, 'application/octet-stream'),
            {
                text: '',
                truncated: false,
                supported: false,
            },
        );
    });
});
