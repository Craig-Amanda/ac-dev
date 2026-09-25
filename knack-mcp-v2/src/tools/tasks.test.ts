import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { z } from 'zod';

import type { AnyToolDef } from '../registry.js';
import {
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import type { RuntimeMetadata } from '../types.js';
import { createTask, listTasks } from './tasks.js';

const parseArgs = (tool: AnyToolDef, raw: Record<string, unknown>) =>
    z.object(tool.input).parse(raw);

/** The task captured from the Builder on NPS Test App object_108, verbatim. */
const TASK = {
    name: 'Task Test 2',
    object_key: 'object_1',
    type: 'actions',
    schedule: { repeat: 'daily', date: '09/25/2026', time: '9:15AM' },
    run_status: 'running',
    action: {
        action: 'record',
        criteria: [{ field: 'field_1', operator: 'is not blank', value: '' }],
        values: [
            {
                field: 'field_2',
                type: 'record',
                input: 'field_1',
                connection_field: '',
                value: '',
            },
        ],
        email: {},
    },
    key: 'task_1',
    scheduled: true,
};

function makeMetadata(tasks: unknown[]): RuntimeMetadata {
    return {
        objects: [
            {
                key: 'object_1',
                name: 'Staff',
                tasks,
                fields: [
                    { key: 'field_1', name: 'Name', type: 'short_text' },
                    { key: 'field_2', name: 'Copy', type: 'short_text' },
                    {
                        key: 'field_3',
                        name: 'Secret',
                        type: 'short_text',
                        meta: { description: '_mcp_hidden' },
                    },
                ],
            },
            { key: 'object_2', name: 'Other', tasks: [], fields: [] },
        ],
    };
}

/** A fake Knack whose POST really adds the task, so the read-back sees it. */
function setup(tasks: unknown[] = [TASK]) {
    const app = makeApp();
    const metadata = makeMetadata(tasks);
    const fake = makeFakeContext({
        apps: [app],
        runtimeMetadata: { [app.appKey]: metadata },
        responses: (apiPath, init) => {
            if (
                apiPath === '/objects/object_1/tasks' &&
                init?.method === 'POST'
            ) {
                const sent = JSON.parse(init.body as string) as Record<
                    string,
                    unknown
                >;
                const stored = { ...sent, key: 'task_2', scheduled: true };
                (
                    (metadata.objects as Array<Record<string, unknown>>)[0]
                        .tasks as unknown[]
                ).push(stored);
                return { ok: true, status: 200, body: { task: stored } };
            }
            return { ok: false, status: 404, body: {} };
        },
    });
    fake.ctx.state.activeAppKey = app.appKey;
    return fake;
}

const NEW_TASK = {
    objectKey: 'object_1',
    name: 'Nightly copy',
    schedule: { repeat: 'daily', date: '09/26/2026', time: '2:00AM' },
    action: JSON.stringify(TASK.action),
};

describe('knack_list_tasks', () => {
    it('lists tasks from the metadata with their object and state', async () => {
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await listTasks.handler(parseArgs(listTasks, {}), ctx),
        );
        assert.equal(payload.taskCount, 1);
        const [task] = payload.tasks as Array<Record<string, unknown>>;
        assert.equal(task.key, 'task_1');
        assert.equal(task.objectKey, 'object_1');
        assert.equal(task.runStatus, 'running');
        assert.equal(task.action, 'record');
        assert.equal(requests.length, 0);
    });

    it('narrows to one object', async () => {
        const { ctx } = setup();
        const payload = payloadOf(
            await listTasks.handler(
                parseArgs(listTasks, { objectKey: 'object_2' }),
                ctx,
            ),
        );
        assert.equal(payload.taskCount, 0);
    });
});

describe('knack_create_task', () => {
    it('is a write, and creates the task paused by default', async () => {
        assert.equal(createTask.access, 'write');
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await createTask.handler(parseArgs(createTask, NEW_TASK), ctx),
        );
        assert.equal(payload.ok, true, JSON.stringify(payload));
        assert.equal(payload.taskKey, 'task_2');
        assert.equal(payload.verified, true);
        assert.equal(payload.runStatus, 'paused');
        const sent = requests[0].body as Record<string, unknown>;
        assert.equal(sent.run_status, 'paused');
        assert.equal(sent.object_key, 'object_1');
        assert.equal(sent.type, 'actions');
    });

    it('previewOnly sends nothing', async () => {
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await createTask.handler(
                parseArgs(createTask, { ...NEW_TASK, previewOnly: true }),
                ctx,
            ),
        );
        assert.equal(payload.previewOnly, true);
        assert.equal(requests.length, 0);
    });

    it('refuses a hidden field or an unknown one before any request', async () => {
        const { ctx, requests } = setup();
        const withHidden = {
            ...TASK.action,
            values: [{ field: 'field_3', type: 'value', value: 'x' }],
        };
        const hidden = payloadOf(
            await createTask.handler(
                parseArgs(createTask, {
                    ...NEW_TASK,
                    action: JSON.stringify(withHidden),
                }),
                ctx,
            ),
        );
        assert.equal(hidden.error, 'HIDDEN_FIELD');

        const withUnknown = {
            ...TASK.action,
            criteria: [{ field: 'field_99', operator: 'is', value: 'x' }],
        };
        const unknown = payloadOf(
            await createTask.handler(
                parseArgs(createTask, {
                    ...NEW_TASK,
                    action: JSON.stringify(withUnknown),
                }),
                ctx,
            ),
        );
        assert.equal(unknown.error, 'UNKNOWN_FIELD');
        assert.equal(requests.length, 0);
    });

    it('refuses an action that is not JSON', async () => {
        const { ctx } = setup();
        const payload = payloadOf(
            await createTask.handler(
                parseArgs(createTask, { ...NEW_TASK, action: '{not json' }),
                ctx,
            ),
        );
        assert.equal(payload.error, 'INVALID_ACTION');
    });
});
