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
import { createTask, deleteTask, listTasks, updateTask } from './tasks.js';

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
            const taskList = (
                metadata.objects as Array<Record<string, unknown>>
            )[0].tasks as Array<Record<string, unknown>>;
            const taskPath = /^\/objects\/object_1\/tasks\/(task_\d+)$/.exec(
                apiPath,
            );
            if (taskPath && init?.method === 'PUT') {
                // As measured: the body replaces the task; key and scheduled are kept.
                const index = taskList.findIndex((t) => t.key === taskPath[1]);
                const sent = JSON.parse(init.body as string) as Record<
                    string,
                    unknown
                >;
                taskList[index] = {
                    ...sent,
                    key: taskPath[1],
                    scheduled: true,
                };
                return {
                    ok: true,
                    status: 200,
                    body: { task: taskList[index] },
                };
            }
            if (taskPath && init?.method === 'DELETE') {
                const index = taskList.findIndex((t) => t.key === taskPath[1]);
                if (index >= 0) taskList.splice(index, 1);
                return { ok: true, status: 200, body: { success: true } };
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

describe('knack_update_task', () => {
    const run = (
        ctx: ReturnType<typeof setup>['ctx'],
        args: Record<string, unknown>,
    ) =>
        updateTask
            .handler(
                parseArgs(updateTask, {
                    objectKey: 'object_1',
                    taskKey: 'task_1',
                    ...args,
                }),
                ctx,
            )
            .then(payloadOf);

    it('sends the whole live task with only the name changed', async () => {
        const { ctx, requests } = setup();
        const result = await run(ctx, { name: 'Renamed' });
        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.verified, true);
        const sent = requests[0].body as Record<string, unknown>;
        assert.equal(requests[0].method, 'PUT');
        assert.equal(requests[0].apiPath, '/objects/object_1/tasks/task_1');
        assert.equal(sent.name, 'Renamed');
        // The measured hazard: a partial body cleared run_status. The whole task goes.
        assert.equal(sent.run_status, 'running');
        assert.deepEqual(sent.action, TASK.action);
        assert.deepEqual(sent.schedule, TASK.schedule);
        assert.equal('key' in sent, false);
    });

    it('merges a partial schedule and changes the running state', async () => {
        const { ctx, requests } = setup();
        const result = await run(ctx, {
            schedule: { time: '4:00AM' },
            runStatus: 'paused',
        });
        assert.equal(result.verified, true, JSON.stringify(result));
        const sent = requests[0].body as Record<string, unknown>;
        assert.deepEqual(sent.schedule, { ...TASK.schedule, time: '4:00AM' });
        assert.equal(sent.run_status, 'paused');
    });

    it('warns when a preview would turn a paused task on', async () => {
        const { ctx, requests } = setup([{ ...TASK, run_status: 'paused' }]);
        const result = await run(ctx, {
            runStatus: 'running',
            previewOnly: true,
        });
        assert.match(String(result.warning), /turns the task on/);
        assert.equal(requests.length, 0);
    });

    it('refuses an unknown task, an empty change and a hidden field', async () => {
        const { ctx, requests } = setup();
        assert.equal(
            (await run(ctx, { taskKey: 'task_9', name: 'x' })).error,
            'TASK_NOT_FOUND',
        );
        assert.equal((await run(ctx, {})).error, 'NOTHING_TO_CHANGE');
        const hidden = await run(ctx, {
            action: JSON.stringify({
                ...TASK.action,
                values: [{ field: 'field_3', type: 'value', value: 'x' }],
            }),
        });
        assert.equal(hidden.error, 'HIDDEN_FIELD');
        assert.equal(requests.length, 0);
    });
});

describe('knack_delete_task', () => {
    it('is a delete, previews, then deletes and verifies', async () => {
        assert.equal(deleteTask.access, 'delete');
        const { ctx, requests } = setup();
        const preview = payloadOf(
            await deleteTask.handler(
                parseArgs(deleteTask, {
                    objectKey: 'object_1',
                    taskKey: 'task_1',
                }),
                ctx,
            ),
        );
        assert.equal(preview.action, 'delete_task_preflight');
        assert.equal(requests.length, 0);

        const done = payloadOf(
            await deleteTask.handler(
                parseArgs(deleteTask, {
                    objectKey: 'object_1',
                    taskKey: 'task_1',
                    confirm: true,
                }),
                ctx,
            ),
        );
        assert.equal(done.ok, true);
        assert.equal(done.verified, true);
        assert.equal(requests[0].method, 'DELETE');
    });

    it('refuses a task that is not there, since Knack would answer success', async () => {
        const { ctx, requests } = setup();
        const result = payloadOf(
            await deleteTask.handler(
                parseArgs(deleteTask, {
                    objectKey: 'object_1',
                    taskKey: 'task_9',
                    confirm: true,
                }),
                ctx,
            ),
        );
        assert.equal(result.error, 'TASK_NOT_FOUND');
        assert.equal(requests.length, 0);
    });
});
