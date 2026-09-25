/**
 * Scheduled task tools: list, create, update and delete the tasks on an object.
 *
 * Tasks ride in the public application metadata as `objects[].tasks` (seen 24 September
 * on NPS Test App), so listing needs no API key. Creating one is
 * `POST /objects/:key/tasks`, captured from the Builder on 25 September: the body is the
 * task (name, object_key, type "actions", schedule, run_status, action) and Knack
 * answers with it plus its new `key` (`task_1`, …). `PUT` and `DELETE` on
 * `/objects/:key/tasks/:taskKey` were measured the same day on NP Place Playground; see
 * knack_update_task and knack_delete_task for what they do.
 */
import { z } from 'zod';

import type { AppConfig } from '../config.js';
import type { KnackContext } from '../context.js';

import { VIEW_CACHE_STALE_NOTE } from '../lib/field-payload.js';
import { describeExclusion } from '../lib/field-exclusion.js';
import { getRuntimeArray } from '../lib/metadata.js';
import { deepEqual } from '../lib/structural-diff.js';
import { asRecord, parseJsonInput } from '../lib/util.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { makeTextResponse, toolReplies } from '../response.js';

type RawTask = Record<string, unknown>;

/** Every task in the metadata, with the object it runs on. */
function readTasks(metadata: unknown, objectKey?: string): RawTask[] {
    return (getRuntimeArray(metadata, 'objects') ?? [])
        .map((entry) => asRecord(entry))
        .filter((object): object is Record<string, unknown> =>
            Boolean(object && (!objectKey || object.key === objectKey)),
        )
        .flatMap((object) =>
            (Array.isArray(object.tasks) ? object.tasks : [])
                .map((task) => asRecord(task))
                .filter((task): task is RawTask => Boolean(task))
                .map((task) => ({ ...task, object_key: object.key })),
        );
}

/** The field keys a task action reads or writes: criteria fields, value targets and inputs. */
function taskFieldKeys(action: Record<string, unknown>): string[] {
    const keys = new Set<string>();
    for (const list of [action.criteria, action.values]) {
        for (const entry of Array.isArray(list) ? list : []) {
            const rule = asRecord(entry);
            for (const key of [rule?.field, rule?.input]) {
                if (typeof key !== 'string') continue;
                // "field_1.field_2" reaches across a connection; both halves are fields.
                for (const part of key.split('.')) {
                    if (/^field_\d+$/.test(part)) keys.add(part);
                }
            }
        }
    }
    return [...keys];
}

type Refusal = [error: string, message: string];

/** The caller's action JSON as an object with an `action` string, or why not. */
function parseTaskAction(
    text: string,
): { taskAction: Record<string, unknown> } | { error: Refusal } {
    let parsed: unknown;
    try {
        parsed = parseJsonInput('action', text);
    } catch (error) {
        return {
            error: [
                'INVALID_ACTION',
                `action must be valid JSON: ${(error as Error).message}. Nothing was sent.`,
            ],
        };
    }
    const taskAction = asRecord(parsed);
    if (!taskAction || typeof taskAction.action !== 'string') {
        return {
            error: [
                'INVALID_ACTION',
                'action must be a JSON object with an "action" string (e.g. "record", "email"). Nothing was sent.',
            ],
        };
    }
    return { taskAction };
}

/**
 * Why a task action cannot run on `objectKey`, or null: the object must exist, and every
 * field the action names must exist in the app and not be `_mcp_hidden`.
 */
async function checkTaskTarget(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    taskAction: Record<string, unknown>,
): Promise<Refusal | null> {
    const { schema } = await ctx.getSchema(app);
    if (!schema?.objects?.some((entry) => entry.key === objectKey)) {
        return [
            'OBJECT_NOT_FOUND',
            `${objectKey} was not found in this app's schema. Nothing was sent.`,
        ];
    }
    const exclusions = await ctx.getFieldExclusions(app);
    const allKnown = new Set(
        schema.objects.flatMap((entry) =>
            (entry.fields ?? []).map((field) => field.key),
        ),
    );
    const fieldKeys = taskFieldKeys(taskAction);
    const hidden = fieldKeys.filter((key) => exclusions.hidden.has(key));
    if (hidden.length) {
        return [
            'HIDDEN_FIELD',
            `${hidden.map((key) => describeExclusion(exclusions, key)).join('; ')}, so a task cannot use it. Nothing was sent.`,
        ];
    }
    const unknown = fieldKeys.filter((key) => !allKnown.has(key));
    if (unknown.length) {
        return [
            'UNKNOWN_FIELD',
            `${unknown.join(', ')} ${unknown.length === 1 ? 'is not a field' : 'are not fields'} in this app. Nothing was sent.`,
        ];
    }
    return null;
}

/** A task's schedule as the Builder sends it. */
const SCHEDULE_REPEAT = z.enum(['daily', 'weekly', 'monthly']);
const SCHEDULE_DATE = /^\d{2}\/\d{2}\/\d{4}$/;
const SCHEDULE_TIME = /^\d{1,2}:\d{2}(AM|PM)$/;

/** One task read fresh from the metadata, bypassing the cache. */
async function readLiveTask(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    taskKey: string,
): Promise<RawTask | null> {
    ctx.caches.runtimeMetadata.delete(app.appKey);
    return (
        readTasks(await ctx.getRuntimeMetadata(app), objectKey).find(
            (task) => task.key === taskKey,
        ) ?? null
    );
}

/** The parts of a task a person reads to tell two versions apart. */
function taskSummary(task: RawTask | null) {
    return task
        ? {
              name: task.name ?? null,
              runStatus: task.run_status ?? null,
              schedule: task.schedule ?? null,
              action: task.action ?? null,
          }
        : null;
}

export const listTasks = defineTool({
    name: 'knack_list_tasks',
    description:
        'List scheduled tasks (per object or app-wide): schedule, running or paused, and what each does.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string().optional(),
    },
    handler: async ({ appKey, objectKey }, ctx) => {
        const app = ctx.getApp(appKey);
        const metadata = await ctx.getRuntimeMetadata(app);
        if (!metadata) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message:
                    'Runtime metadata could not be fetched, so tasks cannot be listed. That is not an app with no tasks.',
            });
        }
        const tasks = readTasks(metadata, objectKey).map((task) => {
            const action = asRecord(task.action) ?? {};
            return {
                key: task.key ?? null,
                name: task.name ?? null,
                objectKey: task.object_key,
                runStatus: task.run_status ?? null,
                schedule: task.schedule ?? null,
                action: action.action ?? null,
                criteria: action.criteria ?? [],
                values: action.values ?? [],
                ...(asRecord(action.email) &&
                Object.keys(asRecord(action.email)!).length
                    ? { email: action.email }
                    : {}),
            };
        });
        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            ...(objectKey ? { objectKey } : {}),
            taskCount: tasks.length,
            tasks,
        });
    },
});

/**
 * Create a scheduled task on an object.
 *
 * A task runs on its own, on the live app, against every record its criteria match, so
 * a new one is **paused** unless running is asked for: a person turns it on after
 * checking it in the Builder. Fields the action names must exist on the object, and a
 * `_mcp_hidden` field is refused, as it is for record writes.
 */
export const createTask = defineTool({
    name: 'knack_create_task',
    description:
        'Create a scheduled task on an object (paused unless runStatus is "running"); reads it back to verify.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        name: z.string(),
        schedule: z.object({
            repeat: SCHEDULE_REPEAT,
            date: z
                .string()
                .regex(SCHEDULE_DATE)
                .describe('First run date, MM/DD/YYYY as the Builder sends it'),
            time: z
                .string()
                .regex(SCHEDULE_TIME)
                .describe('Run time, e.g. "9:15AM"'),
        }),
        action: z
            .string()
            .describe(
                'JSON task action, e.g. {"action":"record","criteria":[{"field":"field_1","operator":"is not blank","value":""}],"values":[{"field":"field_2","type":"value","value":"x"}],"email":{}}',
            ),
        runStatus: z.enum(['paused', 'running']).default('paused'),
        previewOnly: z
            .boolean()
            .optional()
            .describe('Return the task without creating it'),
    },
    handler: async (
        { appKey, objectKey, name, schedule, action, runStatus, previewOnly },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);
        const { respond, refuse } = toolReplies(app.appKey, 'create_task', {
            objectKey,
        });

        if (!name.trim()) {
            return refuse(
                'INVALID_NAME',
                'name cannot be blank. Nothing was sent.',
            );
        }
        const parsedAction = parseTaskAction(action);
        if ('error' in parsedAction) return refuse(...parsedAction.error);
        const taskAction = parsedAction.taskAction;
        const fieldProblem = await checkTaskTarget(
            ctx,
            app,
            objectKey,
            taskAction,
        );
        if (fieldProblem) return refuse(...fieldProblem);

        const body = {
            name,
            object_key: objectKey,
            type: 'actions',
            schedule,
            run_status: runStatus,
            action: { email: {}, ...taskAction },
        };
        if (previewOnly) {
            return respond({ ok: true, previewOnly: true, wouldSend: body });
        }

        const result = await ctx.request(app, `/objects/${objectKey}/tasks`, {
            method: 'POST',
            body: JSON.stringify(body),
        });
        const created = asRecord(asRecord(result.body)?.task);
        const taskKey = typeof created?.key === 'string' ? created.key : null;
        if (!result.ok || !taskKey) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message: 'Knack did not create the task.',
            });
        }

        // Read back from fresh metadata: the task is there and in the state asked for.
        const stored = await readLiveTask(ctx, app, objectKey, taskKey);
        const verified = Boolean(stored) && stored?.run_status === runStatus;

        return respond({
            ok: true,
            status: result.status,
            taskKey,
            runStatus: stored?.run_status ?? null,
            verified,
            ...(verified
                ? {}
                : {
                      warning: stored
                          ? `The task was created but reads back as run_status "${String(stored.run_status)}", not "${runStatus}". Check it in the Builder.`
                          : 'The task was created but could not be read back. Check it in the Builder.',
                  }),
            ...(runStatus === 'paused'
                ? {
                      note: 'The task is paused. A person turns it on in the Builder after checking it.',
                  }
                : {}),
            cacheNote: VIEW_CACHE_STALE_NOTE,
        });
    },
});

const scheduleInput = z.object({
    repeat: SCHEDULE_REPEAT.optional(),
    date: z.string().regex(SCHEDULE_DATE).optional().describe('MM/DD/YYYY'),
    time: z.string().regex(SCHEDULE_TIME).optional().describe('e.g. "9:15AM"'),
});

/**
 * Change a task's name, schedule, action or running state.
 *
 * `PUT /objects/:key/tasks/:taskKey` replaces the task with the body sent. Measured on
 * NP Place Playground on 25 September: a body of `{name}` alone answered 200 but did
 * **not** rename the task, and cleared its `run_status` to null. A full body — the live
 * task with the change merged in — renamed it and kept everything else. So this reads
 * the task fresh, merges only what was asked for, sends the whole task, and reads it
 * back. `before` in the response is the complete restore point.
 */
export const updateTask = defineTool({
    name: 'knack_update_task',
    description:
        "Change a scheduled task's name, schedule, action or running state; sends the whole live task back with only that changed.",
    access: 'write',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        taskKey: z.string(),
        name: z.string().optional(),
        schedule: scheduleInput.optional().describe('Only the parts to change'),
        action: z
            .string()
            .optional()
            .describe('JSON task action, replacing the current one whole'),
        runStatus: z.enum(['paused', 'running']).optional(),
        previewOnly: z
            .boolean()
            .optional()
            .describe('Return the merged task without sending it'),
    },
    handler: async (
        {
            appKey,
            objectKey,
            taskKey,
            name,
            schedule,
            action,
            runStatus,
            previewOnly,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);
        const { respond, refuse } = toolReplies(app.appKey, 'update_task', {
            objectKey,
            taskKey,
        });

        const scheduleChanges = Object.fromEntries(
            Object.entries(schedule ?? {}).filter(([, value]) => value),
        );
        if (
            name === undefined &&
            !Object.keys(scheduleChanges).length &&
            action === undefined &&
            runStatus === undefined
        ) {
            return refuse(
                'NOTHING_TO_CHANGE',
                'Pass at least one of name, schedule, action or runStatus. Nothing was sent.',
            );
        }
        if (name !== undefined && !name.trim()) {
            return refuse(
                'INVALID_NAME',
                'name cannot be blank. Nothing was sent.',
            );
        }

        const live = await readLiveTask(ctx, app, objectKey, taskKey);
        if (!live) {
            return refuse(
                'TASK_NOT_FOUND',
                `${taskKey} was not found on ${objectKey}. Check knack_list_tasks. Nothing was sent.`,
            );
        }

        let taskAction = asRecord(live.action) ?? {};
        if (action !== undefined) {
            const parsedAction = parseTaskAction(action);
            if ('error' in parsedAction) return refuse(...parsedAction.error);
            taskAction = { email: {}, ...parsedAction.taskAction };
            const fieldProblem = await checkTaskTarget(
                ctx,
                app,
                objectKey,
                taskAction,
            );
            if (fieldProblem) return refuse(...fieldProblem);
        }

        // The live task minus the two keys Knack owns, with the changes merged in.
        const { key: _key, scheduled: _scheduled, ...rest } = live;
        void _key;
        void _scheduled;
        const body: Record<string, unknown> = {
            ...rest,
            object_key: objectKey,
            ...(name !== undefined ? { name } : {}),
            schedule: {
                ...(asRecord(live.schedule) ?? {}),
                ...scheduleChanges,
            },
            ...(runStatus !== undefined ? { run_status: runStatus } : {}),
            action: taskAction,
        };
        const turningOn =
            runStatus === 'running' && live.run_status !== 'running';

        if (previewOnly) {
            return respond({
                ok: true,
                previewOnly: true,
                wouldSend: body,
                before: taskSummary(live),
                ...(turningOn
                    ? {
                          warning:
                              'This turns the task on: it will run on the live app against every record its criteria match.',
                      }
                    : {}),
            });
        }

        const result = await ctx.request(
            app,
            `/objects/${objectKey}/tasks/${taskKey}`,
            { method: 'PUT', body: JSON.stringify(body) },
        );
        if (!result.ok) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message: 'Knack refused the change. The task is as it was.',
                before: taskSummary(live),
            });
        }

        const stored = await readLiveTask(ctx, app, objectKey, taskKey);
        const mismatched = ['name', 'schedule', 'run_status', 'action'].filter(
            (property) => !deepEqual(stored?.[property], body[property]),
        );
        const verified = Boolean(stored) && mismatched.length === 0;

        return respond({
            ok: true,
            status: result.status,
            verified,
            ...(verified
                ? {}
                : {
                      warning: stored
                          ? `Read back from Knack, ${mismatched.join(', ')} did not take the value sent. Check the task in the Builder; before restores it.`
                          : 'The task could not be read back. Check it in the Builder.',
                  }),
            after: taskSummary(stored),
            before: taskSummary(live),
            cacheNote: VIEW_CACHE_STALE_NOTE,
        });
    },
});

/**
 * Delete a task. `DELETE /objects/:key/tasks/:taskKey` answers `{"success":true}` even
 * for a task that no longer exists (measured 25 September), so existence is checked
 * before and after rather than taken from the reply.
 */
export const deleteTask = defineTool({
    name: 'knack_delete_task',
    description:
        'Delete a scheduled task; previews unless confirm is true, and reads back that it is gone.',
    access: 'delete',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        taskKey: z.string(),
        confirm: z
            .boolean()
            .optional()
            .default(false)
            .describe(
                'Must be true to delete; otherwise a preview is returned',
            ),
    },
    handler: async ({ appKey, objectKey, taskKey, confirm }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);
        const { respond } = toolReplies(app.appKey, 'delete_task', {
            objectKey,
            taskKey,
        });

        const live = await readLiveTask(ctx, app, objectKey, taskKey);
        if (!live) {
            return respond({
                ok: false,
                error: 'TASK_NOT_FOUND',
                message: `${taskKey} was not found on ${objectKey}. Check knack_list_tasks. Nothing was sent.`,
            });
        }
        if (!confirm) {
            return respond({
                ok: false,
                action: 'delete_task_preflight',
                message: `This would permanently delete the task "${String(live.name)}" (${taskKey}). This cannot be undone. Pass confirm: true only after explicitly confirming this with the user.`,
                task: taskSummary(live),
            });
        }

        const result = await ctx.request(
            app,
            `/objects/${objectKey}/tasks/${taskKey}`,
            { method: 'DELETE' },
        );
        if (!result.ok) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message: 'Knack refused the delete. The task is still there.',
            });
        }
        const stillThere = await readLiveTask(ctx, app, objectKey, taskKey);
        return respond({
            ok: true,
            status: result.status,
            verified: stillThere === null,
            ...(stillThere
                ? {
                      warning:
                          'Knack answered success but the task is still listed. Check the Builder.',
                  }
                : {}),
            deleted: taskSummary(live),
            cacheNote: VIEW_CACHE_STALE_NOTE,
        });
    },
});

export const taskTools: AnyToolDef[] = [
    listTasks,
    createTask,
    updateTask,
    deleteTask,
];
