/**
 * Scheduled task tools: list the tasks on an object, and create one.
 *
 * Tasks ride in the public application metadata as `objects[].tasks` (seen 24 September
 * on NPS Test App), so listing needs no API key. Creating one is
 * `POST /objects/:key/tasks`, captured from the Builder on 25 September: the body is the
 * task (name, object_key, type "actions", schedule, run_status, action) and Knack
 * answers with it plus its new `key` (`task_1`, …). Editing and deleting a task are not
 * captured yet.
 */
import { z } from 'zod';

import { VIEW_CACHE_STALE_NOTE } from '../lib/field-payload.js';
import { describeExclusion } from '../lib/field-exclusion.js';
import { getRuntimeArray } from '../lib/metadata.js';
import { asRecord, parseJsonInput } from '../lib/util.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { makeTextResponse } from '../response.js';

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
            repeat: z.enum(['daily', 'weekly', 'monthly']),
            date: z
                .string()
                .regex(/^\d{2}\/\d{2}\/\d{4}$/)
                .describe('First run date, MM/DD/YYYY as the Builder sends it'),
            time: z
                .string()
                .regex(/^\d{1,2}:\d{2}(AM|PM)$/)
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
        const respond = (payload: Record<string, unknown>) =>
            makeTextResponse({
                appKey: app.appKey,
                action: 'create_task',
                objectKey,
                ...payload,
            });
        const refuse = (error: string, message: string) =>
            respond({ ok: false, error, message });

        if (!name.trim()) {
            return refuse(
                'INVALID_NAME',
                'name cannot be blank. Nothing was sent.',
            );
        }
        let parsed: unknown;
        try {
            parsed = parseJsonInput('action', action);
        } catch (error) {
            return refuse(
                'INVALID_ACTION',
                `action must be valid JSON: ${(error as Error).message}. Nothing was sent.`,
            );
        }
        const taskAction = asRecord(parsed);
        if (!taskAction || typeof taskAction.action !== 'string') {
            return refuse(
                'INVALID_ACTION',
                'action must be a JSON object with an "action" string (e.g. "record", "email"). Nothing was sent.',
            );
        }

        const { schema } = await ctx.getSchema(app);
        const object = schema?.objects?.find(
            (entry) => entry.key === objectKey,
        );
        if (!object) {
            return refuse(
                'OBJECT_NOT_FOUND',
                `${objectKey} was not found in this app's schema. Nothing was sent.`,
            );
        }
        const exclusions = await ctx.getFieldExclusions(app);
        const allKnown = new Set(
            (schema?.objects ?? []).flatMap((entry) =>
                (entry.fields ?? []).map((field) => field.key),
            ),
        );
        const fieldKeys = taskFieldKeys(taskAction);
        const hidden = fieldKeys.filter((key) => exclusions.hidden.has(key));
        if (hidden.length) {
            return refuse(
                'HIDDEN_FIELD',
                `${hidden.map((key) => describeExclusion(exclusions, key)).join('; ')}, so a task cannot use it. Nothing was sent.`,
            );
        }
        const unknown = fieldKeys.filter((key) => !allKnown.has(key));
        if (unknown.length) {
            return refuse(
                'UNKNOWN_FIELD',
                `${unknown.join(', ')} ${unknown.length === 1 ? 'is not a field' : 'are not fields'} in this app. Nothing was sent.`,
            );
        }

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
        ctx.caches.runtimeMetadata.delete(app.appKey);
        const stored = readTasks(
            await ctx.getRuntimeMetadata(app),
            objectKey,
        ).find((task) => task.key === taskKey);
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

export const taskTools: AnyToolDef[] = [listTasks, createTask];
