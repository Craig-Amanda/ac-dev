import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { z } from 'zod';

import type { KnackApiResult } from '../http.js';
import type { AnyToolDef } from '../registry.js';
import {
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import type { RuntimeMetadata } from '../types.js';
import { createRecords, updateRecords } from './records.js';
import { describeFieldShape, getObject } from './schema.js';

const parseArgs = (tool: AnyToolDef, raw: Record<string, unknown>) =>
    z.object(tool.input).parse(raw);

const METADATA: RuntimeMetadata = {
    application: { settings: { timezone: 'London' } },
    objects: [
        {
            key: 'object_1',
            name: 'Bookings',
            fields: [
                { key: 'field_1', name: 'Name', type: 'short_text' },
                {
                    key: 'field_2',
                    name: 'Starts',
                    type: 'date_time',
                    format: {
                        date_format: 'dd/mm/yyyy',
                        time_format: 'Ignore Time',
                    },
                },
                {
                    key: 'field_3',
                    name: 'Due',
                    type: 'date_time',
                    format: {
                        date_format: 'mm/dd/yyyy',
                        time_format: 'HH MM (military)',
                    },
                },
            ],
        },
    ],
};

const ok = (body: unknown): KnackApiResult => ({ ok: true, status: 200, body });

function setup(respond: () => KnackApiResult = () => ok({ id: 'rec1' })) {
    const app = makeApp();
    const fake = makeFakeContext({
        apps: [app],
        runtimeMetadata: { [app.appKey]: METADATA },
        responses: respond,
    });
    fake.ctx.state.activeAppKey = app.appKey;
    return fake;
}

describe('knack_get_object shows each date field’s own order', () => {
    it('lists dateFormat and dateHasTime beside date fields only', async () => {
        const { ctx } = setup();
        const payload = payloadOf(
            await getObject.handler(
                parseArgs(getObject, { objectKey: 'object_1' }),
                ctx,
            ),
        );
        const fields = payload.fields as Array<Record<string, unknown>>;
        assert.equal(fields[0].dateFormat, undefined);
        assert.equal(fields[1].dateFormat, 'dd/mm/yyyy');
        assert.equal(fields[1].dateHasTime, false);
        assert.equal(fields[2].dateFormat, 'mm/dd/yyyy');
        assert.equal(fields[2].dateHasTime, true);
    });
});

describe('knack_create_records dates', () => {
    it('refuses a date the field’s order cannot hold and sends nothing', async () => {
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [{ field_1: 'A', field_3: '13/09/2026' }],
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.equal(payload.action, 'batch_create_records_preflight');
        const [error] = payload.errors as string[];
        assert.match(error, /^records\[0\]: DATE_INVALID: field_3 \(Due\)/);
        assert.match(error, /mm\/dd\/yyyy/);
        assert.match(error, /13\/09\/2026/);
        assert.deepEqual(requests, []);
    });

    it('sends the same text to a day-first field and flags it ambiguous', async () => {
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [{ field_2: '03/09/2026' }],
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.deepEqual(requests[0].body, { field_2: '03/09/2026' });
        assert.match(
            String(payload.dateWarnings),
            /field_2 read "03\/09\/2026" as 3 September 2026 \(dd\/mm\/yyyy\)/,
        );
    });

    it('says in the dry run how each date was read', async () => {
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [{ field_2: '03/09/2026', field_3: '03/09/2026' }],
                    dryRun: true,
                }),
                ctx,
            ),
        );
        assert.equal(payload.dryRun, true);
        assert.deepEqual(requests, []);
        const dates = payload.dateFields as Array<Record<string, unknown>>;
        assert.deepEqual(
            dates.map((date) => [date.field, date.format, date.understood]),
            [
                ['field_2', 'dd/mm/yyyy', '3 September 2026'],
                ['field_3', 'mm/dd/yyyy', '9 March 2026'],
            ],
        );
        assert.equal(dates[0].input, '03/09/2026');
        assert.equal(dates[0].ambiguous, true);
        assert.match(String(payload.dateTimeZone), /London/);
    });

    it('checks a _raw key and a structured range the same way', async () => {
        const { ctx } = setup();
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [
                        {
                            field_2_raw: {
                                date: '01/09/2026',
                                to: { date: '31/02/2026' },
                            },
                        },
                    ],
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(
            String((payload.errors as string[])[0]),
            /end of the range/,
        );
    });

    it('leaves a record with no date field alone', async () => {
        const { ctx } = setup();
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [{ field_1: '13/13/2026' }],
                    dryRun: true,
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.dateFields, undefined);
    });
});

describe('knack_update_records dates', () => {
    it('refuses an impossible date by id and sends nothing', async () => {
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await updateRecords.handler(
                parseArgs(updateRecords, {
                    objectKey: 'object_1',
                    records: [
                        { recordId: 'r1', data: { field_2: '31/02/2026' } },
                    ],
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(
            String((payload.errors as string[])[0]),
            /^records\[0\]\.data: DATE_INVALID: field_2 \(Starts\) is dd\/mm\/yyyy/,
        );
        assert.deepEqual(requests, []);
    });

    it('says in the dry run how each date was read', async () => {
        const { ctx } = setup();
        const payload = payloadOf(
            await updateRecords.handler(
                parseArgs(updateRecords, {
                    objectKey: 'object_1',
                    records: [
                        {
                            recordId: 'r1',
                            data: {
                                field_3: {
                                    date: '09/30/2026',
                                    hours: '9',
                                    minutes: '15',
                                    am_pm: 'AM',
                                },
                            },
                        },
                    ],
                    dryRun: true,
                }),
                ctx,
            ),
        );
        const [date] = payload.dateFields as Array<Record<string, unknown>>;
        assert.equal(date.understood, '30 September 2026, 9:15 am');
        assert.equal(date.format, 'mm/dd/yyyy');
    });

    it('refuses a where update whose shared date is impossible, before any query', async () => {
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await updateRecords.handler(
                parseArgs(updateRecords, {
                    objectKey: 'object_1',
                    where: {
                        filters: {
                            match: 'and',
                            rules: [
                                {
                                    field: 'field_1',
                                    operator: 'is',
                                    value: 'A',
                                },
                            ],
                        },
                        data: { field_3: '13/09/2026' },
                    },
                    confirm: true,
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(String((payload.errors as string[])[0]), /DATE_INVALID/);
        assert.deepEqual(requests, []);
    });
});

describe('knack_describe_field_shape for date_time', () => {
    it('does not imply month first, and says the order is per field', async () => {
        const { ctx } = setup();
        const payload = payloadOf(
            await describeFieldShape.handler(
                parseArgs(describeFieldShape, { fieldType: 'date_time' }),
                ctx,
            ),
        );
        const shape = payload.valueShape as Record<string, string>;
        assert.doesNotMatch(shape.rawShape, /01\/15\/2024/);
        assert.match(shape.notes, /per field, not per app/);
    });
});

describe('ISO dates through the record tools', () => {
    it('sends an ISO date in each field’s own order', async () => {
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [{ field_2: '2026-09-03', field_3: '2026-09-03' }],
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.deepEqual(requests[0].body, {
            field_2: { date: '03/09/2026' },
            field_3: { date: '09/03/2026' },
        });
    });

    it('shows the converted value in the dry run and sends nothing', async () => {
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [{ field_2: '2026-09-03' }],
                    dryRun: true,
                }),
                ctx,
            ),
        );
        assert.deepEqual(requests, []);
        assert.deepEqual(payload.wouldCreate, [
            { field_2: { date: '03/09/2026' } },
        ]);
        const [date] = payload.dateFields as Array<Record<string, unknown>>;
        assert.deepEqual(date.sent, { date: '03/09/2026' });
        assert.equal(date.understood, '3 September 2026');
    });

    it('refuses an ISO date that is not a real day', async () => {
        const { ctx, requests } = setup();
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [{ field_2: '2026-02-30' }],
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(String((payload.errors as string[])[0]), /DATE_INVALID/);
        assert.deepEqual(requests, []);
    });
});

describe('the check after a write', () => {
    it('warns when Knack’s response holds a different day from the one intended', async () => {
        const { ctx } = setup(() =>
            ok({
                id: 'rec1',
                field_2_raw: { iso_timestamp: '2026-03-09T00:00:00.000Z' },
            }),
        );
        const payload = payloadOf(
            await createRecords.handler(
                parseArgs(createRecords, {
                    objectKey: 'object_1',
                    records: [{ field_2: '03/09/2026' }],
                }),
                ctx,
            ),
        );
        assert.match(
            (payload.dateWarnings as string[]).join(' '),
            /records\[0\]: DATE_MISMATCH: field_2 \(Starts\)/,
        );
    });

    it('stays quiet when the response agrees, on an update too', async () => {
        const { ctx } = setup(() =>
            ok({
                id: 'r1',
                field_2_raw: { iso_timestamp: '2026-09-30T00:00:00.000Z' },
            }),
        );
        const payload = payloadOf(
            await updateRecords.handler(
                parseArgs(updateRecords, {
                    objectKey: 'object_1',
                    records: [
                        { recordId: 'r1', data: { field_2: '30/09/2026' } },
                    ],
                }),
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.dateWarnings, undefined);
    });

    it('checks every record a where update wrote against its response', async () => {
        // A list for the lookup, then one response per PUT: r2 comes back on the wrong day.
        const fake = makeFakeContext({
            apps: [makeApp()],
            runtimeMetadata: { Demo: METADATA },
            responses: (apiPath, init) =>
                init?.method === 'PUT'
                    ? ok({
                          id: apiPath.split('/').pop(),
                          field_2_raw: {
                              iso_timestamp: apiPath.endsWith('r2')
                                  ? '2026-03-09T00:00:00.000Z'
                                  : '2026-09-03T00:00:00.000Z',
                          },
                      })
                    : ok({
                          total_records: 2,
                          records: [{ id: 'r1' }, { id: 'r2' }],
                      }),
        });
        fake.ctx.state.activeAppKey = 'Demo';
        const payload = payloadOf(
            await updateRecords.handler(
                parseArgs(updateRecords, {
                    objectKey: 'object_1',
                    where: {
                        filters: {
                            match: 'and',
                            rules: [
                                {
                                    field: 'field_1',
                                    operator: 'is',
                                    value: 'A',
                                },
                            ],
                        },
                        data: { field_2: '2026-09-03' },
                    },
                    confirm: true,
                }),
                fake.ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.deepEqual(
            fake.requests.filter((r) => r.method === 'PUT').map((r) => r.body),
            [
                { field_2: { date: '03/09/2026' } },
                { field_2: { date: '03/09/2026' } },
            ],
        );
        const warnings = payload.dateWarnings as string[];
        assert.equal(warnings.length, 1);
        assert.match(warnings[0], /^records\[1\]: DATE_MISMATCH: field_2/);
    });
});
