import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    ApiUsageTracker,
    MAX_BURST_WAIT_MS,
    describeApiUsage,
    describeRequestCost,
    describeReset,
    parseRateLimitHeaders,
    percentUsed,
} from './rate-limit.js';

const NOW = Date.parse('2026-09-30T08:03:08Z');

/** The two real responses pasted from the app, 8.5 minutes apart. */
const SAMPLE = {
    'x-planlimit-limit': '75000',
    'x-planlimit-remaining': '37501',
    'x-planlimit-reset': '57413141',
    'x-ratelimit-limit': '10',
    'x-ratelimit-remaining': '8',
    'x-ratelimit-reset': '1790755388',
};

const getter = (headers: Record<string, string>) => (name: string) =>
    headers[name] ?? null;

describe('parseRateLimitHeaders', () => {
    it('reads the plan limit as milliseconds until reset and the burst limit as epoch seconds', () => {
        const reading = parseRateLimitHeaders(getter(SAMPLE), NOW)!;
        assert.deepEqual(reading.plan, {
            limit: 75000,
            remaining: 37501,
            resetsAt: NOW + 57413141,
        });
        assert.deepEqual(reading.burst, {
            limit: 10,
            remaining: 8,
            resetsAt: 1790755388 * 1000,
        });
        // The sample's plan reset is 00:00 UTC the next day.
        assert.equal(
            new Date(reading.plan!.resetsAt).toISOString().slice(11, 16),
            '00:00',
        );
    });

    it('returns undefined when no rate limit header is present', () => {
        assert.equal(parseRateLimitHeaders(getter({}), NOW), undefined);
    });

    it('drops a limit with a missing or malformed header instead of throwing', () => {
        const reading = parseRateLimitHeaders(
            getter({
                ...SAMPLE,
                'x-planlimit-remaining': 'lots',
                'x-ratelimit-reset': '',
            }),
            NOW,
        );
        assert.equal(reading, undefined);

        const planOnly = parseRateLimitHeaders(
            getter({ ...SAMPLE, 'x-ratelimit-limit': '' }),
            NOW,
        )!;
        assert.ok(planOnly.plan);
        assert.equal(planOnly.burst, undefined);
    });

    it('accepts a burst reset sent as epoch milliseconds or seconds until reset', () => {
        const ms = parseRateLimitHeaders(
            getter({ ...SAMPLE, 'x-ratelimit-reset': '1790755388000' }),
            NOW,
        )!;
        assert.equal(ms.burst!.resetsAt, 1790755388000);

        const until = parseRateLimitHeaders(
            getter({ ...SAMPLE, 'x-ratelimit-reset': '2' }),
            NOW,
        )!;
        assert.equal(until.burst!.resetsAt, NOW + 2000);
    });
});

describe('ApiUsageTracker', () => {
    const reading = (remaining: number, resetInMs = 1000) => ({
        plan: { limit: 75000, remaining: 37501, resetsAt: NOW + 3_600_000 },
        burst: { limit: 10, remaining, resetsAt: NOW + resetInMs },
    });

    it('counts calls per app and reports the latest reading', () => {
        const t = new ApiUsageTracker();
        t.begin('A');
        t.record('A', reading(8), NOW);
        t.begin('A');
        t.record('A', undefined, NOW);
        assert.equal(t.calls('A'), 2);
        assert.equal(t.calls('B'), 0);
        assert.equal(t.plan('A', NOW)?.remaining, 37501);
    });

    it('forgets a reading once its reset has passed', () => {
        const t = new ApiUsageTracker();
        t.record('A', reading(8), NOW);
        assert.ok(t.plan('A', NOW));
        assert.equal(t.plan('A', NOW + 3_600_001), undefined);
        assert.equal(t.burst('A', NOW + 1001), undefined);
    });

    it('does not wait while burst requests remain, counting those in flight', () => {
        const t = new ApiUsageTracker();
        t.record('A', reading(3), NOW);
        assert.equal(t.burstWaitMs('A', NOW), 0);
        t.begin('A');
        t.begin('A');
        assert.equal(t.burstWaitMs('A', NOW), 0);
        t.begin('A');
        // 3 remaining, 3 in flight: the next one would be the 11th in the window.
        assert.equal(t.burstWaitMs('A', NOW), 1025);
    });

    it('caps the burst wait and never waits without a reading', () => {
        const t = new ApiUsageTracker();
        assert.equal(t.burstWaitMs('A', NOW), 0);
        t.record('A', reading(0, 60_000), NOW);
        assert.equal(t.burstWaitMs('A', NOW), MAX_BURST_WAIT_MS);
    });

    it('does not count a request that threw before a response', () => {
        const t = new ApiUsageTracker();
        t.begin('A');
        t.abandon('A');
        assert.equal(t.calls('A'), 0);
    });

    it('refuses only what cannot fit in the known daily allowance', () => {
        const t = new ApiUsageTracker();
        assert.equal(t.budgetShortfall('A', 1_000_000, NOW), undefined);
        t.record('A', reading(8), NOW);
        assert.equal(t.budgetShortfall('A', 37501, NOW), undefined);
        const message = t.budgetShortfall('A', 37502, NOW)!;
        assert.match(message, /37502 API calls but only 37501 remain/);
        assert.match(message, /Nothing was sent/);
    });

    it('reports the plan as exhausted only at zero remaining', () => {
        const t = new ApiUsageTracker();
        t.record(
            'A',
            {
                plan: { limit: 100, remaining: 0, resetsAt: NOW + 1000 },
            },
            NOW,
        );
        assert.equal(t.planExhausted('A', NOW), true);
        assert.equal(t.planExhausted('A', NOW + 1001), false);
        assert.equal(t.planExhausted('B', NOW), false);
    });
});

describe('the daily allowance is shared across apps on one account', () => {
    const plan = (remaining: number, limit = 75000, resetIn = 3_600_000) => ({
        plan: { limit, remaining, resetsAt: NOW + resetIn },
    });
    const accountOf = (appKey: string) =>
        appKey.startsWith('Acme') ? 'account:acme' : `app:${appKey}`;

    it('reads a reading taken by another app on the same account', () => {
        const t = new ApiUsageTracker(accountOf);
        t.record('AcmeSales', plan(1000), NOW);
        assert.equal(t.plan('AcmeHR', NOW)?.remaining, 1000);
        assert.ok(t.budgetShortfall('AcmeHR', 1001, NOW));
        assert.equal(t.budgetShortfall('AcmeHR', 1000, NOW), undefined);
    });

    it('does not share with an app on another account, or with no account set', () => {
        const t = new ApiUsageTracker(accountOf);
        t.record('AcmeSales', plan(1000), NOW);
        assert.equal(t.plan('Other', NOW), undefined);
        // Without a resolver every app is its own account.
        const alone = new ApiUsageTracker();
        alone.record('A', plan(1000), NOW);
        assert.equal(alone.plan('B', NOW), undefined);
    });

    it('keeps call counts and burst limits per app', () => {
        const t = new ApiUsageTracker(accountOf);
        t.record(
            'AcmeSales',
            {
                ...plan(1000),
                burst: { limit: 10, remaining: 0, resetsAt: NOW + 1000 },
            },
            NOW,
        );
        t.record('AcmeHR', plan(999), NOW);
        assert.equal(t.calls('AcmeSales'), 1);
        assert.equal(t.calls('AcmeHR'), 1);
        assert.ok(t.burstWaitMs('AcmeSales', NOW) > 0);
        assert.equal(t.burstWaitMs('AcmeHR', NOW), 0);
    });

    it('keeps the lower remaining when responses arrive out of order', () => {
        const t = new ApiUsageTracker(accountOf);
        t.record('AcmeSales', plan(900), NOW);
        // A slower response, sent earlier, carrying a higher remaining.
        t.record('AcmeHR', plan(950, 75000, 3_600_500), NOW);
        assert.equal(t.plan('AcmeSales', NOW)?.remaining, 900);
        t.record('AcmeHR', plan(899), NOW);
        assert.equal(t.plan('AcmeSales', NOW)?.remaining, 899);
    });

    it('takes a higher remaining once the daily window has reset or the plan changed', () => {
        const t = new ApiUsageTracker(accountOf);
        t.record('AcmeSales', plan(10), NOW);
        t.record('AcmeSales', plan(75000, 75000, 3_600_000 + 86_400_000), NOW);
        assert.equal(t.plan('AcmeHR', NOW)?.remaining, 75000);

        t.record('AcmeSales', plan(20000, 150000, 3_600_000 + 86_400_000), NOW);
        assert.equal(t.plan('AcmeHR', NOW)?.limit, 150000);
    });

    it('reports when the account was last read, whichever app read it', () => {
        const t = new ApiUsageTracker(accountOf);
        t.record('AcmeSales', plan(1000), NOW);
        assert.equal(t.readAt('AcmeHR'), NOW);
        assert.equal(describeApiUsage(t, 'AcmeHR', NOW).plan?.remaining, 1000);
    });
});

describe('describing usage', () => {
    it('shows used, percent and reset for the sample', () => {
        // Read a second before the burst window closes, as the real call was.
        const at = NOW - 1000;
        const t = new ApiUsageTracker();
        t.record('A', parseRateLimitHeaders(getter(SAMPLE), at), at);
        const usage = describeApiUsage(t, 'A', at);
        assert.equal(usage.plan?.used, 37499);
        assert.equal(usage.plan?.percentUsed, 50);
        assert.equal(usage.plan?.resetsAt.slice(11, 16), '00:00');
        assert.equal(usage.burstLimit, 10);
        assert.equal(usage.callsThisSession, 1);
    });

    it('rounds the reset to the nearest second, and keeps the burst limit after its window lapses', () => {
        const t = new ApiUsageTracker();
        const early = Date.parse('2026-09-30T23:59:59.950Z');
        t.record(
            'A',
            {
                plan: { limit: 10000, remaining: 9848, resetsAt: early },
                burst: { limit: 10, remaining: 9, resetsAt: NOW + 1000 },
            },
            NOW,
        );
        const usage = describeApiUsage(t, 'A', NOW + 60_000);
        assert.equal(usage.plan?.resetsAt, '2026-10-01T00:00:00.000Z');
        // The window's own reading is gone, but its size is still known.
        assert.equal(t.burst('A', NOW + 60_000), undefined);
        assert.equal(usage.burstLimit, 10);
    });

    it('is null before any reading', () => {
        const usage = describeApiUsage(new ApiUsageTracker(), 'A', NOW);
        assert.equal(usage.plan, null);
        assert.equal(usage.burstLimit, null);
        assert.equal(usage.readAt, null);
    });

    it('computes percent used to one decimal', () => {
        assert.equal(
            percentUsed({ limit: 75000, remaining: 36948, resetsAt: 0 }),
            50.7,
        );
        assert.equal(percentUsed({ limit: 0, remaining: 0, resetsAt: 0 }), 0);
    });

    it('describes a reset as a UTC time and a countdown', () => {
        assert.equal(
            describeReset(Date.parse('2026-10-01T00:00:00Z'), NOW),
            'at 00:00 UTC (in 15h 57m)',
        );
        // A reset a few seconds before midnight still reads as midnight.
        assert.equal(
            describeReset(Date.parse('2026-09-30T23:59:53Z'), NOW),
            'at 00:00 UTC (in 15h 57m)',
        );
        assert.equal(describeReset(NOW + 90_000, NOW), 'at 08:05 UTC (in 2m)');
    });
});

describe('describeRequestCost', () => {
    const plan = (remaining: number) => ({
        plan: { limit: 75000, remaining, resetsAt: NOW + 3_600_000 },
    });

    it('says nothing about a cheap request with plenty of allowance', () => {
        const t = new ApiUsageTracker();
        const before = t.snapshot();
        for (let i = 0; i < 5; i++) t.record('A', plan(40000), NOW);
        assert.equal(describeRequestCost(t, before, NOW), undefined);
    });

    it('reports a request that made many calls', () => {
        const t = new ApiUsageTracker();
        const before = t.snapshot();
        for (let i = 0; i < 25; i++) t.record('A', plan(40000), NOW);
        const note = describeRequestCost(t, before, NOW)!;
        assert.match(note, /A: this request made 25 API calls/);
        assert.match(note, /40000 of 75000 left \(46.7% used\)/);
    });

    it('counts only the calls made since the snapshot', () => {
        const t = new ApiUsageTracker();
        for (let i = 0; i < 30; i++) t.record('A', plan(40000), NOW);
        const before = t.snapshot();
        t.record('A', plan(40000), NOW);
        assert.equal(describeRequestCost(t, before, NOW), undefined);
    });

    it('warns on a cheap request once the allowance runs low, and again when nearly spent', () => {
        const t = new ApiUsageTracker();
        let before = t.snapshot();
        t.record('A', plan(15000), NOW); // 80% used
        assert.match(describeRequestCost(t, before, NOW)!, /Running low/);
        before = t.snapshot();
        t.record('A', plan(3000), NOW); // 96% used
        assert.match(describeRequestCost(t, before, NOW)!, /Nearly spent/);
    });
});
