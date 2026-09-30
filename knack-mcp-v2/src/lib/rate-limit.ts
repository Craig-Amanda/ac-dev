/**
 * Knack's rate-limit response headers, and what the server does with them.
 *
 * Knack sends two independent limits on an authenticated response. The allowance is
 * per Knack account, so every app on the account draws on the same one:
 *
 *   x-planlimit-limit / -remaining / -reset   the daily plan allowance; `reset` is
 *                                             milliseconds until it resets (00:00 UTC)
 *   x-ratelimit-limit / -remaining / -reset   a short burst limit (10, about one
 *                                             second); `reset` is epoch seconds
 *
 * Both are Knack's own figures, so they already include calls made by the front end,
 * Make and any other client. Nothing here counts calls to estimate the allowance: it
 * only records the latest reading, and how many calls this server itself made.
 *
 * Pure logic: the clock is always passed in.
 */

/** One limit, with its reset as an absolute time. */
export type LimitReading = {
    limit: number;
    remaining: number;
    /** Epoch milliseconds when the allowance resets. */
    resetsAt: number;
};

export type RateLimitReading = {
    /** The account's daily plan allowance. */
    plan?: LimitReading;
    /** The short burst limit. */
    burst?: LimitReading;
};

type HeaderGetter = (name: string) => string | null;

function parseCount(value: string | null): number | undefined {
    if (value === null || value.trim() === '') return undefined;
    const n = Number(value);
    return Number.isFinite(n) && n >= 0 ? n : undefined;
}

/**
 * Turn a burst reset into epoch milliseconds. Knack sends epoch seconds; the other
 * shapes are accepted by magnitude so a format change degrades to a sensible time
 * rather than one thousands of years away.
 */
function burstResetToEpochMs(value: number, now: number): number {
    if (value >= 1e12) return value; // epoch milliseconds
    if (value >= 1e9) return value * 1000; // epoch seconds
    return now + value * 1000; // seconds until reset
}

function readLimit(
    get: HeaderGetter,
    prefix: string,
    resetToEpochMs: (value: number) => number,
): LimitReading | undefined {
    const limit = parseCount(get(`${prefix}-limit`));
    const remaining = parseCount(get(`${prefix}-remaining`));
    const reset = parseCount(get(`${prefix}-reset`));
    if (limit === undefined || remaining === undefined || reset === undefined) {
        return undefined;
    }
    return { limit, remaining, resetsAt: resetToEpochMs(reset) };
}

/**
 * Read both limits from a response's headers. A header that is missing or malformed
 * leaves that limit out; this never throws.
 */
export function parseRateLimitHeaders(
    get: HeaderGetter,
    now: number,
): RateLimitReading | undefined {
    const plan = readLimit(get, 'x-planlimit', (ms) => now + ms);
    const burst = readLimit(get, 'x-ratelimit', (v) =>
        burstResetToEpochMs(v, now),
    );
    if (!plan && !burst) return undefined;
    return { ...(plan ? { plan } : {}), ...(burst ? { burst } : {}) };
}

/** A reading whose reset has passed no longer says anything about the current window. */
function live(
    reading: LimitReading | undefined,
    now: number,
): LimitReading | undefined {
    return reading && reading.resetsAt > now ? reading : undefined;
}

type AppUsage = {
    burst?: LimitReading;
    readAt?: number;
    /** Authenticated calls this server has made for the app since it started. */
    calls: number;
    /** Requests sent whose response has not been recorded yet. */
    inFlight: number;
};

export type UsageSnapshot = Map<string, number>;

/** The longest a request waits for the burst window to reset. */
export const MAX_BURST_WAIT_MS = 2000;

/** A request making at least this many calls is reported to the caller. */
export const REPORT_CALLS_THRESHOLD = 25;

/** Percent of the daily allowance used at which every response carries a warning. */
export const WARN_PERCENT_USED = 80;
export const CRITICAL_PERCENT_USED = 95;

/** Two plan readings whose resets are this close belong to the same daily window. */
const SAME_WINDOW_MS = 60_000;

export class ApiUsageTracker {
    private readonly apps = new Map<string, AppUsage>();
    /** The daily allowance belongs to the Knack account, so it is kept per account. */
    private readonly plans = new Map<
        string,
        { plan: LimitReading; readAt: number }
    >();

    /**
     * @param accountOf Which Knack account an app belongs to, so apps on one account
     *   share one daily reading. Defaults to each app being its own account.
     */
    constructor(
        private readonly accountOf: (appKey: string) => string = (appKey) =>
            appKey,
    ) {}

    private entry(appKey: string): AppUsage {
        let entry = this.apps.get(appKey);
        if (!entry) {
            entry = { calls: 0, inFlight: 0 };
            this.apps.set(appKey, entry);
        }
        return entry;
    }

    /** A request is about to be sent. */
    begin(appKey: string): void {
        this.entry(appKey).inFlight += 1;
    }

    /** The request finished, with or without headers. Counts one call. */
    record(
        appKey: string,
        reading: RateLimitReading | undefined,
        now: number,
    ): void {
        const entry = this.entry(appKey);
        entry.calls += 1;
        entry.inFlight = Math.max(0, entry.inFlight - 1);
        if (reading?.plan) this.recordPlan(appKey, reading.plan, now);
        if (reading?.burst) entry.burst = reading.burst;
        if (reading) entry.readAt = now;
    }

    /**
     * Within one daily window `remaining` only falls, so of two readings the lower is the
     * newer even if its response arrived first. A new window replaces the old reading.
     */
    private recordPlan(appKey: string, plan: LimitReading, now: number): void {
        const account = this.accountOf(appKey);
        const held = live(this.plans.get(account)?.plan, now);
        // A changed limit means the plan changed, so the new figures stand.
        const sameWindow =
            held &&
            held.limit === plan.limit &&
            Math.abs(held.resetsAt - plan.resetsAt) < SAME_WINDOW_MS;
        if (sameWindow && held.remaining < plan.remaining) return;
        this.plans.set(account, { plan, readAt: now });
    }

    /** A request that threw before any response: it never used an allowance. */
    abandon(appKey: string): void {
        const entry = this.entry(appKey);
        entry.inFlight = Math.max(0, entry.inFlight - 1);
    }

    calls(appKey: string): number {
        return this.apps.get(appKey)?.calls ?? 0;
    }

    /** Per-app call counts now, to diff against later. */
    snapshot(): UsageSnapshot {
        return new Map([...this.apps].map(([key, e]) => [key, e.calls]));
    }

    /**
     * The latest daily reading for the app's account, from any app on it, or undefined
     * once its reset has passed.
     */
    plan(appKey: string, now: number): LimitReading | undefined {
        return live(this.plans.get(this.accountOf(appKey))?.plan, now);
    }

    burst(appKey: string, now: number): LimitReading | undefined {
        return live(this.apps.get(appKey)?.burst, now);
    }

    /** When the app's latest reading was taken; the account's if that is newer. */
    readAt(appKey: string): number | undefined {
        const own = this.apps.get(appKey)?.readAt;
        const account = this.plans.get(this.accountOf(appKey))?.readAt;
        return own === undefined || account === undefined
            ? (own ?? account)
            : Math.max(own, account);
    }

    /**
     * How long to wait before sending another request so the burst window is not
     * exceeded. Requests already in flight count against what remains.
     */
    burstWaitMs(appKey: string, now: number): number {
        const entry = this.apps.get(appKey);
        const burst = live(entry?.burst, now);
        if (!entry || !burst) return 0;
        if (burst.remaining - entry.inFlight > 0) return 0;
        return Math.min(MAX_BURST_WAIT_MS, burst.resetsAt - now + 25);
    }

    /** True when the daily allowance is known to be spent. */
    planExhausted(appKey: string, now: number): boolean {
        const plan = this.plan(appKey, now);
        return plan !== undefined && plan.remaining <= 0;
    }

    /**
     * If `estimate` requests cannot fit in the daily allowance that is known to remain,
     * say so. An unknown allowance never refuses.
     */
    budgetShortfall(
        appKey: string,
        estimate: number,
        now: number,
    ): string | undefined {
        const plan = this.plan(appKey, now);
        if (!plan || estimate <= plan.remaining) return undefined;
        return (
            `This would make about ${estimate} API calls but only ${plan.remaining} remain ` +
            `of the account's daily allowance of ${plan.limit} API calls (as read from ${appKey}), which resets ${describeReset(plan.resetsAt, now)}. ` +
            'Nothing was sent. Make it smaller, or wait for the reset.'
        );
    }
}

export function percentUsed(reading: LimitReading): number {
    if (reading.limit <= 0) return 0;
    return (
        Math.round(
            ((reading.limit - reading.remaining) / reading.limit) * 1000,
        ) / 10
    );
}

/** "at 00:00 UTC (in 15h 56m)". */
export function describeReset(resetsAt: number, now: number): string {
    // Knack's reset lands a second either side of the minute; show the minute.
    const iso = new Date(Math.round(resetsAt / 60000) * 60000).toISOString();
    const at = `${iso.slice(11, 16)} UTC`;
    const minutes = Math.max(0, Math.round((resetsAt - now) / 60000));
    const inText =
        minutes >= 60
            ? `${Math.floor(minutes / 60)}h ${minutes % 60}m`
            : `${minutes}m`;
    return `at ${at} (in ${inText})`;
}

/** The compact block returned by `knack_list_apps`. */
export function describeApiUsage(
    tracker: ApiUsageTracker,
    appKey: string,
    now: number,
) {
    const plan = tracker.plan(appKey, now);
    const burst = tracker.burst(appKey, now);
    const readAt = tracker.readAt(appKey);
    return {
        plan: plan
            ? {
                  limit: plan.limit,
                  remaining: plan.remaining,
                  used: plan.limit - plan.remaining,
                  percentUsed: percentUsed(plan),
                  resetsAt: new Date(plan.resetsAt).toISOString(),
              }
            : null,
        burst: burst
            ? {
                  limit: burst.limit,
                  remaining: burst.remaining,
                  resetsAt: new Date(burst.resetsAt).toISOString(),
              }
            : null,
        readAt: readAt ? new Date(readAt).toISOString() : null,
        callsThisSession: tracker.calls(appKey),
    };
}

/**
 * The note appended to a tool response when the request was expensive or the daily
 * allowance is running low, so an ordinary cheap request costs nothing extra.
 */
export function describeRequestCost(
    tracker: ApiUsageTracker,
    before: UsageSnapshot,
    now: number,
): string | undefined {
    const lines: string[] = [];
    for (const [appKey, after] of tracker.snapshot()) {
        const made = after - (before.get(appKey) ?? 0);
        if (made <= 0) continue;
        const plan = tracker.plan(appKey, now);
        const used = plan ? percentUsed(plan) : 0;
        const costly = made >= REPORT_CALLS_THRESHOLD;
        if (!costly && used < WARN_PERCENT_USED) continue;

        const parts = [`${appKey}: this request made ${made} API calls.`];
        if (plan) {
            parts.push(
                `Account daily allowance: ${plan.remaining} of ${plan.limit} left (${used}% used), resets ${describeReset(plan.resetsAt, now)}.`,
            );
            if (used >= CRITICAL_PERCENT_USED) {
                parts.push('Nearly spent: hold off on bulk work.');
            } else if (used >= WARN_PERCENT_USED) {
                parts.push('Running low.');
            }
        }
        lines.push(parts.join(' '));
    }
    return lines.length ? lines.join('\n') : undefined;
}
