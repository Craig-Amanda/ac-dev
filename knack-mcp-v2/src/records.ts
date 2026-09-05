/**
 * Record reads under an app's optional dataAccess policy, and small record helpers.
 */
import type { AppConfig } from './config.js';
import type { KnackContext } from './context.js';
import type { KnackApiResult } from './http.js';
import { asRecord } from './lib/util.js';

export function buildRecordSearchParams({
    page,
    rowsPerPage,
    q,
    filters,
    sortField,
    sortOrder,
}: {
    page: number;
    rowsPerPage: number;
    q?: string;
    filters?: string | Record<string, unknown>;
    sortField?: string;
    sortOrder?: 'asc' | 'desc';
}): URLSearchParams {
    const params = new URLSearchParams();
    params.set('page', String(page));
    params.set('rows_per_page', String(rowsPerPage));
    if (q) params.set('q', q);
    const trimmedSortField = sortField?.trim();
    if (sortField !== undefined && !trimmedSortField) {
        throw new Error('sortField cannot be empty.');
    }
    if (sortOrder !== undefined && !trimmedSortField) {
        throw new Error('sortOrder requires sortField.');
    }
    if (trimmedSortField) {
        params.set('sort_field', trimmedSortField);
        params.set('sort_order', sortOrder === 'desc' ? 'desc' : 'asc');
    }
    if (filters !== undefined) {
        if (typeof filters === 'string') {
            const trimmed = filters.trim();
            if (!trimmed) throw new Error('filters string cannot be empty.');
            if (trimmed.startsWith('{') || trimmed.startsWith('['))
                JSON.parse(trimmed);
            params.set('filters', trimmed);
        } else {
            params.set('filters', JSON.stringify(filters));
        }
    }
    return params;
}

/**
 * Resolve and enforce the app's read policy before exposing record data.
 * @returns The object's schema entry, the permitted requested fields and the row cap.
 */
export async function getPermittedReadFields(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    requestedFieldKeys: string[],
) {
    const policy = app.dataAccess;
    if (
        policy?.allowedObjectKeys &&
        !policy.allowedObjectKeys.includes(objectKey)
    ) {
        throw new Error(
            `Read access to ${objectKey} is not allowed by this app's dataAccess policy.`,
        );
    }

    const { schema } = await ctx.getSchema(app);
    const object = schema?.objects?.find((entry) => entry.key === objectKey);
    if (!object)
        throw new Error(
            `Object ${objectKey} was not found in the available schema.`,
        );

    const knownFields = new Set(
        (object.fields || []).map((field) => field.key),
    );
    const policyFields = policy?.allowedFieldKeys?.[objectKey];
    const redactedFields = new Set(policy?.redactedFieldKeys || []);
    const fields = requestedFieldKeys.map((key) => key.trim()).filter(Boolean);

    for (const fieldKey of fields) {
        if (!knownFields.has(fieldKey)) {
            throw new Error(
                `Field ${fieldKey} does not belong to ${objectKey}.`,
            );
        }
        if (policyFields && !policyFields.includes(fieldKey)) {
            throw new Error(
                `Field ${fieldKey} is not allowed by this app's dataAccess policy.`,
            );
        }
        if (redactedFields.has(fieldKey)) {
            throw new Error(
                `Field ${fieldKey} is redacted by this app's dataAccess policy.`,
            );
        }
    }

    return { object, fields, maxRecords: policy?.maxRecordsPerQuery || 1000 };
}

/** Field keys a Knack filter tree touches. */
export function getFilterFieldKeys(
    filters: string | Record<string, unknown> | undefined,
): string[] {
    if (filters === undefined) return [];
    const parsed = typeof filters === 'string' ? JSON.parse(filters) : filters;
    const fields = new Set<string>();
    const visit = (value: unknown): void => {
        if (Array.isArray(value)) {
            value.forEach(visit);
            return;
        }
        const record = asRecord(value);
        if (!record) return;
        if (typeof record.field === 'string') fields.add(record.field);
        Object.values(record).forEach(visit);
    };
    visit(parsed);
    return [...fields];
}

/**
 * Validate everything that can reveal data through a query (filters, sort, free text).
 * @returns The maximum records permitted for the app.
 */
export async function validateReadQuery(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    options: {
        filters?: string | Record<string, unknown>;
        q?: string;
        sortField?: string;
    },
): Promise<number> {
    if (!app.dataAccess) {
        return (await getPermittedReadFields(ctx, app, objectKey, []))
            .maxRecords;
    }
    const requested = [
        ...getFilterFieldKeys(options.filters),
        ...(options.sortField ? [options.sortField] : []),
    ];
    const { maxRecords } = await getPermittedReadFields(
        ctx,
        app,
        objectKey,
        requested,
    );
    if (options.q?.trim()) {
        throw new Error(
            'Free-text search is disabled for apps with a dataAccess policy because it can search unapproved fields. Use approved structured filters instead.',
        );
    }
    return maxRecords;
}

/** A record reduced to its id and the approved fields (formatted and `_raw`). */
export function projectRecordFields(
    value: unknown,
    fieldKeys: string[],
): Record<string, unknown> {
    const record = asRecord(value) || {};
    const projected: Record<string, unknown> = {
        id: record.id || record._id || null,
    };
    for (const fieldKey of fieldKeys) {
        projected[fieldKey] = record[fieldKey] ?? null;
        if (`${fieldKey}_raw` in record)
            projected[`${fieldKey}_raw`] = record[`${fieldKey}_raw`];
    }
    return projected;
}

/** Apply the app's read policy to a record or record-list response; a no-op without one. */
export async function applyRecordReadPolicy(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    result: KnackApiResult,
): Promise<KnackApiResult> {
    if (!app.dataAccess) return result;

    const { schema } = await ctx.getSchema(app);
    const object = schema?.objects?.find((entry) => entry.key === objectKey);
    const defaultFields = (object?.fields || [])
        .map((field) => field.key)
        .filter((key) => !app.dataAccess?.redactedFieldKeys?.includes(key));
    const policyFields = app.dataAccess.allowedFieldKeys?.[objectKey];
    const { fields } = await getPermittedReadFields(
        ctx,
        app,
        objectKey,
        policyFields || defaultFields,
    );

    const body = asRecord(result?.body);
    if (!body) return result;
    if (Array.isArray(body.records)) {
        return {
            ...result,
            body: {
                ...body,
                records: body.records.map((record) =>
                    projectRecordFields(record, fields),
                ),
            },
        };
    }
    return { ...result, body: projectRecordFields(body, fields) };
}

/** Records from a list or single-record response. */
export function getRecordsFromResponse(
    result: unknown,
): Record<string, unknown>[] {
    const body = asRecord(asRecord(result)?.body);
    const records = body?.records;
    if (Array.isArray(records)) {
        return records
            .map(asRecord)
            .filter((record): record is Record<string, unknown> =>
                Boolean(record),
            );
    }
    return body ? [body] : [];
}

/** A Knack numeric or formatted currency value as a number, or null. */
export function getNumericValue(value: unknown): number | null {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value !== 'string') return null;
    const parsed = Number(value.replace(/[^0-9.-]/g, ''));
    return Number.isFinite(parsed) ? parsed : null;
}

/** A stable date bucket from Knack's display and raw date shapes. */
export function bucketDate(
    value: unknown,
    granularity: 'day' | 'month' | 'year',
): string | null {
    const raw = asRecord(value);
    const text =
        typeof value === 'string'
            ? value
            : typeof raw?.iso === 'string'
              ? raw.iso
              : typeof raw?.date === 'string'
                ? raw.date
                : null;
    const isoMatch = text?.match(/(\d{4})[-/](\d{1,2})(?:[-/](\d{1,2}))?/);
    const ukMatch = text?.match(/(\d{1,2})[-/](\d{1,2})[-/](\d{4})/);
    const year = isoMatch?.[1] || ukMatch?.[3];
    const monthValue = isoMatch?.[2] || ukMatch?.[2];
    const dayValue = isoMatch?.[3] || ukMatch?.[1];
    if (!year || !monthValue) return null;
    const month = monthValue.padStart(2, '0');
    const day = dayValue?.padStart(2, '0');
    if (granularity === 'year') return year;
    if (granularity === 'month') return `${year}-${month}`;
    return day ? `${year}-${month}-${day}` : null;
}
