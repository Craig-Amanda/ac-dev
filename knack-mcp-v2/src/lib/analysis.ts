import { type CachedSchema } from '../types.js';

export type AppOverviewRelationship = {
    fromObjectKey: string;
    fromObjectName: string | undefined;
    fieldKey: string;
    fieldName: string | undefined;
    toObjectKey: string;
    toObjectName: string;
};

export type AppOverviewResult = {
    objectCount: number;
    totalFields: number;
    relationshipCount: number;
    objects: Array<Record<string, unknown>>;
    relationships: AppOverviewRelationship[];
};

/**
 * Build the object/field/connection summary shared by knack_get_app_overview and
 * knack_app_deep_dive, so the two tools cannot drift out of sync.
 *
 * @param schema Cached schema for the app.
 * @param includeFieldDetails When true, include every field's name/type per object (verbose).
 */
export function buildAppOverview(
    schema: CachedSchema,
    includeFieldDetails: boolean,
): AppOverviewResult {
    const objects = schema.objects || [];
    const objectKeyToName = new Map<string, string>(
        objects.map((obj) => [obj.key, obj.name || obj.key]),
    );

    const relationships: AppOverviewRelationship[] = [];

    const objectSummaries = objects.map((obj) => {
        const fields = obj.fields || [];
        const typeCounts: Record<string, number> = {};
        for (const field of fields) {
            const t = field.type || 'unknown';
            typeCounts[t] = (typeCounts[t] || 0) + 1;
        }

        const connections = fields.filter((f) => f.type === 'connection');
        for (const cf of connections) {
            if (cf.connectedObject) {
                relationships.push({
                    fromObjectKey: obj.key,
                    fromObjectName: obj.name,
                    fieldKey: cf.key,
                    fieldName: cf.name,
                    toObjectKey: cf.connectedObject,
                    toObjectName:
                        objectKeyToName.get(cf.connectedObject) ||
                        cf.connectedObject,
                });
            }
        }

        const summary: Record<string, unknown> = {
            key: obj.key,
            name: obj.name,
            fieldCount: fields.length,
            connectionCount: connections.length,
            typeSummary: Object.entries(typeCounts)
                .map(([type, count]) => ({ type, count }))
                .sort((a, b) => b.count - a.count),
        };

        if (includeFieldDetails) {
            summary.fields = fields.map((f) => ({
                key: f.key,
                name: f.name,
                type: f.type,
                connectedObject: f.connectedObject || undefined,
            }));
        }

        return summary;
    });

    return {
        objectCount: objects.length,
        totalFields: objects.reduce(
            (sum, obj) => sum + (obj.fields || []).length,
            0,
        ),
        relationshipCount: relationships.length,
        objects: objectSummaries,
        relationships,
    };
}

export type DataModelObjectMetric = {
    objectKey: string;
    objectName: string | undefined;
    fieldCount: number;
};

export type DataModelAnalysis = {
    summary: {
        totalObjects: number;
        totalFields: number;
        avgFieldCount: number;
        minFieldCount: number;
        maxFieldCount: number;
        connectedObjectCount: number;
        isolatedObjectCount: number;
    };
    fieldTypeDistribution: Array<{
        type: string;
        count: number;
        percentage: number;
    }>;
    highFieldCountObjects: DataModelObjectMetric[];
    lowFieldCountObjects: DataModelObjectMetric[];
    isolatedObjects: DataModelObjectMetric[];
    observations: string[];
};

/**
 * Build the design-feedback analysis shared by knack_analyze_data_model and
 * knack_app_deep_dive, so the two tools cannot drift out of sync.
 *
 * @param schema Cached schema for the app.
 */
export function buildDataModelAnalysis(
    schema: CachedSchema,
): DataModelAnalysis {
    const objects = schema.objects || [];
    const totalObjects = objects.length;
    const totalFields = objects.reduce(
        (sum, obj) => sum + (obj.fields || []).length,
        0,
    );

    const globalTypeCounts = new Map<string, number>();
    const objectMetrics = objects.map((obj) => {
        const fields = obj.fields || [];
        const typeCounts: Record<string, number> = {};
        for (const field of fields) {
            const t = field.type || 'unknown';
            typeCounts[t] = (typeCounts[t] || 0) + 1;
            globalTypeCounts.set(t, (globalTypeCounts.get(t) || 0) + 1);
        }
        const connectionCount = fields.filter(
            (f) => f.type === 'connection',
        ).length;
        return {
            objectKey: obj.key,
            objectName: obj.name,
            fieldCount: fields.length,
            connectionCount,
            typeCounts,
        };
    });

    const avgFieldCount = totalObjects
        ? Math.round(totalFields / totalObjects)
        : 0;
    const maxFieldCount = objectMetrics.reduce(
        (max, m) => Math.max(max, m.fieldCount),
        0,
    );
    const minFieldCount =
        objectMetrics.reduce(
            (min, m) => Math.min(min, m.fieldCount),
            Infinity,
        ) === Infinity
            ? 0
            : objectMetrics.reduce(
                  (min, m) => Math.min(min, m.fieldCount),
                  Infinity,
              );

    const connectedObjectKeys = new Set<string>(
        objects.flatMap((obj) =>
            (obj.fields || [])
                .filter((f) => f.type === 'connection' && f.connectedObject)
                .flatMap((f) => [obj.key, f.connectedObject as string]),
        ),
    );

    // Consistent with connectedObjectKeys above: an object counts as "connected" if it
    // owns a connection field OR is the target of one elsewhere in the schema. Using
    // m.connectionCount === 0 here (own outgoing fields only) would let a pure
    // connection target — e.g. a core "Users" object other objects point at but that
    // has no outgoing connections itself — be reported as both connected (in
    // connectedObjectCount) and isolated (here) in the same response.
    const isolatedObjects = objectMetrics
        .filter((m) => !connectedObjectKeys.has(m.objectKey))
        .map((m) => ({
            objectKey: m.objectKey,
            objectName: m.objectName,
            fieldCount: m.fieldCount,
        }));

    // Objects are flagged as high-field when they exceed twice the app average or the absolute
    // minimum of 30 fields, whichever is larger. 30 is chosen as a practical Knack threshold
    // above which a single object often becomes hard to maintain.
    const MIN_HIGH_FIELD_THRESHOLD = 30;
    const highFieldThreshold = Math.max(
        avgFieldCount * 2,
        MIN_HIGH_FIELD_THRESHOLD,
    );
    const highFieldCountObjects = objectMetrics
        .filter((m) => m.fieldCount >= highFieldThreshold)
        .map((m) => ({
            objectKey: m.objectKey,
            objectName: m.objectName,
            fieldCount: m.fieldCount,
        }))
        .sort((a, b) => b.fieldCount - a.fieldCount);

    // Objects with 2 or fewer fields are flagged as potentially stub/lookup tables.
    // Knack auto-creates a primary text field for every object, so ≤ 2 means only
    // that auto-field plus at most one user-added field — a likely placeholder or lookup list.
    const LOW_FIELD_COUNT_THRESHOLD = 2;
    const lowFieldCountObjects = objectMetrics
        .filter((m) => m.fieldCount <= LOW_FIELD_COUNT_THRESHOLD)
        .map((m) => ({
            objectKey: m.objectKey,
            objectName: m.objectName,
            fieldCount: m.fieldCount,
        }));

    const fieldTypeDistribution = [...globalTypeCounts.entries()]
        .map(([type, count]) => ({
            type,
            count,
            percentage: totalFields
                ? Math.round((count / totalFields) * 100)
                : 0,
        }))
        .sort((a, b) => b.count - a.count);

    const connectionPct = totalObjects
        ? Math.round((connectedObjectKeys.size / totalObjects) * 100)
        : 0;
    const observations: string[] = [];
    if (isolatedObjects.length > 0) {
        observations.push(
            `${isolatedObjects.length} object(s) have no connections at all (neither an outgoing connection field nor being the target of one elsewhere) — they may be standalone lookup tables or unused.`,
        );
    }
    if (highFieldCountObjects.length > 0) {
        observations.push(
            `${highFieldCountObjects.length} object(s) exceed ${highFieldThreshold} fields — consider whether any could be split into related objects.`,
        );
    }
    if (lowFieldCountObjects.length > 0) {
        observations.push(
            `${lowFieldCountObjects.length} object(s) have ≤ ${LOW_FIELD_COUNT_THRESHOLD} fields — these may be stub/placeholder tables or simple lookup lists.`,
        );
    }
    observations.push(
        `${connectionPct}% of objects participate in at least one connection relationship.`,
    );

    return {
        summary: {
            totalObjects,
            totalFields,
            avgFieldCount,
            minFieldCount,
            maxFieldCount,
            connectedObjectCount: connectedObjectKeys.size,
            isolatedObjectCount: isolatedObjects.length,
        },
        fieldTypeDistribution,
        highFieldCountObjects,
        lowFieldCountObjects,
        isolatedObjects,
        observations,
    };
}
