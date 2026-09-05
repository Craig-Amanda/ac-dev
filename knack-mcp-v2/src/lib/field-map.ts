import { type CachedFieldMap, type CachedSchema } from '../types.js';
import { debugLog } from './log.js';
import { asRecord } from './util.js';

export function normaliseAlias(text: string): string {
    return text
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
}

export function getFieldTypeByKey(
    schema: CachedSchema | null,
): Record<string, string | null> {
    const fieldTypeByKey: Record<string, string | null> = {};
    for (const obj of schema?.objects || []) {
        for (const field of obj.fields || []) {
            fieldTypeByKey[field.key] = field.type || null;
        }
    }
    return fieldTypeByKey;
}

export function generateStrictFieldMapFromSchema(
    schema: CachedSchema,
): CachedFieldMap {
    const map: CachedFieldMap = {};
    const collidingAliases = new Set<string>();

    for (const obj of schema.objects || []) {
        for (const field of obj.fields || []) {
            const fieldName = (field.name || '').trim();
            if (!fieldName) continue;

            const alias = `${obj.key}.${normaliseAlias(fieldName)}`;
            if (!alias || !/^object_\d+\.[a-z0-9_]+$/.test(alias)) continue;

            const existing = map[alias];
            if (!existing) {
                map[alias] = {
                    fieldKey: field.key,
                    fieldType: field.type || null,
                };
                continue;
            }

            if (existing.fieldKey !== field.key) {
                collidingAliases.add(alias);
            }
        }
    }

    if (collidingAliases.size > 0) {
        debugLog('strict_fieldmap_alias_collisions_detected', {
            collisionCount: collidingAliases.size,
            sample: [...collidingAliases].slice(0, 50),
        });
    }

    return map;
}

export function coerceFieldMap(
    value: unknown,
    schema: CachedSchema | null,
): CachedFieldMap | null {
    const raw = asRecord(value);
    if (!raw) return null;

    const fieldTypeByKey = getFieldTypeByKey(schema);
    const map: CachedFieldMap = {};

    for (const [alias, entry] of Object.entries(raw)) {
        if (typeof entry === 'string') {
            if (!/^field_\d+$/i.test(entry)) continue;
            map[alias] = {
                fieldKey: entry,
                fieldType: fieldTypeByKey[entry] ?? null,
            };
            continue;
        }

        const rec = asRecord(entry);
        if (!rec) continue;
        const fieldKey = typeof rec.fieldKey === 'string' ? rec.fieldKey : null;
        if (!fieldKey || !/^field_\d+$/i.test(fieldKey)) continue;
        const fieldType =
            typeof rec.fieldType === 'string'
                ? rec.fieldType
                : (fieldTypeByKey[fieldKey] ?? null);

        map[alias] = {
            fieldKey,
            fieldType,
        };
    }

    return Object.keys(map).length ? map : null;
}

export function resolveAliasToFieldKey(
    fieldMap: CachedFieldMap,
    alias: string,
): string | null {
    const entry = fieldMap[alias];
    if (!entry) return null;
    return entry.fieldKey;
}
