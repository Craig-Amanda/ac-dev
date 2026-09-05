import {
    type CachedField,
    type CachedObject,
    type CachedSchema,
} from '../types.js';

export type SeedCsvObject = {
    objectKey: string;
    objectName: string;
    suggestedUniqueImportKey: string;
    csvContent: string;
    notes: string[];
};

export type SeedCsvWorkbook = {
    importOrder: Array<{
        objectKey: string;
        objectName: string;
        suggestedUniqueImportKey: string;
    }>;
    objects: SeedCsvObject[];
};

export type ExternalConnectionLookup = {
    objectKey: string;
    objectName?: string;
    values: string[];
    source: 'api';
    lookupField: 'identifier';
};

export const CONNECTION_DISPLAY_VALUE_PRIORITY = [
    'identifier',
    'display',
    'name',
    'label',
    'id',
] as const;

export type SeedObjectMeta = {
    object: CachedObject;
    objectName: string;
    uniqueImportKey: string;
    uniqueImportField?: CachedField;
    labelField?: CachedField;
    syntheticLabelField?: string;
    rowCount: number;
    uniqueValues: string[];
    usedPlaceholderChoiceFields: string[];
    skippedFields: string[];
};

export const NON_IMPORTABLE_FIELD_TYPES = new Set([
    'auto_increment',
    'equation',
    'sum',
    'count',
    'average',
    'min',
    'max',
    'concatenation',
    'file',
    'image',
    'signature',
    'timer',
    'password',
]);

export const SAMPLE_FIRST_NAMES = [
    'Avery',
    'Jordan',
    'Casey',
    'Morgan',
    'Riley',
    'Taylor',
];
export const SAMPLE_LAST_NAMES = [
    'Bennett',
    'Carter',
    'Diaz',
    'Foster',
    'Hayes',
    'Morgan',
];
export const SAMPLE_COMPANY_PREFIXES = [
    'Acme',
    'Bluebird',
    'Cedar',
    'Northwind',
    'Summit',
    'Harbor',
];
export const SAMPLE_COMPANY_SUFFIXES = [
    'Logistics',
    'Health',
    'Supply',
    'Advisory',
    'Labs',
    'Services',
];
export const SAMPLE_STREETS = [
    '100 Main St',
    '245 Oak Ave',
    '18 Market St',
    '77 River Rd',
    '910 Sunset Blvd',
    '62 Cedar Ln',
];
export const SAMPLE_CITIES = [
    'Austin',
    'Denver',
    'Madison',
    'Phoenix',
    'Raleigh',
    'Seattle',
];
export const SAMPLE_STATES = ['TX', 'CO', 'WI', 'AZ', 'NC', 'WA'];

export function toSnakeCase(value: string): string {
    return value
        .trim()
        .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
        .replace(/[^a-zA-Z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '')
        .replace(/_+/g, '_')
        .toLowerCase();
}

export function singularize(value: string): string {
    const trimmed = value.trim();
    if (trimmed.endsWith('ies') && trimmed.length > 3)
        return `${trimmed.slice(0, -3)}y`;
    if (trimmed.endsWith('ses') && trimmed.length > 3)
        return trimmed.slice(0, -2);
    if (trimmed.endsWith('s') && !trimmed.endsWith('ss') && trimmed.length > 1)
        return trimmed.slice(0, -1);
    return trimmed;
}

export function humanizeObjectName(value: string): string {
    return singularize(value.replace(/[_-]+/g, ' ')).trim() || 'Record';
}

export function makeSyntheticImportKey(objectName: string): string {
    const slug = toSnakeCase(singularize(objectName)) || 'record';
    return /(_id|_code|_sku|_key|_email)$/.test(slug) ? slug : `${slug}_code`;
}

export function makeSyntheticLabelField(objectName: string): string {
    const slug = toSnakeCase(singularize(objectName)) || 'record';
    return slug.endsWith('_name') ? slug : `${slug}_name`;
}

export function makeKeyPrefix(objectName: string): string {
    const parts = toSnakeCase(singularize(objectName))
        .split('_')
        .filter(Boolean);
    const base =
        parts.length > 1
            ? parts.map((part) => part[0]).join('')
            : (parts[0] || 'rec').slice(0, 4);
    return base.toUpperCase();
}

export function makeUniqueValue(objectName: string, index: number): string {
    return `${makeKeyPrefix(objectName)}-${String(index + 1).padStart(3, '0')}`;
}

export function inferLabelValue(objectName: string, rowIndex: number): string {
    const lowerName = objectName.toLowerCase();
    if (
        /(company|client|customer|vendor|supplier|partner|agency|business|organization|organisation)/.test(
            lowerName,
        )
    ) {
        return `${SAMPLE_COMPANY_PREFIXES[rowIndex % SAMPLE_COMPANY_PREFIXES.length]} ${SAMPLE_COMPANY_SUFFIXES[rowIndex % SAMPLE_COMPANY_SUFFIXES.length]}`;
    }
    if (/(employee|user|contact|person|member|staff|owner)/.test(lowerName)) {
        return `${SAMPLE_FIRST_NAMES[rowIndex % SAMPLE_FIRST_NAMES.length]} ${SAMPLE_LAST_NAMES[rowIndex % SAMPLE_LAST_NAMES.length]}`;
    }
    const humanName = humanizeObjectName(objectName);
    return `${humanName} ${rowIndex + 1}`;
}

export function escapeCsvCell(value: string): string {
    if (/[",\n]/.test(value)) {
        return `"${value.replace(/"/g, '""')}"`;
    }
    return value;
}

export function buildCsv(
    headers: string[],
    rows: Array<Record<string, string>>,
): string {
    const headerLine = headers.map(escapeCsvCell).join(',');
    const dataLines = rows.map((row) =>
        headers.map((header) => escapeCsvCell(row[header] || '')).join(','),
    );
    return [headerLine, ...dataLines].join('\n');
}

export function isImportableField(field: CachedField): boolean {
    return !NON_IMPORTABLE_FIELD_TYPES.has((field.type || '').toLowerCase());
}

export function getFieldHeader(field: CachedField): string {
    return field.name?.trim() || field.key;
}

export function getMultipartHeaders(field: CachedField): string[] {
    const header = getFieldHeader(field);
    switch ((field.type || '').toLowerCase()) {
        case 'name':
            return [
                `${header} Title`,
                `${header} First`,
                `${header} Middle`,
                `${header} Last`,
                `${header} Suffix`,
            ];
        case 'address':
            return [
                `${header} Street`,
                `${header} Street 2`,
                `${header} City`,
                `${header} State`,
                `${header} Zip`,
                `${header} Country`,
            ];
        default:
            return [header];
    }
}

export function chooseUniqueImportField(
    fields: CachedField[],
): CachedField | undefined {
    const pattern =
        /\b(code|sku|external id|external_id|import key|import_key|unique key|unique_key|email|record key|record_key|id)\b/i;
    return fields.find((field) => {
        const type = (field.type || '').toLowerCase();
        return (
            !['connection', 'multiple_choice', 'address', 'name'].includes(
                type,
            ) && pattern.test(getFieldHeader(field))
        );
    });
}

export function chooseLabelField(
    fields: CachedField[],
): CachedField | undefined {
    const preferred = fields.find((field) =>
        /\b(name|title|label)\b/i.test(getFieldHeader(field)),
    );
    if (preferred) return preferred;
    return fields.find((field) =>
        ['short_text', 'paragraph_text', 'email', 'name'].includes(
            (field.type || '').toLowerCase(),
        ),
    );
}

export function getDefaultChoiceOptions(field: CachedField): string[] {
    if ((field.type || '').toLowerCase() === 'user_roles') {
        return ['Admin', 'Manager', 'Viewer'];
    }
    return ['Option A', 'Option B', 'Option C'];
}

export function getSeedRowCount(
    fields: CachedField[],
    minimumRows: number,
): number {
    const optionCount = fields.reduce(
        (max, field) => Math.max(max, field.choiceOptions?.length || 0),
        0,
    );
    return Math.max(minimumRows, Math.min(optionCount || minimumRows, 6));
}

export function buildSeedObjectMeta(
    object: CachedObject,
    minimumRows: number,
): SeedObjectMeta {
    const objectName = object.name || object.key;
    const importableFields = (object.fields || []).filter(isImportableField);
    const uniqueImportField = chooseUniqueImportField(importableFields);
    const labelField = chooseLabelField(importableFields);
    const uniqueImportKey = uniqueImportField
        ? getFieldHeader(uniqueImportField)
        : makeSyntheticImportKey(objectName);
    const syntheticLabelField = labelField
        ? undefined
        : makeSyntheticLabelField(objectName);
    const rowCount = getSeedRowCount(importableFields, minimumRows);

    return {
        object,
        objectName,
        uniqueImportKey,
        uniqueImportField,
        labelField,
        syntheticLabelField,
        rowCount,
        uniqueValues: Array.from({ length: rowCount }, (_, index) =>
            makeUniqueValue(objectName, index),
        ),
        usedPlaceholderChoiceFields: [],
        skippedFields: (object.fields || [])
            .filter((field) => !isImportableField(field))
            .map((field) => getFieldHeader(field)),
    };
}

export function topologicallySortObjects(
    objects: CachedObject[],
): CachedObject[] {
    const objectsByKey = new Map(objects.map((object) => [object.key, object]));
    const dependents = new Map<string, Set<string>>();
    const indegree = new Map<string, number>(
        objects.map((object) => [object.key, 0]),
    );

    for (const object of objects) {
        for (const field of object.fields || []) {
            if (
                (field.type || '').toLowerCase() !== 'connection' ||
                !field.connectedObject ||
                !objectsByKey.has(field.connectedObject)
            )
                continue;
            if (!dependents.has(field.connectedObject))
                dependents.set(field.connectedObject, new Set());
            const downstream = dependents.get(field.connectedObject);
            if (!downstream?.has(object.key)) {
                downstream?.add(object.key);
                indegree.set(object.key, (indegree.get(object.key) || 0) + 1);
            }
        }
    }

    const queue = objects
        .filter((object) => (indegree.get(object.key) || 0) === 0)
        .sort((a, b) => (a.name || a.key).localeCompare(b.name || b.key));
    const ordered: CachedObject[] = [];

    while (queue.length) {
        const next = queue.shift();
        if (!next) continue;
        ordered.push(next);
        for (const dependentKey of dependents.get(next.key) || []) {
            const remaining = (indegree.get(dependentKey) || 0) - 1;
            indegree.set(dependentKey, remaining);
            if (remaining === 0) {
                const dependent = objectsByKey.get(dependentKey);
                if (dependent) {
                    queue.push(dependent);
                    queue.sort((a, b) =>
                        (a.name || a.key).localeCompare(b.name || b.key),
                    );
                }
            }
        }
    }

    if (ordered.length === objects.length) return ordered;

    const seen = new Set(ordered.map((object) => object.key));
    const remaining = objects
        .filter((object) => !seen.has(object.key))
        .sort((a, b) => (a.name || a.key).localeCompare(b.name || b.key));
    return [...ordered, ...remaining];
}

export function populateMultipartField(
    row: Record<string, string>,
    headers: string[],
    rowIndex: number,
): void {
    if (headers.length === 5) {
        row[headers[0]] = rowIndex % 2 === 0 ? 'Ms' : 'Mr';
        row[headers[1]] =
            SAMPLE_FIRST_NAMES[rowIndex % SAMPLE_FIRST_NAMES.length];
        row[headers[2]] = '';
        row[headers[3]] =
            SAMPLE_LAST_NAMES[rowIndex % SAMPLE_LAST_NAMES.length];
        row[headers[4]] = '';
        return;
    }

    row[headers[0]] = SAMPLE_STREETS[rowIndex % SAMPLE_STREETS.length];
    row[headers[1]] = rowIndex % 3 === 0 ? `Suite ${rowIndex + 100}` : '';
    row[headers[2]] = SAMPLE_CITIES[rowIndex % SAMPLE_CITIES.length];
    row[headers[3]] = SAMPLE_STATES[rowIndex % SAMPLE_STATES.length];
    row[headers[4]] = `78${String(rowIndex).padStart(3, '0')}`;
    row[headers[5]] = 'USA';
}

export function populateScalarField(
    row: Record<string, string>,
    field: CachedField,
    meta: SeedObjectMeta,
    metasByKey: Map<string, SeedObjectMeta>,
    externalConnectionLookups: Map<string, ExternalConnectionLookup>,
    rowIndex: number,
): void {
    const header = getFieldHeader(field);
    const fieldType = (field.type || '').toLowerCase();
    const lowerHeader = header.toLowerCase();
    const shouldUseMultipleValuesOnAlternatingRows = Boolean(
        field.allowsMultiple && rowIndex % 2 === 1,
    );

    if (meta.uniqueImportField?.key === field.key) {
        row[header] = meta.uniqueValues[rowIndex];
        return;
    }

    if (meta.labelField?.key === field.key) {
        row[header] = inferLabelValue(meta.objectName, rowIndex);
        return;
    }

    switch (fieldType) {
        case 'connection': {
            const connectedMeta = field.connectedObject
                ? metasByKey.get(field.connectedObject)
                : undefined;
            const externalLookup = field.connectedObject
                ? externalConnectionLookups.get(field.connectedObject)
                : undefined;
            if (!connectedMeta && externalLookup?.values.length) {
                const selectedValues = [
                    externalLookup.values[
                        rowIndex % externalLookup.values.length
                    ],
                ];
                if (
                    shouldUseMultipleValuesOnAlternatingRows &&
                    externalLookup.values.length > 1
                ) {
                    selectedValues.push(
                        externalLookup.values[
                            (rowIndex + 1) % externalLookup.values.length
                        ],
                    );
                }
                row[header] = selectedValues.join(',');
                return;
            }

            if (!connectedMeta) {
                row[header] = makeUniqueValue(
                    field.connectedObject || header,
                    0,
                );
                return;
            }

            const selectedValues = [
                connectedMeta.uniqueValues[
                    rowIndex % connectedMeta.uniqueValues.length
                ],
            ];
            if (
                shouldUseMultipleValuesOnAlternatingRows &&
                connectedMeta.uniqueValues.length > 1
            ) {
                selectedValues.push(
                    connectedMeta.uniqueValues[
                        (rowIndex + 1) % connectedMeta.uniqueValues.length
                    ],
                );
            }
            row[header] = selectedValues.join(',');
            return;
        }
        case 'multiple_choice':
        case 'user_roles': {
            const options = field.choiceOptions?.length
                ? field.choiceOptions
                : getDefaultChoiceOptions(field);
            if (!field.choiceOptions?.length) {
                meta.usedPlaceholderChoiceFields.push(header);
            }
            const selectedValues = [options[rowIndex % options.length]];
            if (
                shouldUseMultipleValuesOnAlternatingRows &&
                options.length > 1
            ) {
                selectedValues.push(options[(rowIndex + 1) % options.length]);
            }
            row[header] = selectedValues.join(',');
            return;
        }
        case 'email':
            row[header] =
                `${toSnakeCase(singularize(meta.objectName)) || 'record'}${rowIndex + 1}@example.com`;
            return;
        case 'phone':
            row[header] = `555010${String(rowIndex + 1).padStart(3, '0')}`;
            return;
        case 'number':
        case 'currency':
            row[header] = ((rowIndex + 1) * 1250).toFixed(
                fieldType === 'currency' ? 2 : 0,
            );
            return;
        case 'boolean':
        case 'yes_no':
            row[header] = rowIndex % 2 === 0 ? 'Yes' : 'No';
            return;
        case 'rating':
            row[header] = String((rowIndex % 5) + 1);
            return;
        case 'date_time':
            row[header] = `2026-01-${String(rowIndex + 5).padStart(2, '0')}`;
            return;
        case 'paragraph_text':
        case 'rich_text':
            row[header] =
                `Sample ${humanizeObjectName(meta.objectName).toLowerCase()} notes for workflow testing row ${rowIndex + 1}.`;
            return;
        case 'link':
            row[header] =
                `https://example.com/${toSnakeCase(singularize(meta.objectName)) || 'record'}/${rowIndex + 1}`;
            return;
        case 'short_text':
        default:
            row[header] = lowerHeader.includes('status')
                ? `Active ${rowIndex + 1}`
                : lowerHeader.includes('code') ||
                    lowerHeader.includes('sku') ||
                    lowerHeader.includes('id')
                  ? meta.uniqueValues[rowIndex]
                  : `${inferLabelValue(meta.objectName, rowIndex)} ${header}`;
            return;
    }
}

export function generateSeedCsvWorkbook(
    schema: CachedSchema,
    options?: {
        objectKeys?: string[];
        rowsPerObject?: number;
        externalConnectionLookups?: Record<string, ExternalConnectionLookup>;
    },
): SeedCsvWorkbook {
    const requestedKeys = options?.objectKeys?.length
        ? new Set(options.objectKeys)
        : null;
    const selectedObjects = (schema.objects || []).filter(
        (object) => !requestedKeys || requestedKeys.has(object.key),
    );
    const orderedObjects = topologicallySortObjects(selectedObjects);
    const metas = orderedObjects.map((object) =>
        buildSeedObjectMeta(object, Math.max(options?.rowsPerObject || 4, 2)),
    );
    const metasByKey = new Map(metas.map((meta) => [meta.object.key, meta]));
    const externalConnectionLookups = new Map(
        Object.entries(options?.externalConnectionLookups || {}),
    );

    const objects: SeedCsvObject[] = metas.map((meta) => {
        const headers: string[] = [];
        const rows = Array.from(
            { length: meta.rowCount },
            () => ({}) as Record<string, string>,
        );
        const importableFields = (meta.object.fields || []).filter(
            isImportableField,
        );

        const pushHeader = (header: string) => {
            if (!headers.includes(header)) headers.push(header);
        };

        pushHeader(meta.uniqueImportKey);
        if (meta.syntheticLabelField) {
            pushHeader(meta.syntheticLabelField);
        }

        for (const field of importableFields) {
            for (const header of getMultipartHeaders(field)) {
                pushHeader(header);
            }
        }

        rows.forEach((row, rowIndex) => {
            row[meta.uniqueImportKey] = meta.uniqueValues[rowIndex];
            if (meta.syntheticLabelField) {
                row[meta.syntheticLabelField] = inferLabelValue(
                    meta.objectName,
                    rowIndex,
                );
            }

            for (const field of importableFields) {
                const multipartHeaders = getMultipartHeaders(field);
                if (multipartHeaders.length > 1) {
                    populateMultipartField(row, multipartHeaders, rowIndex);
                } else {
                    populateScalarField(
                        row,
                        field,
                        meta,
                        metasByKey,
                        externalConnectionLookups,
                        rowIndex,
                    );
                }
            }
        });

        const notes: string[] = [];
        if (!meta.uniqueImportField) {
            notes.push(
                `Suggested unique import key "${meta.uniqueImportKey}" is synthetic so child CSVs have a stable lookup value.`,
            );
        }
        for (const field of importableFields.filter(
            (entry) => (entry.type || '').toLowerCase() === 'connection',
        )) {
            const connectedMeta = field.connectedObject
                ? metasByKey.get(field.connectedObject)
                : undefined;
            const externalLookup = field.connectedObject
                ? externalConnectionLookups.get(field.connectedObject)
                : undefined;
            if (connectedMeta) {
                notes.push(
                    `Connection field "${getFieldHeader(field)}" uses ${connectedMeta.objectName}.${connectedMeta.uniqueImportKey} as the import lookup value.`,
                );
                continue;
            }
            if (externalLookup) {
                notes.push(
                    `Connection field "${getFieldHeader(field)}" uses existing ${externalLookup.objectName || field.connectedObject || 'connected object'} display values fetched from the API (${externalLookup.lookupField}).`,
                );
                continue;
            }
            notes.push(
                `Connection field "${getFieldHeader(field)}" uses ${field.connectedObject || 'the connected object'} via an existing unique lookup field as the import lookup value.`,
            );
        }
        if (meta.usedPlaceholderChoiceFields.length) {
            const uniquePlaceholderFields = Array.from(
                new Set(meta.usedPlaceholderChoiceFields),
            );
            notes.push(
                `Schema metadata did not expose exact option labels for ${uniquePlaceholderFields.join(', ')}; placeholder option labels were used and should be replaced before import if needed.`,
            );
        }
        if (meta.skippedFields.length) {
            notes.push(
                `Skipped non-importable/system fields: ${meta.skippedFields.join(', ')}.`,
            );
        }

        return {
            objectKey: meta.object.key,
            objectName: meta.objectName,
            suggestedUniqueImportKey: meta.uniqueImportKey,
            csvContent: buildCsv(headers, rows),
            notes,
        };
    });

    return {
        importOrder: metas.map((meta) => ({
            objectKey: meta.object.key,
            objectName: meta.objectName,
            suggestedUniqueImportKey: meta.uniqueImportKey,
        })),
        objects,
    };
}

/**
 * Connected parent objects that sit outside the seed selection.
 *
 * These are the objects whose existing records a seed import must reference rather than
 * generate, so they are the only candidates for an API-backed display-value lookup.
 *
 * @param schema Cached schema for the app.
 * @param objectKeys The objects being seeded; defaults to every object in the schema.
 * @returns The external parent objects, sorted by name.
 */
export function getExternalSeedConnectionTargets(
    schema: CachedSchema,
    objectKeys?: string[],
): CachedObject[] {
    const selectedKeys = new Set(
        objectKeys?.length
            ? objectKeys
            : (schema.objects || []).map((object) => object.key),
    );
    const objectsByKey = new Map(
        (schema.objects || []).map((object) => [object.key, object]),
    );
    const targets = new Map<string, CachedObject>();

    for (const object of schema.objects || []) {
        if (!selectedKeys.has(object.key)) continue;
        for (const field of object.fields || []) {
            if (
                (field.type || '').toLowerCase() !== 'connection' ||
                !field.connectedObject ||
                selectedKeys.has(field.connectedObject)
            )
                continue;
            const target = objectsByKey.get(field.connectedObject);
            if (target) {
                targets.set(target.key, target);
            }
        }
    }

    return [...targets.values()].sort((left, right) =>
        (left.name || left.key).localeCompare(right.name || right.key),
    );
}
