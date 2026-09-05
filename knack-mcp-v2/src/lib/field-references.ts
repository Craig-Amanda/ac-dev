import {
    type CachedFieldMap,
    type CachedFieldReferenceIndex,
    type CachedSchema,
    type CachedViewMap,
    type FieldReference,
    type ViewContextMap,
} from '../types.js';

export function getStringFromUnknown(value: unknown): string | null {
    if (typeof value === 'string') {
        const trimmed = value.trim();
        return trimmed ? trimmed : null;
    }

    if (Array.isArray(value)) {
        const strings = value
            .map((entry) => getStringFromUnknown(entry))
            .filter((entry): entry is string => Boolean(entry));
        if (!strings.length) return null;
        return strings.join(', ');
    }

    if (value && typeof value === 'object') {
        const rec = value as Record<string, unknown>;
        const candidates = [
            'value',
            'text',
            'email',
            'to',
            'message',
            'subject',
            'name',
        ];
        for (const key of candidates) {
            if (!(key in rec)) continue;
            const candidate = getStringFromUnknown(rec[key]);
            if (candidate) return candidate;
        }
    }

    return null;
}

export function truncateText(
    text: string | null,
    maxLength = 2000,
): string | null {
    if (!text) return null;
    if (text.length <= maxLength) return text;
    return `${text.slice(0, maxLength)}…`;
}

export function extractKtlKeywordsFromText(
    text: string,
): Array<{ keyword: string; snippet: string }> {
    // Boundary is "start of string or any non-word character" rather than just
    // whitespace/'>' — otherwise a keyword wrapped in punctuation (parentheses,
    // quotes, a leading colon/comma) is silently missed.
    const regex = /(?:^|[^a-zA-Z0-9_])(_[a-zA-Z0-9_]+)/g;
    const hits: Array<{ keyword: string; snippet: string }> = [];
    let match: RegExpExecArray | null;

    while ((match = regex.exec(text)) !== null) {
        const keyword = match[1];
        const start = Math.max(0, (match.index || 0) - 40);
        const end = Math.min(text.length, (match.index || 0) + 200);
        const snippet = text.slice(start, end).replace(/\s+/g, ' ').trim();
        hits.push({ keyword, snippet });
    }

    return hits;
}

export function escapeRegExpLiteral(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * True when `text` contains `keyword` as a whole token (bounded by start/end of
 * string or a non-word character on both sides), not merely as a substring of a
 * longer word. A plain `.includes()` check would treat "_hideField" as still
 * "kept" if the new text instead contains an unrelated "_hideFieldWasRemoved".
 */
export function containsKtlKeywordToken(
    text: string,
    keyword: string,
): boolean {
    const pattern = new RegExp(
        `(?:^|[^a-zA-Z0-9_])${escapeRegExpLiteral(keyword)}(?:$|[^a-zA-Z0-9_])`,
    );
    return pattern.test(text);
}

export function extractFieldKeysFromString(text: string): string[] {
    const matches = text.match(/field_\d+/gi) || [];
    return [...new Set(matches.map((match) => match.toLowerCase()))];
}

export function truncateReferenceText(text: string, maxLength = 300): string {
    const normalised = text.replace(/\s+/g, ' ').trim();
    if (normalised.length <= maxLength) return normalised;
    return `${normalised.slice(0, maxLength)}...`;
}

export function classifyFieldReference(
    sourceType: FieldReference['sourceType'],
    pathParts: string[],
): string[] {
    const joined = pathParts.join('.').toLowerCase();
    const classes = new Set<string>([sourceType]);

    if (sourceType === 'schema') {
        classes.add('schemaMetadata');
    }

    if (sourceType === 'fieldMap') {
        classes.add('fieldAlias');
    }

    if (sourceType === 'viewMap') {
        classes.add('view');
    }

    if (
        /(rule|rules|filter|filters|criteria|condition|conditions)/.test(joined)
    ) {
        classes.add('rule');
    }

    if (/(record|records)/.test(joined)) {
        classes.add('record');
    }

    if (classes.has('view') && classes.has('rule') && classes.has('record')) {
        classes.add('viewRecordRule');
    }

    return [...classes];
}

export function addFieldReference(
    index: CachedFieldReferenceIndex,
    dedupe: Set<string>,
    reference: FieldReference,
): void {
    const dedupeKey = JSON.stringify({
        fieldKey: reference.fieldKey,
        sourceType: reference.sourceType,
        matchType: reference.matchType,
        path: reference.path,
        alias: reference.alias || null,
        objectKey: reference.objectKey || null,
        viewKey: reference.viewKey || null,
    });

    if (dedupe.has(dedupeKey)) return;
    dedupe.add(dedupeKey);

    if (!index[reference.fieldKey]) {
        index[reference.fieldKey] = [];
    }

    index[reference.fieldKey].push(reference);
}

export function scanNodeForFieldReferences(
    node: unknown,
    context: {
        sourceType: FieldReference['sourceType'];
        pathParts: string[];
        dedupe: Set<string>;
        index: CachedFieldReferenceIndex;
        objectKey?: string;
        objectName?: string;
        fieldName?: string;
        alias?: string;
        viewKey?: string;
        viewName?: string;
        viewType?: string;
        sceneKey?: string;
        sceneName?: string;
        sceneSlug?: string;
        seen?: WeakSet<object>;
    },
): void {
    if (node === null || node === undefined) return;

    if (typeof node === 'string') {
        const fieldKeys = extractFieldKeysFromString(node);
        if (!fieldKeys.length) return;

        for (const fieldKey of fieldKeys) {
            addFieldReference(context.index, context.dedupe, {
                fieldKey,
                sourceType: context.sourceType,
                matchType: 'value',
                path: context.pathParts.join('.'),
                classification: classifyFieldReference(
                    context.sourceType,
                    context.pathParts,
                ),
                containingText: truncateReferenceText(node),
                objectKey: context.objectKey,
                objectName: context.objectName,
                fieldName: context.fieldName,
                alias: context.alias,
                viewKey: context.viewKey,
                viewName: context.viewName,
                viewType: context.viewType,
                sceneKey: context.sceneKey,
                sceneName: context.sceneName,
                sceneSlug: context.sceneSlug,
            });
        }
        return;
    }

    if (Array.isArray(node)) {
        node.forEach((entry, index) => {
            scanNodeForFieldReferences(entry, {
                ...context,
                pathParts: [...context.pathParts, String(index)],
            });
        });
        return;
    }

    if (typeof node !== 'object') return;

    const seen = context.seen || new WeakSet<object>();
    if (seen.has(node)) return;
    seen.add(node);

    for (const [key, value] of Object.entries(
        node as Record<string, unknown>,
    )) {
        const nextPathParts = [...context.pathParts, key];

        if (/^field_\d+$/i.test(key)) {
            const fieldKey = key.toLowerCase();
            addFieldReference(context.index, context.dedupe, {
                fieldKey,
                sourceType: context.sourceType,
                matchType: 'propertyKey',
                path: nextPathParts.join('.'),
                classification: [
                    ...classifyFieldReference(
                        context.sourceType,
                        nextPathParts,
                    ),
                    'propertyKey',
                ],
                containingText: null,
                objectKey: context.objectKey,
                objectName: context.objectName,
                fieldName: context.fieldName,
                alias: context.alias,
                viewKey: context.viewKey,
                viewName: context.viewName,
                viewType: context.viewType,
                sceneKey: context.sceneKey,
                sceneName: context.sceneName,
                sceneSlug: context.sceneSlug,
            });
        }

        scanNodeForFieldReferences(value, {
            ...context,
            pathParts: nextPathParts,
            seen,
        });
    }
}

export function buildFieldReferenceIndex(params: {
    schema: CachedSchema | null;
    fieldMap: CachedFieldMap | null;
    viewMap: CachedViewMap | null;
    viewContextMap: ViewContextMap;
}): CachedFieldReferenceIndex {
    const index: CachedFieldReferenceIndex = {};
    const dedupe = new Set<string>();

    for (const obj of params.schema?.objects || []) {
        for (const field of obj.fields || []) {
            addFieldReference(index, dedupe, {
                fieldKey: field.key.toLowerCase(),
                sourceType: 'schema',
                matchType: 'definition',
                path: `schema.objects.${obj.key}.fields.${field.key}`,
                classification: ['schema', 'schemaMetadata', 'fieldDefinition'],
                containingText: field.name || null,
                objectKey: obj.key,
                objectName: obj.name,
                fieldName: field.name,
            });

            scanNodeForFieldReferences(field, {
                sourceType: 'schema',
                pathParts: ['schema', 'objects', obj.key, 'fields', field.key],
                dedupe,
                index,
                objectKey: obj.key,
                objectName: obj.name,
                fieldName: field.name,
            });
        }
    }

    for (const [alias, entry] of Object.entries(params.fieldMap || {})) {
        addFieldReference(index, dedupe, {
            fieldKey: entry.fieldKey.toLowerCase(),
            sourceType: 'fieldMap',
            matchType: 'alias',
            path: `fieldMap.${alias}`,
            classification: ['fieldMap', 'fieldAlias'],
            containingText: alias,
            alias,
        });

        scanNodeForFieldReferences(entry, {
            sourceType: 'fieldMap',
            pathParts: ['fieldMap', alias],
            dedupe,
            index,
            alias,
        });
    }

    for (const [viewKey, viewAttrs] of Object.entries(params.viewMap || {})) {
        const sceneContext = params.viewContextMap[viewKey] || {};
        const viewName =
            typeof viewAttrs.name === 'string' ? viewAttrs.name : undefined;
        const viewType =
            typeof viewAttrs.type === 'string' ? viewAttrs.type : undefined;

        scanNodeForFieldReferences(viewAttrs, {
            sourceType: 'viewMap',
            pathParts: ['viewMap', viewKey],
            dedupe,
            index,
            viewKey,
            viewName,
            viewType,
            sceneKey: sceneContext.sceneKey,
            sceneName: sceneContext.sceneName,
            sceneSlug: sceneContext.sceneSlug,
        });
    }

    for (const references of Object.values(index)) {
        references.sort((left, right) => left.path.localeCompare(right.path));
    }

    return index;
}

export function collectEmailNodes(
    node: unknown,
    pathParts: string[] = [],
    out: Array<{
        path: string;
        action: string | null;
        to: string | null;
        cc: string | null;
        bcc: string | null;
        subject: string | null;
        message: string | null;
    }> = [],
    seen = new WeakSet<object>(),
) {
    if (!node || typeof node !== 'object') return out;
    if (seen.has(node)) return out;
    seen.add(node);

    if (Array.isArray(node)) {
        node.forEach((item, index) =>
            collectEmailNodes(item, [...pathParts, String(index)], out, seen),
        );
        return out;
    }

    const rec = node as Record<string, unknown>;
    const action = typeof rec.action === 'string' ? rec.action : null;
    const to = getStringFromUnknown(
        rec.to ?? rec.to_email ?? rec.recipient ?? rec.recipients ?? rec.email,
    );
    const cc = getStringFromUnknown(rec.cc);
    const bcc = getStringFromUnknown(rec.bcc);
    const subject = getStringFromUnknown(
        rec.subject ?? rec.email_subject ?? rec.title,
    );
    const message = getStringFromUnknown(
        rec.message ?? rec.email_message ?? rec.body ?? rec.text,
    );

    const hasRecipientKey = [
        'to',
        'to_email',
        'recipient',
        'recipients',
        'email',
        'cc',
        'bcc',
    ].some((key) => key in rec);
    const isEmailAction = (action || '').toLowerCase() === 'email';
    if (isEmailAction || hasRecipientKey) {
        out.push({
            path: pathParts.length ? pathParts.join('.') : '$',
            action,
            to,
            cc,
            bcc,
            subject,
            message,
        });
    }

    for (const [key, value] of Object.entries(rec)) {
        if (value && typeof value === 'object') {
            collectEmailNodes(value, [...pathParts, key], out, seen);
        }
    }

    return out;
}
