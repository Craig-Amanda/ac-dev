import { asRecord } from './util.js';
import {
    type CachedField,
    type CachedFieldMap,
    type CachedSchema,
    type CachedViewMap,
    type RuntimeMetadata,
    type SceneInfo,
    type SceneViewInfo,
    type ViewContextMap,
    type ViewFieldSettings,
    type ViewFieldSettingsSummary,
} from '../types.js';
import { extractBoolean, extractChoiceOptions } from './view-templates.js';
import {
    coerceFieldMap,
    generateStrictFieldMapFromSchema,
} from './field-map.js';
import {
    type SceneViewLinks,
    collectNavigationRefs,
    resolveViewAttributes,
} from './view-safety.js';

export function getObjectAtPath(root: unknown, ...keys: string[]): unknown {
    let current: unknown = root;
    for (const key of keys) {
        const rec = asRecord(current);
        if (!rec || !(key in rec)) return null;
        current = rec[key];
    }
    return current;
}

export function isRuntimeMetadataPayload(
    value: unknown,
): value is RuntimeMetadata {
    const payload = asRecord(value);
    if (!payload) return false;

    const hasApplication = asRecord(payload.application) !== null;
    const hasObjects = Array.isArray(payload.objects);
    const hasScenes = Array.isArray(payload.scenes);

    return hasApplication || hasObjects || hasScenes;
}

export function parseRuntimeSchema(body: unknown): CachedSchema | null {
    const directObjects = getObjectAtPath(body, 'objects');
    const nestedObjects = getObjectAtPath(body, 'application', 'objects');
    const objectsRaw = Array.isArray(directObjects)
        ? directObjects
        : Array.isArray(nestedObjects)
          ? nestedObjects
          : null;

    if (!objectsRaw) return null;

    const objects: NonNullable<CachedSchema['objects']> = [];

    for (const objectItem of objectsRaw) {
        const obj = asRecord(objectItem);
        if (!obj) continue;

        const objectKey = typeof obj.key === 'string' ? obj.key : null;
        if (!objectKey) continue;

        const objectName = typeof obj.name === 'string' ? obj.name : undefined;
        const fieldsRaw = Array.isArray(obj.fields) ? obj.fields : [];
        const fields: CachedField[] = [];

        for (const fieldItem of fieldsRaw) {
            const field = asRecord(fieldItem);
            if (!field) continue;
            const fieldKey = typeof field.key === 'string' ? field.key : null;
            if (!fieldKey) continue;
            const fieldMeta = asRecord(field.meta);
            const fieldDescription =
                typeof field.description === 'string'
                    ? field.description
                    : typeof fieldMeta?.description === 'string'
                      ? fieldMeta.description
                      : undefined;

            const fieldFormat = asRecord(field.format);
            const fieldRelationship = asRecord(field.relationship);
            const connectedObject =
                (typeof fieldFormat?.object === 'string'
                    ? fieldFormat.object
                    : undefined) ||
                (typeof fieldRelationship?.object === 'string'
                    ? fieldRelationship.object
                    : undefined);
            const choiceOptions = extractChoiceOptions(
                field.options,
                fieldFormat?.options,
                fieldFormat?.choices,
                fieldMeta?.options,
                fieldMeta?.choices,
            );
            // Knack's real connection cardinality lives at relationship.has /
            // relationship.belongs_to ('one'|'many'), not any of the boolean-ish keys
            // below (those were never observed on a live connection field). Treat either
            // side reporting 'many' as multiple; only count as one-to-one when both sides
            // explicitly say 'one'.
            const relationshipCardinality =
                fieldRelationship?.has === 'many' ||
                fieldRelationship?.belongs_to === 'many'
                    ? true
                    : fieldRelationship?.has === 'one' &&
                        fieldRelationship?.belongs_to === 'one'
                      ? false
                      : undefined;
            const allowsMultiple = extractBoolean(
                relationshipCardinality,
                field.multiple,
                field.allow_multiple,
                field.allowMultiple,
                fieldFormat?.multiple,
                fieldFormat?.allow_multiple,
                fieldFormat?.allowMultiple,
                fieldMeta?.multiple,
                fieldMeta?.allow_multiple,
                fieldMeta?.allowMultiple,
                fieldRelationship?.multiple,
                fieldRelationship?.hasMany,
                fieldRelationship?.many,
            );
            const required = extractBoolean(
                field.required,
                fieldFormat?.required,
                fieldMeta?.required,
            );

            fields.push({
                key: fieldKey,
                name: typeof field.name === 'string' ? field.name : undefined,
                type: typeof field.type === 'string' ? field.type : undefined,
                required,
                description: fieldDescription,
                connectedObject,
                choiceOptions: choiceOptions.length ? choiceOptions : undefined,
                allowsMultiple,
            });
        }

        objects.push({ key: objectKey, name: objectName, fields });
    }

    return objects.length ? { objects } : null;
}

export function parseRuntimeFieldMap(body: unknown): CachedFieldMap | null {
    const schema = parseRuntimeSchema(body);
    if (schema?.objects?.length) {
        const strictMap = generateStrictFieldMapFromSchema(schema);
        if (Object.keys(strictMap).length) return strictMap;
    }

    const direct = getObjectAtPath(body, 'fieldMap');
    const nested = getObjectAtPath(body, 'application', 'fieldMap');
    return coerceFieldMap(direct ?? nested, schema);
}

export function parseRuntimeViewMap(body: unknown): CachedViewMap | null {
    const direct = getObjectAtPath(body, 'viewMap');
    const nested = getObjectAtPath(body, 'application', 'viewMap');
    const rawMap = asRecord(direct) || asRecord(nested);

    if (rawMap) {
        const parsed: CachedViewMap = {};
        for (const [viewKey, attrs] of Object.entries(rawMap)) {
            const attributes = asRecord(attrs);
            if (!attributes) continue;
            parsed[viewKey] = attributes;
        }
        if (Object.keys(parsed).length) return parsed;
    }

    const directScenes = getObjectAtPath(body, 'scenes');
    const nestedScenes = getObjectAtPath(body, 'application', 'scenes');
    const scenesRaw = Array.isArray(directScenes)
        ? directScenes
        : Array.isArray(nestedScenes)
          ? nestedScenes
          : null;

    if (!scenesRaw) return null;

    const viewMap: CachedViewMap = {};
    for (const sceneItem of scenesRaw) {
        const scene = asRecord(sceneItem);
        if (!scene) continue;

        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];
        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (!view) continue;

            const viewKey = typeof view.key === 'string' ? view.key : null;
            if (!viewKey) continue;

            const attributes = asRecord(view.attributes) || view;
            viewMap[viewKey] = attributes;
        }
    }

    return Object.keys(viewMap).length ? viewMap : null;
}

/**
 * Resolve a field key from a Knack view layout item.
 *
 * @param item A form input, search field, or displayed view column.
 * @returns The configured Knack field key when the item represents a field.
 */
export function getViewLayoutFieldKey(
    item: Record<string, unknown>,
): string | undefined {
    const field = item.field;
    if (typeof field === 'string' && /^field_\d+$/i.test(field)) {
        return field;
    }

    const fieldRecord = asRecord(field);
    if (
        fieldRecord &&
        typeof fieldRecord.key === 'string' &&
        /^field_\d+$/i.test(fieldRecord.key)
    ) {
        return fieldRecord.key;
    }

    return typeof item.id === 'string' && /^field_\d+$/i.test(item.id)
        ? item.id
        : undefined;
}

/**
 * Resolve the object-field metadata that applies to a record-backed view.
 *
 * @param attributes Raw Knack view attributes.
 * @param schema Cached object schema.
 * @returns Field metadata keyed by field key, or an empty map when the view object is unknown.
 */
export function getViewObjectFields(
    attributes: Record<string, unknown>,
    schema: CachedSchema | null | undefined,
): Map<string, CachedField> {
    const source = asRecord(attributes.source);
    const objectKey = typeof source?.object === 'string' ? source.object : null;
    const object = schema?.objects?.find((entry) => entry.key === objectKey);
    return new Map((object?.fields || []).map((field) => [field.key, field]));
}

/**
 * Extract configured default values while retaining false, zero, and empty-string defaults.
 *
 * @param item A Knack view layout field item.
 * @returns The explicitly configured defaults, if any.
 */
export function getViewFieldDefaults(
    item: Record<string, unknown>,
): Record<string, unknown> | undefined {
    const defaults: Record<string, unknown> = {};
    const format = asRecord(item.format);
    const candidates = [item, format].filter(
        (candidate): candidate is Record<string, unknown> => Boolean(candidate),
    );

    for (const candidate of candidates) {
        for (const [key, value] of Object.entries(candidate)) {
            if (
                key === 'default' ||
                key === 'conn_default' ||
                key.startsWith('default_')
            ) {
                defaults[key] = value;
            }
        }
    }

    return Object.keys(defaults).length ? defaults : undefined;
}

/**
 * Extract the configured field settings from a view layout without interpreting conditional rules.
 *
 * Requiredness is resolved from the owning object schema. Defaults and read-only state are view
 * settings. A missing value is intentionally omitted so callers do not confuse an absent setting
 * with an explicit false value.
 *
 * @param attributes Raw Knack view attributes.
 * @returns A compact field-settings summary suitable for MCP tool responses.
 */
export function getViewFieldSettings(
    attributes: Record<string, unknown>,
    fieldsByKey: Map<string, CachedField> = new Map(),
): ViewFieldSettingsSummary {
    const fields: ViewFieldSettings[] = [];
    const seen = new Set<string>();

    const addField = (
        value: unknown,
        layout: ViewFieldSettings['layout'],
        sourcePath: string,
    ): void => {
        const item = asRecord(value);
        if (!item) return;

        const fieldKey = getViewLayoutFieldKey(item);
        if (!fieldKey) return;

        const dedupeKey = `${sourcePath}:${fieldKey}`;
        if (seen.has(dedupeKey)) return;
        seen.add(dedupeKey);

        const format = asRecord(item.format);
        const rules = Array.isArray(item.rules)
            ? item.rules
            : Array.isArray(item.visibility_rules)
              ? item.visibility_rules
              : Array.isArray(item.visibilityRules)
                ? item.visibilityRules
                : undefined;

        fields.push({
            fieldKey,
            fieldType:
                fieldsByKey.get(fieldKey)?.type ??
                (typeof item.type === 'string' ? item.type : undefined),

            label:
                typeof item.label === 'string'
                    ? item.label
                    : typeof item.name === 'string'
                      ? item.name
                      : undefined,
            objectRequired: fieldsByKey.get(fieldKey)?.required,
            readOnly: extractBoolean(
                item.read_only,
                item.readOnly,
                format?.read_only,
                format?.readOnly,
            ),
            defaults: getViewFieldDefaults(item),
            rules,
            layout,
            sourcePath,
        });
    };

    const visitContainer = (value: unknown, path: string): void => {
        const container = asRecord(value);
        if (!container) return;

        const inputs = Array.isArray(container.inputs) ? container.inputs : [];
        inputs.forEach((input, index) =>
            addField(input, 'form-input', `${path}.inputs[${index}]`),
        );

        const searchFields = Array.isArray(container.fields)
            ? container.fields
            : [];
        searchFields.forEach((field, index) =>
            addField(field, 'search-field', `${path}.fields[${index}]`),
        );

        const groups = Array.isArray(container.groups) ? container.groups : [];
        groups.forEach((group, index) =>
            visitContainer(group, `${path}.groups[${index}]`),
        );

        const columns = Array.isArray(container.columns)
            ? container.columns
            : [];
        columns.forEach((column, index) => {
            const columnPath = `${path}.columns[${index}]`;
            addField(column, 'view-column', columnPath);
            visitContainer(column, columnPath);
        });
    };

    visitContainer(attributes, '$');

    return {
        configuredFieldCount: fields.length,
        requiredFieldCount: fields.filter(
            (field) => field.objectRequired === true,
        ).length,
        readOnlyFieldCount: fields.filter((field) => field.readOnly === true)
            .length,
        fields,
        ...(Object.hasOwn(attributes, 'rules')
            ? { viewRules: attributes.rules }
            : {}),
    };
}

export function parseRuntimeViewContextMap(body: unknown): ViewContextMap {
    const directScenes = getObjectAtPath(body, 'scenes');
    const nestedScenes = getObjectAtPath(body, 'application', 'scenes');
    const scenesRaw = Array.isArray(directScenes)
        ? directScenes
        : Array.isArray(nestedScenes)
          ? nestedScenes
          : null;

    if (!scenesRaw) return {};

    const contextMap: ViewContextMap = {};
    for (const sceneItem of scenesRaw) {
        const scene = asRecord(sceneItem);
        if (!scene) continue;

        const sceneKey = typeof scene.key === 'string' ? scene.key : undefined;
        const sceneName =
            typeof scene.name === 'string' ? scene.name : undefined;
        const sceneSlug =
            typeof scene.slug === 'string' ? scene.slug : undefined;
        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];

        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (!view) continue;
            const viewKey = typeof view.key === 'string' ? view.key : null;
            if (!viewKey) continue;
            contextMap[viewKey] = { sceneKey, sceneName, sceneSlug };
        }
    }

    return contextMap;
}

/**
 * Find one view's raw definition inside a runtime metadata payload.
 *
 * The guard's preflight needs a view's declared type and the layout key carrying its
 * link columns. Knack serves no per-view route to a REST API key — every candidate host
 * answers `scenes/<scene>/views/<view>` with a web-server HTML 404, so the preflight
 * failed with COULD_NOT_VERIFY_VIEW on every mutation and the menu blocks, the cascade
 * check and the human confirmation were all unreachable.
 *
 * The application payload carries the whole definition, on a route that does work and
 * that this server already reads. Sourcing the preflight from it needs no new endpoint
 * and no builder session.
 *
 * Returns the view object as it appears in the payload — `{key, attributes: {...}}` —
 * which `resolveViewAttributes` already unwraps.
 *
 * @param body Runtime metadata payload.
 * @param sceneKey Scene holding the view.
 * @param viewKey View to find.
 * @returns The raw view record, or null when either key is absent.
 */
export function findRawViewInMetadata(
    body: unknown,
    sceneKey: string,
    viewKey: string,
): Record<string, unknown> | null {
    const directScenes = getObjectAtPath(body, 'scenes');
    const nestedScenes = getObjectAtPath(body, 'application', 'scenes');
    const scenesRaw = Array.isArray(directScenes)
        ? directScenes
        : Array.isArray(nestedScenes)
          ? nestedScenes
          : null;

    if (!scenesRaw) return null;

    for (const sceneItem of scenesRaw) {
        const scene = asRecord(sceneItem);
        if (!scene || scene.key !== sceneKey) continue;

        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];
        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (view && view.key === viewKey) return view;
        }
        // The scene was found and the view was not in it. Keep scanning rather than
        // returning: a duplicate scene key would otherwise mask a later match, and a
        // wrong "not found" here becomes a refusal on a legitimate mutation.
    }

    return null;
}

/**
 * Collect, for every view in the app, the pages that view links to.
 *
 * The cascade rule needs the app's whole link graph, not the mutating view's corner of
 * it: Knack deletes a child page when its **last** referring link goes, and nothing in
 * one view's definition says whether another view still points at the same page. Built
 * from the same runtime payload the preflight reads, so the referrer count and the
 * view being changed cannot disagree about what links where.
 *
 * @param body Runtime metadata, in either the bare or `application`-wrapped shape.
 * @returns Per-scene view links, keyed by scene key. Empty when the payload carries no
 *     scenes — callers must treat that as "not measured" rather than "nothing links".
 */
export function collectSceneViewLinks(
    body: unknown,
): Map<string, SceneViewLinks[]> {
    const directScenes = getObjectAtPath(body, 'scenes');
    const nestedScenes = getObjectAtPath(body, 'application', 'scenes');
    const scenesRaw = Array.isArray(directScenes)
        ? directScenes
        : Array.isArray(nestedScenes)
          ? nestedScenes
          : null;

    const linksByScene = new Map<string, SceneViewLinks[]>();
    if (!scenesRaw) return linksByScene;

    for (const sceneItem of scenesRaw) {
        const scene = asRecord(sceneItem);
        if (!scene) continue;
        const sceneKey = typeof scene.key === 'string' ? scene.key : null;
        if (!sceneKey) continue;

        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];
        const views: SceneViewLinks[] = [];
        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (!view) continue;
            const viewKey = typeof view.key === 'string' ? view.key : null;
            if (!viewKey) continue;

            const attributes = resolveViewAttributes(view);
            if (!attributes) continue;
            // The same collector the guard runs on the view being mutated, so a link
            // shape it can see in one place it can see everywhere. A shape it cannot
            // read contributes no referrer, which keeps the count conservative: an
            // uncounted referrer leaves a page doomed, never spares one.
            // Navigation only. The broad collector is right for the view being
            // mutated and wrong here: an extra "link" makes a page look
            // multi-referenced, which spares it and skips the prompt.
            views.push({
                viewKey,
                childSceneRefs: collectNavigationRefs(attributes),
            });
        }

        // A duplicate scene key would otherwise drop the first scene's views, and a
        // dropped referrer is a page reported as doomed that is not.
        const existing = linksByScene.get(sceneKey);
        linksByScene.set(sceneKey, existing ? [...existing, ...views] : views);
    }

    return linksByScene;
}

export function parseRuntimeScenes(body: unknown): SceneInfo[] {
    const directScenes = getObjectAtPath(body, 'scenes');
    const nestedScenes = getObjectAtPath(body, 'application', 'scenes');
    const scenesRaw = Array.isArray(directScenes)
        ? directScenes
        : Array.isArray(nestedScenes)
          ? nestedScenes
          : null;

    if (!scenesRaw) return [];

    const scenes: SceneInfo[] = [];
    for (const sceneItem of scenesRaw) {
        const scene = asRecord(sceneItem);
        if (!scene) continue;

        const sceneKey = typeof scene.key === 'string' ? scene.key : null;
        if (!sceneKey) continue;

        const sceneName =
            typeof scene.name === 'string' ? scene.name : undefined;
        const sceneSlug =
            typeof scene.slug === 'string' ? scene.slug : undefined;
        const parentRef =
            typeof scene.parent === 'string' && scene.parent.trim()
                ? scene.parent.trim()
                : undefined;
        const viewsRaw = Array.isArray(scene.views) ? scene.views : [];

        const views: SceneViewInfo[] = [];
        for (const viewItem of viewsRaw) {
            const view = asRecord(viewItem);
            if (!view) continue;
            const viewKey = typeof view.key === 'string' ? view.key : null;
            if (!viewKey) continue;
            const attributes = asRecord(view.attributes) || view;
            const viewName =
                typeof attributes.name === 'string'
                    ? attributes.name
                    : undefined;
            const viewType =
                typeof attributes.type === 'string'
                    ? attributes.type
                    : undefined;
            views.push({ viewKey, viewName, viewType });
        }

        scenes.push({
            sceneKey,
            sceneName,
            sceneSlug,
            parentRef,
            views,
        });
    }

    return scenes;
}
