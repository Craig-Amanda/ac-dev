export type CachedField = {
    key: string;
    name?: string;
    type?: string;
    required?: boolean;
    description?: string;
    connectedObject?: string;
    choiceOptions?: string[];
    allowsMultiple?: boolean;
};

export type CachedObject = {
    key: string;
    name?: string;
    fields?: CachedField[];
};

export type CachedSchema = {
    objects?: CachedObject[];
};

export type CachedFieldMapEntry = {
    fieldKey: string;
    fieldType?: string | null;
};

export type CachedFieldMap = Record<string, CachedFieldMapEntry>;

export type CachedViewMap = Record<string, Record<string, unknown>>;

export type ViewFieldSettings = {
    fieldKey: string;
    fieldType?: string;
    label?: string;
    objectRequired?: boolean;
    readOnly?: boolean;
    defaults?: Record<string, unknown>;
    rules?: unknown[];
    layout: 'form-input' | 'search-field' | 'view-column';
    sourcePath: string;
};

export type ViewFieldSettingsSummary = {
    configuredFieldCount: number;
    requiredFieldCount: number;
    readOnlyFieldCount: number;
    fields: ViewFieldSettings[];
    viewRules?: unknown;
};

export type ViewContextMap = Record<
    string,
    { sceneKey?: string; sceneName?: string; sceneSlug?: string }
>;

export type SceneViewInfo = {
    viewKey: string;
    viewName: string | undefined;
    viewType: string | undefined;
};

export type SceneInfo = {
    sceneKey: string;
    sceneName: string | undefined;
    sceneSlug: string | undefined;

    /**
     * The scene this one hangs off, when it is a child page. Required to work out
     * which pages a cascade delete takes with it — a doomed child page may own
     * children of its own.
     *
     * Knack writes a **slug** here, not a `scene_N` key, so it must be resolved
     * through the slug index rather than compared against `sceneKey`.
     */

    parentRef: string | undefined;
    views: SceneViewInfo[];
};

export type FieldReference = {
    fieldKey: string;
    sourceType: 'schema' | 'fieldMap' | 'viewMap';
    matchType: 'definition' | 'value' | 'propertyKey' | 'alias';
    path: string;
    classification: string[];
    containingText?: string | null;
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
};

export type CachedFieldReferenceIndex = Record<string, FieldReference[]>;

export type CacheSource = 'runtime' | 'file';

export type CacheEntry<T> = {
    value: T;
    source: CacheSource;
    loadedAt: number;
    expiresAt: number;
};

export type RuntimeMetadata = Record<string, unknown>;

export type TemplateFieldDescriptor = {
    key: string;
    name: string;
    type: string;
    /**
     * The connection field this column reaches through, when the column shows a
     * field belonging to a connected record rather than to the view's own object.
     * Emitted as `connection: { key }` beside the column's own `field: { key }`.
     *
     * Measured 2026-09-04 from two builder copy requests: six of fourteen columns
     * in one table carried it, all through the same connection. It is the
     * reference a repoint most often misses, because changing the view's source
     * leaves these untouched and the columns keep rendering values from the old
     * relationship.
     */
    connectionKey?: string;
};

export const NON_FORM_FIELD_TYPES = new Set([
    'auto_increment',
    'sum',
    'count',
    'average',
    'min',
    'max',
    'equation',
    'concatenation',
]);
