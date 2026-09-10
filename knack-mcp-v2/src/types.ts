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
    /** Key of the object's display field (Knack's `identifier`), when the metadata names one. */
    identifier?: string;
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
    /**
     * Who a `login` view lets through, copied from the raw view when it carries them.
     * Measured 7 September (TESTING.md Tier 7): the roles live on the login view and
     * nowhere else — not on the scene holding it, and not on any page beneath it.
     * Absent on every other view type, so a rebuilt page can carry them back and a
     * snapshot without them is a snapshot of an app that had none.
     */
    allowedProfiles?: string[];
    limitProfileAccess?: boolean;
    registrationType?: string;
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
    /**
     * Knack's `type` — `page`, `user`, `authentication`, or absent on child pages.
     * `authentication` is the scene Knack inserts above a page when a login is added
     * to it; it is what the upward walk in lib/page-access.ts looks for.
     */
    sceneType?: string;
    /**
     * Knack's `authenticated`, kept verbatim and **not** to be read as "is public".
     * Measured 7 September: a page directly under a login scene carried
     * `authenticated: false`, the same as a genuinely public one. Protection is a
     * property of ancestry, which is why it is resolved by a walk, not read here.
     */
    authenticated?: boolean;
    /** Seen on `type: "user"` scenes; kept so a snapshot preserves them. */
    allowedProfiles?: string[];
    limitProfileAccess?: boolean;
    /**
     * The view keys the page's layout names, in layout order — Knack's `scene.groups`,
     * flattened.
     *
     * `views` is what exists on a page; this is what **renders**. Measured 10 September
     * (TESTING.md Tier 8): with a populated layout Knack renders only the keys it
     * names, and a view absent from it exists, opens by its builder URL, and appears
     * on neither the front end nor the back end. An **empty array** is the ordinary
     * state of a page built in the builder and means the opposite — every view renders.
     *
     * Optional deliberately, and absent is not empty: a caller that cannot supply the
     * layout must not be read as "this page renders everything", which is exactly the
     * wrong way round. Read it through `unrenderedViewKeys`, which returns null rather
     * than guessing.
     */
    layoutViewKeys?: string[];
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
