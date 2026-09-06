import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, it } from 'node:test';

import {
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import type { RuntimeMetadata } from '../types.js';
import {
    getView,
    getViewPayloadTemplate,
    listPageReferrers,
    listScenes,
    listViews,
    planViewRepointTool,
    snapshotApp,
    viewTools,
} from './views.js';

/**
 * A small app in the runtime-metadata shape parseRuntimeScenes and friends read:
 * `application.scenes[].views[]` carrying key/type/columns or links. scene_2 is a child
 * page of scene_1 (Knack writes the parent as a slug), owned through view_1's link column.
 */
const TABLE_VIEW = {
    key: 'view_1',
    name: 'Contacts table',
    type: 'table',
    title: 'Contacts',
    source: {
        object: 'object_1',
        criteria: { match: 'all', rules: [], groups: [] },
        sort: [{ field: 'field_1', order: 'asc' }],
        limit: '',
    },
    columns: [
        { type: 'field', field: { key: 'field_1' }, header: 'Name' },
        {
            type: 'field',
            field: { key: 'field_3' },
            connection: { key: 'field_2' },
            header: 'Company',
        },
        { type: 'link', header: 'Edit', scene: 'edit-contact' },
    ],
    links: [],
    groups: [],
    inputs: [],
    no_data_text: 'No Contact Records',
};

function makeMetadata(): RuntimeMetadata {
    return {
        application: {
            name: 'Demo',
            slug: 'demo',
            account: { slug: 'acme' },
            objects: [
                {
                    key: 'object_1',
                    name: 'Contact',
                    fields: [
                        {
                            key: 'field_1',
                            name: 'Name',
                            type: 'short_text',
                            required: true,
                        },
                        {
                            key: 'field_2',
                            name: 'Company',
                            type: 'connection',
                            relationship: {
                                object: 'object_2',
                                has: 'one',
                                belongs_to: 'many',
                            },
                        },
                    ],
                },
                {
                    key: 'object_2',
                    name: 'Company',
                    fields: [
                        {
                            key: 'field_3',
                            name: 'Company name',
                            type: 'short_text',
                        },
                    ],
                },
            ],
            scenes: [
                {
                    key: 'scene_1',
                    name: 'Contacts',
                    slug: 'contacts',
                    views: [
                        TABLE_VIEW,
                        {
                            key: 'view_2',
                            name: 'Nav',
                            type: 'menu',
                            links: [
                                {
                                    name: 'Contacts',
                                    type: 'scene',
                                    scene: 'contacts',
                                },
                            ],
                        },
                        {
                            key: 'view_3',
                            name: 'Notes',
                            type: 'rich_text',
                            content: '<p>Hi</p>',
                        },
                    ],
                },
                {
                    key: 'scene_2',
                    name: 'Edit contact',
                    slug: 'edit-contact',
                    parent: 'contacts',
                    views: [
                        {
                            key: 'view_4',
                            name: 'Edit form',
                            type: 'form',
                            source: { object: 'object_1' },
                            groups: [
                                {
                                    columns: [
                                        {
                                            inputs: [
                                                {
                                                    field: { key: 'field_1' },
                                                    read_only: true,
                                                },
                                            ],
                                        },
                                    ],
                                },
                            ],
                        },
                        {
                            key: 'view_5',
                            name: 'Contact card',
                            type: 'details',
                            source: { object: 'object_1' },
                            columns: [
                                { type: 'field', field: { key: 'field_1' } },
                            ],
                        },
                    ],
                },
                { key: 'scene_3', name: 'Reports', slug: 'reports', views: [] },
            ],
        },
    };
}

function makeCtx(overrides: Parameters<typeof makeApp>[0] = {}) {
    const app = makeApp(overrides);
    return {
        app,
        ...makeFakeContext({
            apps: [app],
            runtimeMetadata: { [app.appKey]: makeMetadata() },
        }),
    };
}

/** A context whose runtime metadata fetch fails, so every scene read comes back empty. */
function makeEmptyCtx() {
    const app = makeApp();
    return makeFakeContext({
        apps: [app],
        runtimeMetadata: { [app.appKey]: null },
    });
}

describe('knack_snapshot_app and knack_get_view_payload_template are read-access', () => {
    it('are declared read, since neither sends anything to Knack', () => {
        const byName = new Map(viewTools.map((tool) => [tool.name, tool]));
        assert.equal(byName.get('knack_snapshot_app')?.access, 'read');
        assert.equal(
            byName.get('knack_get_view_payload_template')?.access,
            'read',
        );
    });

    it('knack_snapshot_app works on an app with readonly:true and no view-mutation opt-in', async () => {
        const tmpDir = fs.mkdtempSync(
            path.join(os.tmpdir(), 'knack-mcp-v2-views-readonly-'),
        );
        try {
            const { ctx } = makeCtx({
                appFolder: tmpDir,
                readonly: true,
                allowViewMutation: false,
                allowDelete: false,
                allowDiagnostics: false,
            });
            const result = payloadOf(
                await snapshotApp.handler({ appKey: 'Demo' }, ctx),
            );
            assert.equal(result.ok, true);
        } finally {
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });

    it('knack_get_view_payload_template works the same way', async () => {
        const { ctx } = makeCtx({
            readonly: true,
            allowViewMutation: false,
            allowDelete: false,
            allowDiagnostics: false,
        });
        const result = payloadOf(
            await getViewPayloadTemplate.handler(
                { appKey: 'Demo', viewType: 'table', objectKey: 'object_1' },
                ctx,
            ),
        );
        assert.equal(result.ok, true);
    });
});

describe('knack_list_scenes', () => {
    it('lists every scene with counts, views and builder URLs on request', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await listScenes.handler(
                {
                    appKey: 'Demo',
                    includeViews: true,
                    includeBuilderUrls: true,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.sceneCount, 3);
        assert.equal(result.totalViewCount, 5);
        const scenes = result.scenes as Array<Record<string, unknown>>;
        assert.equal(scenes[0].sceneKey, 'scene_1');
        assert.equal(scenes[0].sceneSlug, 'contacts');
        assert.equal(scenes[0].viewCount, 3);
        assert.deepEqual(
            (scenes[0].views as Array<Record<string, unknown>>).map(
                (view) => view.viewKey,
            ),
            ['view_1', 'view_2', 'view_3'],
        );
        assert.match(String(scenes[0].builderUrl), /pages\/scene_1$/);
    });

    it('omits views and builder URLs by default', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await listScenes.handler(
                {
                    appKey: 'Demo',
                    includeViews: false,
                    includeBuilderUrls: false,
                },
                ctx,
            ),
        );
        const scenes = result.scenes as Array<Record<string, unknown>>;
        assert.equal('views' in scenes[0], false);
        assert.equal('builderUrl' in scenes[0], false);
    });

    it('reports no scene data when the metadata cannot be read', async () => {
        const { ctx } = makeEmptyCtx();
        const result = payloadOf(
            await listScenes.handler(
                {
                    appKey: 'Demo',
                    includeViews: false,
                    includeBuilderUrls: false,
                },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.match(String(result.message), /No scene data available/);
    });
});

describe('knack_list_views', () => {
    it('lists views with scene context and a type summary', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await listViews.handler(
                { appKey: 'Demo', maxResults: 100, includeBuilderUrls: true },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.totalViews, 5);
        const views = result.views as Array<Record<string, unknown>>;
        assert.deepEqual(views[0], {
            viewKey: 'view_1',
            viewName: 'Contacts table',
            viewType: 'table',
            sceneKey: 'scene_1',
            sceneName: 'Contacts',
            sceneSlug: 'contacts',
            builderUrl: views[0].builderUrl,
        });
        assert.match(
            String(views[0].builderUrl),
            /pages\/scene_1\/views\/view_1\/table$/,
        );
        assert.deepEqual(result.viewTypeSummary, [
            { type: 'table', count: 1 },
            { type: 'menu', count: 1 },
            { type: 'rich_text', count: 1 },
            { type: 'form', count: 1 },
            { type: 'details', count: 1 },
        ]);
    });

    it('filters by scene and by type, case-insensitively, and honours maxResults', async () => {
        const { ctx } = makeCtx();
        const byScene = payloadOf(
            await listViews.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'SCENE_2',
                    maxResults: 100,
                    includeBuilderUrls: false,
                },
                ctx,
            ),
        );
        assert.equal(byScene.totalViews, 2);
        assert.deepEqual(byScene.filters, {
            sceneKey: 'SCENE_2',
            viewType: null,
        });

        const byType = payloadOf(
            await listViews.handler(
                {
                    appKey: 'Demo',
                    viewType: 'Menu',
                    maxResults: 100,
                    includeBuilderUrls: false,
                },
                ctx,
            ),
        );
        assert.equal(byType.totalViews, 1);
        assert.equal(
            (byType.views as Array<Record<string, unknown>>)[0].viewKey,
            'view_2',
        );

        const capped = payloadOf(
            await listViews.handler(
                { appKey: 'Demo', maxResults: 2, includeBuilderUrls: false },
                ctx,
            ),
        );
        assert.equal(capped.totalViews, 2);
    });

    it('reports no scene data when the metadata cannot be read', async () => {
        const { ctx } = makeEmptyCtx();
        const result = payloadOf(
            await listViews.handler(
                { appKey: 'Demo', maxResults: 100, includeBuilderUrls: false },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.match(String(result.message), /No scene data available/);
    });
});

describe('knack_get_view', () => {
    it('detail context returns the scene context and builder URLs', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_4',
                    detail: 'context',
                    includeRaw: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.viewKey, 'view_4');
        assert.deepEqual(result.context, {
            sceneKey: 'scene_2',
            sceneName: 'Edit contact',
            sceneSlug: 'edit-contact',
        });
        const urls = result.builderUrls as Record<string, unknown>;
        assert.match(String(urls.view), /pages\/scene_2\/views\/view_4\/form$/);
    });

    it('detail context reports an unknown view with the available count', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_99',
                    detail: 'context',
                    includeRaw: false,
                },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.equal(result.availableViewKeyCount, 5);
        assert.match(
            String(result.message),
            /View context not found for view key: view_99/,
        );
    });

    it('detail fields returns the configured field settings with requiredness from the schema', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_4',
                    detail: 'fields',
                    includeRaw: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.source, 'runtime');
        assert.equal(result.schemaSource, 'runtime');
        assert.equal(result.viewName, 'Edit form');
        assert.equal(result.viewType, 'form');
        const settings = result.fieldSettings as Record<string, unknown>;
        assert.equal(settings.configuredFieldCount, 1);
        assert.equal(settings.requiredFieldCount, 1);
        assert.equal(settings.readOnlyFieldCount, 1);
        const fields = settings.fields as Array<Record<string, unknown>>;
        assert.equal(fields[0].fieldKey, 'field_1');
        assert.equal(fields[0].layout, 'form-input');
        assert.equal('attributes' in result, false);
    });

    it('detail fields reports an unknown view with a key sample', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_99',
                    detail: 'fields',
                    includeRaw: false,
                },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.equal(result.availableViewKeyCount, 5);
        assert.deepEqual(result.availableViewKeySample, [
            'view_1',
            'view_2',
            'view_3',
            'view_4',
            'view_5',
        ]);
    });

    it('detail attributes returns fieldSettings only by default, with the includeRaw note', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    detail: 'attributes',
                    includeRaw: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal('attributes' in result, false);
        assert.match(String(result.note), /includeRaw: true/);
        assert.equal(
            (result.fieldSettings as Record<string, unknown>)
                .configuredFieldCount,
            2,
        );
        const urls = result.builderUrls as Record<string, unknown>;
        assert.match(String(urls.view), /scene_1\/views\/view_1\/table$/);
    });

    it('detail attributes with includeRaw inlines the raw view through getInlineDetail', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    detail: 'attributes',
                    includeRaw: true,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.attributesIncluded, true);
        assert.equal(typeof result.attributesSizeBytes, 'number');
        assert.deepEqual(result.attributes, TABLE_VIEW);
        assert.equal(result.attributeSummary, undefined);
        assert.equal('note' in result, false);
    });

    it('detail attributes is refused in the handler when the app disallows diagnostics', async () => {
        const { ctx } = makeCtx({ allowDiagnostics: false });
        await assert.rejects(
            getView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    detail: 'attributes',
                    includeRaw: false,
                },
                ctx,
            ),
            /does not allow diagnostic tools/,
        );
        // The same view is still readable at the read-level details.
        const fields = payloadOf(
            await getView.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    detail: 'fields',
                    includeRaw: false,
                },
                ctx,
            ),
        );
        assert.equal(fields.ok, true);
    });
});

describe('knack_plan_view_repoint', () => {
    it('splits a view into scope, display, navigation and other references', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await planViewRepointTool.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    includeScopedFields: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.viewType, 'table');
        assert.equal(result.sourceObject, 'object_1');
        assert.deepEqual(result.distinctDisplayKeys, ['field_2']);
        assert.equal((result.displayConnections as unknown[]).length, 1);
        assert.equal(result.scopedFields, undefined);
        assert.equal(typeof result.scopedFieldCount, 'number');
        const notes = result.notes as string[];
        assert.match(
            notes[2],
            /RETARGET \(change source\.object, currently object_1\)/,
        );
        assert.ok(
            notes.some((note) =>
                /DISPLAY connection\(s\), field\(s\) field_2/.test(note),
            ),
        );
    });

    it('includes scoped fields on request', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await planViewRepointTool.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_1',
                    includeScopedFields: true,
                },
                ctx,
            ),
        );
        assert.ok(Array.isArray(result.scopedFields));
    });

    it('reports an unknown view', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await planViewRepointTool.handler(
                {
                    appKey: 'Demo',
                    viewKey: 'view_99',
                    includeScopedFields: false,
                },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.match(
            String(result.message),
            /View not found in view metadata: view_99/,
        );
    });
});

describe('knack_get_view_payload_template (build from type)', () => {
    const base = {
        appKey: 'Demo',
        maxFields: 12,
        includeSourceGuidance: false,
    };

    it('builds a table from the schema and derives the page layout from the scene', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getViewPayloadTemplate.handler(
                {
                    ...base,
                    viewType: 'grid',
                    objectKey: 'object_1',
                    sceneKey: 'scene_1',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.action, 'view_payload_template');
        assert.equal(result.requestedViewType, 'grid');
        assert.equal(result.canonicalViewType, 'table');
        assert.equal(result.derivedFromSchema, true);
        assert.equal(result.schemaSource, 'runtime');
        assert.equal(result.layoutDerivedFromScene, true);
        assert.deepEqual(result.existingViewKeysUsed, [
            'view_1',
            'view_2',
            'view_3',
        ]);
        assert.deepEqual(result.fieldKeysUsed, ['field_1', 'field_2']);
        assert.equal(result.payloadIncluded, true);
        assert.match(
            String(result.viewSourceShapeNote),
            /includeSourceGuidance: true/,
        );
        assert.equal('viewSourceShape' in result, false);

        const payload = result.payload as Record<string, unknown>;
        assert.equal(payload.type, 'table');
        assert.equal(payload.name, 'Grid');
        assert.equal(payload.no_data_text, 'No Contact Records');
        assert.deepEqual(
            (payload.source as Record<string, unknown>).object,
            'object_1',
        );
        const pageGroups = payload.pageGroups as Array<Record<string, unknown>>;
        assert.equal(pageGroups.length, 4);
        assert.deepEqual(pageGroups[3], {
            columns: [{ keys: ['new'], width: 100 }],
        });
        const columns = payload.columns as Array<Record<string, unknown>>;
        assert.equal(columns[0].header, 'Name');

        const notes = result.notes as string[];
        assert.ok(
            notes.some((note) =>
                /Derived 3 existing view key\(s\) from scene scene_1/.test(
                    note,
                ),
            ),
        );
        assert.ok(notes.includes('Knack stores grid views as type `table`.'));
    });

    it('applies columnConnections, scoping, filters and the guidance opt-in', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getViewPayloadTemplate.handler(
                {
                    ...base,
                    includeSourceGuidance: true,
                    viewType: 'table',
                    objectKey: 'object_1',
                    fieldKeys: ['field_1', 'field_3'],
                    columnConnections: JSON.stringify({
                        field_3: 'field_2',
                        field_9: 'field_2',
                    }),
                    connectionKey: 'field_2',
                    relationshipType: 'foreign',
                    filters: JSON.stringify({
                        match: 'all',
                        rules: [
                            { field: 'field_1', operator: 'is', value: 'x' },
                        ],
                        groups: [
                            [{ field: 'field_1', operator: 'is', value: 'y' }],
                        ],
                    }),
                    sort: JSON.stringify([{ field: 'field_1', order: 'desc' }]),
                    noDataText: 'Nothing here',
                    existingViewKeys: ['view_1'],
                    sceneKey: 'scene_1',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.layoutDerivedFromScene, false);
        assert.ok('viewSourceShape' in result);
        const payload = result.payload as Record<string, unknown>;
        const columns = payload.columns as Array<Record<string, unknown>>;
        assert.deepEqual(columns[1].connection, { key: 'field_2' });
        const source = payload.source as Record<string, unknown>;
        assert.equal(source.connection_key, 'field_2');
        assert.equal(source.relationship_type, 'foreign');
        assert.deepEqual(source.sort, [{ field: 'field_1', order: 'desc' }]);
        assert.equal(payload.no_data_text, 'Nothing here');

        const notes = result.notes as string[];
        assert.ok(
            notes.some((note) =>
                /1 of 2 column\(s\) will reach through a connection/.test(note),
            ),
        );
        assert.ok(
            notes.some((note) =>
                /columnConnections named 1 field\(s\).*\(field_9\)/.test(note),
            ),
        );
        assert.ok(
            notes.some((note) => /Filter carries 1 group\(s\)/.test(note)),
        );
        assert.ok(
            notes.some((note) =>
                /existingViewKeys omits 2 view\(s\)/.test(note),
            ),
        );
        assert.ok(
            notes.some((note) => /Source is scoped through field_2/.test(note)),
        );
    });

    it('refuses a template without objectKey, or viewType, or a half-specified hop', async () => {
        const { ctx } = makeCtx();
        await assert.rejects(
            getViewPayloadTemplate.handler({ ...base, viewType: 'form' }, ctx),
            /objectKey is required/,
        );
        await assert.rejects(
            getViewPayloadTemplate.handler(
                { ...base, objectKey: 'object_1' },
                ctx,
            ),
            /viewType is required unless fromViewKey is given/,
        );
        await assert.rejects(
            getViewPayloadTemplate.handler(
                {
                    ...base,
                    viewType: 'form',
                    objectKey: 'object_1',
                    parentSourceObject: 'object_2',
                },
                ctx,
            ),
            /must be passed together/,
        );
    });

    it('validates columnConnections shape and refuses it on a form', async () => {
        const { ctx } = makeCtx();
        await assert.rejects(
            getViewPayloadTemplate.handler(
                {
                    ...base,
                    viewType: 'table',
                    objectKey: 'object_1',
                    columnConnections: '[]',
                },
                ctx,
            ),
            /must be a JSON object mapping/,
        );
        await assert.rejects(
            getViewPayloadTemplate.handler(
                {
                    ...base,
                    viewType: 'table',
                    objectKey: 'object_1',
                    columnConnections: JSON.stringify({ field_3: 'Contact' }),
                },
                ctx,
            ),
            /must be a connection field key like "field_3"/,
        );
        await assert.rejects(
            getViewPayloadTemplate.handler(
                {
                    ...base,
                    viewType: 'form',
                    objectKey: 'object_1',
                    columnConnections: JSON.stringify({ field_1: 'field_2' }),
                },
                ctx,
            ),
            /does not apply to a form template/,
        );
    });
});

describe('knack_get_view_payload_template (clone from view)', () => {
    const base = {
        appKey: 'Demo',
        maxFields: 12,
        includeSourceGuidance: false,
    };

    it('clones a view with identifiers stripped and pageGroups rebuilt from the source page', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getViewPayloadTemplate.handler(
                { ...base, fromViewKey: 'view_1' },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.action, 'view_payload_template_from_view');
        assert.equal(result.sourceViewKey, 'view_1');
        assert.equal(result.sourceViewType, 'table');
        assert.equal(result.targetViewType, 'table');
        assert.equal(result.requestedTargetViewType, null);
        assert.equal(result.sourceSceneKey, 'scene_1');
        assert.equal(result.targetSceneKey, 'scene_1');
        assert.deepEqual(result.existingViewKeysUsed, [
            'view_1',
            'view_2',
            'view_3',
        ]);

        const payload = result.payload as Record<string, unknown>;
        assert.equal('key' in payload, false);
        assert.equal('_id' in payload, false);
        assert.equal(payload.name, 'Contacts table Copy');
        assert.equal(payload.title, 'Contacts');
        assert.equal(payload.no_data_text, 'No Contact Records');
        assert.deepEqual(payload.columns, TABLE_VIEW.columns);
        assert.equal((payload.pageGroups as unknown[]).length, 4);

        const notes = result.notes as string[];
        assert.equal(
            notes[0],
            'The payload was cloned from existing view metadata with key/_id removed.',
        );
        assert.equal(
            notes[1],
            'The cloned view type was preserved from the source view.',
        );
        assert.match(notes[2], /rebuilt using 3 existing view key\(s\)/);
    });

    it('converts details to list, deriving no_data_text, onto a chosen target page', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getViewPayloadTemplate.handler(
                {
                    ...base,
                    fromViewKey: 'view_5',
                    viewType: 'list',
                    sceneKey: 'scene_3',
                    name: 'Cards',
                    title: 'All cards',
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.targetViewType, 'list');
        assert.equal(result.targetSceneKey, 'scene_3');
        assert.deepEqual(result.existingViewKeysUsed, []);
        const payload = result.payload as Record<string, unknown>;
        assert.equal(payload.type, 'list');
        assert.equal(payload.name, 'Cards');
        assert.equal(payload.title, 'All cards');
        assert.equal(payload.no_data_text, 'No Contact Records');
        assert.equal('pageGroups' in payload, false);
        const notes = result.notes as string[];
        assert.ok(
            notes.some((note) =>
                /source view carried no no_data_text/.test(note),
            ),
        );
        assert.ok(
            notes.some((note) =>
                /No pageGroups were derived automatically/.test(note),
            ),
        );
    });

    it('refuses a conversion other than details/list', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getViewPayloadTemplate.handler(
                { ...base, fromViewKey: 'view_1', viewType: 'form' },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.equal(result.sourceViewType, 'table');
        assert.equal(result.requestedTargetViewType, 'form');
        assert.match(
            String(result.message),
            /only supports details\/list conversion/,
        );
    });

    it('reports an unknown source view', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await getViewPayloadTemplate.handler(
                { ...base, fromViewKey: 'view_99' },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.match(
            String(result.message),
            /View not found in view metadata: view_99/,
        );
    });
});

describe('knack_snapshot_app', () => {
    let tmpDir: string;

    before(() => {
        tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'knack-mcp-v2-views-'));
    });

    after(() => {
        fs.rmSync(tmpDir, { recursive: true, force: true });
    });

    it('writes a snapshot carrying the scene tree and the named view', async () => {
        const { ctx } = makeCtx({ appFolder: tmpDir });
        const result = payloadOf(
            await snapshotApp.handler(
                { appKey: 'Demo', sceneKey: 'scene_1', viewKey: 'view_1' },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.action, 'snapshot_app');
        assert.equal(result.viewIncluded, true);
        const snapshotPath = String(result.snapshotPath);
        assert.ok(
            snapshotPath.startsWith(path.join(tmpDir, 'schema', 'snapshots')),
        );
        assert.match(path.basename(snapshotPath), /-manual-view_1-\d+\.json$/);

        const written = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));
        assert.equal(written.action, 'manual');
        assert.equal(written.viewKey, 'view_1');
        assert.equal(written.view.key, 'view_1');
        assert.equal(written.scenes.length, 3);
        assert.equal(
            written.schemaPath,
            path.join(tmpDir, 'schema', 'schema.json'),
        );
    });

    it('fetches runtime metadata only once when a view is named', async () => {
        const { ctx, runtimeMetadataFetches } = makeCtx({
            appFolder: tmpDir,
        });
        const result = payloadOf(
            await snapshotApp.handler(
                { appKey: 'Demo', sceneKey: 'scene_1', viewKey: 'view_1' },
                ctx,
            ),
        );
        assert.equal(result.ok, true);
        assert.deepEqual(runtimeMetadataFetches, ['Demo']);
    });

    it('writes a scenes-only snapshot when no view is named', async () => {
        const { ctx } = makeCtx({ appFolder: tmpDir });
        const result = payloadOf(
            await snapshotApp.handler({ appKey: 'Demo' }, ctx),
        );
        assert.equal(result.ok, true);
        assert.equal(result.viewIncluded, false);
        assert.match(
            path.basename(String(result.snapshotPath)),
            /-manual-app-\d+\.json$/,
        );
    });

    it('refuses viewKey without sceneKey before touching the disk', async () => {
        const { ctx } = makeCtx({ appFolder: tmpDir });
        const result = payloadOf(
            await snapshotApp.handler(
                { appKey: 'Demo', viewKey: 'view_1' },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.equal(result.error, 'INVALID_INPUT');
    });

    it('refuses when the named view is not in the scene', async () => {
        const { ctx } = makeCtx({ appFolder: tmpDir });
        const result = payloadOf(
            await snapshotApp.handler(
                { appKey: 'Demo', sceneKey: 'scene_3', viewKey: 'view_1' },
                ctx,
            ),
        );
        assert.equal(result.ok, false);
        assert.equal(result.error, 'COULD_NOT_VERIFY_VIEW');
        assert.match(String(result.message), /view_1 was not found in scene_3/);
    });

    it('refuses when the metadata cannot be fetched', async () => {
        const app = makeApp({ appFolder: tmpDir });
        const { ctx } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { Demo: null },
        });
        const withView = payloadOf(
            await snapshotApp.handler(
                { appKey: 'Demo', sceneKey: 'scene_1', viewKey: 'view_1' },
                ctx,
            ),
        );
        assert.equal(withView.error, 'COULD_NOT_VERIFY_VIEW');

        const scenesOnly = payloadOf(
            await snapshotApp.handler({ appKey: 'Demo' }, ctx),
        );
        assert.equal(scenesOnly.ok, false);
        assert.equal(scenesOnly.error, 'SNAPSHOT_FAILED');
    });
});

describe('knack_list_page_referrers', () => {
    /** The fixture's scene_2 hangs off scene_1 and only view_1 links to it. */
    it('says a sole referrer means removal destroys the page', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await listPageReferrers.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_2',
                    includeDescendants: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        const page = result.page as Record<string, unknown>;
        assert.equal(page.referrerCount, 1);
        assert.deepEqual(page.referrers, [
            { sceneKey: 'scene_1', viewKey: 'view_1' },
        ]);
        assert.match(String(page.consequence), /DESTROYS/);
    });

    it('will not guess where a page with two referrers would land', async () => {
        // The case the operator asked about. A transfer has only ever been measured
        // with one referrer left, so naming a winner here would be invention.
        const metadata = makeMetadata();
        const scenes = (
            metadata.application as { scenes: Record<string, unknown>[] }
        ).scenes;
        (scenes[2].views as unknown[]) = [
            {
                key: 'view_9',
                name: 'Second route',
                type: 'table',
                columns: [
                    { type: 'link', header: 'Edit', scene: 'edit-contact' },
                ],
            },
        ];
        const app = makeApp();
        const { ctx } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { [app.appKey]: metadata },
        });

        const result = payloadOf(
            await listPageReferrers.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_2',
                    includeDescendants: false,
                },
                ctx,
            ),
        );

        const page = result.page as Record<string, unknown>;
        assert.equal(page.referrerCount, 2);
        assert.match(String(page.consequence), /has not been measured/);
        assert.doesNotMatch(String(page.consequence), /DESTROYS/);
        // And it says how to make the destination certain rather than leaving it there.
        assert.match(
            String(page.consequence),
            /remove the links you do not want/i,
        );
    });

    it('refuses rather than reporting "nobody links here" when links are unreadable', async () => {
        // A scene list with no per-scene view links cannot answer the question. An
        // empty referrer set would say every page dies on its next link removal.
        const metadata = makeMetadata();
        for (const scene of (
            metadata.application as { scenes: Record<string, unknown>[] }
        ).scenes) {
            delete scene.views;
        }
        const app = makeApp();
        const { ctx } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { [app.appKey]: metadata },
        });

        const result = payloadOf(
            await listPageReferrers.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_2',
                    includeDescendants: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'REFERRERS_UNAVAILABLE');
        assert.match(String(result.message), /not an answer of "nobody"/);
    });

    it('names a missing page as missing, and says why a snapshot key may not exist', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await listPageReferrers.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_404',
                    includeDescendants: false,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'SCENE_NOT_FOUND');
        assert.match(String(result.message), /new key/);
    });

    it('reports descendants with their own referrers when asked', async () => {
        const { ctx } = makeCtx();
        const result = payloadOf(
            await listPageReferrers.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    includeDescendants: true,
                },
                ctx,
            ),
        );

        const descendants = result.descendants as Record<string, unknown>[];
        assert.deepEqual(
            descendants.map((page) => page.sceneKey),
            ['scene_2'],
        );
        assert.equal(descendants[0].referrerCount, 1);
    });

    it('is advertised in the view tool set', () => {
        assert.ok(
            viewTools.some((tool) => tool.name === 'knack_list_page_referrers'),
        );
    });
});
