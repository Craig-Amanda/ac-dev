import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import type { RuntimeMetadata } from '../types.js';
import {
    analysisTools,
    analyzeDataModel,
    appDeepDive,
    generateSeedCsvs,
    getAppOverview,
    getContextBundle,
    listFieldReferences,
    searchEmails,
    searchKtlKeywords,
} from './analysis.js';

/**
 * One realistic runtime payload shared by every test: two objects joined by a
 * connection, two scenes, a table view whose title and description carry KTL keywords,
 * and a form whose record rule and email rule reference fields.
 */
const RUNTIME_METADATA: RuntimeMetadata = {
    application: { name: 'Demo', slug: 'demo', account: { slug: 'acct' } },
    objects: [
        {
            key: 'object_1',
            name: 'Companies',
            fields: [
                {
                    key: 'field_1',
                    name: 'Company Name',
                    type: 'short_text',
                    required: true,
                },
                {
                    key: 'field_2',
                    name: 'Status',
                    type: 'multiple_choice',
                    format: { options: ['Active', 'Inactive'] },
                },
            ],
        },
        {
            key: 'object_2',
            name: 'Contacts',
            fields: [
                {
                    key: 'field_3',
                    name: 'Full Name',
                    type: 'short_text',
                    required: true,
                },
                {
                    key: 'field_4',
                    name: 'Company',
                    type: 'connection',
                    relationship: {
                        object: 'object_1',
                        has: 'one',
                        belongs_to: 'many',
                    },
                },
                { key: 'field_5', name: 'Email', type: 'email' },
            ],
        },
    ],
    scenes: [
        {
            key: 'scene_1',
            name: 'Home',
            slug: 'home',
            views: [
                {
                    key: 'view_1',
                    name: 'Companies Table',
                    type: 'table',
                    title: 'Companies _ktl_hide',
                    description: 'Uses the _ktl_ token and (_ktl_hide) again',
                    source: { object: 'object_1' },
                    columns: [
                        { field: { key: 'field_1' } },
                        { field: { key: 'field_2' } },
                    ],
                },
                {
                    key: 'view_2',
                    name: 'Add Contact',
                    type: 'form',
                    source: { object: 'object_2' },
                    groups: [
                        {
                            columns: [
                                {
                                    inputs: [
                                        {
                                            field: { key: 'field_3' },
                                            label: 'Full Name',
                                        },
                                        {
                                            field: { key: 'field_4' },
                                            label: 'Company',
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                    rules: {
                        records: [
                            {
                                action: 'record',
                                values: [
                                    { field: 'field_4', type: 'connection' },
                                ],
                            },
                        ],
                        emails: [
                            {
                                action: 'email',
                                to: 'ops@example.com',
                                subject: 'New contact created',
                                message: 'A contact was added for field_3.',
                            },
                        ],
                    },
                },
            ],
        },
        {
            key: 'scene_2',
            name: 'Contacts',
            slug: 'contacts',
            parent: 'home',
            views: [
                {
                    key: 'view_3',
                    name: 'Contacts Table',
                    type: 'table',
                    source: { object: 'object_2' },
                    columns: [{ field: { key: 'field_3' } }],
                },
            ],
        },
    ],
};

function warmContext(
    extra: Parameters<typeof makeFakeContext>[0] = {},
): ReturnType<typeof makeFakeContext> {
    return makeFakeContext({
        runtimeMetadata: { Demo: RUNTIME_METADATA },
        ...extra,
    });
}

function coldContext(): ReturnType<typeof makeFakeContext> {
    return makeFakeContext({ runtimeMetadata: { Demo: null } });
}

describe('analysisTools catalogue', () => {
    it('lists the eight tools in order, all read-only', () => {
        assert.deepEqual(
            analysisTools.map((tool) => tool.name),
            [
                'knack_get_context_bundle',
                'knack_get_app_overview',
                'knack_analyze_data_model',
                'knack_app_deep_dive',
                'knack_list_field_references',
                'knack_search_ktl_keywords',
                'knack_search_emails',
                'knack_generate_seed_csvs',
            ],
        );
        assert.ok(analysisTools.every((tool) => tool.access === 'read'));
    });
});

describe('knack_get_context_bundle', () => {
    it('refuses an unbounded request', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await getContextBundle.handler(
                { appKey: 'Demo', includeViewAttributes: false },
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(
            String(payload.message),
            /^Provide at least one objectKey, fieldAlias, or viewKey\./,
        );
    });

    it('bundles objects, aliases and views with sources', async () => {
        const { ctx, requests } = warmContext();
        const payload = payloadOf(
            await getContextBundle.handler(
                {
                    appKey: 'Demo',
                    objectKeys: ['object_2', 'object_9', 'object_2'],
                    fieldAliases: [
                        'object_2.field_4',
                        'object_1.company_name',
                        'object_1.field_999',
                        'object_9.field_1',
                        'field_1',
                    ],
                    viewKeys: ['view_2', 'view_404'],
                    includeViewAttributes: true,
                },
                ctx,
            ),
        );

        assert.equal(payload.ok, true);
        assert.equal(requests.length, 0);
        assert.deepEqual(payload.requested, {
            objectKeys: ['object_2', 'object_9'],
            fieldAliases: [
                'object_2.field_4',
                'object_1.company_name',
                'object_1.field_999',
                'object_9.field_1',
                'field_1',
            ],
            viewKeys: ['view_2', 'view_404'],
            includeViewAttributes: true,
        });
        assert.deepEqual(payload.sources, {
            schema: 'runtime',
            fieldMap: 'runtime',
            viewMap: 'runtime',
            viewContext: 'runtime',
        });

        const objects = payload.objects as Array<Record<string, unknown>>;
        assert.equal(objects.length, 2);
        assert.equal(objects[0].found, true);
        assert.equal(objects[0].key, 'object_2');
        const fields = objects[0].fields as Array<Record<string, unknown>>;
        assert.equal(fields.length, 3);
        assert.equal(fields[1].connectedObject, 'object_1');
        assert.match(
            String(fields[1].builderUrl),
            /objects\/object_2\/fields\/field_4\/settings$/,
        );
        assert.deepEqual(objects[1], { found: false, key: 'object_9' });

        const aliases = payload.aliases as Array<Record<string, unknown>>;
        assert.deepEqual(aliases[0], {
            found: true,
            alias: 'object_2.field_4',
            fieldKey: 'field_4',
            fieldType: 'connection',
        });
        assert.deepEqual(aliases[1], {
            found: true,
            alias: 'object_1.company_name',
            fieldKey: 'field_1',
            fieldType: 'short_text',
        });
        assert.equal(aliases[2].found, false);
        assert.equal(
            aliases[2].message,
            'field_999 was not found on object_1.',
        );
        assert.equal(aliases[3].found, false);
        assert.match(
            String(aliases[3].message),
            /^object_9 was not found in the cached schema/,
        );
        assert.equal(aliases[4].found, false);
        assert.match(String(aliases[4].message), /^Alias not found\./);

        const views = payload.views as Array<Record<string, unknown>>;
        assert.equal(views[0].found, true);
        assert.equal(views[0].viewKey, 'view_2');
        assert.equal(views[0].sceneKey, 'scene_1');
        assert.equal(views[0].sceneSlug, 'home');
        assert.equal(views[0].viewName, 'Add Contact');
        assert.equal(views[0].viewType, 'form');
        assert.match(
            String(views[0].builderUrl),
            /pages\/scene_1\/views\/view_2\/form$/,
        );
        assert.equal(views[0].attributesIncluded, true);
        assert.equal(
            (views[0].attributes as { name: string }).name,
            'Add Contact',
        );
        assert.equal(typeof views[0].attributesSizeBytes, 'number');
        const settings = views[0].fieldSettings as {
            configuredFieldCount: number;
        };
        assert.equal(settings.configuredFieldCount, 2);

        assert.equal(views[1].found, false);
        assert.equal(views[1].viewKey, 'view_404');
        assert.equal(views[1].builderUrl, null);
        assert.equal(views[1].fieldSettings, null);
        assert.equal(views[1].attributesIncluded, false);
    });

    it('leaves attributes out unless asked and skips unneeded loads', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await getContextBundle.handler(
                {
                    appKey: 'Demo',
                    fieldAliases: ['object_1.status'],
                    includeViewAttributes: false,
                },
                ctx,
            ),
        );
        assert.deepEqual(payload.sources, {
            schema: null,
            fieldMap: 'runtime',
            viewMap: null,
            viewContext: null,
        });
        assert.deepEqual(payload.objects, []);
        assert.deepEqual(payload.views, []);
        assert.equal(
            (payload.aliases as Array<{ fieldKey: string }>)[0].fieldKey,
            'field_2',
        );
    });
});

describe('knack_get_app_overview', () => {
    it('summarises objects and relationships', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await getAppOverview.handler(
                { appKey: 'Demo', includeFieldDetails: false },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.source, 'runtime');
        assert.equal(payload.objectCount, 2);
        assert.equal(payload.totalFields, 5);
        assert.equal(payload.relationshipCount, 1);
        const relationships = payload.relationships as Array<
            Record<string, unknown>
        >;
        assert.equal(relationships[0].fromObjectKey, 'object_2');
        assert.equal(relationships[0].fieldKey, 'field_4');
        assert.equal(relationships[0].toObjectKey, 'object_1');
        assert.equal((payload.objects as unknown[]).length, 2);
    });

    it('reports a missing schema', async () => {
        const { ctx } = coldContext();
        const payload = payloadOf(
            await getAppOverview.handler(
                { appKey: 'Demo', includeFieldDetails: false },
                ctx,
            ),
        );
        assert.deepEqual(payload, {
            ok: false,
            appKey: 'Demo',
            message: 'No schema available from runtime API or schema.json.',
        });
    });
});

describe('knack_analyze_data_model', () => {
    it('returns the analysis with its source', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await analyzeDataModel.handler({ appKey: 'Demo' }, ctx),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.appKey, 'Demo');
        assert.equal(payload.source, 'runtime');
        assert.ok(payload.summary);
        assert.ok(Array.isArray(payload.fieldTypeDistribution));
        assert.ok(Array.isArray(payload.observations));
        assert.deepEqual(payload.isolatedObjects, []);
    });

    it('reports a missing schema with the cache hint', async () => {
        const { ctx } = coldContext();
        const payload = payloadOf(
            await analyzeDataModel.handler({ appKey: 'Demo' }, ctx),
        );
        assert.equal(payload.ok, false);
        assert.match(
            String(payload.message),
            /^No schema available\. Run knack_cache with refresh: true and warm: true/,
        );
    });
});

describe('knack_app_deep_dive', () => {
    it('combines the data model with a UI summary', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await appDeepDive.handler(
                {
                    appKey: 'Demo',
                    includeFieldDetails: false,
                    includeScenes: false,
                    maxRelationshipsListed: 200,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.source, 'runtime');
        const dataModel = payload.dataModel as Record<string, unknown>;
        assert.equal(dataModel.objectCount, 2);
        assert.equal(dataModel.relationshipCount, 1);
        assert.equal(dataModel.relationshipsTruncated, false);
        assert.equal((dataModel.relationships as unknown[]).length, 1);
        assert.ok(dataModel.analysisSummary);
        assert.deepEqual(payload.ui, {
            available: true,
            sceneCount: 2,
            totalViewCount: 3,
            viewTypeSummary: [
                { type: 'table', count: 2 },
                { type: 'form', count: 1 },
            ],
        });
        assert.equal((payload.nextSteps as string[]).length, 3);
    });

    it('caps relationships and lists scenes on request', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await appDeepDive.handler(
                {
                    appKey: 'Demo',
                    includeFieldDetails: true,
                    includeScenes: true,
                    maxRelationshipsListed: 0,
                },
                ctx,
            ),
        );
        const dataModel = payload.dataModel as Record<string, unknown>;
        assert.equal(dataModel.relationshipsTruncated, true);
        assert.deepEqual(dataModel.relationships, []);
        assert.equal(dataModel.relationshipCount, 1);
        const ui = payload.ui as Record<string, unknown>;
        assert.deepEqual(ui.scenes, [
            {
                sceneKey: 'scene_1',
                sceneName: 'Home',
                sceneSlug: 'home',
                viewCount: 2,
            },
            {
                sceneKey: 'scene_2',
                sceneName: 'Contacts',
                sceneSlug: 'contacts',
                viewCount: 1,
            },
        ]);
    });

    it('reports a missing schema', async () => {
        const { ctx } = coldContext();
        const payload = payloadOf(
            await appDeepDive.handler(
                {
                    appKey: 'Demo',
                    includeFieldDetails: false,
                    includeScenes: false,
                    maxRelationshipsListed: 200,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(
            String(payload.message),
            /^No schema available from runtime API or schema\.json\. Run knack_cache/,
        );
    });
});

describe('knack_list_field_references', () => {
    it('lists every reference to a field with counts and builder links', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await listFieldReferences.handler(
                {
                    appKey: 'Demo',
                    fieldKey: 'FIELD_4',
                    groupByView: false,
                    maxResults: 200,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.source, 'runtime');
        assert.equal(payload.fieldKey, 'field_4');
        assert.equal(payload.classification, undefined);
        assert.match(
            String((payload.builderUrls as { field: string }).field),
            /objects\/object_2\/fields\/field_4\/settings$/,
        );

        const references = payload.references as Array<Record<string, unknown>>;
        assert.equal(payload.totalReferences, references.length);
        assert.equal(payload.returnedReferences, references.length);
        const paths = references.map((reference) => reference.path as string);
        assert.ok(paths.includes('schema.objects.object_2.fields.field_4'));
        assert.ok(paths.includes('fieldMap.object_2.company'));
        assert.ok(
            paths.includes('viewMap.view_2.rules.records.0.values.0.field'),
        );
        assert.ok(
            paths.includes(
                'viewMap.view_2.groups.0.columns.0.inputs.1.field.key',
            ),
        );

        const sources = (
            payload.countsBySource as Array<{ sourceType: string }>
        ).map((entry) => entry.sourceType);
        assert.deepEqual([...sources].sort(), [
            'fieldMap',
            'schema',
            'viewMap',
        ]);
        const classes = (
            payload.countsByClassification as Array<{ classification: string }>
        ).map((entry) => entry.classification);
        assert.ok(classes.includes('viewRecordRule'));

        const ruleRef = references.find(
            (reference) =>
                reference.path ===
                'viewMap.view_2.rules.records.0.values.0.field',
        );
        assert.ok(ruleRef);
        assert.equal(ruleRef.sceneKey, 'scene_1');
        const urls = ruleRef.builderUrls as Record<string, string | null>;
        assert.match(String(urls.scene), /pages\/scene_1$/);
        assert.match(String(urls.view), /pages\/scene_1\/views\/view_2\/form$/);
        assert.equal(urls.field, null);
    });

    it('filters by classification and honours maxResults', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await listFieldReferences.handler(
                {
                    appKey: 'Demo',
                    fieldKey: 'field_4',
                    classification: 'viewRecordRule',
                    groupByView: false,
                    maxResults: 1,
                },
                ctx,
            ),
        );
        assert.equal(payload.classification, 'viewRecordRule');
        assert.equal(payload.totalReferences, 1);
        assert.equal(payload.returnedReferences, 1);
        const references = payload.references as Array<Record<string, unknown>>;
        assert.equal(
            references[0].path,
            'viewMap.view_2.rules.records.0.values.0.field',
        );
        assert.deepEqual(payload.countsBySource, [
            { sourceType: 'viewMap', count: 1 },
        ]);
    });

    it('groups record-rule references per view (the legacy find_views shape)', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await listFieldReferences.handler(
                {
                    appKey: 'Demo',
                    fieldKey: 'field_4',
                    classification: 'viewRecordRule',
                    groupByView: true,
                    maxResults: 100,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.source, 'runtime');
        assert.equal(payload.fieldKey, 'field_4');
        assert.equal(payload.totalMatches, 1);
        assert.equal(payload.totalViews, 1);
        assert.equal(payload.totalReferences, undefined);
        const results = payload.results as Array<Record<string, unknown>>;
        assert.equal(results[0].viewKey, 'view_2');
        assert.equal(results[0].viewName, 'Add Contact');
        assert.equal(results[0].viewType, 'form');
        assert.equal(results[0].sceneKey, 'scene_1');
        assert.equal(results[0].sceneName, 'Home');
        assert.equal(results[0].sceneSlug, 'home');
        assert.deepEqual(results[0].matchedPaths, [
            'viewMap.view_2.rules.records.0.values.0.field',
        ]);
        assert.equal(results[0].matchCount, 1);
        assert.equal((results[0].matches as unknown[]).length, 1);
        const urls = results[0].builderUrls as Record<string, string>;
        assert.match(urls.scene, /pages\/scene_1$/);
        assert.match(urls.view, /pages\/scene_1\/views\/view_2\/form$/);
    });

    it('groups every view reference when no classification is given', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await listFieldReferences.handler(
                {
                    appKey: 'Demo',
                    fieldKey: 'field_4',
                    groupByView: true,
                    maxResults: 100,
                },
                ctx,
            ),
        );
        assert.equal(payload.totalViews, 1);
        const results = payload.results as Array<Record<string, unknown>>;
        assert.equal(results[0].matchCount, 2);
        assert.equal(payload.totalMatches as number, 2);
    });

    it('reports the true total when maxResults truncates the raw references before grouping', async () => {
        // field_4 carries two raw references onto one view. maxResults: 1 keeps only
        // one of them, so totalMatches must still say 2 — a caller reading only
        // totalMatches must not see its own truncation cap reflected back as the total.
        const { ctx } = warmContext();
        const payload = payloadOf(
            await listFieldReferences.handler(
                {
                    appKey: 'Demo',
                    fieldKey: 'field_4',
                    groupByView: true,
                    maxResults: 1,
                },
                ctx,
            ),
        );
        assert.equal(payload.totalMatches, 2);
        assert.equal(payload.returnedMatches, 1);
        assert.equal(payload.totalViews, 1);
        const results = payload.results as Array<Record<string, unknown>>;
        assert.equal(results[0].matchCount, 1);
    });

    it('returns empty counts for a field nobody references', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await listFieldReferences.handler(
                {
                    appKey: 'Demo',
                    fieldKey: 'field_999',
                    groupByView: false,
                    maxResults: 200,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.totalReferences, 0);
        assert.equal(payload.returnedReferences, 0);
        assert.deepEqual(payload.references, []);
        assert.equal((payload.builderUrls as { field: null }).field, null);
    });
});

describe('knack_search_ktl_keywords', () => {
    it('finds underscore keywords in titles and descriptions', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await searchKtlKeywords.handler(
                { appKey: 'Demo', maxResults: 100 },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.source, 'runtime');
        assert.equal(payload.keywordFilter, null);
        assert.equal(payload.totalMatches, 1);
        assert.deepEqual(payload.topKeywords, [
            { keyword: '_ktl_hide', viewCount: 1 },
            { keyword: '_ktl_', viewCount: 1 },
        ]);
        const results = payload.results as Array<Record<string, unknown>>;
        assert.equal(results[0].viewKey, 'view_1');
        assert.equal(results[0].sceneKey, 'scene_1');
        assert.equal(results[0].sceneSlug, 'home');
        assert.deepEqual(results[0].matchedKeywords, ['_ktl_hide', '_ktl_']);
        assert.equal(results[0].hitCount, 3);
        const snippets = results[0].snippets as Array<Record<string, string>>;
        assert.equal(snippets[0].source, 'title');
        assert.equal(snippets[1].source, 'description');
    });

    it('narrows to one keyword', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await searchKtlKeywords.handler(
                { appKey: 'Demo', keyword: ' _KTL_HIDE ', maxResults: 100 },
                ctx,
            ),
        );
        assert.equal(payload.keywordFilter, ' _KTL_HIDE ');
        const results = payload.results as Array<Record<string, unknown>>;
        assert.equal(results[0].hitCount, 2);
        assert.deepEqual(results[0].matchedKeywords, ['_ktl_hide']);
    });

    it('reports a missing view map', async () => {
        const { ctx } = coldContext();
        const payload = payloadOf(
            await searchKtlKeywords.handler(
                { appKey: 'Demo', maxResults: 100 },
                ctx,
            ),
        );
        assert.deepEqual(payload, {
            ok: false,
            appKey: 'Demo',
            message: 'No view map available from runtime API or viewMap.json.',
        });
    });
});

describe('knack_search_emails', () => {
    it('finds email rules with recipient and subject', async () => {
        const { ctx } = warmContext();
        const payload = payloadOf(
            await searchEmails.handler(
                { appKey: 'Demo', includeMessage: false, maxResults: 100 },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.source, 'runtime');
        assert.equal(payload.query, null);
        assert.equal(payload.includeMessage, false);
        assert.equal(payload.totalMatches, 1);
        assert.deepEqual(payload.results, [
            {
                viewKey: 'view_2',
                viewName: 'Add Contact',
                viewType: 'form',
                sceneKey: 'scene_1',
                sceneName: 'Home',
                sceneSlug: 'home',
                path: '$.rules.emails.0',
                action: 'email',
                to: 'ops@example.com',
                cc: null,
                bcc: null,
                subject: 'New contact created',
            },
        ]);
    });

    it('includes the message and applies the text filter', async () => {
        const { ctx } = warmContext();
        const withMessage = payloadOf(
            await searchEmails.handler(
                {
                    appKey: 'Demo',
                    query: 'OPS@example',
                    includeMessage: true,
                    maxResults: 100,
                },
                ctx,
            ),
        );
        assert.equal(withMessage.totalMatches, 1);
        assert.equal(
            (withMessage.results as Array<{ message: string }>)[0].message,
            'A contact was added for field_3.',
        );

        const filtered = payloadOf(
            await searchEmails.handler(
                {
                    appKey: 'Demo',
                    query: 'nobody@nowhere',
                    includeMessage: false,
                    maxResults: 100,
                },
                ctx,
            ),
        );
        assert.equal(filtered.totalMatches, 0);
        assert.deepEqual(filtered.results, []);
    });

    it('reports a missing view map', async () => {
        const { ctx } = coldContext();
        const payload = payloadOf(
            await searchEmails.handler(
                { appKey: 'Demo', includeMessage: false, maxResults: 100 },
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.equal(
            payload.message,
            'No view map available from runtime API or viewMap.json.',
        );
    });
});

describe('knack_generate_seed_csvs', () => {
    const baseArgs = {
        appKey: 'Demo',
        rowsPerObject: 4,
        useExistingConnectionValues: false,
        confirmExistingConnectionValueFetch: false,
    };

    it('generates one CSV per object in import order without touching the API', async () => {
        const { ctx, requests } = warmContext();
        const payload = payloadOf(
            await generateSeedCsvs.handler(baseArgs, ctx),
        );
        assert.equal(payload.ok, true);
        assert.equal(payload.source, 'runtime');
        assert.equal(payload.objectCount, 2);
        assert.equal(requests.length, 0);
        const importOrder = payload.importOrder as Array<{ objectKey: string }>;
        assert.deepEqual(
            importOrder.map((entry) => entry.objectKey),
            ['object_1', 'object_2'],
        );
        const objects = payload.objects as Array<Record<string, unknown>>;
        assert.equal(objects.length, 2);
        assert.match(String(objects[0].csvContent), /Company Name/);
        assert.deepEqual(payload.apiCallEstimate, {
            requiresApiKey: false,
            estimatedCalls: 0,
            basis: 'No authenticated API calls requested.',
            targets: [],
        });
        assert.deepEqual(payload.externalConnectionFetches, []);
        assert.match(
            String(payload.note),
            /^Connection values reference each object’s suggested unique import key\./,
        );
    });

    it('requires confirmation before fetching external parent values', async () => {
        const { ctx, requests } = warmContext();
        const payload = payloadOf(
            await generateSeedCsvs.handler(
                {
                    ...baseArgs,
                    objectKeys: ['object_2'],
                    useExistingConnectionValues: true,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.equal(payload.confirmationRequired, true);
        assert.equal(requests.length, 0);
        assert.match(
            String(payload.message),
            /re-run with confirmExistingConnectionValueFetch:true/,
        );
        assert.deepEqual(payload.apiCallEstimate, {
            requiresApiKey: true,
            estimatedCalls: 1,
            basis: 'One authenticated records-list request per connected parent object not included in objectKeys, limited to the first page with up to 4 rows.',
            targets: [
                {
                    objectKey: 'object_1',
                    objectName: 'Companies',
                    plannedApiPath:
                        '/objects/object_1/records?page=1&rows_per_page=4',
                },
            ],
        });
    });

    it('fetches external parent display values once confirmed', async () => {
        const { ctx, requests } = warmContext({
            responses: {
                'GET /objects/object_1/records?page=1&rows_per_page=4': {
                    ok: true,
                    status: 200,
                    body: {
                        records: [
                            { id: 'a', identifier: 'Acme Ltd' },
                            { id: 'b', identifier: 'Globex' },
                            { id: 'c', identifier: 'acme ltd' },
                        ],
                    },
                },
            },
        });
        const payload = payloadOf(
            await generateSeedCsvs.handler(
                {
                    ...baseArgs,
                    objectKeys: ['object_2'],
                    useExistingConnectionValues: true,
                    confirmExistingConnectionValueFetch: true,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.deepEqual(requests, [
            {
                apiPath: '/objects/object_1/records?page=1&rows_per_page=4',
                method: 'GET',
                body: null,
            },
        ]);
        assert.deepEqual(payload.externalConnectionFetches, [
            {
                objectKey: 'object_1',
                objectName: 'Companies',
                apiPath: '/objects/object_1/records?page=1&rows_per_page=4',
                fetchedValues: 2,
                ok: true,
            },
        ]);
        assert.equal(payload.objectCount, 1);
        const objects = payload.objects as Array<Record<string, unknown>>;
        assert.equal(objects[0].objectKey, 'object_2');
        assert.match(String(objects[0].csvContent), /Acme Ltd/);
        assert.match(
            String(payload.note),
            /API-fetched existing display values/,
        );
    });

    it('reads the parent object’s display field from real-shaped records, not the id', async () => {
        // Live Knack records carry no top-level `identifier`; the object's metadata
        // names its display field (`identifier: "field_1"`) and each record has that
        // field as `field_1` / `field_1_raw`. Measured on 6 September: without this the
        // CSV cell fell through to the record id, which imports but is not what the
        // note promised.
        const metadata: RuntimeMetadata = {
            ...RUNTIME_METADATA,
            objects: (
                RUNTIME_METADATA.objects as Array<Record<string, unknown>>
            ).map((object) =>
                object.key === 'object_1'
                    ? { ...object, identifier: 'field_1' }
                    : object,
            ),
        };
        const { ctx } = makeFakeContext({
            runtimeMetadata: { Demo: metadata },
            responses: {
                'GET /objects/object_1/records?page=1&rows_per_page=4': {
                    ok: true,
                    status: 200,
                    body: {
                        records: [
                            {
                                id: '6a9d000000000000000000a1',
                                field_1: '=Acme Ltd',
                                field_1_raw: '=Acme Ltd',
                            },
                            {
                                id: '6a9d000000000000000000a2',
                                field_1: 'Globex',
                                field_1_raw: 'Globex',
                            },
                        ],
                    },
                },
            },
        });
        const payload = payloadOf(
            await generateSeedCsvs.handler(
                {
                    ...baseArgs,
                    objectKeys: ['object_2'],
                    useExistingConnectionValues: true,
                    confirmExistingConnectionValueFetch: true,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        const objects = payload.objects as Array<Record<string, unknown>>;
        const csv = String(objects[0].csvContent);
        assert.match(csv, /'=Acme Ltd/, 'display value, formula-escaped');
        assert.match(csv, /Globex/);
        assert.doesNotMatch(csv, /6a9d000000000000000000a1/, 'no record id');
        assert.match(
            String((objects[0].notes as string[]).join('\n')),
            /fetched from the API \(field_1\)/,
        );
    });

    it('reports a failed external fetch without aborting', async () => {
        const { ctx } = warmContext({
            responses: () => ({
                ok: false,
                status: 500,
                body: { error: 'boom' },
            }),
        });
        const payload = payloadOf(
            await generateSeedCsvs.handler(
                {
                    ...baseArgs,
                    objectKeys: ['object_2'],
                    useExistingConnectionValues: true,
                    confirmExistingConnectionValueFetch: true,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        const fetches = payload.externalConnectionFetches as Array<
            Record<string, unknown>
        >;
        assert.equal(fetches[0].ok, false);
        assert.equal(fetches[0].fetchedValues, 0);
        assert.equal(fetches[0].message, 'Request failed with status 500.');
    });

    it('reports a missing schema', async () => {
        const { ctx } = coldContext();
        const payload = payloadOf(
            await generateSeedCsvs.handler(baseArgs, ctx),
        );
        assert.deepEqual(payload, {
            ok: false,
            appKey: 'Demo',
            message: 'No schema available from runtime API or schema.json.',
        });
    });

    it('does not read a connected parent object outside dataAccess.allowedObjectKeys', async () => {
        // The parent is read here only to borrow its display values, so the read-access
        // half of a policy applies to it exactly as it would to a direct read.
        const app = makeApp({
            dataAccess: { allowedObjectKeys: ['object_2'] },
        });
        const { ctx, requests } = warmContext({ apps: [app] });
        const payload = payloadOf(
            await generateSeedCsvs.handler(
                {
                    ...baseArgs,
                    objectKeys: ['object_2'],
                    useExistingConnectionValues: true,
                    confirmExistingConnectionValueFetch: true,
                },
                ctx,
            ),
        );
        assert.equal(payload.ok, true);
        assert.equal(requests.length, 0);
        const apiCallEstimate = payload.apiCallEstimate as Record<
            string,
            unknown
        >;
        assert.equal(apiCallEstimate.requiresApiKey, false);
        assert.equal(apiCallEstimate.estimatedCalls, 0);
        assert.deepEqual(payload.policyBlockedConnectionTargets, [
            { objectKey: 'object_1', objectName: 'Companies' },
        ]);
    });
});
