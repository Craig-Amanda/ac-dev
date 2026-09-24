/**
 * Scene (page) mutation tools — changes to a page's own properties, as opposed to the
 * views on it, which view-mutations.ts owns.
 *
 * The endpoints here were captured from Knack's Builder UI network traffic, not from the
 * public REST API reference, and authenticate the same way as every other request in this
 * server. None of them can delete or rebuild a page, so the view guard's cascade check,
 * human confirmation and scene-tree snapshot do not apply; what each tool does instead is
 * read fresh, change only what it was asked to, and read back.
 */
import { z } from 'zod';

import type { AppConfig } from '../config.js';
import type { KnackContext } from '../context.js';
import { VIEW_CACHE_STALE_NOTE } from '../lib/field-payload.js';
import { findRawSceneInMetadata, getRuntimeArray } from '../lib/metadata.js';
import { deepEqual } from '../lib/structural-diff.js';
import { asRecord, parseJsonInput } from '../lib/util.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { makeTextResponse } from '../response.js';

type RawRule = Record<string, unknown>;

/** The scene verbatim off fresh metadata: every tool here reads before and after writing. */
async function readLiveScene(
    ctx: KnackContext,
    app: AppConfig,
    sceneKey: string,
): Promise<Record<string, unknown> | null> {
    ctx.caches.runtimeMetadata.delete(app.appKey);
    return findRawSceneInMetadata(await ctx.getRuntimeMetadata(app), sceneKey);
}

/**
 * Give each incoming page rule the `key` it will be stored under, checked against the
 * rules the page already has.
 *
 * The Builder names page rules `submit_0`, `submit_1`, … in the order they were added
 * (captured 24 September on NPS Test App scene_220). Knack stores whatever key it is
 * sent, and two rules sharing one key on a page is a state the Builder never creates.
 *
 * @param existing The page's live rules, verbatim.
 * @param incoming The caller's new rules, in the order they will be appended.
 * @returns `incoming` with every rule carrying a key; throw a plain Error to refuse.
 */
export function assignPageRuleKeys(
    existing: RawRule[],
    incoming: RawRule[],
): RawRule[] {
    const taken = new Set(existing.map((rule) => rule.key));
    // Highest number plus one, not the count: a deleted rule leaves a gap, so
    // submit_0 + submit_2 would otherwise hand out submit_2 again. Keys outside the
    // submit_N pattern are still clash-checked, just never numbered from.
    let next =
        Math.max(
            -1,
            ...[...taken].map((key) =>
                typeof key === 'string' && /^submit_\d+$/.test(key)
                    ? Number(key.slice('submit_'.length))
                    : -1,
            ),
        ) + 1;
    return incoming.map((rule, index) => {
        if (rule.key !== undefined && typeof rule.key !== 'string') {
            throw new Error(`rules[${index}].key must be a string.`);
        }
        const key = rule.key ?? `submit_${next++}`;
        if (taken.has(key)) {
            throw new Error(
                `rules[${index}].key "${key}" is already used on this page. Omit key to have the next free one assigned. Nothing was sent.`,
            );
        }
        taken.add(key);
        return { ...rule, key };
    });
}

/**
 * Append page rules to a scene without disturbing the ones it already has.
 *
 * `POST /scenes/:key/rules` takes the page's **whole** rules array and replaces what is
 * stored — measured 24 September on NPS Test App scene_220: a second rule was saved by
 * a POST whose body carried both `submit_0` and `submit_1`. Sending only the new rule
 * would therefore delete every rule already on the page. This tool reads the live array
 * verbatim off fresh metadata (not through `readSceneRules`, which keeps only the fields
 * the read tools display and would strip `message`, `url`, `existing_page` and the rest
 * from every existing rule on the way back), appends, sends the lot, and reads it back.
 *
 * No snapshot file: `rulesBefore` in the response is the complete restore point, since
 * POSTing it unchanged puts the page back exactly as it was.
 */
export const addPageRules = defineTool({
    name: 'knack_add_page_rules',
    description:
        "Append page rules (show/hide views, message, redirect) to a page's existing rules; reads them live first.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        rules: z.string().describe('JSON array of page rule objects to append'),
        previewOnly: z
            .boolean()
            .optional()
            .describe('Return the merged rules without sending them'),
    },
    handler: async ({ appKey, sceneKey, rules, previewOnly }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        const respond = (payload: Record<string, unknown>) =>
            makeTextResponse({
                appKey: app.appKey,
                action: 'add_page_rules',
                sceneKey,
                ...payload,
            });
        const refuse = (error: string, message: string) =>
            respond({ ok: false, error, message });

        const parsed = parseJsonInput<unknown>('rules', rules);
        if (!Array.isArray(parsed) || parsed.length === 0) {
            throw new Error(
                'rules must be a non-empty JSON array of rule objects.',
            );
        }
        const incoming = parsed.map((entry, index) => {
            const record = asRecord(entry);
            if (!record) {
                throw new Error(
                    `rules[${index}] must be a JSON object, not ${JSON.stringify(entry)}.`,
                );
            }
            return record;
        });

        const scene = await readLiveScene(ctx, app, sceneKey);
        if (!scene) {
            return refuse(
                'SCENE_NOT_FOUND',
                `${sceneKey} was not found in this app's metadata. Nothing was sent.`,
            );
        }
        const existing: RawRule[] = (
            Array.isArray(scene.rules) ? scene.rules : []
        ).filter((entry): entry is RawRule => asRecord(entry) !== null);

        const added = assignPageRuleKeys(existing, incoming);

        // A rule naming a view that is not on this page hides nothing, and knack_get_scene
        // would report it as dangling from the moment it was written.
        const viewsOnPage = new Set(
            (Array.isArray(scene.views) ? scene.views : []).map(
                (view) => asRecord(view)?.key,
            ),
        );
        const foreignViewKeys = added.flatMap((rule) =>
            (Array.isArray(rule.view_keys) ? rule.view_keys : []).filter(
                (key) => !viewsOnPage.has(key),
            ),
        );
        if (foreignViewKeys.length) {
            return refuse(
                'VIEW_NOT_ON_PAGE',
                `view_keys ${foreignViewKeys.join(', ')} are not on ${sceneKey}. A page rule can only show or hide that page's own views. Nothing was sent.`,
            );
        }

        const merged = [...existing, ...added];
        const summary = {
            ruleCountBefore: existing.length,
            ruleCountAfter: merged.length,
            addedKeys: added.map((rule) => rule.key),
        };

        if (previewOnly) {
            return respond({
                ok: true,
                previewOnly: true,
                ...summary,
                rules: merged,
            });
        }

        const result = await ctx.request(app, `/scenes/${sceneKey}/rules`, {
            method: 'POST',
            body: JSON.stringify({ rules: merged }),
        });
        if (!result.ok) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message:
                    'Knack refused the write. The rules below are what the page had before, unchanged.',
                rulesBefore: existing,
            });
        }

        // Knack answers {"success":true} whatever it stored, so the stored array is
        // read back and compared rather than trusted.
        const after = await readLiveScene(ctx, app, sceneKey);
        const storedRules = Array.isArray(after?.rules) ? after.rules : null;
        const verified = storedRules !== null && deepEqual(storedRules, merged);

        return respond({
            ok: true,
            status: result.status,
            ...summary,
            verified,
            ...(verified
                ? {}
                : {
                      warning:
                          'The rules read back from Knack differ from what was sent. Check the page in the Builder; rulesBefore restores the previous state.',
                      storedRules,
                  }),
            rulesBefore: existing,
            cacheNote: VIEW_CACHE_STALE_NOTE,
        });
    },
});

/** Tool input name → Knack's scene property, in the order the Builder dialog shows them. */
const PAGE_SETTINGS = {
    name: 'name',
    slug: 'slug',
    print: 'print',
    modal: 'modal',
    keepModalOpen: 'modal_prevent_background_click_close',
} as const;
type PageSettingInput = keyof typeof PAGE_SETTINGS;

/**
 * Change a page's name, URL slug, print link and modal options — the Builder's Page
 * Settings dialog.
 *
 * `PUT /scenes/:key` merges: measured 24 September on NPS Test App scene_220, a body of
 * `{"name":"Finance"}` alone changed the name and left views, layout, rules and every
 * other property identical to a backup taken just before. The Builder resends the page's
 * whole `views` array with every save; this tool deliberately does not, since its copy
 * would come from runtime metadata rather than the Builder's own, and a wrong `views` is
 * the one thing here that could damage the page.
 *
 * A slug change is the consequential one. Knack rewrites the menu links that pointed at
 * the old slug and lists them in `changes.updates` (captured the same day: view_370 on
 * scene_38), which this tool passes back. Bookmarks and any link Knack did not list still
 * point at the old URL. `allowed_profiles` is left out on purpose: who may reach a page
 * is an access change and belongs in its own tool.
 */
export const updatePageSettings = defineTool({
    name: 'knack_update_page_settings',
    description:
        "Change a page's name, URL slug, print link or modal options; sends only what changes, never the views.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        name: z.string().optional(),
        slug: z.string().optional(),
        print: z.boolean().optional(),
        modal: z.boolean().optional(),
        keepModalOpen: z
            .boolean()
            .optional()
            .describe('Modal ignores background clicks until an action'),
        previewOnly: z
            .boolean()
            .optional()
            .describe('Return the changes without sending them'),
    },
    handler: async ({ appKey, sceneKey, previewOnly, ...settings }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        const respond = (payload: Record<string, unknown>) =>
            makeTextResponse({
                appKey: app.appKey,
                action: 'update_page_settings',
                sceneKey,
                ...payload,
            });
        const refuse = (error: string, message: string) =>
            respond({ ok: false, error, message });

        const requested = (
            Object.keys(PAGE_SETTINGS) as PageSettingInput[]
        ).filter((input) => settings[input] !== undefined);
        if (!requested.length) {
            return refuse(
                'NOTHING_TO_CHANGE',
                `Pass at least one of ${Object.keys(PAGE_SETTINGS).join(', ')}. Nothing was sent.`,
            );
        }
        if (settings.name !== undefined && !settings.name.trim()) {
            return refuse(
                'INVALID_NAME',
                'name cannot be blank. Nothing was sent.',
            );
        }

        // The whole app, not just this scene: the slug check needs every other page.
        ctx.caches.runtimeMetadata.delete(app.appKey);
        const metadata = await ctx.getRuntimeMetadata(app);
        const scene = findRawSceneInMetadata(metadata, sceneKey);
        if (!scene) {
            return refuse(
                'SCENE_NOT_FOUND',
                `${sceneKey} was not found in this app's metadata. Nothing was sent.`,
            );
        }

        if (settings.slug !== undefined) {
            // Knack slugs are lower-case words joined by hyphens (finance2, roll-details3);
            // anything else would be a URL the Builder itself never produces.
            if (!/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(settings.slug)) {
                return refuse(
                    'INVALID_SLUG',
                    `slug "${settings.slug}" must be lower-case letters and digits joined by single hyphens, e.g. "finance-reports". Nothing was sent.`,
                );
            }
            const clash = (getRuntimeArray(metadata, 'scenes') ?? [])
                .map((entry) => asRecord(entry))
                .find(
                    (other) =>
                        other !== null &&
                        other.slug === settings.slug &&
                        other.key !== sceneKey,
                );
            if (clash) {
                return refuse(
                    'SLUG_IN_USE',
                    `slug "${settings.slug}" already belongs to ${String(clash.key)} ("${String(clash.name)}"). Nothing was sent.`,
                );
            }
        }

        const modalAfter = settings.modal ?? scene.modal === true;
        if (settings.keepModalOpen === true && !modalAfter) {
            return refuse(
                'NOT_A_MODAL',
                'keepModalOpen only applies to a page shown as a modal. Pass modal: true as well, or leave keepModalOpen out. Nothing was sent.',
            );
        }

        // Only what actually differs goes on the wire, so a repeated call is a no-op.
        const body: Record<string, unknown> = {};
        const before: Record<string, unknown> = {};
        for (const input of requested) {
            const property = PAGE_SETTINGS[input];
            if (deepEqual(scene[property], settings[input])) continue;
            body[property] = settings[input];
            // null, not undefined: an unset property must still show in the JSON reply.
            before[input] = scene[property] ?? null;
        }
        if (!Object.keys(body).length) {
            return respond({
                ok: true,
                unchanged: true,
                message:
                    'The page already has every value passed. Nothing was sent.',
            });
        }

        const slugNote =
            'slug' in body
                ? `The page's URL changes from "${String(scene.slug)}" to "${String(body.slug)}". Bookmarks and outside links to the old URL will stop working. Knack repoints the in-app links it lists in knackUpdated; run knack_list_page_referrers to check nothing else still names the old slug.`
                : undefined;

        if (previewOnly) {
            return respond({
                ok: true,
                previewOnly: true,
                wouldSend: body,
                before,
                ...(slugNote ? { slugNote } : {}),
            });
        }

        const result = await ctx.request(app, `/scenes/${sceneKey}`, {
            method: 'PUT',
            body: JSON.stringify(body),
        });
        if (!result.ok) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message: 'Knack refused the change. The page is as it was.',
            });
        }

        // What else Knack edited as a consequence — for a slug change, the views whose
        // links it repointed. Keys only: the full view definitions would swamp the reply.
        const updates = asRecord(
            asRecord(asRecord(result.body)?.changes)?.updates,
        );
        const knackUpdated = {
            scenes: (Array.isArray(updates?.scenes) ? updates.scenes : []).map(
                (entry) => asRecord(entry)?.key,
            ),
            views: (Array.isArray(updates?.views) ? updates.views : []).map(
                (entry) => ({
                    sceneKey: asRecord(asRecord(entry)?.scene)?.key,
                    viewKey: asRecord(asRecord(entry)?.view)?.key,
                }),
            ),
        };

        const after = await readLiveScene(ctx, app, sceneKey);
        const mismatched = Object.keys(body).filter(
            (property) => !deepEqual(after?.[property], body[property]),
        );

        return respond({
            ok: true,
            status: result.status,
            sent: body,
            before,
            verified: after !== null && mismatched.length === 0,
            ...(mismatched.length
                ? {
                      warning: `Read back from Knack, ${mismatched.join(', ')} did not take the value sent. Check the page in the Builder.`,
                  }
                : {}),
            knackUpdated,
            ...(slugNote ? { slugNote } : {}),
            cacheNote: VIEW_CACHE_STALE_NOTE,
        });
    },
});

export const sceneMutationTools: AnyToolDef[] = [
    addPageRules,
    updatePageSettings,
];
