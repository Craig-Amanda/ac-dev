/**
 * Re-check the cascade rule against a live app.
 *
 * This began as a test of the premise every view-safety rule rested on:
 *
 *   "Knack's view PUT deletes a view's `link` columns and cascade-deletes the child
 *    pages behind them whenever `columns` is replaced — even when the link column is
 *    re-sent byte-for-byte."
 *
 * **That premise is false**, measured on 1 September. Re-sending a link column
 * destroys nothing; a page dies when the definition it receives no longer carries a
 * link to it, and only when that was its last referring link. See the README section
 * "Verifying the premise against a real app" for the runs.
 *
 * The script is still worth having. It re-sends `columns` byte-for-byte and diffs the
 * scene list, which is exactly the arm that should now delete nothing — so on a Knack
 * plan or region that has not been checked, a page disappearing here means the rule
 * differs there and the guard's narrowing is unsafe on that deployment.
 *
 * It should now be a no-op. Point it at a disposable app anyway: it is destructive if
 * the old premise turns out to hold somewhere.
 *
 * Usage:
 *   KNACK_APP_ID=... KNACK_API_KEY=... \
 *     npx tsx scripts/verify-cascade-premise.ts \
 *       --scene scene_1 --view view_2 --confirm-destructive
 *
 * Optional:
 *   --api-base https://api.knack.com/v1   Override for a non-default region.
 *   --dry-run                             Do everything except the PUT. Safe. Use this
 *                                         first to confirm the fixture is right.
 */

const API_BASE_DEFAULT = 'https://api.knack.com/v1';

type Args = {
    sceneKey: string;
    viewKey: string;
    apiBase: string;
    confirmed: boolean;
    dryRun: boolean;
};

function parseArgs(argv: string[]): Args {
    const read = (flag: string): string | undefined => {
        const index = argv.indexOf(flag);
        return index === -1 ? undefined : argv[index + 1];
    };

    const sceneKey = read('--scene');
    const viewKey = read('--view');

    if (!sceneKey || !viewKey) {
        throw new Error(
            'Both --scene and --view are required, e.g. --scene scene_1 --view view_2',
        );
    }

    return {
        sceneKey,
        viewKey,
        apiBase: read('--api-base') ?? API_BASE_DEFAULT,
        confirmed: argv.includes('--confirm-destructive'),
        dryRun: argv.includes('--dry-run'),
    };
}

function requireEnv(name: string): string {
    const value = process.env[name];
    if (!value) throw new Error(`${name} is not set.`);
    return value;
}

// Resolved in main() so a missing flag or env var prints the message rather than a
// stack trace. Assigned before any api() call.
let appId = '';
let apiKey = '';
let args: Args;

async function api(
    path: string,
    init?: RequestInit,
): Promise<{ ok: boolean; status: number; body: unknown }> {
    const response = await fetch(`${args.apiBase}${path}`, {
        ...init,
        headers: {
            'X-Knack-Application-Id': appId,
            'X-Knack-REST-API-Key': apiKey,
            'Content-Type': 'application/json',
            ...(init?.headers ?? {}),
        },
    });

    let body: unknown = null;
    const text = await response.text();
    try {
        body = text ? JSON.parse(text) : null;
    } catch {
        body = text;
    }

    return { ok: response.ok, status: response.status, body };
}

/**
 * Read every scene key the app currently has, so the set can be diffed after the PUT.
 * A page that vanishes between the two reads is what "cascade delete" means here.
 */
/**
 * Read one view out of the application payload.
 *
 * Knack serves no per-view route to a REST API key — `GET /scenes/{s}/views/{v}` comes
 * back as a web-server HTML 404 on every host, which is why the server's own preflight
 * was moved off it. This script was still calling it, so it threw before measuring
 * anything. The application payload carries the whole definition on a route that does
 * answer, and is the same source the guard reads.
 */
async function readView(
    sceneKey: string,
    viewKey: string,
): Promise<Record<string, unknown>> {
    const result = await api(`/applications/${appId}`);
    if (!result.ok) {
        throw new Error(
            `Could not read application metadata (status ${result.status}).`,
        );
    }

    const body = result.body as {
        application?: { scenes?: unknown };
        scenes?: unknown;
    };
    const scenes = Array.isArray(body?.application?.scenes)
        ? body.application.scenes
        : Array.isArray(body?.scenes)
          ? body.scenes
          : null;
    if (!scenes) {
        throw new Error('The application payload contained no scenes array.');
    }

    for (const sceneItem of scenes) {
        const scene = sceneItem as { key?: string; views?: unknown };
        if (scene?.key !== sceneKey) continue;
        const views = Array.isArray(scene.views) ? scene.views : [];
        for (const viewItem of views) {
            const view = viewItem as { key?: string; attributes?: unknown };
            if (view?.key !== viewKey) continue;
            // Runtime metadata nests the real properties under `attributes` on some
            // shapes and returns them flat on others.
            const attributes = view.attributes as
                Record<string, unknown> | undefined;
            return attributes ?? (view as Record<string, unknown>);
        }
        // Keep scanning: a duplicate scene key would otherwise mask a later match.
    }

    throw new Error(`${viewKey} was not found in ${sceneKey}.`);
}

async function readSceneKeys(): Promise<Set<string>> {
    const result = await api('/scenes');
    if (!result.ok) {
        throw new Error(`Could not list scenes (status ${result.status}).`);
    }

    const scenes = (result.body as { scenes?: Array<{ key?: string }> })
        ?.scenes;
    if (!Array.isArray(scenes)) {
        throw new Error('The /scenes response contained no scenes array.');
    }

    return new Set(
        scenes
            .map((scene) => scene?.key)
            .filter((key): key is string => typeof key === 'string'),
    );
}

function findLinkColumns(value: unknown, path = '$'): string[] {
    if (Array.isArray(value)) {
        return value.flatMap((item, index) =>
            findLinkColumns(item, `${path}[${index}]`),
        );
    }

    if (!value || typeof value !== 'object') return [];

    const record = value as Record<string, unknown>;
    const found = record.type === 'link' ? [path] : [];

    return [
        ...found,
        ...Object.entries(record).flatMap(([key, nested]) =>
            findLinkColumns(nested, `${path}.${key}`),
        ),
    ];
}

async function main(): Promise<void> {
    appId = requireEnv('KNACK_APP_ID');
    apiKey = requireEnv('KNACK_API_KEY');
    args = parseArgs(process.argv.slice(2));

    console.log(`App ${appId} — ${args.sceneKey}/${args.viewKey}`);
    console.log(args.dryRun ? 'DRY RUN: no PUT will be sent.\n' : '');

    if (!args.dryRun && !args.confirmed) {
        throw new Error(
            'Refusing to run without --confirm-destructive. If the premise holds, this ' +
                'deletes real pages. Use --dry-run first, and only ever point this at a ' +
                'disposable app.',
        );
    }

    // 1. Before state.
    const before = await readSceneKeys();
    console.log(`Scenes before: ${before.size}`);

    const view = await readView(args.sceneKey, args.viewKey);
    const linkPaths = findLinkColumns(view);

    console.log(`View type: ${String(view.type ?? '(none)')}`);
    console.log(
        `Link columns found: ${linkPaths.length}${
            linkPaths.length ? ` — ${linkPaths.join(', ')}` : ''
        }`,
    );

    if (linkPaths.length === 0) {
        console.log(
            '\nThis view has no link columns, so it cannot demonstrate the premise. ' +
                'Pick a view with a link column pointing at a child page.',
        );
        return;
    }

    if (!Array.isArray(view.columns)) {
        console.log(
            '\nThis view has no top-level `columns` array. The premise is specifically ' +
                'about replacing `columns`; test a table view, or adapt this script to ' +
                'send the layout key this view actually uses.',
        );
        return;
    }

    if (args.dryRun) {
        console.log(
            '\nFixture looks right. Re-run without --dry-run (and with ' +
                '--confirm-destructive) to send the PUT.',
        );
        return;
    }

    // 2. The exact claim: re-send `columns` byte-for-byte, changing nothing.
    console.log('\nSending PUT with columns re-sent byte-for-byte...');
    const put = await api(`/scenes/${args.sceneKey}/views/${args.viewKey}`, {
        method: 'PUT',
        body: JSON.stringify({ columns: view.columns }),
    });
    console.log(`PUT status: ${put.status}`);

    // 3. After state. Knack may not apply the cascade synchronously.
    await new Promise((resolve) => setTimeout(resolve, 3000));
    const after = await readSceneKeys();
    const destroyed = [...before].filter((key) => !after.has(key));

    console.log(`\nScenes after: ${after.size}`);
    console.log('='.repeat(60));
    if (destroyed.length > 0) {
        console.log(
            `PREMISE CONFIRMED — ${destroyed.length} page(s) destroyed:`,
        );
        for (const key of destroyed) console.log(`  ${key}`);
        console.log(
            '\nAn unchanged columns re-send deleted pages. Every guard in view-safety.ts ' +
                'is justified as written.',
        );
    } else {
        console.log('PREMISE NOT REPRODUCED — no pages disappeared.');
        console.log(
            '\nThis does not mean the guard is wrong, but it does mean the rules rest on ' +
                'an unverified claim. Before loosening anything, re-test with a modified ' +
                'columns array (drop the link column) and with a `groups` write, since the ' +
                'trigger may be a different shape than assumed.',
        );
    }
    console.log('='.repeat(60));
    console.log(
        '\nRe-read the view to check whether the link column itself survived — page ' +
            'survival and column survival are separate questions.',
    );
}

main().catch((error: unknown) => {
    console.error(
        `\n${error instanceof Error ? error.message : String(error)}`,
    );
    process.exit(1);
});
