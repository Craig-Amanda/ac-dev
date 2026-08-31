/**
 * Find out which host serves page and view metadata for an app, and what each
 * candidate returns.
 *
 * Every view route in this server is built as `${apiBase}/scenes/<scene>/views/<view>`
 * with `apiBase` defaulting to `https://api.knack.com/v1`. Against a real app all of
 * them returned 404, and the guard's preflight read never succeeded — so every rule
 * that depends on knowing a view's type (the menu blocks, the cascade check, the human
 * confirmation) was unreachable.
 *
 * The path is not the problem. `scenes/<scene>/views/<view>` is correct. The host is:
 * page and view metadata is served by a **regional builder host**, e.g.
 * `https://eu-central-1-builder-write.knack.com/v1`, while `api.knack.com/v1` serves
 * records and the public application payload. One base cannot serve both, so this
 * probe establishes which host answers before any route is changed.
 *
 * It also reports the **shape** of every 200, not just the status. A route that
 * answers with the wrong kind of object is more dangerous than one that 404s: a PUT
 * there would send a view-shaped payload at a page-shaped resource, replacing the page
 * with a single view and destroying every other view on it. Status codes alone cannot
 * distinguish that case.
 *
 * **This script only ever issues GET.** Its single request helper is `get`, and there
 * is no write path in the file. Safe to run against any app.
 *
 * Usage:
 *   KNACK_APP_ID=... KNACK_API_KEY=... \
 *     npx tsx scripts/probe-view-routes.ts \
 *       --scene scene_3 --view view_4 --region eu-central-1
 *
 * Optional:
 *   --region eu-central-1     Builds the builder-write and builder-read candidates.
 *   --base <url>              Add an explicit base to the matrix. Repeatable.
 *   --json                    Emit the raw matrix as JSON.
 */

const PUBLIC_API_BASE = 'https://api.knack.com/v1';

type Args = {
    sceneKey: string;
    viewKey: string;
    bases: string[];
    asJson: boolean;
};

function parseArgs(argv: string[]): Args {
    const read = (flag: string): string | undefined => {
        const index = argv.indexOf(flag);
        return index === -1 ? undefined : argv[index + 1];
    };

    const readAll = (flag: string): string[] => {
        const values: string[] = [];
        argv.forEach((entry, index) => {
            if (entry === flag && argv[index + 1]) values.push(argv[index + 1]);
        });
        return values;
    };

    const sceneKey = read('--scene');
    const viewKey = read('--view');

    if (!sceneKey || !viewKey) {
        throw new Error(
            'Both --scene and --view are required, e.g. --scene scene_3 --view view_4',
        );
    }

    const region = read('--region');
    const bases = [
        PUBLIC_API_BASE,
        // Named separately because "write" in the hostname implies a matching read
        // host. If both answer, the guard reading from one and mutating the other is
        // a correctness question worth settling before relying on the preflight.
        ...(region
            ? [
                  `https://${region}-builder-write.knack.com/v1`,
                  `https://${region}-builder-read.knack.com/v1`,
              ]
            : []),
        ...readAll('--base'),
    ].map((base) => base.replace(/\/+$/, ''));

    return {
        sceneKey,
        viewKey,
        bases: [...new Set(bases)],
        asJson: argv.includes('--json'),
    };
}

type Probe = {
    label: string;
    path: string;
    /** What a correct implementation would expect back, for comparison against shape. */
    expect: 'app' | 'page' | 'view';
};

type Result = Probe & {
    base: string;
    status: number | 'network-error';
    shape: string;
    detail: string;
};

/**
 * Classify a response body by what it structurally is, not by what was asked for.
 *
 * @param body Parsed JSON response body.
 * @returns A short shape name and a human-readable detail line.
 */
function classify(body: unknown): { shape: string; detail: string } {
    if (body === null || typeof body !== 'object') {
        return { shape: 'not-an-object', detail: String(body).slice(0, 70) };
    }

    if (Array.isArray(body)) {
        return { shape: 'array', detail: `${body.length} item(s)` };
    }

    const record = body as Record<string, unknown>;

    if (record.application && typeof record.application === 'object') {
        const app = record.application as Record<string, unknown>;
        const scenes = Array.isArray(app.scenes) ? app.scenes.length : 0;
        return { shape: 'APP', detail: `application, ${scenes} scene(s)` };
    }

    const inner = (record.scene ?? record.page ?? record.view ?? record) as
        Record<string, unknown> | undefined;
    const target = inner && typeof inner === 'object' ? inner : record;

    const looksLikeView =
        typeof target.type === 'string' &&
        ['columns', 'groups', 'links', 'rows', 'inputs', 'source'].some(
            (layoutKey) => layoutKey in target,
        );

    if (looksLikeView) {
        return {
            shape: 'VIEW',
            detail: `key=${String(target.key ?? '?')} type=${String(
                target.type ?? '?',
            )}`,
        };
    }

    // Identity before enumeration. A page carries its own `key`/`slug` alongside a
    // `views` array, so a generic "has an array named views" check run first would
    // label it a collection — and then the wrong-kind-of-object warning below never
    // fires on the one case it exists for. Ask what the object *is* before asking
    // what it contains.
    const looksLikePage =
        'slug' in target ||
        'parent' in target ||
        (Array.isArray(target.views) && typeof target.key === 'string');

    if (looksLikePage) {
        const views = Array.isArray(target.views) ? target.views.length : 0;
        return {
            shape: 'PAGE',
            detail: `key=${String(target.key ?? '?')} slug=${String(
                target.slug ?? '?',
            )} views=${views}`,
        };
    }

    for (const name of ['scenes', 'pages', 'views']) {
        const value = record[name];
        if (Array.isArray(value)) {
            return {
                shape: `COLLECTION(${name})`,
                detail: `${value.length} ${name}`,
            };
        }
    }

    return {
        shape: 'unknown',
        detail: `keys: ${Object.keys(record).slice(0, 6).join(', ')}`,
    };
}

/**
 * Is this body a web-server error page rather than an API response?
 *
 * The distinction matters more than the status code. A 404 carrying JSON means the
 * route exists and rejected the request; a 404 carrying markup means nothing on that
 * host answers the path at all, so no header or credential would change it.
 *
 * @param body Response body, parsed or raw.
 * @returns True when the body is HTML.
 */
function isHtml(body: unknown): boolean {
    return (
        typeof body === 'string' &&
        /^\s*<(!doctype|html)/i.test(body.slice(0, 40))
    );
}

/**
 * Look for the target view inside the application payload.
 *
 * This is the question that decides the fix. The guard's preflight only needs a view's
 * definition — its type, and whatever layout key carries its link columns. If the
 * application payload already contains that, the per-view route is not needed at all
 * and the preflight can be re-sourced onto a route that demonstrably works, rather
 * than waiting on whatever host and credential the builder API wants.
 *
 * It also counts how many scenes carry a `parent`, which settles a separate open
 * question about whether snapshots can rebuild a deleted page hierarchy.
 *
 * @param payload Parsed body of an applications/{appId} response.
 * @param sceneKey Scene to look for.
 * @param viewKey View to look for.
 */
function inspectApplicationPayload(
    payload: unknown,
    sceneKey: string,
    viewKey: string,
): void {
    const root = payload as Record<string, unknown> | null;
    const app = root?.application as Record<string, unknown> | undefined;
    const scenes = Array.isArray(app?.scenes)
        ? (app.scenes as Record<string, unknown>[])
        : [];

    if (!scenes.length) {
        console.log('  The application payload carried no scenes to inspect.');
        return;
    }

    const withParent = scenes.filter(
        (scene) => typeof scene.parent === 'string' && scene.parent.trim(),
    );
    console.log(
        `  scenes: ${scenes.length}, of which ${withParent.length} carry a parent reference.`,
    );
    if (!withParent.length) {
        console.log(
            '  No scene carries a parent, so a snapshot built from this payload cannot',
        );
        console.log(
            '  rebuild a page hierarchy. That is a separate defect from the routing one.',
        );
    }

    const scene = scenes.find((entry) => entry.key === sceneKey);
    if (!scene) {
        console.log(`  ${sceneKey} was not found in the payload.`);
        return;
    }

    const views = Array.isArray(scene.views)
        ? (scene.views as Record<string, unknown>[])
        : [];
    const view = views.find((entry) => entry.key === viewKey);
    if (!view) {
        console.log(
            `  ${sceneKey} was found (${views.length} views) but ${viewKey} was not among them.`,
        );
        return;
    }

    // The guard reads the type off the fetched view and walks its layout for link
    // columns. Report exactly whether both are present here.
    const attributes =
        (view.attributes as Record<string, unknown> | undefined) ?? view;
    const layoutKeys = [
        'columns',
        'groups',
        'links',
        'rows',
        'inputs',
        'source',
    ].filter((key) => key in attributes);

    console.log(
        `  ${viewKey} found in ${sceneKey}. type=${String(
            attributes.type ?? 'MISSING',
        )}`,
    );
    console.log(
        `  layout keys present: ${layoutKeys.join(', ') || 'NONE — cannot walk for link columns'}`,
    );

    if (attributes.type && layoutKeys.length) {
        console.log('');
        console.log(
            '  The payload carries both the view type and its layout, so the preflight can',
        );
        console.log(
            '  be sourced from this route instead of a per-view one. That unblocks the menu',
        );
        console.log(
            '  blocks, the cascade check and the human confirmation without resolving the',
        );
        console.log('  builder host question first.');
    }
}

async function main(): Promise<void> {
    const args = parseArgs(process.argv.slice(2));

    if (!process.env.KNACK_APP_ID || !process.env.KNACK_API_KEY) {
        throw new Error(
            'Set KNACK_APP_ID and KNACK_API_KEY in the environment before running.',
        );
    }
    // Bound to consts rather than read inline: narrowing on process.env does not
    // survive into the request closure below.
    const appId: string = process.env.KNACK_APP_ID;
    const apiKey: string = process.env.KNACK_API_KEY;

    /** The only request helper in this file. GET, always. */
    async function get(url: string): Promise<{
        status: number | 'network-error';
        body: unknown;
    }> {
        try {
            const response = await fetch(url, {
                method: 'GET',
                headers: {
                    'X-Knack-Application-Id': appId,
                    'X-Knack-REST-API-Key': apiKey,
                    'Content-Type': 'application/json',
                },
            });
            const text = await response.text();
            let body: unknown = text;
            try {
                body = JSON.parse(text);
            } catch {
                // Not JSON, and that is itself the finding: a route that exists
                // answers errors as JSON, so markup means the request never reached
                // an API handler on this host.
                body = text;
            }
            return { status: response.status, body };
        } catch (error) {
            return {
                status: 'network-error',
                body: error instanceof Error ? error.message : String(error),
            };
        }
    }

    const { sceneKey, viewKey } = args;

    const probes: Probe[] = [
        // Control. The server reads runtime metadata through this route, so it is known
        // to work on the public base. If it fails everywhere, the rest of the matrix is
        // about credentials, not routing.
        {
            label: 'applications/{appId}',
            path: `/applications/${encodeURIComponent(appId)}`,
            expect: 'app',
        },
        {
            label: 'scenes/{scene}',
            path: `/scenes/${sceneKey}`,
            expect: 'page',
        },
        {
            label: 'scenes/{scene}/views/{view}',
            path: `/scenes/${sceneKey}/views/${viewKey}`,
            expect: 'view',
        },
        // The builder API may scope pages beneath the application rather than at the
        // root of /v1. A root-level 404 does not distinguish that from "no such route
        // on this host at all".
        {
            label: 'applications/{id}/scenes/{scene}',
            path: `/applications/${encodeURIComponent(appId)}/scenes/${sceneKey}`,
            expect: 'page',
        },
        {
            label: 'applications/{id}/../views/{view}',
            path: `/applications/${encodeURIComponent(
                appId,
            )}/scenes/${sceneKey}/views/${viewKey}`,
            expect: 'view',
        },
    ];

    const results: Result[] = [];
    for (const base of args.bases) {
        for (const probe of probes) {
            const { status, body } = await get(`${base}${probe.path}`);
            const { shape, detail } =
                status === 200
                    ? classify(body)
                    : {
                          shape: isHtml(body) ? 'HTML' : '—',
                          detail: isHtml(body)
                              ? 'markup, not an API error: no such route on this host'
                              : typeof body === 'string'
                                ? body.replace(/\s+/g, ' ').slice(0, 58)
                                : JSON.stringify(body).slice(0, 58),
                      };
            results.push({ ...probe, base, status, shape, detail });
        }
    }

    if (args.asJson) {
        console.log(JSON.stringify(results, null, 2));
        return;
    }

    const pad = (value: string, width: number) => value.padEnd(width);
    console.log('\nGET only — nothing was written.');
    console.log(`scene: ${sceneKey}   view: ${viewKey}\n`);

    for (const base of args.bases) {
        console.log(base);
        console.log('-'.repeat(Math.max(base.length, 92)));
        for (const result of results.filter((entry) => entry.base === base)) {
            console.log(
                `  ${pad(result.label, 34)} ${pad(String(result.status), 8)} ${pad(
                    result.shape,
                    18,
                )} ${result.detail}`,
            );
        }
        console.log('');
    }

    const viewOk = results.filter(
        (result) => result.expect === 'view' && result.status === 200,
    );
    const anyControl = results.some(
        (result) => result.expect === 'app' && result.status === 200,
    );

    if (!anyControl) {
        console.log(
            'The control route failed on every base, so nothing here is about routing.',
        );
        console.log('Check KNACK_APP_ID and KNACK_API_KEY first.');
        return;
    }

    // A 200 carrying the wrong kind of object is the dangerous case: a PUT there
    // overwrites a resource of a different kind from the payload sent.
    // Any 200 whose shape is not the expected kind counts, not just a page-for-view
    // swap: a collection or an unrecognised object where a single view was asked for is
    // equally a route a mutation must not touch.
    const expectedShape = { app: 'APP', page: 'PAGE', view: 'VIEW' } as const;
    const mismatched = results.filter(
        (result) =>
            result.status === 200 &&
            result.shape !== expectedShape[result.expect],
    );

    if (mismatched.length) {
        console.log(
            'READ THIS — a route answered with the wrong kind of object:',
        );
        for (const result of mismatched) {
            console.log(
                `  ${result.base}${result.path}`,
                `\n    expected a ${result.expect}, returned a ${result.shape}.`,
            );
        }
        console.log(
            '  A PUT there sends one kind of payload at another kind of resource. Run no',
        );
        console.log('  mutation against it.');
        return;
    }

    if (!viewOk.length) {
        console.log(
            'No base served the view route. Every failure above carrying HTML means the path',
        );
        console.log(
            'does not exist on that host, so no credential would change it.',
        );
        console.log('');
        console.log('Can the view be read without a per-view route?');
        // Re-read from whichever base actually served the control, not a hard-coded
        // one: the base that answers is the base worth inspecting.
        const servingBase = results.find(
            (result) => result.expect === 'app' && result.status === 200,
        )?.base;
        if (!servingBase) {
            console.log(
                '  No base served applications/{appId}, so there is nothing to inspect.',
            );
            return;
        }
        const control = await get(
            `${servingBase}/applications/${encodeURIComponent(appId)}`,
        );
        if (control.status === 200) {
            console.log(`  (from ${servingBase})`);
            inspectApplicationPayload(control.body, sceneKey, viewKey);
        } else {
            console.log(`  applications/{appId} returned ${control.status}.`);
        }
        return;
    }

    console.log('Bases that served the view route, with the right shape:');
    for (const result of viewOk) {
        console.log(`  ${result.base}   (${result.shape} — ${result.detail})`);
    }
    if (viewOk.length > 1) {
        console.log('');
        console.log(
            'More than one answered. If a read host and a write host both serve this route,',
        );
        console.log(
            'settle which the preflight should use before trusting it: a guard that reads',
        );
        console.log(
            'one replica and mutates another can approve a state that is no longer current.',
        );
    }
}

main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
});
