import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { describeAppListForHumans, listAppNames } from './server.js';

/**
 * These tests cover the plain-text banner that leads the knack_list_apps response. The
 * facts it states are already in the structured payload, so what matters here is that
 * the prose cannot say something the payload contradicts — in particular that it never
 * advertises writes on a server started in enforced read-only mode, and never claims a
 * human can be prompted on a client that did not advertise elicitation.
 */

const APPS = [
    {
        appKey: 'ARC',
        appName: 'ARC Beta 1.0',
        appId: 'a1',
        appFolder: '/k/ARC',
        readonly: true,
    },
    {
        appKey: 'GAP',
        appName: 'GAP-Track',
        appId: 'a2',
        appFolder: '/k/GAP',
        readonly: false,
        allowViewMutation: true,
    },
    {
        appKey: 'Spot',
        appName: 'Spot',
        appId: 'a3',
        appFolder: '/k/Spot',
        readonly: false,
    },
] as unknown as Parameters<typeof describeAppListForHumans>[0]['apps'];

const PROMPTS = {
    humanConfirmation: { available: true, client: 'codex-mcp-client 0.151.0' },
    cascadeDeleteBehaviour: {
        summary:
            'A mutation that would delete child pages is put to the user for confirmation.',
    },
};

const REFUSES = {
    humanConfirmation: { available: false, client: 'stub-client 1.0' },
    cascadeDeleteBehaviour: {
        summary:
            'No human can be prompted, so a mutation that would delete child pages is refused outright.',
    },
};

function banner(
    overrides: Partial<Parameters<typeof describeAppListForHumans>[0]> = {},
) {
    return describeAppListForHumans({
        knackAppsDir: '/k',
        activeAppKey: null,
        apps: APPS,
        enforcedReadOnly: false,
        ...PROMPTS,
        ...overrides,
    });
}

describe('describeAppListForHumans', () => {
    it('names the writable apps and the view-mutable subset', () => {
        const text = banner();
        assert.match(text, /Writable: GAP-Track, Spot\./);
        assert.match(text, /View mutation allowed: GAP-Track\./);
        assert.doesNotMatch(text, /Writable:[^.]*ARC Beta/);
    });

    it('never advertises writes in enforced read-only mode', () => {
        const text = banner({ enforcedReadOnly: true });
        assert.match(text, /Writes: none\./);
        assert.match(text, /enforced read-only mode/);
        assert.doesNotMatch(text, /Writable:/);
        assert.doesNotMatch(text, /View mutation allowed:/);
    });

    it('says a human is prompted when the client advertised elicitation', () => {
        const text = banner();
        assert.match(text, /Cascade deletes: a human is prompted\./);
        assert.match(
            text,
            /Client "codex-mcp-client 0\.151\.0" advertised MCP elicitation\./,
        );
        assert.ok(text.includes(PROMPTS.cascadeDeleteBehaviour.summary));
    });

    it('says refused when the client did not advertise elicitation', () => {
        const text = banner(REFUSES);
        assert.match(text, /Cascade deletes: refused\./);
        assert.match(text, /did not advertise MCP elicitation\./);
        assert.doesNotMatch(text, /a human is prompted/);
        assert.ok(text.includes(REFUSES.cascadeDeleteBehaviour.summary));
    });

    it('reuses the structured summary verbatim so the prose cannot drift', () => {
        const summary = 'Something entirely different happens here.';
        const text = banner({ cascadeDeleteBehaviour: { summary } });
        assert.ok(text.includes(summary));
    });

    it('falls back to a generic client label when the client is unknown', () => {
        const text = banner({
            humanConfirmation: { available: true, client: null },
        });
        assert.match(text, /This client advertised MCP elicitation\./);
    });

    it('reports the app count, folder and active app', () => {
        const text = banner({ activeAppKey: 'GAP' });
        assert.match(
            text,
            /Knack apps: 3 discovered in \/k\. Active app: GAP\./,
        );
        assert.match(banner(), /Active app: none\./);
    });

    it('reports no writable apps without claiming otherwise', () => {
        const readonlyOnly = [APPS[0]] as typeof APPS;
        const text = banner({ apps: readonlyOnly });
        assert.match(text, /Writable: none\. View mutation allowed: none\./);
    });
});

describe('listAppNames', () => {
    it('lists every name up to the limit', () => {
        assert.equal(listAppNames(['a', 'b', 'c'], 3), 'a, b, c');
    });

    it('truncates with a count of what was dropped', () => {
        assert.equal(listAppNames(['a', 'b', 'c', 'd'], 2), 'a, b +2 more');
    });

    it('reports an empty list as none', () => {
        assert.equal(listAppNames([]), 'none');
    });
});
