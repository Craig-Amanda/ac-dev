import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import { ALL_TOOLS } from './tools/index.js';

/**
 * The README and docs/FEATURES.html both enumerate the tool catalogue by hand. A tool
 * added without a row in each is how they went stale (#65 shipped with the README still
 * counting 53), so the catalogue itself is the fixture: every registered tool must be
 * named in both, and both must state the counts the registry actually has.
 */
const DOCS = [
    { label: 'README.md', path: '../README.md' },
    { label: 'docs/FEATURES.html', path: '../docs/FEATURES.html' },
].map((doc) => ({
    ...doc,
    text: readFileSync(new URL(doc.path, import.meta.url), 'utf8'),
}));

const fullCount = ALL_TOOLS.length;
// --readonly advertises the `read` level and nothing else.
const readOnlyCount = ALL_TOOLS.filter((tool) => tool.access === 'read').length;

/** Whole-name match: `knack_get_view` must not be satisfied by `knack_get_view_payload_template`. */
function namesTool(text: string, name: string): boolean {
    return new RegExp(`(?<![A-Za-z0-9_])${name}(?![A-Za-z0-9_])`).test(text);
}

describe('documentation drift', () => {
    for (const doc of DOCS) {
        it(`${doc.label} names every registered tool`, () => {
            const missing = ALL_TOOLS.map((tool) => tool.name).filter(
                (name) => !namesTool(doc.text, name),
            );
            assert.deepEqual(
                missing,
                [],
                `${doc.label} has no entry for: ${missing.join(', ')}. Add a row for each, then run npm run format.`,
            );
        });

        it(`${doc.label} names no tool the registry does not have`, () => {
            const registered = new Set(ALL_TOOLS.map((tool) => tool.name));
            // Only names inside code formatting count: prose may mention a legacy tool.
            const pattern =
                doc.label === 'README.md'
                    ? /^\| `(knack_[a-z_]+)`/gm
                    : /<td>\s*<code>(knack_[a-z_]+)<\/code>\s*<\/td>/g;
            const stale = [...doc.text.matchAll(pattern)]
                .map((match) => match[1])
                .filter((name) => !registered.has(name));
            assert.deepEqual(
                stale,
                [],
                `${doc.label} still lists: ${stale.join(', ')}. Remove or rename the row.`,
            );
        });
    }

    it('README.md states the catalogue counts', () => {
        const readme = DOCS[0].text;
        const stated = /(\d+) tools in full mode, (\d+) in read-only mode/.exec(
            readme,
        );
        assert.ok(stated, 'README.md no longer states the tool counts.');
        assert.deepEqual(
            { full: Number(stated[1]), readOnly: Number(stated[2]) },
            { full: fullCount, readOnly: readOnlyCount },
        );
    });

    it('docs/FEATURES.html states the catalogue counts', () => {
        const features = DOCS[1].text;
        const full = /<b>(\d+)<\/b>\s*<span>tools in full mode<\/span>/.exec(
            features,
        );
        const readOnly =
            /<b>(\d+)<\/b>\s*<span>tools in read-only mode<\/span>/.exec(
                features,
            );
        assert.ok(
            full && readOnly,
            'docs/FEATURES.html no longer states the tool counts.',
        );
        assert.deepEqual(
            { full: Number(full[1]), readOnly: Number(readOnly[1]) },
            { full: fullCount, readOnly: readOnlyCount },
        );
    });
});
