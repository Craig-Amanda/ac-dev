import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import eslintConfigPrettier from 'eslint-config-prettier';

export default tseslint.config(
    {
        // .claude/worktrees/ holds other sessions' checkouts, each with its own tsconfig.
        ignores: ['**/dist/**', '**/node_modules/**', '.claude/worktrees/**'],
    },
    js.configs.recommended,
    ...tseslint.configs.recommended.map((config) => ({
        ...config,
        files: ['knack-mcp-v2/src/**/*.ts'],
    })),
    {
        // Node ESM scripts run directly with `node`, not compiled.
        // .claude/ holds the Claude Code hooks, code map and skill scripts.
        files: ['knack-mcp-v2/scripts/**/*.mjs', '.claude/**/*.mjs'],
        languageOptions: {
            globals: {
                console: 'readonly',
                process: 'readonly',
                Buffer: 'readonly',
            },
        },
    },
    {
        files: ['Scripts/**/*.js'],
        languageOptions: {
            // Google Apps Script: sloppy-mode script, not a module.
            sourceType: 'script',
        },
    },
    {
        rules: {
            // Intentional best-effort cleanup (e.g. `try { x.close() } catch {}`).
            'no-empty': ['error', { allowEmptyCatch: true }],
            'prefer-regex-literals': 'error',
        },
    },
    eslintConfigPrettier,
);
