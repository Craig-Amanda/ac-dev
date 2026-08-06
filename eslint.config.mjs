import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import eslintConfigPrettier from 'eslint-config-prettier';

export default tseslint.config(
    {
        ignores: ['**/dist/**', '**/node_modules/**'],
    },
    js.configs.recommended,
    ...tseslint.configs.recommended.map((config) => ({
        ...config,
        files: ['knack-mcp/src/**/*.ts'],
    })),
    {
        files: ['knack-mcp/src/**/*.ts'],
        languageOptions: {
            parserOptions: {
                project: './knack-mcp/tsconfig.json',
                tsconfigRootDir: import.meta.dirname,
            },
        },
        rules: {
            // The MCP SDK's tool-registration API is intentionally variadic
            // and untyped; wrapping it cleanly needs `any` rather than
            // fighting the SDK's own types.
            '@typescript-eslint/no-explicit-any': 'warn',
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
