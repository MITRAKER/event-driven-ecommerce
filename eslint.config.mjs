// @ts-check
/**
 * ESLint configuration: typescript-eslint's strictest type-aware presets, plus the
 * NASA/JPL "Power of Ten" rules adapted to TypeScript. Every rule below is tagged with
 * the JPL rule it implements; docs/CODING_STANDARD.md explains each one.
 * Run with --max-warnings 0 (JPL rule 10: zero warnings).
 */
import eslint from '@eslint/js';
import { defineConfig } from 'eslint/config';
import jest from 'eslint-plugin-jest';
import globals from 'globals';
import tseslint from 'typescript-eslint';
import jpl from './tools/eslint-rules/index.mjs';

/** Syntax that the JPL rules forbid outright. */
const JPL_BANNED_SYNTAX = [
  { selector: 'LabeledStatement', message: 'JPL-1: no labels (goto-style jumps).' },
  { selector: 'WithStatement', message: 'JPL-1: no with statements.' },
  { selector: 'WhileStatement', message: 'JPL-2: use a for loop with a fixed upper bound instead of while.' },
  {
    selector: 'DoWhileStatement',
    message: 'JPL-2: use a for loop with a fixed upper bound instead of do/while.',
  },
  { selector: 'ForStatement[test=null]', message: 'JPL-2: every for loop needs a termination test.' },
  {
    selector: 'ForInStatement',
    message: 'JPL-2: use for...of over Object.keys()/entries() instead of for...in.',
  },
  { selector: 'Program > VariableDeclaration[kind="let"]', message: 'JPL-6: no module-level mutable state.' },
  {
    selector: 'PropertyDefinition > ArrowFunctionExpression',
    message: 'JPL-9: no function-valued class fields; write a method so the call graph stays static.',
  },
  { selector: 'ImportExpression', message: 'JPL-8: no dynamic import().' },
  { selector: 'TSEnumDeclaration', message: 'JPL-8: use a union of string literals instead of enum.' },
];

const JPL_RULES = {
  // JPL-1: simple control flow, no recursion
  'no-restricted-syntax': ['error', ...JPL_BANNED_SYNTAX],
  'jpl/no-recursion': 'error',
  complexity: ['error', 10],
  'max-depth': ['error', 3],
  'max-nested-callbacks': ['error', 3],
  'no-nested-ternary': 'error',
  // JPL-2: loops must terminate
  'no-constant-condition': ['error', { checkLoops: 'all' }],
  'no-unmodified-loop-condition': 'error',
  // JPL-4: short functions (one printed page), small files, few parameters
  'max-lines-per-function': ['error', { max: 60, skipBlankLines: true, skipComments: true }],
  'max-lines': ['error', { max: 300, skipBlankLines: true, skipComments: true }],
  'max-statements': ['error', 25],
  'max-params': 'off',
  '@typescript-eslint/max-params': ['error', { max: 4 }],
  // JPL-6: smallest possible scope
  'no-var': 'error',
  'prefer-const': 'error',
  'block-scoped-var': 'error',
  'no-param-reassign': 'error',
  'no-shadow': 'off',
  '@typescript-eslint/no-shadow': 'error',
  '@typescript-eslint/prefer-readonly': 'error',
  // JPL-7: check every return value, validate every parameter
  '@typescript-eslint/no-floating-promises': 'error',
  '@typescript-eslint/no-misused-promises': 'error',
  // No truthiness tricks: 0 and '' are falsy, so `if (count)` and `if (name)` hide bugs. Compare explicitly.
  '@typescript-eslint/strict-boolean-expressions': ['error', { allowNumber: false, allowString: false }],
  '@typescript-eslint/switch-exhaustiveness-check': ['error', { considerDefaultExhaustiveForUnions: true }],
  '@typescript-eslint/explicit-function-return-type': ['error', { allowExpressions: true }],
  '@typescript-eslint/return-await': ['error', 'always'],
  '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
  // JPL-8: no metaprogramming tricks (the TypeScript stand-in for the C preprocessor)
  'no-eval': 'error',
  'no-new-func': 'error',
  'no-proto': 'error',
  'no-extend-native': 'error',
  // JPL-9: no untyped indirection (any, non-null assertions); see strictTypeChecked for no-unsafe-*
  '@typescript-eslint/no-explicit-any': 'error',
  '@typescript-eslint/no-non-null-assertion': 'error',
};

export default defineConfig(
  { ignores: ['**/dist/**', '**/coverage/**', '**/node_modules/**'] },
  { linterOptions: { reportUnusedDisableDirectives: 'error' } },
  eslint.configs.recommended,
  tseslint.configs.strictTypeChecked,
  tseslint.configs.stylisticTypeChecked,
  {
    languageOptions: {
      parserOptions: { projectService: true, tsconfigRootDir: import.meta.dirname },
      globals: globals.node,
    },
    plugins: { jpl },
    rules: {
      ...JPL_RULES,
      // NestJS modules are empty classes carrying a decorator.
      '@typescript-eslint/no-extraneous-class': ['error', { allowWithDecorator: true }],
      '@typescript-eslint/restrict-template-expressions': ['error', { allowNumber: true }],
    },
  },
  {
    files: ['**/*.spec.ts'],
    ...jest.configs['flat/recommended'],
    rules: {
      ...jest.configs['flat/recommended'].rules,
      // Test files describe many cases in one callback; the JPL page limit targets production code.
      'max-lines-per-function': 'off',
      'max-statements': 'off',
      'max-nested-callbacks': ['error', 5],
      '@typescript-eslint/unbound-method': 'off',
      'jest/unbound-method': 'error',
    },
  },
  {
    files: ['**/*.mjs', '**/*.js'],
    extends: [tseslint.configs.disableTypeChecked],
    rules: { '@typescript-eslint/explicit-function-return-type': 'off' },
  },
);
