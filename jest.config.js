/**
 * One Jest run for the whole monorepo, so coverage is measured and enforced in one place.
 * Each workspace keeps its own "jest" block in package.json (used by `npm test -w <name>`).
 * @type {import('jest').Config}
 */
module.exports = {
  projects: ['<rootDir>/packages/*', '<rootDir>/services/*'],
  collectCoverageFrom: [
    'packages/*/src/**/*.ts',
    'services/*/src/**/*.ts',
    '!**/*.spec.ts',
    '!**/main.ts',
    '!**/app.module.ts',
    '!**/index.ts',
    '!**/testing.ts',
  ],
  coverageReporters: ['text-summary', 'text', 'lcov', 'json-summary'],
  coverageThreshold: {
    global: { statements: 80, branches: 70, functions: 80, lines: 80 },
  },
};
