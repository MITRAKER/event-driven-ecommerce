/**
 * One Jest run for the whole monorepo, so coverage is measured and enforced in one place.
 * Each workspace keeps its own "jest" block in package.json (used by `npm test -w <name>`), which
 * takes its transforms from jest.preset.js.
 * @type {import('jest').Config}
 */
module.exports = {
  projects: ['<rootDir>/packages/*', '<rootDir>/services/*'],
  // These globs are matched against paths relative to each project's rootDir, which is the
  // workspace's src/ folder (see the "jest" block in its package.json). A glob written from the
  // repository root, like 'services/*/src/**/*.ts', would match nothing, and Jest then reports
  // 0/0 lines as "Unknown%" and lets every threshold pass. CI fails the build if that happens.
  collectCoverageFrom: [
    '**/*.ts',
    '!**/*.spec.ts',
    '!**/main.ts',
    '!**/app.module.ts',
    '!**/index.ts',
    '!**/testing.ts',
  ],
  coverageReporters: ['text-summary', 'text', 'lcov', 'json-summary'],
  coverageThreshold: {
    global: { statements: 90, branches: 85, functions: 90, lines: 90 },
  },
};
