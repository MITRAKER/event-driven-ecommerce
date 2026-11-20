/*
 * Jest settings shared by every workspace. Each workspace's "jest" block in its package.json names
 * this file as its preset, so `npm test -w <name>` and the root run (jest.config.js) use the same
 * transforms. Jest reads the preset's path from the block's rootDir (src/), hence ../../../.
 *
 * The tests run as CommonJS, like the compiled services. Some dependencies are published only as
 * ES modules (NestJS 12 and later). Node.js loads those with require() at runtime, but Jest's own
 * module system can't, so Jest compiles them to CommonJS first.
 */

/** Packages published only as ES modules, as a regex alternative ('@nestjs|another-package'). */
const ESM_ONLY_PACKAGES = '@nestjs';
/** A path separator: Jest matches transform patterns against the path as the OS writes it. */
const SEP = '[/\\\\]';

/** @type {import('jest').Config} */
module.exports = {
  testEnvironment: 'node',
  transform: {
    // Our TypeScript, compiled and type-checked by ts-jest (what `preset: 'ts-jest'` sets up).
    '^.+\\.tsx?$': ['ts-jest', {}],
    // The ES-module packages: import/export and import() become require(), and import.meta.url,
    // which CommonJS doesn't have, becomes the file's URL.
    [`${SEP}node_modules${SEP}(${ESM_ONLY_PACKAGES})${SEP}.+\\.js$`]: [
      'babel-jest',
      {
        babelrc: false,
        configFile: false,
        plugins: [
          'babel-plugin-transform-import-meta',
          '@babel/plugin-transform-dynamic-import',
          '@babel/plugin-transform-modules-commonjs',
        ],
      },
    ],
  },
  // Jest leaves node_modules untransformed, except the packages above.
  transformIgnorePatterns: [`/node_modules/(?!(${ESM_ONLY_PACKAGES})/)`],
};
