import { noRecursion } from './no-recursion.mjs';

/** Local ESLint plugin holding the project's NASA/JPL "Power of Ten" rules. */
export default {
  meta: { name: 'eslint-plugin-jpl', version: '1.0.0' },
  rules: { 'no-recursion': noRecursion },
};
