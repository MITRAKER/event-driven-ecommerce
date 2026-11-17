/**
 * Tests for our JPL-1 lint rule, run with Node's built-in test runner: `npm run test:tools`.
 * RuleTester lints each snippet with the rule alone and checks exactly which errors it reports.
 */
import { describe, it } from 'node:test';
import { RuleTester } from 'eslint';
import { noRecursion } from './no-recursion.mjs';

// RuleTester needs a describe/it pair; hand it node:test's.
RuleTester.describe = describe;
RuleTester.it = it;

const tester = new RuleTester({ languageOptions: { ecmaVersion: 2022, sourceType: 'module' } });

/** The error RuleTester should see for a function `name` that calls itself. */
const callsItself = (name) => ({ messageId: 'recursion', data: { name } });

tester.run('jpl/no-recursion', noRecursion, {
  valid: [
    // The bounded loop that the rule asks for instead.
    'function sum(n) { let total = 0; for (let i = 0; i < n; i += 1) total += i; return total; }',
    // A call to a different function, even one declared inside.
    'function outer() { function inner() { return 1; } return inner(); }',
    // Same method name on another object, or on the parent class: not a call to itself.
    'class Tree { size(other) { return other.size(); } }',
    'class Child extends Base { handle(e) { return super.handle(e); } }',
    // Known limit, kept visible on purpose: indirect recursion (a -> b -> a) needs a whole-program
    // call graph, which a lint rule cannot build. Code review checks it (docs/CODING_STANDARD.md).
    'function a() { return b(); } function b() { return a(); }',
  ],
  invalid: [
    {
      code: 'function factorial(n) { return n <= 1 ? 1 : n * factorial(n - 1); }',
      errors: [callsItself('factorial')],
    },
    {
      code: 'const countdown = (n) => (n === 0 ? 0 : countdown(n - 1));',
      errors: [callsItself('countdown')],
    },
    {
      // A named function expression can call itself by either name.
      code: 'const run = function loop(n) { return n === 0 ? 0 : loop(n - 1) + run(n - 1); };',
      errors: [callsItself('loop'), callsItself('run')],
    },
    {
      code: 'class Tree { walk(node) { return this.walk(node.left); } }',
      errors: [callsItself('walk')],
    },
    {
      // Still recursion when the call sits inside a callback nested in the method.
      code: 'class Tree { walk(node) { node.children.forEach((child) => this.walk(child)); } }',
      errors: [callsItself('walk')],
    },
    {
      code: 'const tree = { walk(node) { return this.walk(node.left); } };',
      errors: [callsItself('walk')],
    },
  ],
});
