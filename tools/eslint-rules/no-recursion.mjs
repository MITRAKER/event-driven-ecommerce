/**
 * JPL Power of Ten, rule 1: "no direct or indirect recursion".
 *
 * Flags a function that calls itself, by name (`function f() { f() }`, `const f = () => f()`)
 * or as a method (`method() { this.method() }`), including calls from nested callbacks.
 * Indirect recursion (a -> b -> a) needs a whole-program call graph, which a lint rule
 * cannot see; docs/CODING_STANDARD.md covers how that part is checked in review.
 * Tests: no-recursion.test.mjs, run by `npm run test:tools`.
 */

/** @typedef {{ kind: 'plain' | 'method', name: string }} SelfName */

/** `function f() {}` or `const g = function f() {}`: the name the function gives itself. */
function ownName(/** @type {any} */ node) {
  return node.id ? [{ kind: 'plain', name: node.id.name }] : [];
}

/** `const f = () => {}`: the variable that holds the function. */
function bindingName(/** @type {any} */ parent) {
  const isBinding = parent.type === 'VariableDeclarator' && parent.id.type === 'Identifier';
  return isBinding ? [{ kind: 'plain', name: parent.id.name }] : [];
}

/** `method() {}` in a class or an object literal: the member that holds the function. */
function memberName(/** @type {any} */ parent) {
  const isMember = ['MethodDefinition', 'PropertyDefinition', 'Property'].includes(parent.type);
  const named = isMember && parent.key.type === 'Identifier';
  return named ? [{ kind: 'method', name: parent.key.name }] : [];
}

/**
 * Every name a function can call itself by. `const g = function f() {}` has two: `f` and `g`.
 * @returns {SelfName[]}
 */
function selfNames(/** @type {any} */ node) {
  const parent = node.parent;
  if (!parent) return ownName(node);
  return [...ownName(node), ...bindingName(parent), ...memberName(parent)];
}

/**
 * The name a call goes to: `f()` is a plain call, `this.m()` a method call. Anything else
 * (`other.m()`, `super.m()`, `fns[0]()`) cannot be a call to the enclosing function.
 * @returns {SelfName | null}
 */
function calledName(/** @type {any} */ callee) {
  if (callee.type === 'Identifier') return { kind: 'plain', name: callee.name };
  const isThisMember = callee.type === 'MemberExpression' && callee.object.type === 'ThisExpression';
  if (isThisMember && callee.property.type === 'Identifier') {
    return { kind: 'method', name: callee.property.name };
  }
  return null;
}

/** @type {import('eslint').Rule.RuleModule} */
export const noRecursion = {
  meta: {
    type: 'problem',
    docs: { description: 'Disallow functions that call themselves (JPL rule 1)' },
    schema: [],
    messages: {
      recursion: "JPL-1: '{{name}}' calls itself. Use a bounded loop instead of recursion.",
    },
  },
  create(context) {
    /** The names of every function we are inside, innermost last. @type {SelfName[][]} */
    const stack = [];
    const enter = (node) => stack.push(selfNames(node));
    const exit = () => stack.pop();
    const isEnclosing = (/** @type {SelfName} */ called) =>
      stack.some((names) => names.some((fn) => fn.kind === called.kind && fn.name === called.name));

    return {
      FunctionDeclaration: enter,
      'FunctionDeclaration:exit': exit,
      FunctionExpression: enter,
      'FunctionExpression:exit': exit,
      ArrowFunctionExpression: enter,
      'ArrowFunctionExpression:exit': exit,
      CallExpression(node) {
        const called = calledName(node.callee);
        if (called !== null && isEnclosing(called)) {
          context.report({ node, messageId: 'recursion', data: { name: called.name } });
        }
      },
    };
  },
};
