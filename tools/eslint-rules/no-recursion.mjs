/**
 * JPL Power of Ten, rule 1: "no direct or indirect recursion".
 *
 * Flags a function that calls itself, by name (`function f() { f() }`, `const f = () => f()`)
 * or as a method (`method() { this.method() }`), including calls from nested callbacks.
 * Indirect recursion (a -> b -> a) needs a whole-program call graph, which a lint rule
 * cannot see; docs/CODING_STANDARD.md covers how that part is checked in review.
 */

/** @param {import('estree').Node & { parent?: any }} node */
function functionName(node) {
  if (node.type === 'FunctionDeclaration' && node.id) return { kind: 'plain', name: node.id.name };
  const parent = node.parent;
  if (!parent) return null;
  if (parent.type === 'VariableDeclarator' && parent.id.type === 'Identifier') {
    return { kind: 'plain', name: parent.id.name };
  }
  if (node.type === 'FunctionExpression' && node.id) return { kind: 'plain', name: node.id.name };
  if (
    (parent.type === 'MethodDefinition' || parent.type === 'PropertyDefinition') &&
    parent.key.type === 'Identifier'
  ) {
    return { kind: 'method', name: parent.key.name };
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
    /** @type {({ kind: string, name: string } | null)[]} */
    const stack = [];
    const enter = (node) => stack.push(functionName(node));
    const exit = () => stack.pop();

    /** @param {any} callee */
    function calledName(callee) {
      if (callee.type === 'Identifier') return { kind: 'plain', name: callee.name };
      if (
        callee.type === 'MemberExpression' &&
        callee.object.type === 'ThisExpression' &&
        callee.property.type === 'Identifier'
      ) {
        return { kind: 'method', name: callee.property.name };
      }
      return null;
    }

    return {
      FunctionDeclaration: enter,
      'FunctionDeclaration:exit': exit,
      FunctionExpression: enter,
      'FunctionExpression:exit': exit,
      ArrowFunctionExpression: enter,
      'ArrowFunctionExpression:exit': exit,
      CallExpression(node) {
        const called = calledName(node.callee);
        if (!called) return;
        const hit = stack.some((fn) => fn !== null && fn.kind === called.kind && fn.name === called.name);
        if (hit) context.report({ node, messageId: 'recursion', data: { name: called.name } });
      },
    };
  },
};
