# Coding standard: the NASA/JPL "Power of Ten", adapted to TypeScript

This repository follows **"The Power of Ten: Rules for Developing Safety-Critical Code"**, published in 2006 by Gerard J. Holzmann of NASA's Jet Propulsion Laboratory (JPL). The rules were written for C code that flies on spacecraft. They are short enough to remember and, more importantly, **strict enough that a tool can check most of them**. Code that follows them stays simple enough for a reviewer to understand and for an analyser to reason about.

Our code runs on Node.js, a garbage-collected runtime, not on a flight computer, so some rules translate directly, some need adapting, and one (rule 3) only survives in spirit. For each rule this document gives:

- **The rule**: Holzmann's original, in one or two sentences.
- **Why**: the problem it prevents.
- **Our adaptation**: what it means for TypeScript in this repo.
- **Enforced by**: the exact compiler flag, ESLint rule, runtime check or CI job that catches a violation, with file paths.
- **Not checked by tools**: what is left to code review. Being honest about this matters: a standard that claims more automation than it has teaches people to stop looking.

Where to find the machinery:

| What                                | File                                                                                                                        |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Compiler flags                      | [`tsconfig.base.json`](../tsconfig.base.json) (every workspace extends it)                                                  |
| Lint rules, each tagged `JPL-n`     | [`eslint.config.mjs`](../eslint.config.mjs) (`JPL_BANNED_SYNTAX` and `JPL_RULES`)                                           |
| Custom rule: no recursion           | [`tools/eslint-rules/no-recursion.mjs`](../tools/eslint-rules/no-recursion.mjs)                                             |
| Assertions                          | [`packages/contracts/src/invariant.ts`](../packages/contracts/src/invariant.ts) (`invariant()`, `assertNever()`)            |
| Business bounds                     | [`packages/contracts/src/limits.ts`](../packages/contracts/src/limits.ts) (`Limits`)                                        |
| Kafka message boundary              | [`packages/contracts/src/parse-event.ts`](../packages/contracts/src/parse-event.ts) (`parseEvent()`, `InvalidEventError`)   |
| Environment boundary                | [`packages/messaging/src/config.ts`](../packages/messaging/src/config.ts) (`loadMessagingSettings()`)                       |
| Bounded retries and failure classes | [`packages/messaging/src/retry.ts`](../packages/messaging/src/retry.ts), [`errors.ts`](../packages/messaging/src/errors.ts) |
| Bounded batches                     | [`packages/messaging/src/outbox.relay.ts`](../packages/messaging/src/outbox.relay.ts)                                       |
| Bounded connections and waits       | [`packages/messaging/src/database.ts`](../packages/messaging/src/database.ts)                                               |
| HTTP body limit                     | `services/*/src/main.ts` (`app.useBodyParser('json', { limit: '64kb' })`)                                                   |
| Everything at once, like CI         | `npm run check` (see [`CONTRIBUTING.md`](../CONTRIBUTING.md))                                                               |

Every lint message produced by our own configuration starts with `JPL-n:`, so when the linter stops you, the message tells you which rule below to read.

---

## Rule 1: Simple control flow

> **Original:** Restrict all code to very simple control flow constructs: do not use `goto`, `setjmp` or `longjmp`, and do not use direct or indirect recursion.

**Why.** Simple control flow gives an acyclic call graph and code that a reviewer and an analyser can follow from top to bottom. Recursion makes stack depth depend on the input: one unexpectedly deep input overflows the stack, and no tool can prove a bound on it.

**Our adaptation.** TypeScript has no `goto`, but it has its relatives: **labels** (`break outer;`, `continue outer;`) and the `with` statement, which changes what a name means at runtime. Both are banned, and so is recursion. We also keep each function's branching shallow, because deeply nested code is where control flow stops being "simple".

Exceptions (`throw` / `try` / `catch`) are a non-local jump too, and we keep them, because Node.js libraries (pg, kafkajs, NestJS) report failures that way. We make them predictable instead: there is one recovery path for each kind of failure (see rule 5 and `classifyFailure()` in [`errors.ts`](../packages/messaging/src/errors.ts)).

**Enforced by.**

- ESLint `no-restricted-syntax` with the selectors `LabeledStatement` and `WithStatement` (`JPL_BANNED_SYNTAX` in [`eslint.config.mjs`](../eslint.config.mjs)). The compiler also rejects unused labels (`allowUnusedLabels: false` in `tsconfig.base.json`).
- ESLint `jpl/no-recursion`, our own rule in [`tools/eslint-rules/no-recursion.mjs`](../tools/eslint-rules/no-recursion.mjs). It reports a function that calls itself by name (`function f() { f() }`, `const f = () => f()`) or a method that calls itself through `this` (`m() { this.m() }`), including calls made from callbacks nested inside it. A function is known by every name it can call itself by, so `const g = function f() {}` is caught calling either `f` or `g`. The rule has its own tests in [`no-recursion.test.mjs`](../tools/eslint-rules/no-recursion.test.mjs) (`npm run test:tools`).
- ESLint `complexity` (at most 10 paths through a function), `max-depth` (at most 3 nested blocks), `max-nested-callbacks` (at most 3) and `no-nested-ternary`.

**Not checked by tools.** **Indirect recursion** (`a()` calls `b()`, which calls `a()`) needs a whole-program call graph, which a lint rule working one file at a time cannot see. In review, when a function calls another function in the same module or class, check that the callee does not lead back to the caller. Mutually recursive data structures (trees, nested JSON) are the usual temptation: walk them with a bounded loop and an explicit stack instead.

---

## Rule 2: Every loop has a fixed upper bound

> **Original:** All loops must have a fixed upper bound. It must be trivially possible for a checking tool to prove statically that a preset upper bound on the number of iterations of a loop cannot be exceeded.

**Why.** A loop that might not terminate is a hang waiting for the right input. On a server, a hung handler holds a database connection, blocks a Kafka partition and looks healthy to every health check.

**Our adaptation.**

- No `while`, no `do ... while`, no `for (;;)`. Use a `for` loop with a counter and a limit, or `for ... of` over a collection whose size is already bounded.
- No `for ... in` (it also walks inherited properties). Use `for ... of` over `Object.keys()` or `Object.entries()`.
- Array methods (`map`, `filter`, `some`, ...) are bounded by the array's length. That is only a real bound if the array's length is bounded, which is why every array that comes from outside is checked against `Limits` first (rule 3). `parseEvent()` checks that `items` has 1 to 50 entries _before_ it maps over them.
- **Retries are loops too.** Every retry has a maximum number of attempts.
- **Polling is a loop too.** Every poll reads a bounded batch.

**The allowed "forever" loops.** Holzmann's rule makes an exception for loops that are meant not to terminate, such as a process scheduler. A long-running service has two:

- the **outbox relay's timer** (`setInterval` in [`outbox.relay.ts`](../packages/messaging/src/outbox.relay.ts)), and
- the **Kafka consumer loop** (`consumer.run()` in [`event.consumer.ts`](../packages/messaging/src/event.consumer.ts)).

Both run until shutdown, and both stop cleanly in `onModuleDestroy`. What must be bounded is the work **inside each iteration**: one relay tick publishes at most `OUTBOX_BATCH_SIZE` rows, and one Kafka message gets at most `CONSUMER_RETRY_ATTEMPTS` tries. The same is true of an infrastructure failure: when the database is down, the consumer gives up after its bounded retries and throws `RedeliverLaterError`, and Kafka offers the message again later. The waiting can last as long as the outage, but each attempt is bounded and the consumer is never stuck inside one.

**Enforced by.**

- ESLint `no-restricted-syntax` with the selectors `WhileStatement`, `DoWhileStatement`, `ForStatement[test=null]` (a `for` with no termination test) and `ForInStatement`.
- ESLint `no-constant-condition` with `checkLoops: 'all'` (no `for (; true; )`) and `no-unmodified-loop-condition` (a loop whose condition variables never change).
- Runtime bounds, each checked when the service starts or the function is called:
  - `withRetry()` in [`retry.ts`](../packages/messaging/src/retry.ts) asserts `1 <= attempts <= 10` (`MAX_ATTEMPTS`) with `invariant()` before it loops.
  - `CONSUMER_RETRY_ATTEMPTS` must be in 1..10 and `OUTBOX_BATCH_SIZE` in 1..1000 (`loadMessagingSettings()` in [`config.ts`](../packages/messaging/src/config.ts)).
  - The relay's `SELECT ... LIMIT $1`, followed by `invariant(rows.length <= batchSize, ...)` in [`outbox.relay.ts`](../packages/messaging/src/outbox.relay.ts).
  - kafkajs' own retries are configured with explicit limits in [`kafka.ts`](../packages/messaging/src/kafka.ts) (`retries: 8` with a `maxRetryTime` while connecting, `retries: 5` for producers).

**Not checked by tools.** A `for` loop whose limit is itself unbounded (`for (let i = 0; i < input.length; i++)` where `input` was never checked) passes every lint rule. In review, ask of every loop: _what is the largest value its limit can have, and where is that enforced?_

---

## Rule 3: No dynamic memory allocation after initialization

> **Original:** Do not use dynamic memory allocation after initialization.

**Why.** In C, `malloc` and `free` after start-up bring fragmentation, leaks, use-after-free bugs and allocations that can fail at any moment. Allocating everything up front makes the program's memory use a fixed, known number.

**Our adaptation.** This rule has **no direct equivalent** in a garbage-collected language. JavaScript allocates on almost every line (a string, an object literal, a closure), and the garbage collector frees memory for us, so use-after-free cannot happen. What survives is the _intent_: **the resources a service uses must have a known upper bound, whatever the input.** In practice that means bounded inputs, bounded batches, bounded pools and bounded waits:

| Resource                        | Bound                                                                             | Where                                                                                                              |
| ------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| HTTP request body               | 64 kb, refused before parsing                                                     | `app.useBodyParser('json', { limit: '64kb' })` in each service's `src/main.ts`                                     |
| Items, quantities, prices, text | 50 items, quantity 1..1000, price ≤ 10,000,000 cents, sku/customer ids ≤ 64 chars | `Limits` in [`limits.ts`](../packages/contracts/src/limits.ts), applied by DTOs and `parseEvent()`                 |
| Events per order (read model)   | 32 (`Limits.maxEventsPerOrder`), so the timeline cannot grow without limit        | `Limits`, checked in the order-query-service projection                                                            |
| Database connections            | `DATABASE_POOL_MAX`, 1..50 (default 10)                                           | `max` on the `pg` pool in [`database.ts`](../packages/messaging/src/database.ts), range-checked in `config.ts`     |
| Waiting for the database        | 5 s to get a connection, 10 s per statement                                       | `connectionTimeoutMillis` and `statement_timeout` in [`database.ts`](../packages/messaging/src/database.ts)        |
| Outbox batch                    | `OUTBOX_BATCH_SIZE`, 1..1000 (default 100)                                        | [`outbox.relay.ts`](../packages/messaging/src/outbox.relay.ts), [`config.ts`](../packages/messaging/src/config.ts) |
| Retries                         | `CONSUMER_RETRY_ATTEMPTS`, 1..10 (default 5)                                      | [`retry.ts`](../packages/messaging/src/retry.ts), [`config.ts`](../packages/messaging/src/config.ts)               |
| Kafka brokers in the config     | 1..16                                                                             | [`config.ts`](../packages/messaging/src/config.ts)                                                                 |

A message that breaks a bound is not "handled somehow": an HTTP request gets a 400 (or 413 for a large body), and a Kafka event fails `parseEvent()` and goes to the dead-letter topic.

**Enforced by.** Runtime checks only: `Limits` (applied by the class-validator DTOs and by `parseEvent()`), range-checked settings in `loadMessagingSettings()`, the pool configuration and the body-parser limit. No lint rule exists for this one.

**Not checked by tools.** Whether a new collection, cache or `Map` can grow without limit (for example a module-level `Map` used as a cache with no eviction), and whether a new query has a `LIMIT`. In review, ask of every new collection and every new `SELECT` that can return many rows: _what is its maximum size?_ If the answer depends on user input, add the bound to `Limits`.

---

## Rule 4: Functions fit on one printed page

> **Original:** No function should be longer than what can be printed on a single sheet of paper in a standard format with one line per statement and one line per declaration. Typically, this means no more than about 60 lines of code per function.

**Why.** A function you can see all at once is a function you can understand, test and review as one unit. Long functions hide bugs in the middle.

**Our adaptation.** The same 60 lines, plus a few related limits that keep functions and files small.

**Enforced by.** ESLint, all at `error`:

- `max-lines-per-function`: 60 lines, not counting blank lines and comments.
- `max-statements`: 25 statements per function.
- `@typescript-eslint/max-params`: 4 parameters. More than that usually means the function does too much, or the parameters belong in one object.
- `max-lines`: 300 lines per file, not counting blank lines and comments.

Test files (`*.spec.ts`) are exempt from `max-lines-per-function` and `max-statements`, because a `describe` callback holds many test cases; the rule targets production code.

**Not checked by tools.** Whether each function does _one_ thing. A 50-line function can still mix parsing, business rules and SQL. The pattern we use is "functional core, imperative shell": business decisions are small pure functions (`nextState`, `decideReservation`, `charge`, `project`) and the code around them only does I/O.

---

## Rule 5: At least two assertions per function, on average

> **Original:** The assertion density of the code should average a minimum of two assertions per function. Assertions must be side-effect free, and when one fails, an explicit recovery action must be taken.

**Why.** Assertions catch the moment the program's state stops matching what the programmer believed, at the line where it happened, instead of three services later as a corrupt row.

**Our adaptation.** `invariant(condition, message)` in [`invariant.ts`](../packages/contracts/src/invariant.ts):

- It is **side-effect free**: it only reads `condition`.
- It **narrows types**: it is declared `asserts condition`, so the compiler knows the condition holds after the call.
- Its **recovery action is explicit and the same everywhere**: it throws `InvariantError`. In an HTTP request the response is a 500 and the transaction rolls back. In a Kafka consumer, the transaction rolls back and `classifyFailure()` marks the error `permanent`, so the event goes straight to the dead-letter topic with the assertion's message. Nothing ever continues on top of a broken assumption.

`assertNever(value, context)` is its partner for the impossible `default:` branch of an exhaustive `switch`.

Typical assertions in this repo: every `UPDATE` or `INSERT` checks its `rowCount`, `withRetry()` checks its own arguments, the relay checks its batch size, `project()` checks that an event belongs to the view it updates, and `charge()` checks that the amount is a whole number of cents within `Limits`.

**Assertion or validation?** An assertion is for something that **cannot** happen unless there is a bug. Something that **can** happen (a client sends quantity 0) is _validation_ and gets a proper answer such as HTTP 400. Don't use `invariant()` for user input.

**Enforced by.** The helpers exist and are tested (`packages/contracts/src/invariant.spec.ts`), and the type system checks they are used correctly. That is all.

**Not checked by tools.** **Assertion density.** A tool can count `invariant(` calls, but not whether they check anything meaningful (`invariant(true, '...')` would satisfy a counter). We don't enforce "two per function" mechanically. In review, ask of each new function: _what does it assume, and is the important assumption stated?_ Writes to the database must always check their row count.

---

## Rule 6: Declare data in the smallest possible scope

> **Original:** Data objects must be declared at the smallest possible level of scope.

**Why.** The fewer places that can change a value, the fewer places a bug can come from. A variable that only exists inside one block can't be corrupted by code elsewhere.

**Our adaptation.**

- `const` by default, `let` only when the value really changes, never `var` (its scope is the whole function, not the block).
- **No module-level `let`**: mutable state lives in a class instance that Nest creates and destroys, not in a global.
- No shadowing: an inner variable may not reuse an outer name.
- Class fields that are never reassigned are `readonly`; parameters are never reassigned.
- Configuration is read **once**, in `config.ts`, and injected (`MessagingConfig`). Classes do not read `process.env` themselves.

**Enforced by.**

- ESLint `no-var`, `prefer-const`, `block-scoped-var`, `no-param-reassign`, `@typescript-eslint/no-shadow` and `@typescript-eslint/prefer-readonly`.
- ESLint `no-restricted-syntax` with the selector `Program > VariableDeclaration[kind="let"]` (no `let` at module level).
- Compiler flags `noUnusedLocals` and `noUnusedParameters` (a declaration nobody uses is scope wasted).
- `noPropertyAccessFromIndexSignature` forces `env['DATABASE_URL']` instead of `env.DATABASE_URL`, which made scattered environment reads awkward enough that they all moved into `config.ts`.

**Not checked by tools.** A module-level `const` can still hold a mutable object (`const cache = new Map()`), which is global state in disguise. In review, treat a module-level `Map`, `Set`, array or object that is changed after start-up like a module-level `let`.

---

## Rule 7: Check every return value and every parameter

> **Original:** The return value of non-void functions must be checked by each calling function, and the validity of parameters must be checked inside each function.

**Why.** An ignored error code is a failure that nobody hears about. An unchecked parameter lets bad data travel deep into the system before anything notices.

**Our adaptation.** In Node.js the most dangerous ignored return value is a **promise**: an `async` call without `await` runs out of order, and its error goes nowhere. For parameters, we check untrusted data **once, at the boundary**, and turn it into a type that can only hold valid data ("parse, don't validate"). There are three boundaries:

- **Kafka messages**: `parseEvent(raw: unknown): ShopEvent` in [`parse-event.ts`](../packages/contracts/src/parse-event.ts) checks every field (known event type, version, UUID, ISO date, matching order ids, payload shape, `Limits` on items) and returns a typed event or throws `InvalidEventError`. The consumer base class ([`event.consumer.ts`](../packages/messaging/src/event.consumer.ts)) calls it on every message before any handler sees it, and dead-letters what fails.
- **HTTP bodies**: class-validator DTOs (for example `services/order-service/src/orders/create-order.dto.ts`) use the bounds from `Limits`, and `ValidationPipe({ whitelist: true, forbidNonWhitelisted: true })` turns an unknown field into a 400.
- **Environment variables**: `loadMessagingSettings()` in [`config.ts`](../packages/messaging/src/config.ts) reads every setting once at startup and checks its type and range, so a typo stops the service at once with a message like `config: OUTBOX_BATCH_SIZE must be in 1..1000, got 10000`.

Inside the system, the important return values are database results: every write checks `rowCount` with `invariant()`.

**Enforced by.**

- ESLint `@typescript-eslint/no-floating-promises` (a promise must be awaited, returned, or explicitly discarded with `void`) and `@typescript-eslint/no-misused-promises` (no `async` function where a plain callback is expected).
- ESLint `@typescript-eslint/return-await` (`always`), so a rejected promise is caught by the surrounding `try` and appears in the stack trace.
- ESLint `@typescript-eslint/strict-boolean-expressions`: `if (count)` is rejected for numbers and nullable strings, because `0` and `''` are falsy. Write the comparison you mean.
- ESLint `@typescript-eslint/switch-exhaustiveness-check`: add an event type to the `ShopEvent` union, and every `switch` that forgets it fails the build.
- ESLint `@typescript-eslint/explicit-function-return-type` and `@typescript-eslint/no-unused-vars`.
- Compiler flags `strict`, `noImplicitReturns`, `noFallthroughCasesInSwitch` and `noUncheckedIndexedAccess` (`items[0]` has type `Item | undefined`, so you must handle the missing case).
- Runtime: `parseEvent()`, the DTOs with `ValidationPipe`, and `loadMessagingSettings()`.

**Not checked by tools.** TypeScript does not complain when you ignore the result of a synchronous function (`array.filter(...)` on its own line does nothing). Also, the rule asks every function to check its parameters; we deliberately check them once at the boundary and rely on the types inside, so a function that receives a `ShopEvent` does not re-validate it. In review, check that new external input goes through one of the three boundaries above, and that nothing builds a typed value from raw input with a cast (`as`) instead of a parser.

---

## Rule 8: Limit the preprocessor

> **Original:** The use of the preprocessor must be limited to the inclusion of header files and simple macro definitions. Token pasting, variable argument lists and recursive macro calls are not allowed, and conditional compilation should be kept to a minimum.

**Why.** Macros produce code that nobody reads before it runs, and that analysers and reviewers see differently from the compiler. Each conditional-compilation flag doubles the number of program versions that need testing.

**Our adaptation.** TypeScript has no preprocessor, so the rule becomes: **no code that writes or loads code at runtime.** What runs is what is in the repository, and every import is visible to the compiler.

- No `eval`, no `new Function(...)`, no string passed to `setTimeout`.
- No dynamic `import()`: every dependency is a static `import` at the top of the file.
- No tampering with built-ins: no `__proto__`, no adding methods to `Array.prototype` and friends.
- No `enum`: a union of string literals (`'PENDING' | 'CONFIRMED' | 'CANCELLED'`) is plain data with no generated runtime object, and works with exhaustive `switch` checks.
- Behaviour that differs between environments comes from validated settings in `config.ts`, not from code paths that are switched on and off.

**Enforced by.** ESLint `no-eval`, `no-new-func`, `@typescript-eslint/no-implied-eval` (from `strictTypeChecked`), `no-proto` and `no-extend-native`, and `no-restricted-syntax` with the selectors `ImportExpression` (dynamic `import()`) and `TSEnumDeclaration`.

**Not checked by tools.** NestJS relies on **decorators** and `reflect-metadata` (`experimentalDecorators` and `emitDecoratorMetadata` in `tsconfig.base.json`) for dependency injection, routing and validation. That is metaprogramming, and we accept it: decorators are fixed at the point where a class is declared, they are visible in the source, and they are the framework's standard way of working. In review, don't write custom decorators or use `Reflect` directly unless there is no plainer way.

---

## Rule 9: Restrict the use of pointers

> **Original:** The use of pointers should be restricted. Specifically, no more than one level of dereferencing is allowed. Function pointers are not permitted.

**Why.** Pointers make it hard for a tool to know what data a line of code touches. Function pointers make it impossible to know, by reading the code, which function a call will run, so the call graph can't be checked (for example, for recursion).

**Our adaptation.** JavaScript has references, not pointers, and no pointer arithmetic. The equivalent dangers are **values whose type the compiler can no longer see** and **calls whose target is chosen at runtime**:

- **No `any`.** An `any` switches the type checker off for everything it touches. Untrusted data is `unknown` until it is parsed (rule 7).
- **No non-null assertions** (`value!`). They are a promise to the compiler that nobody checks. Handle `undefined` instead, or state the fact with `invariant()`.
- **No function tables.** Event handlers are not looked up in an object like `{ OrderCreated: onCreated, ... }`. Each consumer lists the types it accepts in `handledTypes` and dispatches with a plain `switch (event.eventType)` in `handle()`, so the call graph is static and `switch-exhaustiveness-check` covers it. See `EventConsumer` in [`event.consumer.ts`](../packages/messaging/src/event.consumer.ts) and its subclasses in `services/*/src`.
- **No arrow-function class fields** (`handle = async (e) => { ... }`). A field holding a function can be replaced at runtime; a method is part of the class's fixed shape and is visible to the recursion check.

Callbacks are still allowed where the language and libraries require them (array methods, `withRetry(fn)`, kafkajs' `eachMessage`, Nest's `useFactory`). They are written inline at the call site, so a reader can see exactly what will run.

**Enforced by.**

- ESLint `@typescript-eslint/no-explicit-any`, the `@typescript-eslint/no-unsafe-*` rules from `strictTypeChecked` (`no-unsafe-assignment`, `no-unsafe-call`, `no-unsafe-member-access`, `no-unsafe-return`, `no-unsafe-argument`), and `@typescript-eslint/no-non-null-assertion`.
- ESLint `no-restricted-syntax` with the selector `PropertyDefinition > ArrowFunctionExpression`.
- Compiler flags `strict` (including `strictNullChecks`) and `noUncheckedIndexedAccess`, the TypeScript cousins of "never dereference something that might not be there".

**Not checked by tools.** A table of functions in a plain object or `Map` still passes the linter. So does a type assertion with `as`, which tells the compiler to trust you. In review, reject handler maps (use a `switch`), and accept `as` only right after a check that proves it, as at the end of `parseEvent()`.

---

## Rule 10: All warnings on, zero warnings, analysers every day

> **Original:** All code must be compiled, from the first day of development, with all compiler warnings enabled at the compiler's most pedantic setting. All code must compile with these settings without any warnings. All code must be checked daily with at least one, but preferably more than one, state-of-the-art static source code analyzer and should pass the analyses with zero warnings.

**Why.** A warning that nobody fixes becomes noise, and noise hides the next real problem. Analysers find whole classes of bugs that tests miss, and they improve over time, so code that was clean last month may not be clean today.

**Our adaptation.**

- **The strictest compiler settings** in [`tsconfig.base.json`](../tsconfig.base.json): `strict` plus `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`, `noPropertyAccessFromIndexSignature`, `noImplicitReturns`, `noImplicitOverride`, `noFallthroughCasesInSwitch`, `noUnusedLocals`, `noUnusedParameters`, `allowUnreachableCode: false` and `allowUnusedLabels: false`. TypeScript has no "warning" level here: each of these is an error.
- **The strictest type-aware linting**: typescript-eslint's `strictTypeChecked` and `stylisticTypeChecked` presets, plus the JPL rules above.
- **Zero warnings**: ESLint runs with `--max-warnings 0` (the `lint` script in `package.json`), so a warning fails exactly like an error.
- **No silent opt-outs**: `reportUnusedDisableDirectives: 'error'` makes a stale `// eslint-disable` comment an error itself. Today the code has none.
- **More than one analyser, every day**: `tsc`, ESLint and CodeQL on every pull request, and CodeQL again on a daily schedule.

**Enforced by.** The `quality` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) (format check, build, lint, type-check, tests with coverage) on every pull request, CodeQL in `.github/workflows/codeql.yml` on every pull request and daily, and the husky pre-commit hook (lint-staged runs ESLint with `--max-warnings 0` on staged files).

**Not checked by tools.** Whether a `// eslint-disable-next-line` or `// @ts-expect-error` is justified. Each one must carry a comment explaining why, and a reviewer must agree with it.

---

## Summary

| #   | Rule                                     | TypeScript adaptation                                                                                                              | Enforced by                                                                                                                                 |
| --- | ---------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Simple control flow, no recursion        | No labels, no `with`, no recursion, shallow nesting                                                                                | ESLint `no-restricted-syntax`, `jpl/no-recursion`, `complexity`, `max-depth`, `max-nested-callbacks`; review for indirect recursion         |
| 2   | Every loop has a fixed upper bound       | No `while`, `do…while`, `for(;;)` or `for…in`; bounded `for` loops and array methods; retries, batches and polling all have limits | ESLint `no-restricted-syntax`, `no-constant-condition`, `no-unmodified-loop-condition`; `withRetry` and the relay assert their bounds       |
| 3   | No dynamic allocation after start-up     | Every resource is bounded: inputs (`Limits`), body size, pool size, batch size, retries, timeouts                                  | `Limits`, range-checked config, 64 kb request bodies, pool `max` and timeouts; review                                                       |
| 4   | A function fits on one page              | ≤ 60 lines, ≤ 25 statements, ≤ 4 parameters, files ≤ 300 lines                                                                     | ESLint `max-lines-per-function`, `max-statements`, `@typescript-eslint/max-params`, `max-lines`                                             |
| 5   | Two assertions per function on average   | `invariant()` for "can't happen" facts, such as the row count of every write; `assertNever()` for impossible branches              | `invariant()` and code review (no tool can count meaningful assertions)                                                                     |
| 6   | Smallest possible scope                  | No module-level `let`, no `var`, `const` by default, no shadowing, `readonly` fields, config read once                             | ESLint `no-var`, `prefer-const`, `no-shadow`, `prefer-readonly`, `no-param-reassign`, `no-restricted-syntax`; `noUnusedLocals`              |
| 7   | Check every return value and parameter   | No forgotten promises; untrusted input parsed once at the boundary; every write checks its row count                               | ESLint `no-floating-promises`, `strict-boolean-expressions`, `switch-exhaustiveness-check`; `parseEvent`, DTO validation, config validation |
| 8   | Limit the preprocessor                   | No code that writes or loads code at runtime: no `eval`, `new Function`, dynamic `import()`; no `enum`                             | ESLint `no-eval`, `no-new-func`, `no-implied-eval`, `no-proto`, `no-extend-native`, `no-restricted-syntax`                                  |
| 9   | Restrict pointers, no function pointers  | No `any`, no non-null `!`, no tables of handler functions (dispatch with `switch`), no arrow-function class fields                 | ESLint `no-explicit-any`, `no-unsafe-*`, `no-non-null-assertion`, `no-restricted-syntax`; `strictNullChecks`, `noUncheckedIndexedAccess`    |
| 10  | All warnings, zero warnings, daily tools | The strictest `tsconfig`, type-aware ESLint with `--max-warnings 0`, CodeQL on every PR and every day                              | `tsconfig.base.json`, `eslint.config.mjs`, CI (`ci.yml`), `.github/workflows/codeql.yml`                                                    |

The two loops allowed to run forever, the outbox relay's timer and the Kafka consumer loop, run until shutdown; the work inside each iteration is bounded.

## The other quality gates

The Power of Ten is about the code itself. These gates surround it:

| Gate                   | What it checks                                                                                                                               | Where it runs                                                                  |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| **Prettier**           | One code format (`.prettierrc.json`), so diffs show only real changes                                                                        | `npm run format:check`, CI `quality` job, pre-commit hook                      |
| **Strict `tsc`**       | The compiler flags in rule 10, for every workspace                                                                                           | `npm run build`, `npm run typecheck`, CI `quality` job                         |
| **Jest with coverage** | Unit tests across all workspaces; fails below 90% statements, lines and functions or 85% branches, and CI fails if coverage measures 0 lines | `npm run test:cov` ([`jest.config.js`](../jest.config.js)), CI `quality` job   |
| **End-to-end test**    | The real system in Docker: saga, compensation, price check, read model                                                                       | `npm run test:e2e` ([`tests/e2e/e2e.mjs`](../tests/e2e/e2e.mjs)), CI `e2e` job |
| **CodeQL**             | GitHub's semantic security and quality analysis                                                                                              | `.github/workflows/codeql.yml`: every pull request and daily                   |
| **npm audit**          | Known high-severity vulnerabilities in the dependencies that ship (`--omit=dev`)                                                             | CI `audit` job                                                                 |
| **Dependency review**  | A pull request may not add or upgrade to a dependency version with a known high-severity vulnerability                                       | `.github/workflows/dependency-review.yml`                                      |
| **gitleaks**           | Secrets (passwords, API keys) anywhere in the git history                                                                                    | CI `secrets` job                                                               |
| **hadolint**           | Dockerfile mistakes, such as unpinned packages or running as root                                                                            | CI `docker-lint` job                                                           |
| **commitlint**         | Commit messages follow Conventional Commits ([`commitlint.config.mjs`](../commitlint.config.mjs))                                            | husky `commit-msg` hook                                                        |
| **Semantic PR titles** | The pull request title is a Conventional Commit, because squash merging turns it into the commit on `main`                                   | CI `pr-title` job                                                              |

`npm run check` runs the first three gates and the unit tests locally, in the same order as CI. [`CONTRIBUTING.md`](../CONTRIBUTING.md) explains each script.

## Further reading

- Gerard J. Holzmann, "The Power of Ten: Rules for Developing Safety-Critical Code", _IEEE Computer_, June 2006.
- [typescript-eslint: shared configs](https://typescript-eslint.io/users/configs) (`strictTypeChecked`, `stylisticTypeChecked`).
- [TypeScript: TSConfig reference](https://www.typescriptlang.org/tsconfig) (each flag above, with examples).
- Alexis King, ["Parse, don't validate"](https://lexi-lambda.github.io/blog/2019/11/05/parse-don-t-validate/) (the idea behind `parseEvent()`).
