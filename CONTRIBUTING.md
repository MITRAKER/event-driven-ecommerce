# Contributing

Thanks for your interest in this project. It is a learning project, so questions, fixes and improvements to the explanations in [`LEARNING.md`](LEARNING.md) are all welcome.

By taking part you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md). Please report security problems privately, as described in [`SECURITY.md`](SECURITY.md), never in a public issue.

## Prerequisites

- **Node.js 22.13 or newer.** The exact major version is in [`.nvmrc`](.nvmrc), so `nvm use` picks it up. `package.json` declares the same range in `engines`.
- **Docker** with Docker Compose v2 (`docker compose ...`), for Kafka (Redpanda), Postgres and the end-to-end test.
- **git**.

## Setup

```bash
git clone https://github.com/MITRAKER/event-driven-ecommerce.git
cd event-driven-ecommerce
npm ci                                                   # exact versions from package-lock.json; also installs the git hooks
docker compose up -d redpanda redpanda-console postgres  # infrastructure only
npm run build                                            # compile every workspace
```

Then start the services, one terminal each:

```bash
npm run start:order       # http://localhost:3001
npm run start:inventory   # http://localhost:3002
npm run start:payment     # http://localhost:3003
npm run start:query       # http://localhost:3004
```

Each service creates its own tables on startup (see [Adding a migration](#adding-a-migration)). Redpanda Console, at http://localhost:8080, shows the events as they flow.

To run the whole system in Docker instead, and the end-to-end test against it:

```bash
docker compose up -d --build --wait
npm run test:e2e
docker compose down -v    # stop everything and delete the data
```

## Project layout

```
packages/contracts     event types, createEvent(), parseEvent(), invariant(), Limits
packages/messaging     outbox relay, inbox, EventConsumer base class, retry and DLQ, config, DB and Kafka wiring
services/order-service          POST /orders, GET /orders/:id, the saga (port 3001)
services/inventory-service      stock, prices, reservations (port 3002)
services/payment-service        (fake) payments (port 3003)
services/order-query-service    the CQRS read model (port 3004)
services/*/migrations  numbered .sql files, applied once each at startup
contracts/events       envelope JSON schema and the event catalog
tests/e2e              black-box end-to-end test against the system in Docker
tools/eslint-rules     our custom ESLint rule (no recursion)
docs                   the coding standard
.github                CI/CD workflows, Dependabot, issue forms, PR template, CODEOWNERS
```

This is an npm workspaces monorepo: run a script in one workspace with `-w`, for example `npm test -w @shop/inventory-service`.

## Quality gates

One command runs every check that CI runs, except the Docker-based ones:

```bash
npm run check
```

It runs these root `package.json` scripts in order and stops at the first failure:

| Script                 | What it does                                                                                                      |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `npm run format:check` | Prettier, check only: fails if any file is not formatted (`npm run format` fixes them)                            |
| `npm run build`        | Compiles every workspace with the strict compiler settings from `tsconfig.base.json`                              |
| `npm run lint`         | ESLint with type information and the NASA/JPL rules, `--max-warnings 0` (`npm run lint:fix` fixes what it can)    |
| `npm run typecheck`    | Type-checks every workspace that has a `typecheck` script, including files the build does not emit, such as tests |
| `npm run test:cov`     | Every workspace's Jest tests in one run, failing if coverage drops below the thresholds in `jest.config.js`       |
| `npm run test:tools`   | The tests of our own ESLint rule (`tools/eslint-rules/`), with Node's built-in test runner                        |

The other scripts:

| Script             | What it does                                                                           |
| ------------------ | -------------------------------------------------------------------------------------- |
| `npm test`         | The unit tests without coverage (faster while you work)                                |
| `npm run test:e2e` | The end-to-end test; needs the whole system running in Docker                          |
| `npm run format`   | Formats every file with Prettier                                                       |
| `npm run lint:fix` | ESLint with `--fix`                                                                    |
| `npm run start:*`  | Starts one service from its compiled output (`order`, `inventory`, `payment`, `query`) |
| `prepare`          | Runs automatically on `npm ci` / `npm install` and installs the husky git hooks        |

The code follows the NASA/JPL "Power of Ten" rules adapted to TypeScript. When the linter stops you with a message that starts with `JPL-n:`, read rule _n_ in [`docs/CODING_STANDARD.md`](docs/CODING_STANDARD.md): it explains the rule, why it exists and how to write the code instead. Please don't silence a rule with `eslint-disable`; if you think a rule is wrong for a case, say so in the pull request.

## Commit messages: Conventional Commits

Every commit message follows [Conventional Commits](https://www.conventionalcommits.org/):

```
type(optional scope): short description in the imperative mood

Optional body: what changed and why.

Optional footer, such as: BREAKING CHANGE: what users must do differently
```

The types we use:

| Type       | Use it for                                          | Example                                                     |
| ---------- | --------------------------------------------------- | ----------------------------------------------------------- |
| `feat`     | A new feature                                       | `feat(order-service): let customers cancel a pending order` |
| `fix`      | A bug fix                                           | `fix(messaging): run each migration exactly once`           |
| `docs`     | Documentation only                                  | `docs: explain the outbox relay in lesson 2`                |
| `refactor` | A code change that neither fixes a bug nor adds one | `refactor(inventory-service): extract decideReservation()`  |
| `test`     | Adding or fixing tests                              | `test(payment-service): cover the decline threshold`        |
| `build`    | Build system, dependencies, Dockerfile              | `build: upgrade NestJS to 10.4`                             |
| `ci`       | GitHub Actions workflows                            | `ci: run CodeQL every day`                                  |
| `chore`    | Anything else that doesn't touch the shipped code   | `chore: tidy .gitignore`                                    |

The **scope** is optional. Good scopes are the workspace or area you changed: `contracts`, `messaging`, `order-service`, `inventory-service`, `payment-service`, `order-query-service`, `e2e`, `deps`.

A `!` after the type (`feat(contracts)!: ...`) or a `BREAKING CHANGE:` footer marks a breaking change.

Why a fixed format? Because the release tool reads it: `fix` means a patch release, `feat` a minor release, and a breaking change a major release. It also writes the changelog from these messages, so write the description for a reader of the changelog.

## Git hooks

`npm ci` installs two [husky](https://typicode.github.io/husky/) hooks (in [`.husky/`](.husky)):

- **pre-commit** runs `lint-staged`: ESLint (with `--fix`) and Prettier on the files you staged, nothing else, so it is fast. The configuration is the `lint-staged` block in `package.json`.
- **commit-msg** runs `commitlint` with [`commitlint.config.mjs`](commitlint.config.mjs) and rejects a message that isn't a Conventional Commit. Try it: `echo "fixed stuff" | npx commitlint` fails, `echo "fix: handle an empty basket" | npx commitlint` passes.

Hooks are a convenience, not a guarantee (`git commit --no-verify` skips them), so CI runs the same checks again.

## Pull requests

1. **Keep it small.** One change per pull request. Small PRs get reviewed faster and more carefully.
2. Branch from `main`, and run `npm run check` before you push.
3. **Fill in the template** ([`.github/pull_request_template.md`](.github/pull_request_template.md)): what changed and why, how you tested it, and the checklist.
4. **Use a Conventional Commit as the PR title**, for example `fix(inventory-service): release stock in sku order`. Pull requests are squash-merged and the PR title becomes the commit message on `main`, so it ends up in the changelog. CI checks the title.
5. **CI must be green**: the quality checks, the end-to-end test and the security scans. A reviewer from [`CODEOWNERS`](.github/CODEOWNERS) approves, and the PR is squash-merged.
6. If the change alters behaviour, update the docs in the same PR: `README.md`, the relevant lesson in `LEARNING.md`, and `contracts/events/README.md` for event changes.

## How releases work

Releases are automated with [release-please](https://github.com/googleapis/release-please). You never edit version numbers or `CHANGELOG.md` by hand.

1. Pull requests are merged into `main`, each with a Conventional title.
2. release-please keeps a **release PR** open (titled like `chore(main): release 0.2.0`). It bumps the version and adds the new entries to [`CHANGELOG.md`](CHANGELOG.md) from the commit messages, and it updates itself after every merge.
3. Merging the release PR creates the git tag `vX.Y.Z` and a **GitHub Release**.
4. The same workflow then builds an image for each of the four services and publishes it to **GitHub Container Registry**, for example `ghcr.io/mitraker/event-driven-ecommerce-order-service:X.Y.Z` (and `:latest`).

## Adding a migration

Each service keeps its schema in `services/<service>/migrations/` as numbered SQL files (`001_init.sql`, `002_add_prices.sql`, ...). On startup, `Database.migrate()` in `packages/messaging/src/database.ts` applies every file that hasn't run yet, in name order, each in a transaction, and records it in the `schema_migrations` table. A Postgres advisory lock makes replicas that start at the same time take turns.

So **each migration runs exactly once per database**, which gives you two rules:

- **Never edit a migration that has shipped** (merged to `main`). Databases that already ran it will never run it again, so your edit would only reach new databases, and the two would drift apart.
- **To change the schema, add a new file** with the next number, for example `services/inventory-service/migrations/003_add_product_category.sql`. Write it so it works on a database with real data in it: add a column with a default, backfill with an `UPDATE`, and only then add a `NOT NULL` constraint.

Then update the code and tests that use the table, and run the service once against a fresh database (`docker compose down -v`) and once against an existing one.

## Adding an event type

Events are the contract between services, so a new event touches several places. Using a hypothetical `OrderShipped` as the example:

1. **Define it** in `packages/contracts/src/events.ts`: add `export type OrderShipped = EventEnvelope<'OrderShipped', { orderId: string; ... }>`, add it to the `ShopEvent` union and to `EVENT_TYPES`. Choose the topic of the service that owns the fact (one topic per producing service).
2. **Parse it** in `packages/contracts/src/parse-event.ts`: add a `case` to `parsePayload()` that checks every payload field, with bounds from `Limits`. The `switch-exhaustiveness-check` lint rule tells you if you forget. Add tests to `parse-event.spec.ts`, including invalid payloads.
3. **Publish it** from the producing service with `createEvent()`, inside the same database transaction as the state change, through the outbox. Never send to Kafka directly from business code.
4. **Consume it** in each service that needs it: add the type to the consumer's `handledTypes` and a `case` to the `switch` in its `handle()` (no handler tables, see JPL rule 9). Consumers that don't list it simply ignore it, which is what lets you add events without breaking old services. Update the read model's `project()` in `order-query-service` if the view should show it.
5. **Document it** in `contracts/events/README.md` (the event catalog) and, if it changes the flow, in `README.md` and `LEARNING.md`. Extend `tests/e2e/e2e.mjs` if it changes behaviour a user can see.

**Changing an existing event (versioning).** Consumers may still be reading old messages from Kafka, and other services may be deployed later than yours. So:

- **Adding an optional field** is backward compatible: older consumers ignore it (`parseEvent()` drops fields it doesn't know).
- **Removing or renaming a field, or changing its meaning**, is a breaking change. Bump `EVENT_VERSION`, teach `parseEvent()` to read **both** the old and the new version, deploy the consumers first and the producer last, and only drop the old version once no messages of it remain. Mark the commit as breaking (`feat(contracts)!: ...`).

## Questions

Open an [issue](https://github.com/MITRAKER/event-driven-ecommerce/issues/new/choose) and pick the form that fits. For anything security-related, use [`SECURITY.md`](SECURITY.md) instead.
