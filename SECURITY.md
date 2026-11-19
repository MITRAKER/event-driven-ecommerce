# Security policy

## Supported versions

This is a learning project. Only the **latest release** (see [Releases](https://github.com/MITRAKER/event-driven-ecommerce/releases)) and the `main` branch receive security fixes. Older versions are not patched: please upgrade.

| Version             | Supported |
| ------------------- | --------- |
| Latest release      | Yes       |
| `main`              | Yes       |
| Any earlier release | No        |

## Reporting a vulnerability

**Please do not report security vulnerabilities in public issues, pull requests or discussions.** A public report tells everyone how to exploit the problem before a fix exists.

Report it privately through GitHub instead:

1. Go to the repository's **Security** tab.
2. Click **Report a vulnerability** (GitHub's private vulnerability reporting).
3. Describe the problem: the affected service or file, the steps to reproduce it, what an attacker could do, and the version or commit you tested.

Only the maintainers can see the report, and you can discuss it with them privately in the same place.

## What to expect

- An acknowledgement within **7 days**. This is a project maintained in spare time, so please be patient.
- An assessment of whether the report is a vulnerability, and how serious it is.
- For a confirmed vulnerability: a fix in a new release, and a published GitHub Security Advisory that credits you, unless you prefer to stay anonymous.
- Please give us a reasonable time to release a fix before you disclose the problem publicly.

## Scope and known non-issues

- **Local development credentials.** The usernames and passwords in `docker-compose.yml` and in the services' `start` scripts (such as `shop` / `shop` for Postgres) exist only for running the project on your own machine. They are not secrets and are not used anywhere else. Never reuse them in a real deployment: supply your own credentials through environment variables or a secret store.
- **The payment service is a simulation.** It never handles real card data.
- Vulnerabilities in dependencies are tracked automatically by Dependabot, `npm audit` and dependency review (see [`docs/CODING_STANDARD.md`](docs/CODING_STANDARD.md#the-other-quality-gates)). A report is still welcome if you find one that affects this project in practice.
