<!--
The PR title must be a Conventional Commit, for example:
  fix(inventory-service): release stock in sku order
Pull requests are squash-merged, so the title becomes the commit message on main and a line in CHANGELOG.md.
-->

## What and why

<!-- What does this change, and why is it needed? Link the issue it closes, e.g. "Closes #12". -->

## How it was tested

<!-- Unit tests added or changed, the end-to-end test, manual steps (curl commands, Redpanda Console). -->

## Checklist

- [ ] The PR title is a [Conventional Commit](https://www.conventionalcommits.org/) (`type(optional scope): description`)
- [ ] Tests are added or updated for the change
- [ ] `npm run check` passes locally
- [ ] If behaviour changed: `README.md`, `LEARNING.md` and `contracts/events/README.md` are updated as needed
- [ ] No secrets, credentials or personal data are included
