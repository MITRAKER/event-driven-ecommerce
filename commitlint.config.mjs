/**
 * commitlint: every commit message must be a Conventional Commit, `type(optional scope): description`.
 * release-please reads these messages to choose the next version and write CHANGELOG.md.
 * Scopes are optional and free-form (for example `messaging` or `order-service`); see CONTRIBUTING.md.
 * Run by the husky commit-msg hook (.husky/commit-msg).
 */
export default { extends: ['@commitlint/config-conventional'] };
