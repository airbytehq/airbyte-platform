# AGENTS.md — `oss/airbyte-webapp/`

This file contains agent-specific guidance for the React/TypeScript webapp.
Read the repository-level [AGENTS.md](../../AGENTS.md) first, then the
human-and-agent contribution guide in [CONTRIBUTING.md](./CONTRIBUTING.md).
The project overview and setup basics are in [README.md](./README.md).

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the webapp's development commands,
architecture conventions, testing guidance, and development-only mocking rules.

## Agent workflow

- Use the repository's pinned Node.js and pnpm versions; run commands from
  `oss/airbyte-webapp/`.
- Do not edit or commit generated API artifacts under
  `src/core/api/generated/`; regenerate them with `pnpm generate-client`.
- Keep changes focused on the requested scope and preserve unrelated local
  modifications.
- After editing, re-read only the changed regions and run the narrowest
  checks that cover the change before the required final verification.
