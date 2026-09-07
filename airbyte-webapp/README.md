# airbyte-webapp

This module contains the Airbyte Webapp. It is a React app written in TypeScript.
The webapp compiles to static HTML, JavaScript and CSS, which is served (in OSS) via
nginx in the airbyte-webapp Docker image. This nginx also serves as the reverse proxy
for accessing server APIs in other images.

## Building the webapp

You can build the webapp using Gradle from the repository root:

```sh
# Only compile and build the Docker webapp image:
./gradlew :oss:airbyte-webapp:assemble
# Build the webapp and additional artifacts and run tests:
./gradlew :oss:airbyte-webapp:build
```

## Developing the webapp

For instructions on developing the webapp, see the
[local development documentation](https://docs.airbyte.com/contributing-to-airbyte/developing-locally/#webapp-contributions).

See [CONTRIBUTING.md](./CONTRIBUTING.md) for contribution conventions, testing commands,
architecture guidance, and development-only mocking rules. Agent-specific instructions
are in [AGENTS.md](./AGENTS.md). For code styling, see [STYLEGUIDE.md](./STYLEGUIDE.md).

### Folder structure

The webapp has shared OSS/Cloud code under `src/core/`, `src/area/`, `src/components/ui/`,
`src/pages/`, `src/locales/`, `src/scss/`, `src/test-utils/`, and `src/types/`.
Cloud-specific additions live under `src/cloud/`.

```text
src/
├── App.tsx                    # OSS entrypoint
├── core/                      # Shared core systems, APIs, and utilities
├── area/                      # Shared domain areas
├── components/ui/             # Shared UI primitives
├── pages/                     # Shared route handlers and page components
├── cloud/                     # Cloud-only additions
├── locales/                   # Shared translation files
├── scss/                      # Shared styles and themes
├── test-utils/                # Shared test utilities and mock data
└── types/                     # Shared TypeScript types
```

Both entrypoints share the same routing configuration and most application code:

- `src/App.tsx` is the OSS entrypoint and uses `OSSAuthService`.
- `src/cloud/App.tsx` is the Cloud entrypoint and adds Cloud authentication,
  feature flags, and third-party integrations.

Cloud-specific code may import shared code, but shared code must not import from `cloud/`.

## OSS and Cloud sharing model

Most code is shared between OSS and Cloud. Domain-specific code belongs under `src/area/`,
reusable UI primitives belong under `src/components/ui/`, and Cloud-only features belong
under `src/cloud/`.

See [CONTRIBUTING.md](./CONTRIBUTING.md) for detailed code organization and development
guidance. See [AGENTS.md](./AGENTS.md) for agent-specific instructions.
