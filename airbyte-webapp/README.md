# airbyte-webapp

This module contains the Airbyte Webapp. It is a React app written in TypeScript.
The webapp compiles to static HTML, JavaScript and CSS, which is served (in OSS) via
a nginx in the airbyte-webapp docker image. This nginx also serves as the reverse proxy
for accessing the server APIs in other images.

## Building the webapp

You can build the webapp using Gradle in the root of the repository:

```sh
# Only compile and build the docker webapp image:
./gradlew :oss:airbyte-webapp:assemble
# Build the webapp and additional artifacts and run tests:
./gradlew :oss:airbyte-webapp:build
```

## Developing the webapp

For an instruction how to develop on the webapp, please refer to our [documentation](https://docs.airbyte.com/contributing-to-airbyte/developing-locally/#webapp-contributions).

Please also check our [styleguide](./STYLEGUIDE.md) for details around code styling and best-practises.

### Folder Structure

**Services** folders contain "services" which are usually React Context implementations
(including their corresponding hooks to access them) or similar singleton type services.

**Utils** folders contain any form of static utility functions or hooks not related to any service (otherwise they should go together with that service into a services folder).

```sh
src/
├ App.tsx                    # OSS entrypoint
│
├ core/                      # SHARED: Core systems used by both OSS & Cloud
│ ├ api/                     # API layer (generated code, hooks, types)
│ ├ config/                  # Configuration and environment
│ ├ errors/                  # Error utilities
│ ├ form/                    # Form utilities
│ ├ services/                # Cross-domain services
│ │ ├ analytics/
│ │ ├ auth/
│ │ ├ connectorBuilder/
│ │ ├ features/              # Feature flags
│ │ ├ i18n/
│ │ ├ ConfirmationModal/
│ │ ├ Experiment/
│ │ ├ FormChangeTracker/
│ │ ├ Health/
│ │ ├ Modal/
│ │ ├ navigation/
│ │ ├ Notification/
│ │ └ ui/
│ └ utils/                   # Core utilities & hooks
│
├ area/                      # SHARED: Domain areas used by both OSS & Cloud
│ ├ auth/                    # Authentication domain
│ │ └ components/
│ ├ connection/              # Connection domain
│ │ ├ components/            # Connection-specific components (forms, tables, status)
│ │ ├ types/
│ │ └ utils/
│ ├ connector/               # Connector domain
│ │ ├ components/            # Connector forms, cards, documentation
│ │ ├ types/
│ │ └ utils/
│ ├ connectorBuilder/        # Connector Builder domain
│ │ └ components/
│ ├ layout/                  # Layout components (MainLayout, SideBar)
│ ├ organization/            # Organization domain
│ │ └ components/
│ ├ settings/                # Settings domain
│ │ └ components/
│ └ workspace/               # Workspace domain
│   ├ components/
│   └ utils/
│
├ components/                # SHARED: Reusable UI components
│ └ ui/                      # UI primitives ONLY (Button, Modal, Input, etc.)
│
├ pages/                     # SHARED: Route handlers for both OSS & Cloud
│ ├ routes.tsx
│ ├ connections/
│ ├ connectorBuilder/
│ ├ destination/
│ ├ source/
│ └ ...
│
├ cloud/                     # CLOUD-ONLY: Cloud-specific additions
│ ├ App.tsx                  # Cloud entrypoint
│ ├ cloudRoutes.tsx
│ ├ area/                    # Cloud-specific domain areas
│ │ └ billing/
│ ├ components/              # Cloud-specific components
│ ├ services/                # Cloud-specific services (auth, third-party)
│ │ ├ auth/
│ │ └ thirdParty/
│ └ views/                   # Cloud-specific page views
│
├ locales/                   # SHARED: i18n translation files
├ scss/                      # SHARED: Global styles, variables, themes
├ test-utils/                # SHARED: Test utilities and mock data
└ types/                     # SHARED: Global TypeScript types
```

**Key principles:**
- `core/`, `area/`, `components/ui/`, `pages/` are shared between OSS and Cloud
- `cloud/` only contains Cloud-specific additions (billing, auth, integrations)
- Domain-specific code lives in `area/`
- Only basic reusable UI components in `components/ui/`

### Entrypoints

- `src/App.tsx` is the entrypoint into the OSS version of the webapp.
- `src/cloud/App.tsx` is the entrypoint into the Cloud version of the webapp.

## Testing the webapp

### Unit tests with Jest

To run unit tests interactively, use `pnpm test`. To start a one-off headless run, use
`pnpm test:ci` instead. Unit test files are located in the same directory as the module
they're testing, using the `.test.ts` extension.

### End-to-end (e2e) tests with Playwright

End-to-end tests are written using [Playwright](https://playwright.dev/). The test suite can be found in the `/playwright` directory. For detailed instructions on running and debugging Playwright tests, please refer to the [Playwright README](/playwright/README.md).

#### Using local k8s and `make` (recommended)

##### Running an interactive Playwright session

The most useful way to run tests locally is in Playwright's UI mode, which lets you select which tests and browser to run, see the browser in action, and use the Playwright inspector for debugging.

1. Build and start the OSS backend by running either `make dev.up.oss` or `make build.oss deploy.oss`
2. Run `make test.e2e.oss.open` to start Playwright in UI mode with all dependencies configured.

##### Reproducing CI test results

This triggers headless runs: you won't have a live browser to interact with, just terminal output. This can be useful for debugging the occasional e2e failures that are not reproducible in the typical browser mode. This will print the same output you would see on CI.

1. Build and start the OSS backend by running either `make dev.up.oss` or `make build.oss deploy.oss`
2. Run `make test.e2e.oss`. This will take care of running dependency scripts to spin up a source and destination db and dummy API, and run the Playwright tests.

#### Test setup

When the tests are run as described above, the platform under test is started via kubernetes in the ab namespace. To test connections from real sources and destinations, additional docker containers are started for hosting these. For basic connections, additional postgres instances are started (`createdbsource` and `createdbdestination`).

For testing the connector builder UI, a dummy api server based on a node script is started (`createdummyapi`). It is providing a simple http API with bearer authentication returning a few records of hardcoded data. By running it in the internal airbyte network, the connector builder server can access it under its container name.

The tests instrument a browser to test the full functionality of Airbyte from the frontend, so other components of the platform (scheduler, worker, connector builder server) are also tested in a rudimentary way.

##### Caveats

1. due to an upstream bug with `vite preview` dev servers, running headless tests against an optimized, pre-bundled frontend build will always signal failure with a non-zero exit code, even if all tests pass
2. one early tester reported some cross-browser instability, where tests run with `pnpm test:dev` failed on Chrome but not Electron
