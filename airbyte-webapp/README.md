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

> 💡 Please note that this isn't representing the current state yet, but rather the targeted structure
> we're trying to move towards.

**Services** folders are containing "services" which are usually React Context implementations
(including their corresponding hooks to access them) or similar singleton type services.

**Utils** folders are containing any form of static utility functions or hooks not related to any service (otherwise they should go together with that service into a services folder).

```sh
src/
├ ui/ # All basic UI components
├ locales/ # Translation files
├ scss/ # SCSS variables, themes and utility files
├ types/ # TypeScript types needed across the code base
├ core/ # Core systems fundamental to the application
│ ├ api/ # API/request code
│ ├ config/ # Configuration system
│ ├ services/ # All core systems not belonging to a specific domain
│ └ utils/ # All core utilities not belonging to a specific domain
├ pages/ # Routing and page entry points (not all of the further components though)
│ ├ routes.tsx
│ ├ connections/
│ │ ├ AllConnectionPage/
│ │ └ # ...
│ └ # ...
├ area/ # Code for a specific domain of the webapp
│ ├ connection/
│ │ ├ services/ # Services for this area
│ │ ├ types/ # Types and enums that must be available widely and are not explicitly
│ │ │        # belonging to a component or util (e.g. prop types) in which case they should
│ │ │        # be living within that component/util file.
│ │ ├ utils/ # Utils for this area
│ │ └ components/ # Components related to this area or for pages specific to this area
│ ├ connector/ # Has the same services/, utils/, components/ structure
│ ├ connectorBuilder/
│ ├ settings/
│ └ workspace/
└ cloud/ # Cloud specific code (following a similar structure as above)
```

### Entrypoints

- `src/App.tsx` is the entrypoint into the OSS version of the webapp.
- `src/packages/cloud/App.tsx` is the entrypoint into the Cloud version of the webapp.

## Testing the webapp

### Unit tests with Jest

To run unit tests interactively, use `pnpm test`. To start a one-off headless run, use
`pnpm test:ci` instead. Unit test files are located in the same directory as the module
they're testing, using the `.test.ts` extension.

### End-to-end (e2e) tests with Cypress

There are two separate e2e test suites: one for the open-source build of Airbyte (located
in `cypress/e2e/`).

🚧 We are in the process of migrating e2e tests to Playwright. Our Playwright suite can be found in `/playwright` along with a separate README for running and debugging them.

#### Using local k8s and `make` (recommended)

##### Running an interactive Cypress session

The most useful way to run tests locally is with the `cypress open` command. It opens a
dispatcher window that lets you select which tests and browser to run; the Electron
browser (whose devtools will be very familiar to chrome users) opens a child window, and
having both cypress windows grouped behaves nicely when switching between applications. In
an interactive session, you can use `it.skip` and `it.only` to focus on the tests you care
about; any change to the source file of a running test will cause tests to be
automatically rerun. At the end of a test run, the web page is left "dangling" with all
state present at the end of the last test; you can click around, "inspect element", and
interact with the page however you wish, which makes it easy to incrementally develop
tests.

1. Build and start the OSS backend by running either `make dev.up.oss` or `make build.oss deploy.oss`
2. Run `make test.e2e.oss.open`. This will take care off all dependencies, start Cypress, and launch the interactive GUI.
3. Launch the tests in Electron browser (Chrome is not able to properly connect to the Cypress server in this case, and Electron is what we use on CI anyways!)

##### Reproducing CI test results

This triggers headless runs: you won't have a live browser to interact with, just terminal output. This can be useful for debugging the occasional e2e failures that are not reproducible in the typical browser mode. This will print the same output you would see on CI.

1. Build and start the OSS backend by running either `make dev.up.oss` or `make build.oss deploy.oss`
2. Run `make test.e2e.oss`. This will take care of running dependency scripts to spin up a source and destination db and dummy API, and finally run `npm run cypress:ci` to begin the tests.

#### Test setup

When the tests are run as described above, the platform under test is started via kubernetes in the ab namespace. To test connections from real sources and destinations,
additional docker containers are started for hosting these. For basic connections,
additional postgres instances are started (`createdbsource` and `createdbdestination`).

For testing the connector builder UI, a dummy api server based on a node script is started
(`createdummyapi`). It is providing a simple http API with bearer authentication returning
a few records of hardcoded data. By running it in the internal airbyte network, the
connector builder server can access it under its container name.

The tests in here are instrumenting an Electron instance to test the full functionality of
Airbyte from the frontend, so other components of the platform (scheduler, worker,
connector builder server) are also tested in a rudimentary way.

##### Caveats

1. due to an upstream bug with `vite preview` dev servers, running headless tests against an optimized, pre-bundled frontend build will always signal failure with a non-zero exit code, even if all tests pass
2. one early tester reported some cross-browser instability, where tests run with `pnpm test:dev` failed on Chrome but not Electron
