# airbyte-webapp

This module contains the Airbyte Webapp. It is a React app written in TypeScript.
The webapp compiles to static HTML, JavaScript and CSS, which is served (in OSS) via
a nginx in the airbyte-webapp docker image. This nginx also serves as the reverse proxy
for accessing the server APIs in other images.

## Building the webapp

You can build the webapp using Gradle in the root of the repository:

```sh
# Only compile and build the docker webapp image:
./gradlew :airbyte-webapp:assemble
# Build the webapp and additional artifacts and run tests:
./gradlew :airbyte-webapp:build
```

## Developing the webapp

For an instruction how to develop on the webapp, please refer to our [documentation](https://docs.airbyte.com/contributing-to-airbyte/resources/developing-locally/#develop-on-airbyte-webapp).

Please also check our [styleguide](./STYLEGUIDE.md) for details around code styling and best-practises.

### Folder Structure

> 💡 Please note that this isn't representing the current state yet, but rather the targeted structure
we're trying to move towards.

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

* `src/App.tsx` is the entrypoint into the OSS version of the webapp.
* `src/packages/cloud/App.tsx` is the entrypoint into the Cloud version of the webapp.

## Testing the webapp
### Unit tests with Jest
To run unit tests interactively, use `pnpm test`. To start a one-off headless run, use
`pnpm test:ci` instead. Unit test files are located in the same directory as the module
they're testing, using the `.test.ts` extension.

### End-to-end (e2e) tests with Cypress
There are two separate e2e test suites: one for the open-source build of Airbyte (located
in `cypress/e2e/`), and one for Airbyte Cloud (located in `cypress/cloud-e2e/`). The
Airbyte Cloud e2e tests are open-source, but due to their proprietary nature can only be
run successfully from within the private Airbyte VPN.

#### Running an interactive Cypress session with `pnpm run cypress:open`

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

By default, this command is configured to visit page urls from port 3000 (as used by a
locally-run dev server), not port 8000 (as used by docker-compose's `webapp` service). If
you want to run tests against the dockerized UI instead, leave the `webapp` docker-compose
service running in step 4) and start the test runner with
`CYPRESS_BASE_URL=http://localhost:8000 pnpm run cypress:open` in step 8).

Steps:
1) Build the OSS backend for the current commit with `./gradlew clean build`.
2) Create the test database: `pnpm run createdbsource` and `pnpm run createdbdestination`.
3) Before running the connector builder tests, also start the dummy API server: `pnpm run createdummyapi`
4) The following commands will start long-running servers, so prepare to open a few terminal windows.
5) Start the OSS backend: `BASIC_AUTH_USERNAME="" BASIC_AUTH_PASSWORD="" VERSION=dev docker compose --file ../docker-compose.yaml up`. If you want, follow this with `docker compose stop webapp` to turn off the dockerized frontend build; interactive cypress sessions don't use it.
6) Start the frontend development server with `pnpm start`.
7) Start the cypress test runner with `pnpm run cypress:open`.

#### Reproducing CI test results with `pnpm run cypress:ci` or `pnpm run cypress:ci:record`

Unlike `pnpm run cypress:open`, `pnpm run cypress:ci` and `pnpm run cypress:ci:record` use
the dockerized UI (i.e. they expect the UI at port 8000, rather than port 3000). If the
OSS backend is running but you have run `docker-compose stop webapp`, you'll have to
re-enable it with `docker-compose start webapp`. These trigger headless runs: you won't
have a live browser to interact with, just terminal output.

Steps:
1) Build the OSS backend for the current commit with `./gradlew clean build`.
2) Create the test database: `pnpm run createdbsource` and `pnpm run createdbdestination`.
3) When running the connector builder tests, start the dummy API server: `pnpm run createdummyapi`
4) Start the OSS backend: `BASIC_AUTH_USERNAME="" BASIC_AUTH_PASSWORD="" VERSION=dev docker compose --file ../docker-compose.yaml up`.
5) Start the cypress test run with `pnpm run cypress:ci` or `pnpm run cypress:ci:record`.

#### Test setup

When the tests are run as described above, the platform under test is started via docker
compose on the local docker host. To test connections from real sources and destinations,
additional docker containers are started for hosting these. For basic connections,
additional postgres instances are started (`createdbsource` and `createdbdestination`).

For testing the connector builder UI, a dummy api server based on a node script is started
(`createdummyapi`). It is providing a simple http API with bearer authentication returning
a few records of hardcoded data. By running it in the internal airbyte network, the
connector builder server can access it under its container name.

The tests in here are instrumenting a Chrome instance to test the full functionality of
Airbyte from the frontend, so other components of the platform (scheduler, worker,
connector builder server) are also tested in a rudimentary way.

#### Cloud e2e tests
:rotating_light::construction:The Airbyte Cloud e2e tests will only run successfully from
within the private Airbyte VPN.:construction::rotating_light:

The e2e tests can be run against either a local cloud UI or a full-stack Cloud deployment.

##### Test setup
Pre-configured credentials in dev/stage cloud environments can be found in Lastpass,
`Integration Test Airbyte Login`. To make these credentials (or any others) visible to the
test suite, they can be assigned to shell variables prefixed with `CYPRESS_`—so, for
example, to log in with the email `readme-contrived-example@email-client.biz`, you would
put a line like the following in your shell config:
``` sh
export CYPRESS_TEST_USER_EMAIL="readme-contrived-example@email-client.biz"
export CYPRESS_TEST_USER_PW="<contrived example password>"
```

If you like to track your dotfiles in a git repo, I hope it's obvious that these should
not be committed.

##### Quickstart

For interactive, visual browser-based testing, complete with "Inspect element":
1) configure credentials for a test user in the `CYPRESS_TEST_USER_EMAIL` and `CYPRESS_TEST_USER_PW` environment variables
2) `pnpm cloud-test:dev`
3) in the cypress UI, select "E2E testing" and then your browser of choice to launch the test runner
4) select a spec file to run its tests. Any edits to that spec file, or any cypress support file, will automatically rerun the tests.

For headless, CI-style testing:
1) `pnpm cloud-test` (see caveat #1, however)

##### Caveats
1) due to an upstream bug with `vite preview` dev servers, running headless tests against an optimized, pre-bundled frontend build will always signal failure with a non-zero exit code, even if all tests pass
2) one early tester reported some cross-browser instability, where tests run with `pnpm test:dev` failed on Chrome but not Electron
