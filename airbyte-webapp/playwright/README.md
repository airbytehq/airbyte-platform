# Playwright Acceptance Tests

This directory contains the acceptance tests of the Airbyte webapp, written with [Playwright](https://playwright.dev/). The OSS tests run against the mock backend in `mock/`. The Cloud tests in `tests/cloud/` sign in to a real Cloud environment, so they are end-to-end tests.

## Structure

- `tests/` - Contains all Playwright test files.
- `helpers/` - Contains re-usable helper methods for tests.
- `playwright.config.ts` - Default Playwright configuration (for running against OSS).
- `playwright.cloud-config.ts` - Configuration for running tests against the Airbyte Cloud environment.
- `mock/` - The mock backend. It answers every API call of the OSS tests, so they need no deployed backend.
- `scripts/` - Contains the helper script to run Playwright tests with various options, and the script that writes the route table of the mock backend.

## Running Tests using the Helper Script

You can use the helper script to point the cloud e2e tests at a specific server and webapp. For example, if you are running cloud locally and would like to run the cloud e2e tests against that, you can run:

```bash
pnpm test -- --cloud --serverHost=https://local.airbyte.dev --webappUrl=https://localhost:3000
```

> **Note:**  
> When using `pnpm`, you must use `--` before any options you want to pass to the underlying script.  
> For example, use `pnpm test -- --cloud --serverHost=...` instead of `pnpm test --cloud --serverHost=...`.  
> Otherwise, you may see errors like `ERROR  Unknown options: 'cloud', 'serverHost'`.

### Options

- `--cloud, -c`  
  Run cloud E2E tests using the cloud config (runs only the tests in `/cloud`).

- `--serverHost <url>`  
  Cloud only. Server host for the cloud tests (defaults to frontend-dev).  
  Example: `https://frontend-dev-cloud.airbyte.com` or `https://local.airbyte.dev`

- `--webappUrl <url>`  
  Cloud only. Webapp URL for the cloud tests (the baseURL of the cloud config).  
  Example: `https://localhost:3000` or `https://localhost:3001`

- `--debug`  
  Enable Playwright's [debug mode](https://playwright.dev/docs/debug#playwright-inspector) with Playwright Inspector

- `--ui`  
  Run in Playwright [UI mode](https://playwright.dev/docs/running-tests#run-tests-in-ui-mode).

- `--headed`  
  Run tests in [headed mode](https://playwright.dev/docs/running-tests#run-tests-in-headed-mode) (browser visible).

- `--help, -h`  
  Show help.

### Running Specific Tests

You can run a subset of tests by passing test paths as arguments:

```bash
# Single test path
pnpm test -- tests/builder

# Multiple test paths
pnpm test -- tests/builder tests/connector-crud
```

> **Important:** Test paths must come **after** all the flags/options.

## Running the test suite locally

The OSS tests need no backend and no other services. `pnpm test` starts the webapp dev server, and the mock backend in `mock/` answers every API call.

## When CI runs the tests

Two CI jobs run the OSS tests. Both use the mock backend and install no OSS release.

- **`OSS FE Acceptance`** (`.github/workflows/pr-test-fe-acceptance.yml`):
  - **PR Build:** when a pull request changes files that match the `workflow`, `frontend` or `frontendServers` path filter.
  - **Merge: Build:** on each push to `master` that starts a merge build. Merge: Build skips pushes that only change paths in its `paths-ignore` list, such as `docs/**` or most of `.github/**`.
- **`OSS FE Acceptance on stage`** (`.github/workflows/test-oss-fe-acceptance-stage.yml`): during the stage deploy of Deploy to Stage + Prod, when the deploy runs its tests (`run_tests: true`, the default). The job is non-blocking: a failure does not stop the deploy. The production deploy runs no Playwright tests.

Each job reports its tests to Datadog Test Optimization: `OSS FE Acceptance` as the service `oss-fe-acceptance-test`, and `OSS FE Acceptance on stage` as `stage-oss-fe-acceptance-test`.

## Test Reports

- **`OSS FE Acceptance`:** the job uploads the report and the traces to a GCS bucket. Open them from the links in the `Generate Playwright report and trace links` step. The report is also a run artifact, `playwright-report-oss`.
- **`OSS FE Acceptance on stage`:** the report is a run artifact, `playwright-report-oss-stage`. This job does not upload to GCS.

To view the report of the latest local run, use:

```bash
pnpm exec playwright show-report
```
