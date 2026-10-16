# Playwright E2E Tests

This directory contains end-to-end (E2E) tests for the Airbyte webapp, using [Playwright](https://playwright.dev/).

## Structure

- `tests/` - Contains all Playwright test files.
- `helpers/` - Contains re-usable helper methods for tests.
- `playwright.config.ts` - Default Playwright configuration (for running against OSS).
- `playwright.cloud-config.ts` - Configuration for running tests against the Airbyte Cloud environment.
- `mock/` - The mock backend. It answers every API call of the OSS tests, so they need no deployed backend.
- `scripts/` - The helper script that runs the tests, and the `setup`/`cleanup` scripts that deploy a test Postgres. The Postgres scripts serve CI only: the OSS tests do not use that Postgres, so you do not need to run them locally.

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

## Test Reports

Test reports from CI runs are stored in a GCS bucket, and are viewable in the Playwright job from the `Generate Playwright report and trace links` step. For local runs, to access and view a report from the latest local run, use:

```bash
pnpm exec playwright show-report
```
