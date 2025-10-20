# Playwright E2E Tests

This directory contains end-to-end (E2E) tests for the Airbyte webapp, using [Playwright](https://playwright.dev/).  
🚧 We are currently in the process of migrating from Cypress to Playwright, meaning the e2e tests you need to look at may not be here yet! All new tests should be added here, though. 🚧

## Structure

- `tests/` - Contains all Playwright test files.
- `helpers/` - Contains re-usable helper methods for tests.
- `playwright.config.ts` - Default Playwright configuration (for running against OSS).
- `playwright.cloud-config.ts` - Configuration for running tests against the Airbyte Cloud environment.
- `scripts/` - Contains helper script to run Playwright tests with various options, as well as setup/cleanup scripts for external test dependencies

## Running Tests using the Helper Script

You can use the helper script to point the e2e tests against a specific server and webapp. For example, if you are running cloud locally and would like to run the cloud e2e tests against that, you can run:

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
  Server host to use (defaults to frontend-dev).  
  Example: `https://frontend-dev-cloud.airbyte.com` or `https://local.airbyte.dev`

- `--webappUrl <url>`  
  Webapp environment to use (ie, the baseURL for Playwright tests).
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
pnpm test -- --serverHost="https://local.airbyte.dev" tests/builder

# Multiple test paths
pnpm test -- --serverHost="https://local.airbyte.dev" tests/builder tests/connector-crud
```

> **Important:** Test paths must come **after** all the flags/options. The script automatically ensures the setup project runs first to handle authentication.

## Running the test suite locally

The full Playwright E2E test suite relies on two external Docker containers to be running:

1. Postgres container for connection tests
2. Dummy API for builder tests

The simplest way to boot these up locally is to run the following command from `oss/airbyte-webapp/playwright`:

```bash
pnpm run setup
```

After you are done running the tests, you can clean up both of these containers with

```bash
pnpm run cleanup
```

The setup and cleanup scripts for both Postgres and Dummy API are located in `scripts/`. These scripts automatically detect whether you're running locally (Docker) or in CI (Kubernetes) and use the appropriate deployment method.

## Test Reports

Test reports from CI runs are stored in a GCS bucket, and are viewable in the Playwright job from the `Generate Playwright report and trace links` step. For local runs, to access and view a report from the latest local run, use:

```bash
pnpm exec playwright show-report
```
