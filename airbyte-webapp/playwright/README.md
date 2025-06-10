# Playwright E2E Tests

This directory contains end-to-end (E2E) tests for the Airbyte webapp, using [Playwright](https://playwright.dev/).  
🚧 We are currently in the process of migrating from Cypress to Playwright, meaning the e2e tests you need to look at may not be here yet! All new tests should be added here, though. 🚧

## Structure

- `tests/` - Contains all Playwright test files.
- `playwright.config.ts` - Default Playwright configuration (for running against OSS).
- `playwright.cloud-config.ts` - Configuration for running tests against the Airbyte Cloud environment.
- `scripts/run-playwright.js` - Helper script to run Playwright tests with various options.

## Running Tests using the Helper Script

You can use the helper script to point the e2e tests against a specific server and webapp. For example, if you are running cloud locally and would like to run the cloud e2e tests against that, you can run:

```bash
pnpm test -- --cloud --serverHost=https://local.airbyte.dev
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
  Webapp environment to use.  
  Example: `https://localhost:3000` or `https://localhost:3001`

- `--debug`  
  Enable Playwright's [debug mode](https://playwright.dev/docs/debug#playwright-inspector) with Playwright Inspector

- `--ui`  
  Run in Playwright [UI mode](https://playwright.dev/docs/running-tests#run-tests-in-ui-mode).

- `--headed`  
  Run tests in [headed mode](https://playwright.dev/docs/running-tests#run-tests-in-headed-mode) (browser visible).

- `--help, -h`  
  Show help.
