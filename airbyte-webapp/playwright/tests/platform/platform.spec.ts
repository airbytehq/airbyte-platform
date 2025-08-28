import { test, expect, Page } from "@playwright/test";

const OSS_SECURITY_CHECK_URL = "https://oss.airbyte.com/security-check";

// Helper function for filling setup form
const fillSetupForm = async (page: Page) => {
  // Assert that setup form is properly loaded and visible to user
  const emailInput = page.locator("input[name=email]");
  await expect(emailInput).toBeVisible({ timeout: 10000 });
  await emailInput.fill("test-email@some-domain.com");
  await page.locator("input[name=organizationName]").fill("ACME Corp");
};

const submitButtonClick = async (page: Page) => {
  await page.locator("button[type=submit]").click({ timeout: 10000 });
};

// Helper for setting up different security check scenarios
const setupSecurityCheck = {
  hanging: (page: Page) =>
    page.route(OSS_SECURITY_CHECK_URL, (_route) => {
      // Don't fulfill - simulates ongoing check
    }),
  failed: (page: Page) =>
    page.route(OSS_SECURITY_CHECK_URL, (route) => {
      route.fulfill({ status: 500, body: "oh noes, internal server error 🤷" });
    }),
  open: (page: Page) =>
    page.route(OSS_SECURITY_CHECK_URL, (route) => {
      route.fulfill({ status: 200, body: JSON.stringify({ status: "open" }) });
    }),
  closed: (page: Page) =>
    page.route(OSS_SECURITY_CHECK_URL, (route) => {
      route.fulfill({ status: 200, body: JSON.stringify({ status: "closed" }) });
    }),
};

const navigateToSetup = async (page: Page) => {
  await page.goto("/setup", { timeout: 10000 });
};

const assertSubmitButton = {
  disabled: (page: Page) => expect(page.locator("button[type=submit]")).toBeDisabled({ timeout: 2000 }),
  enabled: (page: Page) => expect(page.locator("button[type=submit]")).toBeEnabled({ timeout: 2000 }),
};

test.describe("Error handling view", () => {
  test("Shows Server Unavailable page", async ({ page }) => {
    // First, let the page load normally
    await page.goto("/", { timeout: 10000 });

    // Now set up the route interception for subsequent requests
    await page.route("**/api/v1/**", (route) => {
      route.fulfill({
        status: 502,
        body: "Failed to fetch",
      });
    });

    // Trigger a refresh to force the error condition
    await page.reload({ timeout: 10000 });

    // Check that the server unavailable message is displayed
    await expect(page.locator("p").filter({ hasText: "Airbyte is temporarily unavailable." })).toBeVisible({});
  });
});

test.describe("Setup actions", () => {
  test.beforeEach(async ({ page }) => {
    // Intercept instance configuration to simulate initial setup not complete
    await page.route("**/api/v1/instance_configuration", async (route) => {
      const response = await route.fetch();
      const json = await response.json();
      json.initialSetupComplete = false;
      await route.fulfill({ response, json });
    });
  });

  test("Should not allow finishing setup while security check is ongoing", async ({ page }) => {
    await setupSecurityCheck.hanging(page);
    await navigateToSetup(page);
    await fillSetupForm(page);

    // Assert that user sees security check running message and submit is disabled
    await expect(page.locator("[data-testid=securityCheckRunning]")).toBeVisible({ timeout: 2000 });
    await assertSubmitButton.disabled(page);
  });

  test("Should allow setting up in case of security check fails", async ({ page }) => {
    await setupSecurityCheck.failed(page);
    await navigateToSetup(page);
    await fillSetupForm(page);
    await assertSubmitButton.enabled(page);
  });

  test("Should show error and require marking checkbox if installation unsecured", async ({ page }) => {
    await setupSecurityCheck.open(page);
    await navigateToSetup(page);
    await fillSetupForm(page);
    await assertSubmitButton.disabled(page);

    // Assert that user sees advanced options and security warning
    await expect(page.locator("[data-testid=advancedOptions]")).toBeVisible({ timeout: 10000 });
    await page.locator("[data-testid=advancedOptions]").click();
    await page.locator("[data-testid=overwriteSecurityCheck]").click({ force: true });

    // Assert setup can proceed after acknowledging security risk
    await assertSubmitButton.enabled(page);
    await expect(
      page.locator(
        "text=I am aware that running Airbyte unsecured will put my data at risk but still would like to continue."
      )
    ).toBeVisible();
  });

  test("Should redirect to connections page after email is entered on closed installation", async ({ page }) => {
    await setupSecurityCheck.closed(page);
    await navigateToSetup(page);
    await expect(page).toHaveURL(/.*\/setup/);
    await fillSetupForm(page);
    await submitButtonClick(page);

    await expect(page).toHaveURL(/.*\/connections/, { timeout: 10000 });
  });
});
