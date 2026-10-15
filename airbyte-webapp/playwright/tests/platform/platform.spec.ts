/**
 * Every test runs against the mock backend in playwright/mock. Each test starts from the backend state in its own
 * scenario, so no test depends on what an earlier test did.
 *
 * Each test is one user journey. Checks that only read the page use `expect.soft`, so one failed check does not hide
 * the checks after it.
 */

import type { Page } from "@playwright/test";

import { errors, expect, scenario, test } from "../../mock";

/** An instance where nobody has submitted the setup page yet. The setup page asks oss.airbyte.com for a security check. */
const freshInstance = scenario({ instance: { initialSetupComplete: false } });

const setupForm = {
  fill: async (page: Page) => {
    const emailInput = page.locator("input[name=email]");
    await expect(emailInput).toBeVisible();
    await emailInput.fill("test-email@some-domain.com");
    await page.locator("input[name=organizationName]").fill("ACME Corp");
  },
  submitButton: (page: Page) => page.locator("button[type=submit]"),
};

test.describe(() => {
  test.use({ airbyteScenario: scenario({}) });

  test("open Airbyte while its server is down", async ({ page, airbyte }) => {
    airbyte.failAll(errors.badGateway());

    await page.goto("/");

    await test.step("the server unavailable message shows", async () => {
      await expect.soft(page.locator("p").filter({ hasText: "Airbyte is temporarily unavailable." })).toBeVisible();
    });
  });
});

test.describe("Initial setup", () => {
  test.describe(() => {
    test.use({ airbyteScenario: freshInstance.with({ external: { securityCheck: "hang" } }) });

    test("set up Airbyte while the security check still runs", async ({ page }) => {
      await page.goto("/setup");
      await setupForm.fill(page);

      await test.step("the security check shows as running", async () => {
        await expect.soft(page.locator("[data-testid=securityCheckRunning]")).toBeVisible();
      });

      await test.step("the submit button is disabled", async () => {
        await expect.soft(setupForm.submitButton(page)).toBeDisabled();
      });
    });
  });

  test.describe(() => {
    test.use({ airbyteScenario: freshInstance.with({ external: { securityCheck: "fail" } }) });

    test("set up Airbyte when the security check fails", async ({ page }) => {
      await page.goto("/setup");
      await setupForm.fill(page);

      await test.step("the submit button is enabled", async () => {
        await expect.soft(setupForm.submitButton(page)).toBeEnabled();
      });
    });
  });

  test.describe(() => {
    test.use({ airbyteScenario: freshInstance.with({ external: { securityCheck: "open" } }) });

    test("set up Airbyte on an unsecured installation by accepting the risk", async ({ page }) => {
      await page.goto("/setup");
      await setupForm.fill(page);

      await test.step("the submit button is disabled", async () => {
        await expect.soft(setupForm.submitButton(page)).toBeDisabled();
      });

      await test.step("accept the risk of an unsecured installation", async () => {
        await page.locator("[data-testid=advancedOptions]").click();
        await page.locator("[data-testid=overwriteSecurityCheck]").click({ force: true });
      });

      await test.step("the submit button is enabled", async () => {
        await expect.soft(setupForm.submitButton(page)).toBeEnabled();
      });

      await test.step("the risk warning shows", async () => {
        await expect
          .soft(
            page.getByText(
              "I am aware that running Airbyte unsecured will put my data at risk but still would like to continue."
            )
          )
          .toBeVisible();
      });
    });
  });

  test.describe(() => {
    test.use({ airbyteScenario: freshInstance.with({ external: { securityCheck: "closed" } }) });

    test("set up Airbyte on a secured installation", async ({ page, airbyte }) => {
      await page.goto("/setup");
      await expect(page).toHaveURL(/\/setup/);
      await setupForm.fill(page);

      await test.step("submit the setup form", async () => {
        await setupForm.submitButton(page).click();
      });

      await test.step("the connections page of the workspace opens", async () => {
        await expect.soft(page).toHaveURL(new RegExp(`/workspaces/${airbyte.workspaceId}/connections`));
      });

      await test.step("the webapp saved the setup form", async () => {
        expect.soft(airbyte.calls("setupInstanceConfiguration")).toEqual([
          {
            email: "test-email@some-domain.com",
            organizationName: "ACME Corp",
            // The mock instance uses the "logging" tracking strategy, so the form sets anonymousDataCollection to true.
            anonymousDataCollection: true,
            initialSetupComplete: true,
            displaySetupWizard: false,
          },
        ]);
      });
    });
  });
});
