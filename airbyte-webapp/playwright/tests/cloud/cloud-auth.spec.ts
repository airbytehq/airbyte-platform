import { test, expect, Page } from "@playwright/test";

import { signIn } from "../../helpers/sign-in";
import { signOut } from "../../helpers/sign-out";

// Helper to parse loginRedirect param from search string
function parseLoginRedirectParam(search: string): string | null {
  const params = new URLSearchParams(search);
  return params.get("loginRedirect");
}

// Helper to assert on login page
async function assertOnLoginPage(page: Page) {
  await expect(page).toHaveURL(/\/login/);
  await expect(page.locator("body")).toContainText(/Continue with Google/);
}

test.describe("Email Authentication on Airbyte Cloud", () => {
  test("sign up", async ({ page }) => {
    await page.goto("./");

    // Expect a title "to contain" a substring.
    await expect(page).toHaveTitle(/Airbyte/);
    await expect(page.locator("body")).toContainText(/Continue with Google/);
    // click on "Sign up" button
    await page.locator("text=Sign up").click();
    // click "sign up using email" button
    await page.locator("text=Sign up using email").click();

    // fill in First name field
    await page.locator("input[name=firstName]").fill("Test");
    // fill in Last name field
    await page.locator("input[name=lastName]").fill("User");
    // fill in Email field
    await page.locator("input[name=email]").fill("integration-test+frontend@airbyte.com");
    // fill in Password field
    await page.locator("input[name=password]").fill("passwordpassword");
    // fill in Confirm password field
    await page.locator("input[name=password-confirm]").fill("passwordpassword");
    // click on "Sign up" button
    await page.locator("text=Sign up").click();

    // expect the user to be redirected to a route containing `/workspaces/`
    await expect(page).toHaveURL(/.*\/workspaces\//);

    await page.goto("./");
    await signOut(page);
  });
  test("sign in", async ({ page }) => {
    await page.goto("./");

    await signIn(page, "integration-test+frontend@airbyte.com", "passwordpassword");

    // expect the user to be redirected to a route containing `/workspaces/`
    await expect(page).toHaveURL(/.*\/workspaces\//);

    await page.goto("./");
    await signOut(page);
  });
  test("login redirect param parsing", async ({ page }) => {
    const testRedirectParamParsing = async (initialPath: string, wait?: boolean) => {
      const pathsWithNoRedirect = ["/", "/login", "/signup", "/settings/account"];
      const expectedRedirectPath = !pathsWithNoRedirect.includes(initialPath) ? initialPath : null;

      await page.goto(initialPath);

      // check login redirect parameter on login page
      await assertOnLoginPage(page);
      const search1 = page.url().split("?")[1] ? `?${page.url().split("?")[1]}` : "";
      expect(parseLoginRedirectParam(search1)).toBe(expectedRedirectPath);

      // wait for cookie banner to disappear if needed
      if (wait) {
        await page.waitForTimeout(6000);
      }

      // check login redirect parameter after navigating to sign up page
      await page.locator("text=Sign up").click();
      await expect(page).toHaveURL(/\/signup/);
      const search2 = page.url().split("?")[1] ? `?${page.url().split("?")[1]}` : "";
      expect(parseLoginRedirectParam(search2)).toBe(expectedRedirectPath);
    };

    const pathsToTest = ["/", "/login", "/signup", "/settings/account", "/connections"];
    for (const path of pathsToTest) {
      await testRedirectParamParsing(path);
    }
  });
});
