import { test, expect } from "@playwright/test";

import { signIn } from "../../helpers/sign-in";
import { signOut } from "../../helpers/sign-out";

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
});
