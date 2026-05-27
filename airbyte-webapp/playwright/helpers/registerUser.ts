import { Page } from "@playwright/test";

import { testEmail, testPassword } from "./testIdentity";

export const registerUser = async (page: Page) => {
  await page.goto("./");
  // click on "Sign up" button
  await page.locator("text=Sign up").click({ noWaitAfter: true });
  // click "sign up using email" button
  await page.locator("text=Sign up using email").click();

  // fill in First name field
  await page.locator("input[name=firstName]").fill("Test");
  // fill in Last name field
  await page.locator("input[name=lastName]").fill("User");
  // fill in Email field
  await page.locator("input[name=email]").fill(testEmail);
  // fill in Password field
  await page.locator("input[name=password]").fill(testPassword);
  // fill in Confirm password field
  await page.locator("input[name=password-confirm]").fill(testPassword);
  // submit the form without waiting on the Keycloak redirect chain
  await page.locator("form").evaluate((form: HTMLFormElement) => setTimeout(() => form.requestSubmit(), 2000));

  const deadline = Date.now() + 20 * 1000;
  while (Date.now() < deadline) {
    const pathname = new URL(page.url()).pathname;
    if (pathname.includes("/organizations") || pathname.includes("/workspaces")) {
      return;
    }
    await page.waitForTimeout(250);
  }

  throw new Error(`Timed out waiting for authenticated route. Current URL: ${page.url()}`);
};
