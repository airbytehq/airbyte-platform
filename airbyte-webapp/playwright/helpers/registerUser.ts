import { Page, expect } from "@playwright/test";

import { testEmail, testPassword } from "./testIdentity";

export const registerUser = async (page: Page) => {
  await page.goto("./");
  // click on "Sign up" button
  await page.locator("text=Sign up").click();
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
  // click on "Sign up" button
  await page.locator("text=Sign up").click();

  // expect the user to be redirected to a route containing `/workspaces/`
  await expect(page).toHaveURL(/.*\/workspaces\//, { timeout: 30 * 1000 });
};
