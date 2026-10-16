import { Page, expect } from "@playwright/test";

import { dismissCookieBanner } from "./dismissCookieBanner";
import { testEmail, testPassword } from "./testIdentity";

export const signIn = async (page: Page, email: string = testEmail, password: string = testPassword) => {
  await page.goto("./login");
  await dismissCookieBanner(page);

  await page.locator("text=Continue with email").click();

  await page.locator("input[name=username]").fill(email);
  await page.locator("input[name=password]").fill(password);
  await page.getByRole("button", { name: "Log in" }).click();

  await expect(page).toHaveURL(/\/workspaces(\/.*)?$/);
};
