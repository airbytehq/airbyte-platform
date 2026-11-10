import { Page, expect } from "@playwright/test";

import { dismissCookieBanner } from "./dismissCookieBanner";
import { registerUser } from "./registerUser";
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

export const signInOrRegister = async (page: Page) => {
  const email = process.env.CLOUD_TEST_EMAIL;
  const password = process.env.CLOUD_TEST_PASSWORD;
  if (email && password) {
    // Stage: fixed Keycloak user, so runs don't create a new account each time
    await signIn(page, email, password);
  } else {
    await registerUser(page);
  }
};
