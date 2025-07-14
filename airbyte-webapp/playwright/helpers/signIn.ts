import { Page, expect } from "@playwright/test";

import { registerUser } from "./registerUser";
import { testEmail, testPassword } from "./testIdentity";

export const signIn = async (page: Page, email: string = testEmail, password: string = testPassword) => {
  await page.goto("./login");

  await page.locator("text=Continue with email").click();

  await page.locator("input[name=username]").fill(email);
  await page.locator("input[name=password]").fill(password);
  await page.getByRole("button", { name: "Log in" }).click();

  await expect(page).toHaveURL(/\/workspaces(\/.*)?$/);
};

export const signInOrRegister = async (page: Page) => {
  await page.goto("./");

  // Check if already signed in by looking for authenticated routes
  let isSignedIn = await page.evaluate(() => {
    return window.location.pathname.includes("/organizations") || window.location.pathname.includes("/workspaces");
  });

  if (isSignedIn) {
    console.log("User is already signed in.");
    return;
  }

  // attempt to sign in
  await page.goto("./");
  await page.locator("text=Continue with email").click();
  await page.locator("input[name=username]").fill(testEmail);
  await page.locator("input[name=password]").fill(testPassword);
  await page.getByRole("button", { name: "Log in" }).click();

  // Wait for either redirect to authenticated route or error message
  const loginResult = await Promise.race([
    page
      .waitForURL((url) => url.pathname.includes("/organizations") || url.pathname.includes("/workspaces"), {
        timeout: 10000,
      })
      .then(() => "success")
      .catch(() => null),
    page
      .locator("text=Invalid email or password")
      .waitFor({ timeout: 10000 })
      .then(() => "invalid")
      .catch(() => null),
  ]);

  if (loginResult === "success") {
    console.log("User signed in successfully.");
    return;
  }

  if (loginResult === "invalid") {
    console.log("Invalid email or password, registering user...");
    await registerUser(page);
    return;
  }

  // fallback: re-check if signed in after login attempt
  isSignedIn = await page.evaluate(() => {
    return window.location.pathname.includes("/organizations") || window.location.pathname.includes("/workspaces");
  });

  if (isSignedIn) {
    console.log("User signed in successfully.");
    return;
  }

  // fallback: register if not signed in and no error detected
  await registerUser(page);
};
