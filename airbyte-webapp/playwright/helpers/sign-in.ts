import { Page, expect } from "@playwright/test";

export const signIn = async (page: Page, email: string, password: string) => {
  await page.goto("./");

  await page.locator("text=Continue with email").click();

  await page.locator("input[name=username]").fill(email);
  await page.locator("input[name=password]").fill(password);
  await page.getByRole("button", { name: "Log in" }).click();

  await expect(page).toHaveURL(/\/workspaces\/.*/);
};
