import { Page, expect } from "@playwright/test";

export const signOut = async (page: Page) => {
  await page.goto("./");

  if (page.url().endsWith("workspaces/")) {
    await page.getByRole("button", { name: "Sign out" }).click();
  } else {
    await page.locator('[data-testid="sidebar.userDropdown"]').click();
    await page.locator('[data-testid="sidebar.signout"]').click();
  }

  expect(page.url()).not.toContain("workspaces");
};
