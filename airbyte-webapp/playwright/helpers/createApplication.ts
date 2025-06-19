import { Page } from "@playwright/test";

export const createApplication = async ({ page }: { page: Page }) => {
  // // Create application and copy env vars
  await page.click('a:has-text("Settings")');
  await page.click('a:has-text("Applications")');
  await page.click('button:has-text("Create an application")');
  await page.fill('input[data-testid="input"][name="name"]', "My Application");
  await page.click('button:has-text("Submit")');
};
