import { Page } from "@playwright/test";

export const cleanUpWorkspace = async (page: Page, workspaceName: string) => {
  await page.goto("./");
  await page.click('a[data-testid="select-workspace-1"]');
  await page.goto(`/settings/workspace`);
  await page.click('button:has-text("Delete your workspace")');
  await page.fill("#confirmation-text", workspaceName);
  await page.click('button:has-text("Delete workspace")');
  await page.waitForURL(/.*\/workspaces(\/[^/]+)?$/);
};

export const cleanUpApplications = async (page: Page, workspaceName?: string) => {
  await page.goto("./");
  if (workspaceName) {
    await page.click(`a:has-text("${workspaceName}")`);
  }
  await page.goto(`/settings/applications`);
  const actionsSelector = '[data-testid="table-cell-0-0__actions"]';
  while ((await page.locator(actionsSelector).count()) > 0) {
    await page.hover(actionsSelector);
    await page.click('button:has-text("Delete")');
    await page.click('button:has-text("Delete application")');
    // Optionally wait for the row to be removed before next iteration
    await page.waitForSelector("text=Application deleted successfully");
  }
};
