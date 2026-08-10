import { Page } from "@playwright/test";

/**
 * Dismisses the DataGrail cookie consent banner if present. The banner renders inside a shadow
 * root, which Playwright locators pierce automatically.
 */
export const dismissCookieBanner = async (page: Page) => {
  const closeButton = page.locator("aside.dg-consent-banner [data-testid='dg-header-close']");

  try {
    await closeButton.click({ timeout: 3000 });
  } catch {
    // Banner not present or already dismissed
  }
};
