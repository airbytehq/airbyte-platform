import { Page } from "@playwright/test";

/**
 * Dismisses the Osano cookie consent banner if present.
 * The banner can intercept pointer events and block clicks on underlying elements.
 */
export const dismissCookieBanner = async (page: Page) => {
  const acceptButton = page.locator(".osano-cm-accept-all, .osano-cm-button--type_accept");
  try {
    await acceptButton.click({ timeout: 3000 });
  } catch {
    // Banner not present or already dismissed
  }
};
