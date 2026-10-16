import { Page } from "@playwright/test";
import { Experiments } from "@src/core/services/Experiment/experiments";

export const featureFlags: Partial<Experiments> = {};

export const setServerFeatureFlags = async (flags: Record<string, string>) => {
  for (const [key, value] of Object.entries(flags)) {
    console.log(
      `[setServerFeatureFlags] Setting feature flag "${key}" to "${value}" calling FF server at ${process.env.AIRBYTE_SERVER_HOST}/api/v1/feature-flags`
    );

    const response = await fetch(`${process.env.AIRBYTE_SERVER_HOST}/api/v1/feature-flags`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({
        key,
        default: value,
      }),
      // agent, // not needed with NODE_TLS_REJECT_UNAUTHORIZED=0
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);

    const responseBody = await response.text();
    if (!response.ok) {
      throw new Error(
        `[setServerFeatureFlags] Failed to set feature flag "${key}": ${response.status} ${response.statusText} - ${responseBody}`
      );
    }
    console.log(`[setServerFeatureFlags] PUT ${process.env.AIRBYTE_SERVER_HOST}/api/v1/feature-flags`, {
      key,
      value,
      status: response.status,
      statusText: response.statusText,
      responseBody,
      requestHeaders: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      requestBody: { key, default: value },
    });
  }
};

// Set EXPERIMENT feature flags
export const setFeatureFlags = (flags: Partial<Experiments>) => {
  if (Object.keys(flags).length === 0) {
    Object.keys(featureFlags).forEach((key) => delete featureFlags[key as keyof Experiments]);
  } else {
    Object.assign(featureFlags, flags);
  }
};

// Inject feature flags and style into the browser context before each test
export async function injectFeatureFlagsAndStyle(page: Page) {
  await page.addInitScript(
    ({ featureFlags }) => {
      // Only set window._e2ePlaywrightEnvironment if we're actually overriding features
      // This prevents cross-test contamination where tests without overrides
      // would still be detected as "E2E environment" and get default flags
      const hasFeatureOverrides = featureFlags && Object.keys(featureFlags).length > 0;
      if (hasFeatureOverrides) {
        window._e2ePlaywrightEnvironment = true; // Mark as Playwright E2E environment only when needed
      }

      window._e2eOverwrites = featureFlags;

      // Hide the react-query devtool button during tests
      const style = document.createElement("style");
      style.setAttribute("data-style", "playwright-injected");
      style.textContent = `
            #react-query-devtool-btn { display: none !important; }
        `;
      document.head.appendChild(style);
    },
    { featureFlags }
  );
}
