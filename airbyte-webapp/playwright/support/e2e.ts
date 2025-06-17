import { Page } from "@playwright/test";
import { FeatureItem, FeatureSet } from "@src/core/services/features/types";
import { Experiments } from "@src/hooks/services/Experiment/experiments";

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

export const featureFlags: Partial<Experiments> = {};
export const featureServiceOverrides: FeatureSet = {};

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
  console.log("Setting feature flags:", flags);
  if (Object.keys(flags).length === 0) {
    Object.keys(featureFlags).forEach((key) => delete featureFlags[key as keyof Experiments]);
  } else {
    Object.assign(featureFlags, flags);
  }
};

// Set FEATURE_SERVICE feature flags
export const setFeatureServiceFlags = (flags: FeatureSet) => {
  if (Object.keys(flags).length === 0) {
    Object.keys(featureServiceOverrides).forEach((key) => delete featureServiceOverrides[key as FeatureItem]);
  } else {
    Object.assign(featureServiceOverrides, flags);
  }
};

// Inject feature flags and style into the browser context before each test
export async function injectFeatureFlagsAndStyle(page: Page) {
  console.log("Injecting feature flags and style into browser context:", {
    featureFlags,
    featureServiceOverrides,
  });

  await page.addInitScript(
    ({ featureFlags, featureServiceOverrides }) => {
      // Log in the browser context
      console.log("E2E: Setting window._e2eOverwrites and window._e2eFeatureOverwrites", {
        featureFlags,
        featureServiceOverrides,
      });

      window._e2eOverwrites = featureFlags;
      window._e2eFeatureOverwrites = featureServiceOverrides;

      // Hide the react-query devtool button during tests
      const style = document.createElement("style");
      style.setAttribute("data-style", "playwright-injected");
      style.textContent = `
            #react-query-devtool-btn { display: none !important; }
        `;
      document.head.appendChild(style);
    },
    { featureFlags, featureServiceOverrides }
  );
}
