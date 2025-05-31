import { Page } from "@playwright/test";
import { FeatureItem, FeatureSet } from "@src/core/services/features/types";
import { Experiments } from "@src/hooks/services/Experiment/experiments";

// Store flags in Node context
export const featureFlags: Partial<Experiments> = {};
export const featureServiceOverrides: FeatureSet = {};

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
