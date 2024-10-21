import registerCypressGrep from "@cypress/grep";
import { FeatureSet } from "@src/core/services/features/types";
import { Experiments } from "@src/hooks/services/Experiment/experiments";
import { requestWorkspaceId, completeInitialSetup } from "commands/api";
require("dd-trace/ci/cypress/support");

export const featureFlags: Partial<Experiments> = {};
export const featureServiceOverrides: FeatureSet = {};

export const setFeatureFlags = (flags: Record<string, boolean>) => {
  Object.assign(featureFlags, flags);
};

export const setFeatureServiceFlags = (flags: Record<string, boolean>) => {
  Object.assign(featureServiceOverrides, flags);
};

Cypress.on("window:load", (window) => {
  // Hide the react-query devtool button during tests, so it never accidentally overlaps some button
  // that cypress needs to click
  const style = document.createElement("style");
  style.setAttribute("data-style", "cypress-injected");
  style.textContent = `
    #react-query-devtool-btn { display: none !important; }
  `;
  window.document.head.appendChild(style);

  window._e2eOverwrites = featureFlags;
  window._e2eFeatureOverwrites = featureServiceOverrides;
});

// we use cypress grep tags to split cypress tests in multiple CI jobs.
registerCypressGrep();

before(() => {
  requestWorkspaceId().then(completeInitialSetup);
});
