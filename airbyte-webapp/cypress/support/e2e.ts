import registerCypressGrep from "@cypress/grep";
import { FeatureItem, FeatureSet } from "@src/core/services/features/types";
import { Experiments } from "@src/hooks/services/Experiment/experiments";
import { requestWorkspaceId, completeInitialSetup } from "commands/api";
require("dd-trace/ci/cypress/support");

export const featureFlags: Partial<Experiments> = {};
export const featureServiceOverrides: FeatureSet = {};

export const setFeatureFlags = (flags: Partial<Experiments>) => {
  if (Object.keys(flags).length === 0) {
    Object.keys(featureFlags).forEach((key) => delete featureFlags[key as keyof Experiments]);
  } else {
    Object.assign(featureFlags, flags);
  }
};

export const setFeatureServiceFlags = (flags: FeatureSet) => {
  if (Object.keys(flags).length === 0) {
    Object.keys(featureServiceOverrides).forEach((key) => delete featureServiceOverrides[key as FeatureItem]);
  } else {
    Object.assign(featureServiceOverrides, flags);
  }
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
