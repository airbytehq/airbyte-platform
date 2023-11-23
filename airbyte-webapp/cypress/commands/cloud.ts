// ***********************************************
// For  examples of custom commands please read more here:
// https://on.cypress.io/custom-commands
// ***********************************************
/// <reference types="cypress" />
import type { Experiments } from "@src/hooks/services/Experiment/experiments";

import { FeatureItem } from "@src/core/services/features/types";
import { TestUserCredentials, testUser } from "support/test-users";

// TODO rewrite to login programmatically, instead of by clicking through the UI. This
// will be faster and less brittle.
Cypress.Commands.add("login", (user: TestUserCredentials = testUser) => {
  if (!user.password || !user.email) {
    throw new Error("A login email and password must be configured - see README for details");
  }
  cy.visit("/login");

  cy.get("[data-testid='login.email']", { timeout: 10000 }).type(user.email);
  cy.get("[data-testid='login.password']").type(user.password);
  cy.get("[data-testid='login.submit']").click();
  cy.hasNavigatedTo("/workspaces");
});

// TODO rewrite to logout programmatically, instead of by clicking through the UI. This
// will be faster and less brittle.
Cypress.Commands.add("logout", () => {
  cy.contains("Settings").click({ force: true });
  // TODO add cy.visitInWorkspace command
  cy.hasNavigatedTo("/settings");
  cy.get("[data-testid='button.signout']").click({ force: true });
  cy.hasNavigatedTo("/login");
});

// The `workspace` argument can either be a 0-based numerical index (e.g. to select the
// second workspace, use `cy.selectWorkspace(1);`) or a string (i.e. the actual
// `workspaceId` from the database).
//
// If no workspace is explicitly provided, the command falls back to selecting the
// `AIRBYTE_TEST_WORKSPACE_ID` environment variable, if provided, or index 0, if not.
Cypress.Commands.add("selectWorkspace", (workspace?: string | number) => {
  let workspaceId: string | number;
  if (workspace === undefined) {
    workspaceId = Cypress.env("AIRBYTE_TEST_WORKSPACE_ID") || 0;
  } else {
    workspaceId = workspace;
  }

  function selectWorkspaceFromList(workspaceId: string | number) {
    typeof workspaceId === "number"
      ? cy
          .get(`[data-testid="select-workspace-${workspaceId}"]`, { timeout: Cypress.config("pageLoadTimeout") })
          .click()
      : cy.visit(`/workspaces/${workspaceId}`);
  }

  cy.url().then((url) => {
    if (url.endsWith("/workspaces")) {
      selectWorkspaceFromList(workspaceId);
    } else {
      cy.visit("/workspaces");
      selectWorkspaceFromList(workspaceId);
    }
  });
});

// Validate SPA routing actions that don't trigger any of cypress's built-in navigation
// detection logic (and thus don't automatically use the longer pageload timeout). Also
// useful for avoiding confusing timeout snafus when the `pageload` event has fired but
// react is still bootstrapping the UI.
Cypress.Commands.add("hasNavigatedTo", (pathSpec: string | RegExp) => {
  const match = typeof pathSpec === "string" ? "include" : "match";
  return cy.location("pathname", { timeout: Cypress.config("pageLoadTimeout") }).should(match, pathSpec);
});

// Override the launchdarkly API to force a response containing specific flag variants.
// Also silences the events and clientstream APIs, since they clutter up the command log
// and add no value to our test setup.
type FeatureFlagOverrides = Partial<Record<FeatureItem, { enabled: boolean }> & Experiments>;
type FlagKey = keyof FeatureFlagOverrides;
type FeatureFlagOverrideResponse = Partial<Record<FlagKey, { value: FeatureFlagOverrides[FlagKey] }>>;
Cypress.Commands.add("setFeatureFlags", (flagOverrides: FeatureFlagOverrides) => {
  // turn off push (EventSource) updates from LaunchDarkly
  cy.intercept({ hostname: /.*clientstream.launchdarkly.com/ }, (req) => {
    req.reply("data: no streaming feature flag data here\n\n", {
      "content-type": "text/event-stream; charset=utf-8",
    });
  });

  // ignore api calls to events endpoint
  cy.intercept({ hostname: /.*events.launchdarkly.com/ }, { body: {}, log: false });

  // return feature flag values in format expected by launchdarkly client
  cy.intercept({ hostname: /.*app.launchdarkly.com/ }, (req) => {
    const body: FeatureFlagOverrideResponse = {};
    Object.entries(flagOverrides).forEach(([featureFlagName, featureFlagValue]) => {
      let flagName = featureFlagName;
      if (Object.values(FeatureItem).includes(featureFlagName as FeatureItem)) {
        flagName = `featureService.${featureFlagName}`;
      }
      body[flagName as FlagKey] = { value: featureFlagValue };
    });
    req.reply({ body });
  });
});

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Cypress {
    interface Chainable {
      login(user?: TestUserCredentials): Chainable<Element>;
      logout(): Chainable<Element>;
      selectWorkspace(workspace?: string | number): Chainable<Element>;
      hasNavigatedTo(pathSpec: string | RegExp): Chainable<string>;
      setFeatureFlags(flagOverrides: FeatureFlagOverrides): Chainable<Element>;
    }
  }
}
