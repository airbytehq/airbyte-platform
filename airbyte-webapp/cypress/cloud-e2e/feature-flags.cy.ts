import { FeatureItem } from "@src/core/services/features/types";

describe("AllowDBTCloudIntegration", () => {
  beforeEach(() => {
    cy.login();
    cy.selectWorkspace();
    cy.contains("Settings").click({ force: true });
  });
  afterEach(cy.logout);

  describe("when true, show dbt-Cloud-specific UI", () => {
    before(() =>
      cy.setFeatureFlags({
        [FeatureItem.AllowDBTCloudIntegration]: { enabled: true },
      })
    );
    it("shows the Integration link in the settings page", () => {
      cy.contains("Integrations");
    });
  });
  describe("when false, hide dbt-Cloud-specific UI", () => {
    before(() => cy.setFeatureFlags({ [FeatureItem.AllowDBTCloudIntegration]: { enabled: false } }));
    it("has no dbt Cloud Integration link in the settings page", () => {
      cy.contains("Account").should("exist");
      cy.contains("Integrations").should("not.exist");
    });
  });
});
