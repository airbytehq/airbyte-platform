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
    it("shows the dbt Cloud Integration link in the settings page", () => {
      cy.contains("dbt Cloud Integration");
    });
  });
  describe("when false, hide dbt-Cloud-specific UI", () => {
    before(() => cy.setFeatureFlags({ [FeatureItem.AllowDBTCloudIntegration]: { enabled: false } }));
    it("has no dbt Cloud Integration link in the settings page", () => {
      cy.contains("User settings").should("exist");
      cy.contains("dbt Cloud Integration").should("not.exist");
    });
  });
});
