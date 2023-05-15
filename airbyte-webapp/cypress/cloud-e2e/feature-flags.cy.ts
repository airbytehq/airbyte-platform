import { FeatureItem } from "@src/hooks/services/Feature/types";

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
        [FeatureItem.AllowDBTCloudIntegration]: true,
      })
    );
    it("shows the dbt Cloud Integration link in the settings page", () => {
      cy.contains("dbt Cloud Integration");
    });
  });
  describe("when false, hide dbt-Cloud-specific UI", () => {
    before(() => cy.setFeatureFlags({ [FeatureItem.AllowDBTCloudIntegration]: false }));
    it("has no dbt Cloud Integration link in the settings page", () => {
      cy.contains("dbt Cloud Integration").should("not.exist");
    });
  });
});
