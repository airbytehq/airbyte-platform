import { submitButtonClick, fillEmail } from "commands/common";

describe("Preferences actions", () => {
  it("Should redirect to connections page after email is entered", () => {
    cy.intercept("POST", "/api/v1/workspaces/get", (req) => {
      req.continue((res) => {
        res.body.initialSetupComplete = false;
        res.send(res.body);
      });
    });
    cy.visit("/preferences");
    cy.url().should("include", `/preferences`);

    fillEmail("test-email-onboarding@test-onboarding-domain.com");
    cy.get("input[name=securityUpdates]").parent().click();

    submitButtonClick();

    cy.url().should("match", /.*\/connections/);
  });
});
