import { submitButtonClick } from "commands/common";

const OSS_SECURITY_CHECK_URL = "https://oss.airbyte.com/security-check";

export const fillSetupForm = () => {
  cy.get("input[name=email]").type("test-email-onboarding@test-onboarding-domain.com");
  cy.get("input[name=organizationName]").type("ACME Corp");
};

describe("Setup actions", () => {
  beforeEach(() => {
    cy.intercept("GET", "/api/v1/instance_configuration", (req) => {
      req.continue((res) => {
        res.body.initialSetupComplete = false;
        res.send(res.body);
      });
    });
  });

  it("Should not allow finishing setup while security check is ongoing", () => {
    cy.intercept("POST", OSS_SECURITY_CHECK_URL, {
      statusCode: 200,
      body: "delayed response",
      delay: 60_000, // delay request for very long, so it will still be loading while the test runs
    });
    cy.visit("/setup");
    cy.url().should("include", `/setup`);

    fillSetupForm();

    cy.get("[data-testid=securityCheckRunning]").should("be.visible");
    cy.get("button[type=submit]").should("be.disabled");
  });

  it("Should allow setting up in case of security check fails", () => {
    cy.intercept("POST", OSS_SECURITY_CHECK_URL, {
      statusCode: 500,
      body: "oh noes, internal server error 🤷",
    });
    cy.visit("/setup");
    cy.url().should("include", `/setup`);

    fillSetupForm();

    cy.get("button[type=submit]").should("be.enabled");
  });

  it("Should show error and require marking checkbox if installation unsecured", () => {
    cy.intercept("POST", OSS_SECURITY_CHECK_URL, {
      statusCode: 200,
      body: {
        status: "open",
      },
    });
    cy.visit("/setup");
    cy.url().should("include", `/setup`);

    fillSetupForm();

    cy.get("button[type=submit]").should("be.disabled");
    cy.get("[data-testid=advancedOptions]").click();
    cy.get("[data-testid=overwriteSecurityCheck]").click({ force: true });

    cy.get("button[type=submit]").should("be.enabled");
  });

  it("Should redirect to connections page after email is entered on closed installation", () => {
    cy.intercept("POST", OSS_SECURITY_CHECK_URL, {
      statusCode: 200,
      body: {
        status: "closed",
      },
    });
    cy.visit("/setup");
    cy.url().should("include", `/setup`);

    fillSetupForm();

    submitButtonClick();

    cy.url().should("match", /.*\/connections/);
  });
});
