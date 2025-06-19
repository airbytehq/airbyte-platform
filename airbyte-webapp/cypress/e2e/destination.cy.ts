import { requestDeleteDestination } from "@cy/commands/api";
import { createE2ETestingDestinationViaApi } from "@cy/commands/connection";
import { DestinationRead } from "@src/core/api/types/AirbyteClient";
import { createE2ETestingDestination, deleteDestination, updateDestination } from "commands/destination";

describe("Destination main actions", () => {
  let destination: DestinationRead;

  afterEach(() => {
    if (destination) {
      requestDeleteDestination({ destinationId: destination.destinationId });
    }
  });

  it("Should redirect from destination list page to create destination page if no destinations are configured", () => {
    cy.intercept("POST", "/api/v1/destinations/list", {
      statusCode: 200,
      body: {
        destinations: [],
      },
    });

    cy.visit("/destination");

    cy.url().should("match", /.*\/destination\/new-destination/);
  });
  it("Create new destination", () => {
    createE2ETestingDestination("E2E Test destination cypress");

    cy.url().should("include", `/destination/`);
  });

  it("Update destination", () => {
    createE2ETestingDestinationViaApi().then((e2eTestingDestination) => {
      destination = e2eTestingDestination;
      updateDestination(
        e2eTestingDestination.name,
        "connectionConfiguration.test_destination.logging_config.max_entry_count",
        "10"
      );

      cy.get("div[data-id='success-result']").should("exist");
      cy.get("input[value='10']").should("exist");
    });
  });

  it("Can edit source again without leaving the page", () => {
    createE2ETestingDestinationViaApi().then((e2eTestingDestination) => {
      destination = e2eTestingDestination;
      updateDestination(
        e2eTestingDestination.name,
        "connectionConfiguration.test_destination.logging_config.max_entry_count",
        "10"
      );

      cy.get("div[data-id='success-result']").should("exist");
      cy.get("input[value='10']").should("exist");
    });

    cy.get("button[type=submit]").should("be.disabled");

    cy.get("input[name='connectionConfiguration.test_destination.logging_config.max_entry_count']").clear();
    cy.get("input[name='connectionConfiguration.test_destination.logging_config.max_entry_count']").type("10");
    cy.get("button[type=submit]").should("be.enabled");
  });

  it("Delete destination", () => {
    createE2ETestingDestinationViaApi().then((e2eTestingDestination) => {
      deleteDestination(e2eTestingDestination.name);

      cy.visit("/destination");
      cy.get("div").contains(e2eTestingDestination.name).should("not.exist");
    });
  });
});
