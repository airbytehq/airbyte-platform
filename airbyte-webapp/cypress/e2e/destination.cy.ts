import { requestDeleteDestination } from "@cy/commands/api";
import { createJsonDestinationViaApi } from "@cy/commands/connection";
import { DestinationRead } from "@src/core/api/types/AirbyteClient";
import { createLocalJsonDestination, deleteDestination, updateDestination } from "commands/destination";

describe.skip("Destination main actions", () => {
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
    createLocalJsonDestination("Test destination cypress", "/local");

    cy.url().should("include", `/destination/`);
  });

  it("Update destination", () => {
    createJsonDestinationViaApi().then((jsonDestination) => {
      destination = jsonDestination;
      updateDestination(jsonDestination.name, "connectionConfiguration.destination_path", "/local/my-json");

      cy.get("div[data-id='success-result']").should("exist");
      cy.get("input[value='/local/my-json']").should("exist");
    });
  });

  it("Can edit source again without leaving the page", () => {
    createJsonDestinationViaApi().then((jsonDestination) => {
      destination = jsonDestination;
      updateDestination(jsonDestination.name, "connectionConfiguration.destination_path", "/local/my-json");

      cy.get("div[data-id='success-result']").should("exist");
      cy.get("input[value='/local/my-json']").should("exist");
    });

    cy.get("button[type=submit]").should("be.disabled");

    cy.get("input[name='connectionConfiguration.destination_path']").clear();
    cy.get("input[name='connectionConfiguration.destination_path']").type("/local/my-json2");
    cy.get("button[type=submit]").should("be.enabled");
  });

  it("Delete destination", () => {
    createJsonDestinationViaApi().then((jsonDestination) => {
      deleteDestination(jsonDestination.name);

      cy.visit("/destination");
      cy.get("div").contains(jsonDestination.name).should("not.exist");
    });
  });
});
