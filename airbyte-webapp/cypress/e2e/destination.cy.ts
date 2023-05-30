import { createJsonDestinationViaApi } from "@cy/commands/connection";
import { createLocalJsonDestination, deleteDestination, updateDestination } from "commands/destination";

describe("Destination main actions", () => {
  it("Create new destination", () => {
    createLocalJsonDestination("Test destination cypress", "/local");

    cy.url().should("include", `/destination/`);
  });

  it("Update destination", () => {
    createJsonDestinationViaApi().then((jsonDestination) => {
      updateDestination(jsonDestination.name, "connectionConfiguration.destination_path", "/local/my-json");

      cy.get("div[data-id='success-result']").should("exist");
      cy.get("input[value='/local/my-json']").should("exist");
    });
  });

  it("Delete destination", () => {
    createJsonDestinationViaApi().then((jsonDestination) => {
      deleteDestination(jsonDestination.name);

      cy.visit("/destination");
      cy.get("div").contains(jsonDestination.name).should("not.exist");
    });
  });
});
