import { createPokeApiSourceViaApi, createPostgresSourceViaApi } from "@cy/commands/connection";
import { submitButtonClick, updateField } from "commands/common";
import { fillPokeAPIForm } from "commands/connector";
import { createPostgresSource, deleteSource, updateSource } from "commands/source";

import { selectServiceType } from "pages/createConnectorPage";
import { openHomepage } from "pages/sidebar";
import { goToSourcePage, openNewSourcePage } from "pages/sourcePage";

describe("Source main actions", () => {
  it("Should redirect from source list page to create source page if no sources are configured", () => {
    cy.intercept("POST", "/api/v1/sources/list", {
      statusCode: 200,
      body: {
        sources: [],
      },
    });

    cy.visit("/source");

    cy.url().should("match", /.*\/source\/new-source/);
  });

  it("Create new source", () => {
    cy.intercept("/api/v1/sources/create").as("createSource");
    createPostgresSource("Test source cypress");

    cy.wait("@createSource", { timeout: 30000 }).then(({ response }) => {
      cy.url().should("include", `/source/${response?.body.sourceId}`);
    });
  });

  it("Update source", () => {
    createPokeApiSourceViaApi().then((pokeApiSource) => {
      updateSource(pokeApiSource.name, "connectionConfiguration.pokemon_name", "ivysaur", true);
    });

    cy.get("div[data-id='success-result']").should("exist");
    cy.get("[data-testid='connectionConfiguration.pokemon_name-listbox-button']").contains("ivysaur").should("exist");
  });

  it("Can edit source again without leaving the page", () => {
    createPokeApiSourceViaApi().then((pokeApiSource) => {
      updateSource(pokeApiSource.name, "connectionConfiguration.pokemon_name", "ivysaur", true);
    });

    cy.get("div[data-id='success-result']").should("exist");
    cy.get("[data-testid='connectionConfiguration.pokemon_name-listbox-button']").contains("ivysaur").should("exist");
    cy.get("button[type=submit]").should("be.disabled");

    updateField("connectionConfiguration.pokemon_name", "bulbasaur", true);
    cy.get("button[type=submit]").should("be.enabled");
  });

  it("Delete source", () => {
    createPostgresSourceViaApi().then((postgresSource) => {
      deleteSource(postgresSource.sourceName);

      cy.visit("/source");
      cy.get("div").contains(postgresSource.sourceName).should("not.exist");
    });
  });
});

describe("Unsaved changes modal on create source page", () => {
  it("Check leaving Source page without any changes", () => {
    goToSourcePage();
    openNewSourcePage();

    openHomepage();

    cy.url().should("include", "/connections");
    cy.get("[data-testid='confirmationModal']").should("not.exist");
  });

  it("Check leaving Source page without any changes after selection type", () => {
    goToSourcePage();
    openNewSourcePage();
    selectServiceType("PokeAPI", "marketplace");

    openHomepage();

    cy.url().should("include", "/connections");
    cy.get("[data-testid='confirmationModal']").should("not.exist");
  });

  it("Check leaving Source page without any changes", () => {
    goToSourcePage();
    openNewSourcePage();
    fillPokeAPIForm("testName", "bulbasaur");

    openHomepage();

    cy.get("[data-testid='confirmationModal']").should("exist");
    cy.get("[data-testid='confirmationModal']").contains("Unsaved changes will be lost");
    cy.get("[data-testid='confirmationModal']").contains(
      "Your changes will be lost if you navigate away from this page. Are you sure you want to leave?"
    );
  });

  it("Check leaving Source page after failing testing", () => {
    cy.intercept("/api/v1/scheduler/sources/check_connection", {
      statusCode: 200,
      body: {
        status: "failed",
        message: "Something went wrong",
        jobInfo: {
          id: "df510ff4-9869-4cdd-bb6b-d73ed6641f2b",
          configType: "check_connection_source",
          configId: "Optional[6371b14b-bc68-4236-bfbd-468e8df8e968]",
          createdAt: 1697449693559,
          endedAt: 1697449702965,
          succeeded: true,
          connectorConfigurationUpdated: false,
          logs: {
            logLines: [],
          },
        },
      },
    }).as("checkSourceUpdateConnection");

    goToSourcePage();
    openNewSourcePage();
    fillPokeAPIForm("testName", "bulbasaur");
    submitButtonClick();

    cy.wait("@checkSourceUpdateConnection", { timeout: 5000 });

    openHomepage();

    cy.get("[data-testid='confirmationModal']").should("exist");
    cy.get("[data-testid='confirmationModal']").contains("Unsaved changes will be lost");
    cy.get("[data-testid='confirmationModal']").contains(
      "Your changes will be lost if you navigate away from this page. Are you sure you want to leave?"
    );
  });
});
