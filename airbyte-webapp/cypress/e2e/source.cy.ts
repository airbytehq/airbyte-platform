import { createPokeApiSourceViaApi, createPostgresSourceViaApi } from "@cy/commands/connection";
import { submitButtonClick } from "commands/common";
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

    cy.wait("@createSource", { timeout: 30000 }).then((interception) => {
      assert("include", `/source/${interception.response?.body.Id}`);
    });
  });

  it("Update source", () => {
    createPokeApiSourceViaApi().then((pokeApiSource) => {
      updateSource(pokeApiSource.name, "connectionConfiguration.pokemon_name", "rattata");
    });

    cy.get("div[data-id='success-result']").should("exist");
    cy.get("input[value='rattata']").should("exist");
  });

  it("Can edit source again without leaving the page", () => {
    createPokeApiSourceViaApi().then((pokeApiSource) => {
      updateSource(pokeApiSource.name, "connectionConfiguration.pokemon_name", "rattata");
    });

    cy.get("div[data-id='success-result']").should("exist");
    cy.get("input[value='rattata']").should("exist");
    cy.get("button[type=submit]").should("be.disabled");

    cy.get("input[name='connectionConfiguration.pokemon_name']").clear();
    cy.get("input[name='connectionConfiguration.pokemon_name']").type("ditto");
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
    selectServiceType("PokeAPI");

    openHomepage();

    cy.url().should("include", "/connections");
    cy.get("[data-testid='confirmationModal']").should("not.exist");
  });

  it("Check leaving Source page without any changes", () => {
    goToSourcePage();
    openNewSourcePage();
    fillPokeAPIForm("testName", "ditto");

    openHomepage();

    cy.get("[data-testid='confirmationModal']").should("exist");
    cy.get("[data-testid='confirmationModal']").contains("Discard changes");
    cy.get("[data-testid='confirmationModal']").contains(
      "There are unsaved changes. Are you sure you want to discard your changes?"
    );
  });

  it("Check leaving Source page after failing testing", () => {
    cy.intercept("/api/v1/scheduler/sources/check_connection").as("checkSourceUpdateConnection");

    goToSourcePage();
    openNewSourcePage();
    fillPokeAPIForm("testName", "name");
    submitButtonClick();

    cy.wait("@checkSourceUpdateConnection", { timeout: 5000 });

    openHomepage();

    cy.get("[data-testid='confirmationModal']").should("exist");
    cy.get("[data-testid='confirmationModal']").contains("Discard changes");
    cy.get("[data-testid='confirmationModal']").contains(
      "There are unsaved changes. Are you sure you want to discard your changes?"
    );
  });
});
