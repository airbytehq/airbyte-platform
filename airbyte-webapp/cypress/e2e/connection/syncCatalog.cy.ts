import {
  createJsonDestinationViaApi,
  createNewConnectionViaApi,
  createFakerSourceViaApi,
} from "@cy/commands/connection";
import { visit } from "@cy/pages/connection/connectionPageObject";
import { setFeatureFlags } from "@cy/support/e2e";
import { DestinationRead, SourceRead, WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

const CATALOG_SEARCH_INPUT = '[data-testid="sync-catalog-search"]';

describe("Sync catalog", () => {
  let fakerSource: SourceRead;
  let jsonDestination: DestinationRead;
  let connection: WebBackendConnectionRead;

  before(() => {
    setFeatureFlags({ "connection.syncCatalogV2": true });

    createFakerSourceViaApi()
      .then((source) => {
        fakerSource = source;
      })
      .then(() => createJsonDestinationViaApi())
      .then((destination) => {
        jsonDestination = destination;
      })
      .then(() => createNewConnectionViaApi(fakerSource, jsonDestination))
      .then((connectionResponse) => {
        connection = connectionResponse;
      });
  });

  after(() => {
    setFeatureFlags({});
  });

  describe("catalog search functionality", () => {
    before(() => {
      visit(connection, "replication");
    });

    it("Should find a nested field in the sync catalog", () => {
      // Intentionally search for a partial match of a nested field (address.city)
      cy.get(CATALOG_SEARCH_INPUT).type("address.cit");
      // Expect the parent field to exist
      cy.contains(/^address$/).should("exist");
      // Exppect the nested field to exist
      cy.contains(/^address\.city$/).should("exist");

      // Search for a stream
      cy.get(CATALOG_SEARCH_INPUT).clear();
      cy.get(CATALOG_SEARCH_INPUT).type("products");
      cy.contains(/^products$/).should("exist");
      cy.contains(/^users$/).should("not.exist");
    });
  });
});
