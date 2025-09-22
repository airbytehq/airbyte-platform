import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "@cy/commands/api";
import {
  createNewConnectionViaApi,
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
} from "@cy/commands/connection";
import { cleanDBSource, runDbQuery } from "@cy/commands/db/db";
import { createUsersTableQuery } from "@cy/commands/db/queries";
import {
  interceptUpdateConnectionRequest,
  interceptValidateMappersRequest,
  waitForUpdateConnectionRequest,
  waitForValidateMappers,
} from "@cy/commands/interceptors";
import { visit } from "@cy/pages/connection/connectionPageObject";
import { setFeatureFlags, setFeatureServiceFlags } from "@cy/support/e2e";
import { DestinationRead, SourceRead, WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

describe("Connection Mappings", { tags: "@connection-configuration" }, () => {
  let postgresSource: SourceRead;
  let postgresDestination: DestinationRead;
  let connection: WebBackendConnectionRead;

  before(() => {
    cleanDBSource();
    runDbQuery(createUsersTableQuery);

    createPostgresSourceViaApi()
      .then((source) => {
        postgresSource = source;
      })
      .then(() => createPostgresDestinationViaApi())
      .then((destination) => {
        postgresDestination = destination;
      })
      .then(() => createNewConnectionViaApi(postgresSource, postgresDestination))
      .then((connectionResponse) => {
        connection = connectionResponse;
      });
  });

  beforeEach(() => {
    interceptUpdateConnectionRequest();
  });

  after(() => {
    setFeatureFlags({});
    setFeatureServiceFlags({});

    // cleanup
    if (postgresSource) {
      requestDeleteSource({ sourceId: postgresSource.sourceId });
    }
    if (postgresDestination) {
      requestDeleteDestination({
        destinationId: postgresDestination.destinationId,
      });
    }
    if (connection) {
      requestDeleteConnection({ connectionId: connection.connectionId });
    }
    cleanDBSource();
  });

  describe("With feature disabled", () => {
    before(() => {
      setFeatureServiceFlags({ MAPPINGS_UI: false });
    });
    it("Shows upsell page if Feature is false", () => {
      visit(connection);
      cy.get('[data-testid="mappings-step"]').click();
      cy.url().should("match", /\/mappings$/);
      cy.get('[data-testid="mappings-upsell-empty-state"]').should("be.visible");
    });
  });

  // eslint-disable-next-line no-only-tests/no-only-tests
  describe.only("With feature enabled", () => {
    before(() => {
      setFeatureServiceFlags({ MAPPINGS_UI: true });
    });

    it("Allows configuring a first mapping", () => {
      interceptValidateMappersRequest();

      visit(connection, "mappings");
      cy.get('[data-testid="add-stream-for-mapping-combobox"]').click();
      cy.get("li").contains("users").click();

      // initial validation call to fetch available fields
      waitForValidateMappers();
      // eslint-disable-next-line cypress/no-unnecessary-waiting
      cy.wait(1000); // seems there is an extra render cycle here.  this is needed for the test to pass as it stands today.
      cy.get('input[placeholder="Select a field"]').should("have.value", "").click();
      cy.get("li").contains("name").click();
      waitForValidateMappers();
      cy.get('[data-testid="submit-mappings"]').click();
      cy.get('[data-testid="refreshModal-save"]').click();
      waitForUpdateConnectionRequest();
      cy.get('[data-testid="notification-connection_settings_change_success"]').should("exist");
    });
  });
});
