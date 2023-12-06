import {
  createNewConnectionViaApi,
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
} from "@cy/commands/connection";
import { fillLocalJsonForm, fillPokeAPIForm } from "@cy/commands/connector";
import { goToDestinationPage, openDestinationConnectionsPage } from "@cy/pages/destinationPage";
import { openSourceConnectionsPage, goToSourcePage } from "@cy/pages/sourcePage";
import { WebBackendConnectionRead, DestinationRead, SourceRead } from "@src/core/api/types/AirbyteClient";
import { RoutePaths, ConnectionRoutePaths } from "@src/pages/routePaths";
import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "commands/api";
import { appendRandomString, submitButtonClick, getSubmitButton } from "commands/common";
import { runDbQuery } from "commands/db/db";
import {
  createUsersTableQuery,
  dropUsersTableQuery,
  createDummyTablesQuery,
  dropDummyTablesQuery,
} from "commands/db/queries";
import {
  interceptCreateConnectionRequest,
  interceptDiscoverSchemaRequest,
  interceptGetSourceDefinitionsRequest,
  interceptGetSourcesListRequest,
  waitForCreateConnectionRequest,
  waitForDiscoverSchemaRequest,
  waitForGetSourceDefinitionsRequest,
  waitForGetSourcesListRequest,
} from "commands/interceptors";

import * as connectionConfigurationForm from "pages/connection/connectionFormPageObject";
import * as connectionListPage from "pages/connection/connectionListPageObject";
import * as newConnectionPage from "pages/connection/createConnectionPageObject";
import { streamDetails } from "pages/connection/StreamDetailsPageObject";
import { StreamRowPageObject } from "pages/connection/StreamRowPageObject";
import { streamsTable } from "pages/connection/StreamsTablePageObject";

describe("Connection - Create new connection", { testIsolation: false }, () => {
  let source: SourceRead;
  let destination: DestinationRead;
  let connectionId: string;

  const dropTables = () => {
    runDbQuery(dropUsersTableQuery, dropDummyTablesQuery(20));
  };
  before(() => {
    dropTables();
    runDbQuery(createUsersTableQuery, createDummyTablesQuery(20));
    createPostgresSourceViaApi().then((pgSource) => {
      source = pgSource;
    });
    createPostgresDestinationViaApi().then((pgDestination) => {
      destination = pgDestination;
    });
  });

  after(() => {
    if (connectionId) {
      requestDeleteConnection({ connectionId });
    }
    if (source) {
      requestDeleteSource({ sourceId: source.sourceId });
    }
    if (destination) {
      requestDeleteDestination({ destinationId: destination.destinationId });
    }

    dropTables();
  });

  describe("Set up connection", () => {
    describe("From connection page", () => {
      it("should open 'New connection' page", () => {
        connectionListPage.visit();
        interceptGetSourcesListRequest();
        interceptGetSourceDefinitionsRequest();

        connectionListPage.clickNewConnectionButton();
        waitForGetSourcesListRequest();
        waitForGetSourceDefinitionsRequest();
      });

      it("should select existing Source from dropdown and click button", () => {
        newConnectionPage.isExistingConnectorTypeSelected("source");
        newConnectionPage.selectExistingConnectorFromList("source", source.name);
      });

      it("should select existing Destination from dropdown and click button", () => {
        interceptDiscoverSchemaRequest();
        newConnectionPage.isExistingConnectorTypeSelected("destination");
        newConnectionPage.selectExistingConnectorFromList("destination", destination.name);
        waitForDiscoverSchemaRequest();
      });

      it("should redirect to 'New connection' configuration page with stream table'", () => {
        newConnectionPage.isAtConnectionConfigurationStep();
      });
    });
    describe("From source page", () => {
      it("can use existing destination", () => {
        goToSourcePage();
        openSourceConnectionsPage(source.name);
        cy.get("button").contains("Create a connection").click();
        cy.get("button").contains(destination.name).click();
        newConnectionPage.isAtConnectionConfigurationStep();
      });
      it("can use new destination", () => {
        // this depends on the source having at least one configured connection already
        createNewConnectionViaApi(source, destination).then((connection) => {
          connectionId = connection.connectionId;

          cy.intercept("/api/v1/destinations/create").as("createDestination");

          goToSourcePage();
          openSourceConnectionsPage(source.name);
          cy.get("button").contains("add destination").click();
          cy.get("button").contains("add a new destination").click();
          cy.location("search").should("eq", `?sourceId=${source.sourceId}&destinationType=new`);

          // confirm toggling the type
          cy.get("label").contains("Select an existing destination").click();
          cy.location("search").should("eq", `?sourceId=${source.sourceId}&destinationType=existing`);

          cy.get("button").contains(destination.name).should("exist");

          cy.get("label").contains("Set up a new destination").click();
          cy.location("search").should("eq", `?sourceId=${source.sourceId}&destinationType=new`);

          fillLocalJsonForm(appendRandomString("LocalJSON Cypress"), "/local");

          cy.get("button").contains("Set up destination").click();

          cy.wait("@createDestination", { timeout: 30000 }).then((interception) => {
            const createdDestinationId = interception.response?.body.destinationId;
            cy.location("search").should("eq", `?sourceId=${source.sourceId}&destinationId=${createdDestinationId}`);

            requestDeleteDestination({ destinationId: createdDestinationId });
          });
          requestDeleteConnection({ connectionId: connection.connectionId });
        });
      });
    });
    describe("From destination page", () => {
      it("can use existing source", () => {
        goToDestinationPage();
        openDestinationConnectionsPage(destination.name);
        cy.get("button").contains("Create a connection").click();
        cy.get("button").contains(source.name).click();
        newConnectionPage.isAtConnectionConfigurationStep();
      });
      it("can use new source", () => {
        // this depends on the source having at least one configured connection already
        createNewConnectionViaApi(source, destination).then((connection) => {
          cy.intercept("/api/v1/sources/create").as("createSource");

          goToDestinationPage();
          openDestinationConnectionsPage(destination.name);
          cy.get("button").contains("add source").click();
          cy.get("button").contains("add a new source").click();
          cy.location("search").should("eq", `?destinationId=${destination.destinationId}&sourceType=new`);

          // confirm can toggle back and
          cy.get("label").contains("Select an existing source").click();
          cy.location("search").should("eq", `?destinationId=${destination.destinationId}&sourceType=existing`);

          cy.get("button").contains(source.name).should("exist");

          cy.get("label").contains("Set up a new source").click();
          cy.location("search").should("eq", `?destinationId=${destination.destinationId}&sourceType=new`);

          const testPokeSourceName = appendRandomString("Cypress Test Poke");
          fillPokeAPIForm(testPokeSourceName, "ditto");
          cy.get("button").contains("Set up source").click();
          cy.wait("@createSource", { timeout: 30000 }).then((interception) => {
            const createdSourceId = interception.response?.body.sourceId;
            newConnectionPage.isAtConnectionConfigurationStep();

            requestDeleteSource({ sourceId: createdSourceId });
          });

          newConnectionPage.isAtConnectionConfigurationStep();
          requestDeleteConnection({ connectionId: connection.connectionId });
        });
      });
    });
  });

  describe("Configuration", () => {
    it("should set 'Replication frequency' to 'Manual'", () => {
      interceptDiscoverSchemaRequest();

      cy.visit(
        `/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}?sourceId=${source.sourceId}&destinationId=${destination.destinationId}`
      );
      waitForDiscoverSchemaRequest();
      connectionConfigurationForm.selectScheduleType("Manual");
    });
  });

  describe("Streams table", () => {
    before(() => {
      interceptDiscoverSchemaRequest();

      cy.visit(
        `/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}?sourceId=${source.sourceId}&destinationId=${destination.destinationId}`
      );
      waitForDiscoverSchemaRequest();
    });
    it("should check columns names in table", () => {
      newConnectionPage.checkColumnNames();
    });

    it("should check total amount of table streams", () => {
      // dummy tables amount + users table
      newConnectionPage.checkAmountOfStreamTableRows(21);
    });

    it("should allow to scroll table to desired stream table row and it should be visible", () => {
      const desiredStreamTableRow = "dummy_table_18";

      newConnectionPage.scrollTableToStream(desiredStreamTableRow);
      newConnectionPage.isStreamTableRowVisible(desiredStreamTableRow);
    });

    it("should filter table by stream name", () => {
      streamsTable.searchStream("dummy_table_10");
      newConnectionPage.checkAmountOfStreamTableRows(1);
    });

    it("should clear stream search input field and show all available streams", () => {
      streamsTable.clearStreamSearch();
      newConnectionPage.checkAmountOfStreamTableRows(21);
    });
  });

  describe.only("Stream", () => {
    before(() => {
      interceptDiscoverSchemaRequest();

      cy.visit(
        `/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}?sourceId=${source.sourceId}&destinationId=${destination.destinationId}`
      );
      waitForDiscoverSchemaRequest();
    });

    const usersStreamRow = new StreamRowPageObject("public", "users");

    it("should have no streams checked by default", () => {
      getSubmitButton().should("be.disabled");
      newConnectionPage.getNoStreamsSelectedError().should("exist");

      // filter table for an sample stream
      streamsTable.searchStream("users");
      newConnectionPage.checkAmountOfStreamTableRows(1);

      usersStreamRow.isStreamSyncEnabled(false);
    });

    it("should have checked sync switch after click", () => {
      usersStreamRow.toggleStreamSync();
      usersStreamRow.isStreamSyncEnabled(true);
    });

    it("should have added stream style after click", () => {
      usersStreamRow.hasAddedStyle(true);
    });

    it("should have unchecked sync switch after click and default stream style", () => {
      usersStreamRow.toggleStreamSync();
      usersStreamRow.isStreamSyncEnabled(false);
      usersStreamRow.hasAddedStyle(false);
    });

    it("should enable form submit after a stream is selected", () => {
      usersStreamRow.toggleStreamSync();
      newConnectionPage.getNoStreamsSelectedError().should("not.exist");
      getSubmitButton().should("be.enabled");
    });

    it("should have data destination name", () => {
      usersStreamRow.checkDestinationNamespace("<destination schema>");
    });

    it("should have destination stream name", () => {
      usersStreamRow.checkDestinationStreamName("users");
    });

    it("should open stream details panel by clicking on stream row", () => {
      usersStreamRow.showStreamDetails();
      streamDetails.isOpen();
    });

    it("should close stream details panel by clicking on close button", () => {
      streamDetails.close();
      streamDetails.isClosed();
    });
  });

  describe("Submit form", () => {
    it("should set up a connection", () => {
      interceptCreateConnectionRequest();
      submitButtonClick(true);

      waitForCreateConnectionRequest().then((interception) => {
        assert.isNotNull(interception.response?.statusCode, "200");
        expect(interception.request.method).to.eq("POST");

        const connection: Partial<WebBackendConnectionRead> = {
          name: `${source.name} → ${destination.name}`,
          scheduleType: "manual",
        };
        expect(interception.request.body).to.contain(connection);
        expect(interception.response?.body).to.contain(connection);

        connectionId = interception.response?.body?.connectionId;
      });
    });

    it("should redirect to connection overview page after connection set up", () => {
      newConnectionPage.isAtConnectionOverviewPage(connectionId);
    });
  });
});
