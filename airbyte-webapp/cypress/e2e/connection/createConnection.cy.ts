import { getWorkspaceId } from "@cy/commands/api/workspace";
import {
  createNewConnectionViaApi,
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
} from "@cy/commands/connection";
import { fillLocalJsonForm, fillPokeAPIForm } from "@cy/commands/connector";
import { StreamRowPageObjectV2 } from "@cy/pages/connection/StreamRowPageObjectV2";
import { streamsTableV2 } from "@cy/pages/connection/StreamsTablePageObjectV2";
import { goToDestinationPage, openDestinationConnectionsPage } from "@cy/pages/destinationPage";
import { openSourceConnectionsPage, goToSourcePage } from "@cy/pages/sourcePage";
import { setFeatureFlags, setFeatureServiceFlags } from "@cy/support/e2e";
import { WebBackendConnectionRead, DestinationRead, SourceRead } from "@src/core/api/types/AirbyteClient";
import { RoutePaths, ConnectionRoutePaths } from "@src/pages/routePaths";
import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "commands/api";
import { appendRandomString, submitButtonClick } from "commands/common";
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
import * as replicationPage from "pages/connection/connectionReplicationPageObject";
import * as newConnectionPage from "pages/connection/createConnectionPageObject";
import { nextButton } from "pages/connection/createConnectionPageObject";
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
        // using ConnectionsListPage.visit() intercepts connections/list endpoint, which will not be called if this is the first connection being created
        cy.visit(`/workspaces/${getWorkspaceId()}/connections`);

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
            cy.location("search").should(
              "eq",
              `?sourceId=${source.sourceId}&destination_tab=marketplace&destinationId=${createdDestinationId}`
            );

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
          fillPokeAPIForm(testPokeSourceName, "bulbasaur");
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
      const dummyStreamRow = new StreamRowPageObject("public", "dummy_table_1");

      dummyStreamRow.toggleStreamSync();
      dummyStreamRow.isStreamSyncEnabled(true);
      dummyStreamRow.selectSyncMode("full_refresh", "overwrite");

      cy.get(nextButton).scrollIntoView();
      cy.get(nextButton).click();
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

    it("should filter table by stream name", () => {
      streamsTable.searchStream("dummy_table_10");
      newConnectionPage.checkAmountOfStreamTableRows(1);
    });
  });

  describe("Streams table V2", () => {
    before(() => {
      setFeatureFlags({ "connection.syncCatalogV2": true });
      setFeatureServiceFlags({ SYNC_CATALOG_V2: true });

      interceptDiscoverSchemaRequest();

      cy.visit(
        `/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}?sourceId=${source.sourceId}&destinationId=${destination.destinationId}`
      );
      waitForDiscoverSchemaRequest();
    });

    after(() => {
      setFeatureFlags({ "connection.syncCatalogV2": false });
      setFeatureServiceFlags({ SYNC_CATALOG_V2: false });
    });

    it("should have no streams checked by default", () => {
      streamsTableV2.isNamespaceCheckboxChecked(false);
      streamsTableV2.areAllStreamsInNamespaceEnabled("public", false);
    });

    it("should verify namespace row", () => {
      streamsTableV2.isNamespaceCheckboxEnabled(true);
      streamsTableV2.isNamespaceCheckboxChecked(false);
      streamsTableV2.isNamespaceNameDisplayed("public", true);
      streamsTableV2.isTotalAmountOfStreamsDisplayed(21, true);
      streamsTableV2.isOpenNamespaceModalGearButtonDisplayed(true);
      streamsTableV2.isSyncModeColumnNameDisplayed();
      streamsTableV2.isFieldsColumnNameDisplayed();
    });

    it("should show 'no selected streams' error ", () => {
      streamsTableV2.isNoStreamsSelectedErrorDisplayed(true);
      streamsTableV2.areAllStreamsInNamespaceEnabled("public", false);
      newConnectionPage.isNextPageButtonEnabled(false);
    });

    it("should NOT show 'no selected streams' error", () => {
      streamsTableV2.toggleNamespaceCheckbox("public", true);
      streamsTableV2.areAllStreamsInNamespaceEnabled("public", true);

      streamsTableV2.isNoStreamsSelectedErrorDisplayed(false);
      newConnectionPage.isNextPageButtonEnabled(false);

      streamsTableV2.toggleNamespaceCheckbox("public", false);
    });

    it("should not replace refresh schema button with form controls", () => {
      streamsTableV2.isRefreshSourceSchemaBtnExist(true);
      streamsTableV2.isToggleExpandCollapseAllStreamsBtnExist(true);

      streamsTableV2.toggleNamespaceCheckbox("public", true);
      streamsTableV2.isRefreshSourceSchemaBtnExist(true);
      streamsTableV2.isToggleExpandCollapseAllStreamsBtnExist(true);

      streamsTableV2.toggleNamespaceCheckbox("public", false);
    });

    it("should enable all streams in namespace", () => {
      streamsTableV2.toggleNamespaceCheckbox("public", true);
      streamsTableV2.areAllStreamsInNamespaceEnabled("public", true);
      streamsTableV2.toggleNamespaceCheckbox("public", false);
      streamsTableV2.areAllStreamsInNamespaceEnabled("public", false);
    });
  });

  describe("Stream", () => {
    before(() => {
      interceptDiscoverSchemaRequest();

      cy.visit(
        `/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}?sourceId=${source.sourceId}&destinationId=${destination.destinationId}`
      );
      waitForDiscoverSchemaRequest();
    });

    const usersStreamRow = new StreamRowPageObject("public", "users");

    it("should have no streams checked by default", () => {
      cy.get(replicationPage.nextButtonOrLink).should("be.disabled");
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

    it("should have unchecked sync switch after click and default stream style", () => {
      usersStreamRow.toggleStreamSync();
      usersStreamRow.isStreamSyncEnabled(false);
    });

    it("should enable form submit after a stream is selected and configured", () => {
      usersStreamRow.toggleStreamSync();
      usersStreamRow.selectSyncMode("full_refresh", "overwrite");
      newConnectionPage.getNoStreamsSelectedError().should("not.exist");
      cy.get(replicationPage.nextButtonOrLink).should("not.be.disabled");
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

  describe("Stream V2", () => {
    before(() => {
      setFeatureFlags({ "connection.syncCatalogV2": true });
      setFeatureServiceFlags({ SYNC_CATALOG_V2: true });

      interceptDiscoverSchemaRequest();

      cy.visit(
        `/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}?sourceId=${source.sourceId}&destinationId=${destination.destinationId}`
      );
      waitForDiscoverSchemaRequest();
    });

    after(() => {
      setFeatureFlags({ "connection.syncCatalogV2": false });
      setFeatureServiceFlags({ SYNC_CATALOG_V2: false });
    });

    const usersStreamRow = new StreamRowPageObjectV2("public", "users");

    it("should enable and disable stream", () => {
      streamsTableV2.filterByStreamOrFieldName("users");
      streamsTableV2.isNamespaceCheckboxEnabled(false);
      usersStreamRow.streamHasDisabledStyle(true);
      usersStreamRow.toggleStreamSync(true);
      usersStreamRow.isStreamSyncEnabled(true);
      usersStreamRow.streamHasDisabledStyle(false);
      usersStreamRow.streamHasAddedStyle(false);
      usersStreamRow.isMissedCursorErrorDisplayed(true);

      usersStreamRow.toggleStreamSync(false);
      usersStreamRow.isStreamSyncEnabled(false);
    });

    it("should expand and collapse stream", () => {
      usersStreamRow.toggleExpandCollapseStream();
      usersStreamRow.isStreamExpanded(true);
      usersStreamRow.toggleExpandCollapseStream();
      usersStreamRow.isStreamExpanded(false);
    });

    it("should enable field", () => {
      usersStreamRow.isStreamSyncEnabled(false);
      usersStreamRow.toggleExpandCollapseStream();
      usersStreamRow.fieldHasDisabledStyle("email", true);

      usersStreamRow.toggleFieldSync("email", true);
      usersStreamRow.fieldHasDisabledStyle("email", false);
      usersStreamRow.fieldHasAddedStyle("email", false);
      usersStreamRow.isFieldSyncCheckboxDisabled("id", true);
      usersStreamRow.isPKField("id", true);

      usersStreamRow.isFieldSyncEnabled("email", true);
      usersStreamRow.isMissedCursorErrorDisplayed(true);
      usersStreamRow.isStreamSyncEnabled(true);
      streamsTableV2.isNamespaceCheckboxMixed(true);
    });

    it("should enable form submit after a stream is selected and configured", () => {
      usersStreamRow.selectSyncMode("full_refresh", "overwrite");
      streamsTableV2.isNoStreamsSelectedErrorDisplayed(false);
      newConnectionPage.isNextPageButtonEnabled(true);
    });
  });

  describe("Submit form", () => {
    it("should set up a connection", () => {
      interceptCreateConnectionRequest();
      cy.get(nextButton).click();
      submitButtonClick(true);

      waitForCreateConnectionRequest().then((interception) => {
        assert.isNotNull(interception.response?.statusCode, "200");
        expect(interception.request.method).to.eq("POST");

        const connection: Partial<WebBackendConnectionRead> = {
          name: `${source.name} → ${destination.name}`,
          scheduleType: "basic",
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

  describe("Editing", () => {
    it("should have added stream style after modifying", () => {
      cy.visit(`/workspaces/${getWorkspaceId()}/connections/${connectionId}/replication`);

      const usersStreamRow = new StreamRowPageObject("public", "dummy_table_1");

      usersStreamRow.toggleStreamSync();
      usersStreamRow.hasAddedStyle(true);

      usersStreamRow.toggleStreamSync();
      usersStreamRow.hasAddedStyle(false);
    });
  });
});
