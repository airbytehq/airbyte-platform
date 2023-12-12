import {
  getPostgresToPostgresUpdateConnectionBody,
  requestDeleteConnection,
  requestDeleteDestination,
  requestDeleteSource,
  requestUpdateConnection,
} from "@cy/commands/api";
import { appendRandomString, deleteEntity, submitButtonClick } from "@cy/commands/common";
import {
  createJsonDestinationViaApi,
  createNewConnectionViaApi,
  createPokeApiSourceViaApi,
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
} from "@cy/commands/connection";
import { runDbQuery } from "@cy/commands/db/db";
import { createUsersTableQuery, dropUsersTableQuery } from "@cy/commands/db/queries";
import {
  interceptGetConnectionRequest,
  interceptUpdateConnectionRequest,
  waitForGetConnectionRequest,
  waitForUpdateConnectionRequest,
} from "@cy/commands/interceptors";
import * as connectionForm from "@cy/pages/connection/connectionFormPageObject";
import { getSyncEnabledSwitch, visit } from "@cy/pages/connection/connectionPageObject";
import * as replicationPage from "@cy/pages/connection/connectionReplicationPageObject";
import * as statusPage from "@cy/pages/connection/statusPageObject";
import { streamsTable } from "@cy/pages/connection/StreamsTablePageObject";
import {
  AirbyteStreamAndConfiguration,
  ConnectionStatus,
  DestinationRead,
  DestinationSyncMode,
  SourceRead,
  SyncMode,
  WebBackendConnectionRead,
} from "@src/core/api/types/AirbyteClient";
import { ConnectionRoutePaths, RoutePaths } from "@src/pages/routePaths";

import * as connectionSettings from "pages/connection/connectionSettingsPageObject";

describe("Connection Configuration", () => {
  let pokeApiSource: SourceRead;
  let postgresSource: SourceRead;
  let jsonDestination: DestinationRead;
  let postgresDestination: DestinationRead;
  let connection: WebBackendConnectionRead | null;

  before(() => {
    runDbQuery(dropUsersTableQuery);
    runDbQuery(createUsersTableQuery);

    createPokeApiSourceViaApi().then((source) => {
      pokeApiSource = source;
    });
    createPostgresSourceViaApi().then((source) => {
      postgresSource = source;
    });
    createJsonDestinationViaApi().then((destination) => {
      jsonDestination = destination;
    });
    createPostgresDestinationViaApi().then((destination) => {
      postgresDestination = destination;
    });
  });

  afterEach(() => {
    if (connection) {
      requestDeleteConnection({ connectionId: connection.connectionId });
      connection = null;
    }
  });

  after(() => {
    if (pokeApiSource) {
      requestDeleteSource({ sourceId: pokeApiSource.sourceId });
    }
    if (postgresSource) {
      requestDeleteSource({ sourceId: postgresSource.sourceId });
    }
    if (jsonDestination) {
      requestDeleteDestination({
        destinationId: jsonDestination.destinationId,
      });
    }
    if (postgresDestination) {
      requestDeleteDestination({
        destinationId: postgresDestination.destinationId,
      });
    }
    runDbQuery(dropUsersTableQuery);
  });

  describe("Replication settings", () => {
    beforeEach(() => {
      interceptGetConnectionRequest();
      interceptUpdateConnectionRequest();
    });

    describe("Replication frequency", { testIsolation: false }, () => {
      let loadedConnection: WebBackendConnectionRead;
      it("Default to manual schedule", () => {
        createNewConnectionViaApi(pokeApiSource, jsonDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "replication");
        });

        waitForGetConnectionRequest().then((interception) => {
          const {
            scheduleType: readScheduleType,
            scheduleData: readScheduleData,
            ...connectionRead
          } = interception.response?.body;
          loadedConnection = connectionRead;

          expect(loadedConnection).not.to.eq(null);
          expect(readScheduleType).to.eq("manual");
          expect(readScheduleData).to.eq(undefined);
        });
      });

      it("Set cron as schedule type", () => {
        connectionForm.expandConfigurationSection();

        connectionForm.selectScheduleType("Cron");
        submitButtonClick();
        waitForUpdateConnectionRequest().then((interception) => {
          // Schedule is pulled out here, but we don't do anything with is as it's legacy
          const { scheduleType, scheduleData, schedule, ...connectionUpdate } = interception.response?.body;
          expect(scheduleType).to.eq("cron");

          expect(scheduleData.cron).to.deep.eq({
            cronTimeZone: "UTC",
            cronExpression: "0 0 12 * * ?",
          });
          expect(loadedConnection).to.deep.eq(connectionUpdate);
        });
        replicationPage.checkSuccessResult();
      });
      it("Set manual as schedule type", () => {
        connectionForm.selectScheduleType("Manual");
        submitButtonClick();
        replicationPage.checkSuccessResult();
        waitForUpdateConnectionRequest().then((interception) => {
          // Schedule is pulled out here, but we don't do anything with is as it's legacy
          const { scheduleType, scheduleData, schedule, ...connectionUpdate } = interception.response?.body;
          expect(scheduleType).to.eq("manual");
          expect(loadedConnection).to.deep.eq(connectionUpdate);
        });
      });

      it("Set hourly as schedule type", () => {
        connectionForm.selectScheduleType("Scheduled");
        connectionForm.selectBasicScheduleData("1-hours");
        submitButtonClick();
        waitForUpdateConnectionRequest().then((interception) => {
          // Schedule is pulled out here, but we don't do anything with is as it's legacy
          const { scheduleType, scheduleData, schedule, ...connectionUpdate } = interception.response?.body;
          expect(scheduleType).to.eq("basic");

          expect(scheduleData.basicSchedule).to.deep.eq({
            timeUnit: "hours",
            units: 1,
          });
          expect(loadedConnection).to.deep.eq(connectionUpdate);
        });

        replicationPage.checkSuccessResult();
      });
    });

    describe("Destination namespace", { testIsolation: false }, () => {
      it("Set destination namespace with 'Custom format' option", () => {
        createNewConnectionViaApi(postgresSource, jsonDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "replication");
        });
        connectionForm.expandConfigurationSection();

        const namespace = "_DestinationNamespaceCustomFormat";
        connectionForm.setupDestinationNamespaceCustomFormat(namespace);

        // Ensures the DestinationNamespace is applied to the streams
        const row = streamsTable.getRow("public", "users");
        row.checkDestinationNamespace(`public${namespace}`);

        submitButtonClick();

        waitForUpdateConnectionRequest().then((interception) => {
          assert.isNotNull(interception.response?.statusCode, "200");
          expect(interception.request.method).to.eq("POST");
          expect(interception.request)
            .property("body")
            .to.contain({
              name: `${connection?.name}`,
              namespaceDefinition: "customformat",
              namespaceFormat: "${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat",
              status: "active",
            });

          const streamToCheck = interception.request.body.syncCatalog.streams.filter(
            (stream: AirbyteStreamAndConfiguration) => stream?.stream?.name.includes("users")
          )[0];

          // should not change the saved source namespace
          expect(streamToCheck.stream).to.contain({
            name: "users",
            namespace: "public",
          });
        });
        replicationPage.checkSuccessResult();
      });
      it("Set destination namespace with 'Custom format' option and interpolates an empty string if relevant", () => {
        createNewConnectionViaApi(pokeApiSource, jsonDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "replication");
        });
        connectionForm.expandConfigurationSection();

        const namespace = "_DestinationNamespaceCustomFormat";
        connectionForm.setupDestinationNamespaceCustomFormat(namespace);

        // Ensures the DestinationNamespace is applied to the streams
        const row = streamsTable.getRow("no-namespace", "pokemon");
        // Because there
        row.checkDestinationNamespace(`${namespace}`);

        submitButtonClick();

        waitForUpdateConnectionRequest().then((interception) => {
          assert.isNotNull(interception.response?.statusCode, "200");
          expect(interception.request.method).to.eq("POST");
          expect(interception.request)
            .property("body")
            .to.contain({
              name: `${connection?.name}`,
              namespaceDefinition: "customformat",
              namespaceFormat: "${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat",
              status: "active",
            });

          const streamToUpdate = interception.request.body.syncCatalog.streams[0];

          expect(streamToUpdate.stream).to.contain({
            name: "pokemon",
          });
          // check that we did NOT set the namespace on the stream
          expect(streamToUpdate.stream).not.to.have.property("namespace");
        });
        replicationPage.checkSuccessResult();
      });

      it("Set destination namespace with 'Mirror source structure' option", () => {
        createNewConnectionViaApi(postgresSource, jsonDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "replication");
        });

        const namespace = "public";

        // Ensures the DestinationNamespace is applied to the streams
        const row = streamsTable.getRow("public", "users");
        row.checkDestinationNamespace(namespace);
      });
      it("Set destination namespace with 'Mirror source structure' option and shows destination fallback if relevant", () => {
        createNewConnectionViaApi(pokeApiSource, jsonDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "replication");
        });

        const namespace = "<destination schema>";

        // Ensures the DestinationNamespace is applied to the streams
        const row = streamsTable.getRow("no-namespace", "pokemon");
        row.checkDestinationNamespace(namespace);
      });

      it("Set destination namespace with 'Destination default' option", () => {
        createNewConnectionViaApi(pokeApiSource, jsonDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "replication");
        });
        connectionForm.expandConfigurationSection();

        connectionForm.setupDestinationNamespaceDefaultFormat();

        const namespace = "<destination schema>";

        // Ensures the DestinationNamespace is applied to the stream rows in table
        const row = streamsTable.getRow("no-namespace", "pokemon");
        row.checkDestinationNamespace(namespace);

        submitButtonClick();

        waitForUpdateConnectionRequest().then((interception) => {
          assert.isNotNull(interception.response?.statusCode, "200");
          expect(interception.request.method).to.eq("POST");
          expect(interception.request)
            .property("body")
            .to.contain({
              name: `${connection?.name}`,
              namespaceDefinition: "destination",
              status: "active",
            });

          const streamToUpdate = interception.request.body.syncCatalog.streams[0];

          expect(streamToUpdate.stream).to.contain({
            name: "pokemon",
          });

          // verify nothing changed in the saved stream
          expect(streamToUpdate.stream).to.not.have.property("namespace");
        });
        replicationPage.checkSuccessResult();
      });
    });

    describe("Destination prefix", { testIsolation: false }, () => {
      it("add destination prefix, set destination namespace custom format, change prefix and make sure that it's applied to all streams", () => {
        createNewConnectionViaApi(pokeApiSource, jsonDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "replication");
        });

        connectionForm.expandConfigurationSection();

        const row = streamsTable.getRow("no-namespace", "pokemon");

        const prefix = "auto_test";
        connectionForm.fillOutDestinationPrefix(prefix);
        connectionForm.setupDestinationNamespaceCustomFormat("_test");

        // Ensures the prefix is applied to the streams
        row.checkDestinationStreamName(`${prefix}pokemon`);

        submitButtonClick();

        waitForUpdateConnectionRequest().then((interception) => {
          assert.isNotNull(interception.response?.statusCode, "200");
          expect(interception.request.method).to.eq("POST");
          expect(interception.request)
            .property("body")
            .to.contain({
              name: `${connection?.name}`,
              prefix: "auto_test",
              namespaceDefinition: "customformat",
              namespaceFormat: "${SOURCE_NAMESPACE}_test",
              status: "active",
            });

          const streamToUpdate = interception.request.body.syncCatalog.streams[0];

          expect(streamToUpdate.config).to.contain({
            aliasName: "pokemon",
            destinationSyncMode: "overwrite",
            selected: true,
          });

          expect(streamToUpdate.stream).to.contain({
            name: "pokemon",
          });
          expect(streamToUpdate.stream.supportedSyncModes).to.contain("full_refresh");
        });
        replicationPage.checkSuccessResult();
      });
    });
  });

  describe("Settings page", () => {
    it("Delete connection", () => {
      createNewConnectionViaApi(pokeApiSource, jsonDestination).then((connectionResponse) => {
        connection = connectionResponse;
        visit(connection);
      });
      connectionSettings.goToSettingsPage();
      deleteEntity();
    });
  });

  describe("Deleted connection", () => {
    beforeEach(() => {
      createNewConnectionViaApi(pokeApiSource, jsonDestination).as("connection");
      cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
        requestDeleteConnection({ connectionId: connection.connectionId });
      });
      createNewConnectionViaApi(postgresSource, postgresDestination).as("postgresConnection");
      cy.get<WebBackendConnectionRead>("@postgresConnection").then((postgresConnection) => {
        const streamToUpdate = postgresConnection.syncCatalog.streams.findIndex(
          (stream) => stream.stream?.name === "users" && stream.stream.namespace === "public"
        );

        const newSyncCatalog = {
          streams: [...postgresConnection.syncCatalog.streams],
        };
        // update so one stream is enabled, to test that you can still filter by enabled/disabled streams
        newSyncCatalog.streams[streamToUpdate].config = {
          ...newSyncCatalog.streams[streamToUpdate].config,
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
          selected: true,
        };

        requestUpdateConnection(
          getPostgresToPostgresUpdateConnectionBody(postgresConnection.connectionId, { syncCatalog: newSyncCatalog })
        );

        requestDeleteConnection({
          connectionId: postgresConnection.connectionId,
        });
      });
    });

    it("should not be listed on connection list page", () => {
      cy.visit(`/${RoutePaths.Connections}`);
      cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
        cy.get("td").contains(connection.name).should("not.exist");
      });
    });
    describe("Job History tab", () => {
      it("can visit the connection", () => {
        cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.JobHistory}/`);
          cy.get("div").contains("This connection has been deleted").should("exist");
        });
      });
      it("cannot toggle enabled/disabled state or trigger a sync", () => {
        cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.JobHistory}/`);
          getSyncEnabledSwitch().should("be.disabled");
          cy.get(statusPage.jobHistoryDropdownMenu).click();
          cy.get(statusPage.resetDataDropdownOption).should("be.disabled");
          cy.contains(/Sync now/).should("be.disabled");
        });
      });
    });
    describe("Replication tab", () => {
      it("Cannot edit fields in Configuration section", () => {
        cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Replication}`);
          cy.get(connectionForm.scheduleTypeDropdown).should("be.disabled");
          cy.get(connectionForm.destinationNamespaceEditButton).should("be.disabled");
          cy.get(connectionForm.destinationPrefixEditButton).should("be.disabled");
          cy.get(replicationPage.nonBreakingChangesPreference).should("be.disabled");
        });
      });
      it("Cannot enable/disable streams", () => {
        cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Replication}`);
          // enable/disable all switch
          cy.get('[data-testid="all-streams-sync-switch"]').should("be.disabled");

          const row = streamsTable.getRow("no-namespace", "pokemon");
          row.checkSyncToggleDisabled();
        });
      });
      it("Cannot change sync mode", () => {
        cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Replication}`);
          const row = streamsTable.getRow("no-namespace", "pokemon");
          row.checkSyncModeDropdownDisabled();
        });
      });
      it("Stream filters are disabled and not applied", () => {
        cy.get<WebBackendConnectionRead>("@postgresConnection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Replication}`);
          // input for filtering streams by name
          cy.get('input[placeholder*="Search stream name"]').should("be.disabled");
          cy.get('input[placeholder*="Search stream name"]').should("be.empty");

          // "hide disabled streams" switch
          cy.get('[data-testid="hideDisableStreams-switch"]').should("be.disabled");
          cy.get('[data-testid="hideDisableStreams-switch"]').should("be.not.checked");
        });
      });
    });

    describe("Settings tab", () => {
      it("Can only edit the connection name", () => {
        interceptUpdateConnectionRequest();

        cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Settings}`);
          const newName = appendRandomString("new connection name");
          // I am not 100% sure why this call is so long.  I assume it may have something to do with the fact
          // that this connection is tombstoned
          cy.get('input[name="notifySchemaChanges"]', { timeout: 8000 }).should("be.disabled");
          cy.get('input[name="connectionName"]').clear();
          cy.get('input[name="connectionName"]').type(newName);
          submitButtonClick();

          waitForUpdateConnectionRequest().then((interception) => {
            expect(interception.request.body).to.deep.equal({
              name: newName,
              connectionId: connection.connectionId,
              notifySchemaChanges: connection.notifySchemaChanges,
            });
          });
        });
      });
    });
    describe("Transformations tab", () => {
      it("cannot edit Normalization form settings", () => {
        cy.get<WebBackendConnectionRead>("@postgresConnection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Transformation}`);

          cy.get('form[data-testid="normalization-form"]').children("fieldset").should("be.disabled");
        });
      });

      it("cannot edit Custom transformations form settings", () => {
        cy.get<WebBackendConnectionRead>("@postgresConnection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Transformation}`);

          cy.get('form[data-testid="custom-transformation-form"]').children("fieldset").should("be.disabled");
        });
      });
    });
  });

  describe("Disabled connection", () => {
    beforeEach(() => {
      createNewConnectionViaApi(postgresSource, postgresDestination)
        .then((connection) => {
          requestUpdateConnection(
            getPostgresToPostgresUpdateConnectionBody(connection.connectionId, {
              status: ConnectionStatus.inactive,
            })
          );
        })
        .as("postgresConnection");
    });

    it("should not be allowed to trigger a reset or a sync", () => {
      cy.get<WebBackendConnectionRead>("@postgresConnection").then((connection) => {
        cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/`);
        cy.get(statusPage.manualSyncButton).should("be.disabled");
        cy.get(statusPage.jobHistoryDropdownMenu).click();
        cy.get(statusPage.resetDataDropdownOption).should("be.disabled");
      });
    });

    it("should be able to edit the connection and refresh source schema", () => {
      interceptUpdateConnectionRequest();
      cy.get<WebBackendConnectionRead>("@postgresConnection").then((postgresConnection) => {
        cy.visit(`/${RoutePaths.Connections}/${postgresConnection.connectionId}/${ConnectionRoutePaths.Replication}`);
        cy.get(replicationPage.refreshSourceSchemaBtn).should("not.be.disabled");
        connectionForm.expandConfigurationSection();
        connectionForm.selectScheduleType("Manual");
        submitButtonClick();

        waitForUpdateConnectionRequest().then((interception) => {
          // Schedule is pulled out here, but we don't do anything with is as it's legacy
          const { scheduleType } = interception.response?.body;
          expect(scheduleType).to.eq("manual");
        });
      });
    });
  });
});
