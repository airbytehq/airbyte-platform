import {
  getUpdateConnectionBody,
  requestDeleteConnection,
  requestDeleteDestination,
  requestDeleteSource,
  requestUpdateConnection,
} from "@cy/commands/api";
import { deleteEntity, submitButtonClick } from "@cy/commands/common";
import {
  createNewConnectionViaApi,
  createPokeApiSourceViaApi,
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
  createE2ETestingDestinationViaApi,
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
import * as connectionSettings from "@cy/pages/connection/connectionSettingsPageObject";
import * as statusPage from "@cy/pages/connection/statusPageObject";
import { streamsTable } from "@cy/pages/connection/StreamsTablePageObject";
import { getTestId } from "@cy/utils/selectors";
import {
  AirbyteStreamAndConfiguration,
  ConnectionStatus,
  DestinationRead,
  SourceRead,
  WebBackendConnectionRead,
} from "@src/core/api/types/AirbyteClient";
import { ConnectionRoutePaths, RoutePaths } from "@src/pages/routePaths";

describe("Connection Configuration", { tags: "@connection-configuration" }, () => {
  let pokeApiSource: SourceRead;
  let postgresSource: SourceRead;
  let E2ETestingDestination: DestinationRead;
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
    createE2ETestingDestinationViaApi().then((destination) => {
      E2ETestingDestination = destination;
    });
    createPostgresDestinationViaApi().then((destination) => {
      postgresDestination = destination;
    });
  });

  after(() => {
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
    if (E2ETestingDestination) {
      requestDeleteDestination({
        destinationId: E2ETestingDestination.destinationId,
      });
    }
    if (postgresDestination) {
      requestDeleteDestination({
        destinationId: postgresDestination.destinationId,
      });
    }
    runDbQuery(dropUsersTableQuery);
  });

  describe("Connection settings", () => {
    beforeEach(() => {
      interceptGetConnectionRequest();
      interceptUpdateConnectionRequest();
    });

    describe("Sync frequency", { testIsolation: false }, () => {
      let loadedConnection: WebBackendConnectionRead;
      it("Default to manual schedule", () => {
        createNewConnectionViaApi(pokeApiSource, postgresDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "settings");
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
        connectionForm.toggleAdvancedSettingsSection();

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
        connectionSettings.checkSuccessResult();
      });

      it("Set manual as schedule type", () => {
        connectionForm.selectScheduleType("Manual");
        submitButtonClick();
        connectionSettings.checkSuccessResult();
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

        connectionSettings.checkSuccessResult();
      });
    });

    describe("Destination namespace", { testIsolation: false }, () => {
      it("Set destination namespace with 'Custom format' option", () => {
        createNewConnectionViaApi(postgresSource, postgresDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "settings");
        });
        connectionForm.toggleAdvancedSettingsSection();

        const namespace = "_DestinationNamespaceCustomFormat";
        connectionForm.setupDestinationNamespaceCustomFormat(namespace);

        // Ensures the DestinationNamespace preview shows and is correct
        cy.get(connectionForm.destinationNamespaceCustomPreview).should("have.text", `public${namespace}`);

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
        connectionSettings.checkSuccessResult();
      });
      it("Set destination namespace with 'Custom format' option and interpolates an empty string if relevant", () => {
        createNewConnectionViaApi(pokeApiSource, postgresDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "settings");
        });
        connectionForm.toggleAdvancedSettingsSection();

        const namespace = "_DestinationNamespaceCustomFormat";
        connectionForm.setupDestinationNamespaceCustomFormat(namespace);

        // Ensures the DestinationNamespace preview shows and is correct
        cy.get(connectionForm.destinationNamespaceCustomPreview).should("have.text", `${namespace}`);

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
            });

          const streamToUpdate = interception.request.body.syncCatalog.streams[0];

          expect(streamToUpdate.stream).to.contain({
            name: "pokemon",
          });
          // check that we did NOT set the namespace on the stream
          expect(streamToUpdate.stream).not.to.have.property("namespace");
        });
        connectionSettings.checkSuccessResult();
      });

      it("Set destination namespace with 'Source-defined' option", () => {
        createNewConnectionViaApi(postgresSource, postgresDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "settings");
        });
        connectionForm.toggleAdvancedSettingsSection();

        const namespace = "public";

        // Ensures the DestinationNamespace preview shows and is correct
        cy.get(connectionForm.destinationNamespaceSourcePreview).should("have.text", namespace);
      });

      it("Set destination namespace with 'Source-defined' option and shows destination fallback if relevant", () => {
        createNewConnectionViaApi(pokeApiSource, postgresDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "settings");
        });
        connectionForm.toggleAdvancedSettingsSection();

        // Ensures the DestinationNamespace preview isn't visible, as there is no namespace in pokeapi
        cy.get(connectionForm.destinationNamespaceSourcePreview).should("not.exist");
      });

      it("Set destination namespace with 'Destination default' option", () => {
        createNewConnectionViaApi(pokeApiSource, postgresDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "settings");
        });
        connectionForm.toggleAdvancedSettingsSection();

        connectionForm.setupDestinationNamespaceDestinationFormat();

        submitButtonClick();

        waitForUpdateConnectionRequest().then((interception) => {
          assert.isNotNull(interception.response?.statusCode, "200");
          expect(interception.request.method).to.eq("POST");
          expect(interception.request)
            .property("body")
            .to.contain({
              name: `${connection?.name}`,
              namespaceDefinition: "destination",
            });

          const streamToUpdate = interception.request.body.syncCatalog.streams[0];

          expect(streamToUpdate.stream).to.contain({
            name: "pokemon",
          });

          // verify nothing changed in the saved stream
          expect(streamToUpdate.stream).to.not.have.property("namespace");
        });
        connectionSettings.checkSuccessResult();
      });
    });

    describe("Destination prefix", { testIsolation: false }, () => {
      it("add destination prefix, set destination namespace custom format, change prefix and make sure that it's applied to all streams", () => {
        createNewConnectionViaApi(pokeApiSource, postgresDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "settings");
        });

        connectionForm.toggleAdvancedSettingsSection();

        const prefix = "auto_test";
        cy.get(connectionForm.streamPrefixInput).type(prefix);
        connectionForm.setupDestinationNamespaceCustomFormat("_test");

        // Ensures the prefix is previewed
        cy.get(connectionForm.streamPrefixPreview).should("contain.text", prefix);

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
        connectionSettings.checkSuccessResult();
      });
      it("can remove destination prefix", () => {
        createNewConnectionViaApi(pokeApiSource, E2ETestingDestination)
          .then((connection) => {
            requestUpdateConnection(
              getUpdateConnectionBody(connection.connectionId, {
                prefix: "auto_test",
              })
            );
          })
          .as("pokeConnection");

        cy.get<WebBackendConnectionRead>("@pokeConnection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Settings}`);
        });

        connectionForm.toggleAdvancedSettingsSection();

        cy.get(connectionForm.streamPrefixPreview).should("contain.text", "auto_test");
        cy.get(connectionForm.streamPrefixInput).clear();
        cy.get(connectionForm.streamPrefixPreview).should("not.exist");

        submitButtonClick();

        waitForUpdateConnectionRequest().then((interception) => {
          assert.isNotNull(interception.response?.statusCode, "200");
          expect(interception.request.method).to.eq("POST");
          expect(interception.request).property("body").to.contain({
            prefix: "",
          });
          expect(interception.response).property("body").to.contain({
            prefix: "",
          });
          connectionSettings.checkSuccessResult();
        });
      });
    });
  });

  describe("Settings page", () => {
    it("Delete connection", () => {
      createNewConnectionViaApi(pokeApiSource, E2ETestingDestination).then((connectionResponse) => {
        connection = connectionResponse;
        visit(connection);
        connectionSettings.goToSettingsPage();
        deleteEntity();
      });
    });
  });

  describe("Deleted connection", () => {
    beforeEach(() => {
      createNewConnectionViaApi(pokeApiSource, postgresDestination).as("connection");
      cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
        requestDeleteConnection({ connectionId: connection.connectionId });
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
          cy.contains(/Sync now/).should("be.disabled");
        });
      });
    });

    describe("Settings tab", () => {
      it("Cannot edit non-name fields", () => {
        cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Settings}`);
          connectionForm.toggleAdvancedSettingsSection();
          cy.get(getTestId("connectionName")).should("be.disabled");
          cy.get(connectionForm.scheduleTypeDropdown).should("be.disabled");
          cy.get(connectionForm.destinationNamespaceListBox).should("be.disabled");
          cy.get(connectionForm.streamPrefixInput).should("be.disabled");
          cy.get(connectionForm.nonBreakingChangesPreference).should("be.disabled");
        });
      });

      it("cannot reset data or delete connection", () => {
        cy.get<WebBackendConnectionRead>("@connection").then((connection) => {
          cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/${ConnectionRoutePaths.Settings}/`);
          cy.get(connectionSettings.resetDataButton).should("not.exist");
          cy.get(connectionSettings.deleteConnectionButton).should("not.exist");
        });
      });
    });
  });

  describe("Disabled connection", () => {
    beforeEach(() => {
      createNewConnectionViaApi(postgresSource, postgresDestination)
        .then((connection) => {
          requestUpdateConnection(
            getUpdateConnectionBody(connection.connectionId, {
              status: ConnectionStatus.inactive,
            })
          );
        })
        .as("postgresConnection");
    });

    it("should show empty streams table", () => {
      cy.get<WebBackendConnectionRead>("@postgresConnection").then((connection) => {
        cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/`);
        cy.contains("users").should("exist");
      });
    });

    it("should not be allowed to trigger a reset or a sync", () => {
      cy.get<WebBackendConnectionRead>("@postgresConnection").then((connection) => {
        cy.visit(`/${RoutePaths.Connections}/${connection.connectionId}/`);
        cy.get(statusPage.manualSyncButton).should("be.disabled");
      });
    });

    it("should be able to edit the connection and refresh source schema", () => {
      interceptUpdateConnectionRequest();
      cy.get<WebBackendConnectionRead>("@postgresConnection").then((postgresConnection) => {
        cy.visit(`/${RoutePaths.Connections}/${postgresConnection.connectionId}/${ConnectionRoutePaths.Replication}`);
        streamsTable.isRefreshSourceSchemaBtnEnabled(true);

        cy.visit(`/${RoutePaths.Connections}/${postgresConnection.connectionId}/${ConnectionRoutePaths.Settings}`);
        connectionForm.selectScheduleType("Scheduled");
        submitButtonClick();

        waitForUpdateConnectionRequest().then((interception) => {
          const { scheduleType } = interception.response?.body;
          expect(scheduleType).to.eq("basic");
        });
      });
    });
  });
});
