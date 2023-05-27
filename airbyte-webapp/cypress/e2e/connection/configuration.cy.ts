import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "@cy/commands/api";
import { Connection, Destination, Source } from "@cy/commands/api/types";
import { deleteEntity, submitButtonClick } from "@cy/commands/common";
import {
  createJsonDestinationViaApi,
  createPokeApiSourceViaApi,
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
  createNewConnectionViaApi,
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
import { visit } from "@cy/pages/connection/connectionPageObject";
import * as replicationPage from "@cy/pages/connection/connectionReplicationPageObject";
import { streamsTable } from "@cy/pages/connection/StreamsTablePageObject";
import { WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

import * as connectionSettings from "pages/connection/connectionSettingsPageObject";

describe("Connection Configuration", () => {
  let pokeApiSource: Source;
  let postgresSource: Source;
  let jsonDestination: Destination;
  let postgresDestination: Destination;
  let connection: Connection | null;

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
      requestDeleteConnection(connection.connectionId);
      connection = null;
    }
  });

  after(() => {
    if (pokeApiSource) {
      requestDeleteSource(pokeApiSource.sourceId);
    }
    if (postgresSource) {
      requestDeleteSource(postgresSource.sourceId);
    }
    if (jsonDestination) {
      requestDeleteDestination(jsonDestination.destinationId);
    }
    if (postgresDestination) {
      requestDeleteDestination(postgresDestination.destinationId);
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

        connectionForm.selectSchedule("Cron");
        submitButtonClick();
        waitForUpdateConnectionRequest().then((interception) => {
          // Schedule is pulled out here, but we don't do anything with is as it's legacy
          const { scheduleType, scheduleData, schedule, ...connectionUpdate } = interception.response?.body;
          expect(scheduleType).to.eq("cron");

          expect(scheduleData.cron).to.deep.eq({ cronTimeZone: "UTC", cronExpression: "0 0 12 * * ?" });
          expect(loadedConnection).to.deep.eq(connectionUpdate);
        });
        replicationPage.checkSuccessResult();
      });
      it("Set manual as schedule type", () => {
        connectionForm.selectSchedule("Manual");
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
        connectionForm.selectSchedule("Every hour");
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
        createNewConnectionViaApi(pokeApiSource, jsonDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "replication");
        });
        connectionForm.expandConfigurationSection();

        const namespace = "_DestinationNamespaceCustomFormat";
        connectionForm.setupDestinationNamespaceCustomFormat(namespace);

        // Ensures the DestinationNamespace is applied to the streams
        const row = streamsTable.getRow("no-namespace", "pokemon");
        row.checkDestinationNamespace(`\${SOURCE_NAMESPACE}${namespace}`);

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
        });
        replicationPage.checkSuccessResult();
      });

      it("Set destination namespace with 'Mirror source structure' option", () => {
        createNewConnectionViaApi(pokeApiSource, jsonDestination).then((connectionResponse) => {
          connection = connectionResponse;
          visit(connection, "replication");
        });

        const namespace = "<source schema>";

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

        // Ensures the DestinationNamespace is applied to the streams
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
              namespaceFormat: "${SOURCE_NAMESPACE}",
              status: "active",
            });

          const streamToUpdate = interception.request.body.syncCatalog.streams[0];

          expect(streamToUpdate.stream).to.contain({
            name: "pokemon",
          });
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
            destinationSyncMode: "append",
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
  it("Delete connection", () => {
    createNewConnectionViaApi(pokeApiSource, jsonDestination).then((connectionResponse) => {
      connection = connectionResponse;
      visit(connection);
    });
    connectionSettings.goToSettingsPage();
    deleteEntity();
  });
});
