import {
  getConnectionCreateRequest,
  getPostgresCreateDestinationBody,
  getPostgresCreateSourceBody,
  requestCreateConnection,
  requestCreateDestination,
  requestCreateSource,
  requestDeleteConnection,
  requestDeleteDestination,
  requestDeleteSource,
  requestSourceDiscoverSchema,
  requestWorkspaceId,
} from "commands/api";
import { Connection, Destination, Source, SyncCatalogStreamConfig } from "commands/api/types";
import { appendRandomString } from "commands/common";
import { runDbQuery } from "commands/db/db";
import {
  createAccountsTableQuery,
  createUserCarsTableQuery,
  dropAccountsTableQuery,
  dropUserCarsTableQuery,
  getCreateUsersTableQuery,
  getDropUsersTableQuery,
} from "commands/db/queries";
import { initialSetupCompleted } from "commands/workspaces";
import { RouteHandler } from "cypress/types/net-stubbing";
import * as connectionPage from "pages/connection/connectionPageObject";
import * as replicationPage from "pages/connection/connectionReplicationPageObject";
import { NewStreamsTablePageObject } from "pages/connection/streamsTablePageObject/NewStreamsTablePageObject";
import { modifySyncCatalogStream } from "utils/connection";

const dropTables = () => {
  runDbQuery(
    getDropUsersTableQuery("users"),
    getDropUsersTableQuery("users2"),
    dropAccountsTableQuery,
    dropUserCarsTableQuery
  );
};

const modifyAccountsTable: RouteHandler = (request) => {
  request.reply((response) => {
    const body: Connection = modifySyncCatalogStream({
      connection: response.body,
      namespace: "public",
      streamName: "accounts",
      modifyStream: (stream) => ({
        ...stream,
        sourceDefinedCursor: true,
        defaultCursorField: ["updated_at"],
      }),
    });

    response.send(body);
  });
};

const saveConnectionAndAssertStreams = (
  ...expectedSyncModes: Array<{ namespace: string; name: string; config: Partial<SyncCatalogStreamConfig> }>
) => {
  replicationPage.clickSaveButton({ interceptUpdateHandler: modifyAccountsTable }).then((connection) => {
    expectedSyncModes.forEach((expected) => {
      const stream = connection.syncCatalog.streams.find(
        ({ stream }) => stream.namespace === expected.namespace && stream.name === expected.name
      );

      expect(stream).to.exist;
      expect(stream?.config).to.contain({
        syncMode: expected.config.syncMode,
        destinationSyncMode: expected.config.destinationSyncMode,
      });
      if (expected.config.cursorField) {
        expect(stream?.config?.cursorField).to.eql(expected.config.cursorField);
      }
    });
  });
};

describe("Connection - sync modes", () => {
  const streamsTable = new NewStreamsTablePageObject();

  let source: Source;
  let destination: Destination;
  let connection: Connection;

  before(() => {
    dropTables();

    runDbQuery(
      getCreateUsersTableQuery("users"),
      getCreateUsersTableQuery("users2"),
      createAccountsTableQuery,
      createUserCarsTableQuery
    );

    initialSetupCompleted();

    requestWorkspaceId()
      .then(() => {
        const sourceRequestBody = getPostgresCreateSourceBody(appendRandomString("Sync Mode Test Source"));
        const destinationRequestBody = getPostgresCreateDestinationBody(
          appendRandomString("Sync Mode Test Destination")
        );

        return requestCreateSource(sourceRequestBody).then((sourceResponse) => {
          source = sourceResponse;
          requestCreateDestination(destinationRequestBody).then((destinationResponse) => {
            destination = destinationResponse;
          });

          return requestSourceDiscoverSchema(source.sourceId).then(({ catalog, catalogId }) => {
            const connectionRequestBody = getConnectionCreateRequest({
              name: appendRandomString("Sync Mode Test connection"),
              sourceId: source.sourceId,
              destinationId: destination.destinationId,
              syncCatalog: catalog,
              sourceCatalogId: catalogId,
            });
            return requestCreateConnection(connectionRequestBody).then((connectionResponse) => {
              connection = connectionResponse;
            });
          });
        });
      })
      .then(() => {
        cy.intercept({ method: "POST", url: "**/connections/get", times: 1 }, (request) => {
          request.reply((response) => {
            const body: Connection = modifySyncCatalogStream({
              connection: response.body,
              namespace: "public",
              streamName: "accounts",
              modifyStream: (stream) => ({
                ...stream,
                sourceDefinedCursor: true,
                defaultCursorField: ["updated_at"],
              }),
            });

            response.send(body);
          });
        }).as("getConnectionWithModifiedStream");

        connectionPage.visit(connection, "replication", false);
        cy.wait("@getConnectionWithModifiedStream", { timeout: 20000 });
      });
  });

  after(() => {
    if (connection) {
      requestDeleteConnection(connection.connectionId);
    }
    if (source) {
      requestDeleteSource(source.sourceId);
    }
    if (destination) {
      requestDeleteDestination(destination.destinationId);
    }

    dropTables();
  });

  describe("Full refresh | Overwrite", () => {
    it("selects sync mode", () => {
      streamsTable.searchStream("users");
      streamsTable.selectSyncMode("Full refresh", "Overwrite");
    });

    it("does not require primary key or cursor", () => {
      streamsTable.isCursorNonExist("public", "users");
      streamsTable.isPrimaryKeyNonExist("public", "users");
    });

    it("saves", () => {
      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "users",
        config: {
          syncMode: "full_refresh",
          destinationSyncMode: "overwrite",
        },
      });
    });
  });

  describe("Full refresh | Append", () => {
    before(() => {
      streamsTable.searchStream("users");
    });

    it("selects sync mode", () => {
      streamsTable.selectSyncMode("Full refresh", "Append");
    });

    it("does not require primary key or cursor", () => {
      streamsTable.isCursorNonExist("public", "users");
      streamsTable.isPrimaryKeyNonExist("public", "users");
    });

    it("saves", () => {
      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "users",
        config: {
          syncMode: "full_refresh",
          destinationSyncMode: "append",
        },
      });
    });
  });

  describe("Incremental | Deduped + history", () => {
    describe("with source-defined primary keys", () => {
      before(() => {
        streamsTable.searchStream("users2");
        streamsTable.selectSyncMode("Incremental", "Deduped + history");
      });

      it("should be able to select cursor", () => {
        streamsTable.hasEmptyCursorSelect("public", "users2");
        streamsTable.selectCursorField("users2", "updated_at");
      });

      it("has source-defined primary key", () => {
        streamsTable.checkPreFilledPrimaryKeyField("users2", "id");
      });

      it("saves", () => {
        saveConnectionAndAssertStreams({
          namespace: "public",
          name: "users2",
          config: {
            syncMode: "incremental",
            destinationSyncMode: "append_dedup",
            cursorField: ["updated_at"],
            primaryKey: [["id"]],
          },
        });
      });
    });

    describe("with source-defined cursor and primary keys", () => {
      before(() => {
        streamsTable.searchStream("accounts");
        streamsTable.selectSyncMode("Incremental", "Deduped + history");
      });

      it("has source-defined cursor", () => {
        streamsTable.checkPreFilledCursorField("accounts", "updated_at");
      });

      it("has source-defined primary key", () => {
        streamsTable.checkPreFilledPrimaryKeyField("accounts", "id");
      });

      it("saves", () => {
        saveConnectionAndAssertStreams({
          namespace: "public",
          name: "accounts",
          config: {
            syncMode: "incremental",
            destinationSyncMode: "append_dedup",
            cursorField: ["updated_at"],
            primaryKey: [["id"]],
          },
        });
      });
    });

    describe("with selectable primary keys", () => {
      before(() => {
        streamsTable.searchStream("user_cars");
        streamsTable.selectSyncMode("Incremental", "Deduped + history");
      });

      it("has empty cursor and primary key selects", () => {
        streamsTable.hasEmptyCursorSelect("public", "user_cars");
        streamsTable.hasEmptyPrimaryKeySelect("public", "user_cars");
        replicationPage.getSaveButton().should("be.disabled");
      });

      it("can save when stream is disabled", () => {
        streamsTable.disableStream("public", "user_cars");
        replicationPage.getSaveButton().should("be.enabled");
        streamsTable.enableStream("public", "user_cars");
      });

      it("should be able to select cursor", () => {
        streamsTable.selectCursorField("user_cars", "created_at");
      });

      it("can select single primary key", () => {
        streamsTable.selectPrimaryKeyField("user_cars", ["car_id"]);
      });

      it("can select multiple primary keys", () => {
        streamsTable.selectPrimaryKeyField("user_cars", ["car_id", "user_id"]);
      });

      it("saves", () => {
        saveConnectionAndAssertStreams({
          namespace: "public",
          name: "user_cars",
          config: {
            syncMode: "incremental",
            destinationSyncMode: "append_dedup",
            cursorField: ["created_at"],
            primaryKey: [["car_id"], ["user_id"]],
          },
        });
      });
    });
  });

  describe("Incremental | Append", () => {
    before(() => {
      streamsTable.searchStream("users");
    });

    it("selects sync mode", () => {
      streamsTable.selectSyncMode("Incremental", "Append");
    });

    it("has selectable cursor", () => {
      replicationPage.getSaveButton().should("be.disabled");
      streamsTable.hasEmptyCursorSelect("public", "users");
    });

    it("does not require a primary key", () => {
      streamsTable.isPrimaryKeyNonExist("public", "users");
    });

    it("can save when stream is disabled", () => {
      streamsTable.disableStream("public", "users");
      replicationPage.getSaveButton().should("be.enabled");
      streamsTable.enableStream("public", "users");
    });

    it("selects cursor", () => {
      streamsTable.selectCursorField("users", "updated_at");
    });

    it("saves", () => {
      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "users",
        config: {
          syncMode: "incremental",
          destinationSyncMode: "append",
          cursorField: ["updated_at"],
        },
      });
    });
  });
});
