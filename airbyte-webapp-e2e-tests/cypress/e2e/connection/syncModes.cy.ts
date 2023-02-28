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
import {
  Connection,
  Destination,
  DestinationSyncMode,
  Source,
  SourceSyncMode,
  SyncCatalogStreamConfig,
} from "commands/api/types";
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

const modifyAccountsTableInterceptHandler: RouteHandler = (request) => {
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
  replicationPage
    .clickSaveButton({ interceptUpdateHandler: modifyAccountsTableInterceptHandler })
    .then((connection) => {
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
        if (expected.config.primaryKey) {
          expect(stream?.config?.cursorField).to.eql(expected.config.cursorField);
        }
      });
    });
};

describe.skip("Connection - sync modes", () => {
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

    requestWorkspaceId().then(() => {
      const sourceRequestBody = getPostgresCreateSourceBody(appendRandomString("Sync Mode Test Source"));
      const destinationRequestBody = getPostgresCreateDestinationBody(appendRandomString("Sync Mode Test Destination"));

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

  beforeEach(() => {
    connectionPage.visit(connection, "replication", { interceptGetHandler: modifyAccountsTableInterceptHandler });
  });

  describe("Full refresh | Overwrite", () => {
    it("selects and saves", () => {
      streamsTable.searchStream("users");
      streamsTable.selectSyncMode(SourceSyncMode.FullRefresh, DestinationSyncMode.Overwrite);

      // Check cursor and primary key
      streamsTable.checkNoSourceDefinedCursor("public", "users");
      streamsTable.checkNoSourceDefinedPrimaryKeys("public", "users");

      // Save
      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "users",
        config: {
          syncMode: SourceSyncMode.FullRefresh,
          destinationSyncMode: DestinationSyncMode.Overwrite,
        },
      });

      // Confirm after save
      streamsTable.checkNoSourceDefinedCursor("public", "users");
      streamsTable.checkNoSourceDefinedPrimaryKeys("public", "users");
    });
  });

  describe("Full refresh | Append", () => {
    it("selects and saves", () => {
      streamsTable.searchStream("users");
      streamsTable.selectSyncMode(SourceSyncMode.FullRefresh, DestinationSyncMode.Append);

      // Verify primary key and cursor
      streamsTable.checkNoSourceDefinedCursor("public", "users");
      streamsTable.checkNoSourceDefinedPrimaryKeys("public", "users");

      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "users",
        config: {
          syncMode: SourceSyncMode.FullRefresh,
          destinationSyncMode: DestinationSyncMode.Append,
        },
      });

      // Verify changes after save
      streamsTable.checkNoSourceDefinedCursor("public", "users");
      streamsTable.checkNoSourceDefinedPrimaryKeys("public", "users");
    });
  });

  describe("Incremental | Deduped + history", () => {
    it("selects and saves with source-defined primary keys", () => {
      const cursor = "updated_at";
      const primaryKey = "id";

      streamsTable.searchStream("users2");
      streamsTable.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);

      // Select cursor mode
      streamsTable.hasEmptyCursorSelect("public", "users2");
      streamsTable.selectCursor("users2", cursor);
      streamsTable.checkSelectedCursorField("users2", cursor);

      // Check primary key
      streamsTable.checkSourceDefinedPrimaryKeys("users2", primaryKey);

      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "users2",
        config: {
          syncMode: SourceSyncMode.Incremental,
          destinationSyncMode: DestinationSyncMode.AppendDedup,
          cursorField: [cursor],
          primaryKey: [[primaryKey]],
        },
      });

      // Verify changes after save
      streamsTable.checkSelectedCursorField("users2", cursor);
      streamsTable.checkSourceDefinedPrimaryKeys("users2", primaryKey);
    });

    it("selects and saves with source-defined cursor and primary keys", () => {
      const cursor = "updated_at";
      const primaryKey = "id";
      streamsTable.searchStream("accounts");
      streamsTable.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);

      // Check cursor and primary key
      streamsTable.checkSourceDefinedCursor("accounts", cursor);
      streamsTable.checkSourceDefinedPrimaryKeys("accounts", primaryKey);

      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "accounts",
        config: {
          syncMode: SourceSyncMode.Incremental,
          destinationSyncMode: DestinationSyncMode.AppendDedup,
          cursorField: ["updated_at"],
          primaryKey: [["id"]],
        },
      });

      // Verify after save
      streamsTable.checkSourceDefinedCursor("accounts", cursor);
      streamsTable.checkSourceDefinedPrimaryKeys("accounts", primaryKey);
    });

    it("selects and saves with selectable user-defined keys and cursors", () => {
      const cursorValue = "created_at";
      const primaryKeyValue = ["car_id", "user_id"];

      streamsTable.searchStream("user_cars");
      streamsTable.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);

      // Check that cursor and primary key is required
      streamsTable.hasEmptyCursorSelect("public", "user_cars");
      streamsTable.hasEmptyPrimaryKeySelect("public", "user_cars");
      replicationPage.getSaveButton().should("be.disabled");

      // Can save when stream is disabled
      streamsTable.disableStream("public", "user_cars");
      replicationPage.getSaveButton().should("be.enabled");
      streamsTable.enableStream("public", "user_cars");

      // Can select cursor
      streamsTable.selectCursor("user_cars", cursorValue);
      streamsTable.checkSelectedCursorField("user_cars", cursorValue);

      // Can select single primary key
      const singlePrimaryKeyValue = [primaryKeyValue[0]];
      streamsTable.selectPrimaryKeys("user_cars", singlePrimaryKeyValue);
      streamsTable.checkSelectedPrimaryKeys("user_cars", singlePrimaryKeyValue);

      // Unchecks:
      streamsTable.selectPrimaryKeys("user_cars", singlePrimaryKeyValue);

      // Can select multiple values
      streamsTable.selectPrimaryKeys("user_cars", primaryKeyValue);
      streamsTable.checkSelectedPrimaryKeys("user_cars", primaryKeyValue);

      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "user_cars",
        config: {
          syncMode: SourceSyncMode.Incremental,
          destinationSyncMode: DestinationSyncMode.AppendDedup,
          cursorField: [cursorValue],
          primaryKey: [primaryKeyValue],
        },
      });

      // Verify save
      streamsTable.checkSelectedCursorField("user_cars", cursorValue);
      streamsTable.checkSelectedPrimaryKeys("user_cars", primaryKeyValue);
    });
  });

  describe("Incremental | Append", () => {
    it("selects and saves", () => {
      const cursor = "updated_at";

      streamsTable.searchStream("users");
      streamsTable.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.Append);

      // Cursor selection is required
      replicationPage.getSaveButton().should("be.disabled");
      streamsTable.hasEmptyCursorSelect("public", "users");

      // No primary key required
      streamsTable.checkNoSourceDefinedPrimaryKeys("public", "users");

      // Can save if disabled
      streamsTable.disableStream("public", "users");
      replicationPage.getSaveButton().should("be.enabled");
      streamsTable.enableStream("public", "users");

      // Select cursor
      streamsTable.selectCursor("users", cursor);
      streamsTable.checkSelectedCursorField("users", cursor);

      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "users",
        config: {
          syncMode: SourceSyncMode.Incremental,
          destinationSyncMode: DestinationSyncMode.Append,
          cursorField: ["updated_at"],
        },
      });

      // Verify save
      streamsTable.checkSelectedCursorField("users", cursor);
      streamsTable.checkNoSourceDefinedPrimaryKeys("public", "users");
    });
  });
});
