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
  const usersStreamRow = streamsTable.getRow("public", "users");

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
      usersStreamRow.selectSyncMode(SourceSyncMode.FullRefresh, DestinationSyncMode.Overwrite);

      // Check cursor and primary key
      usersStreamRow.hasNoSourceDefinedCursor();
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();

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
      usersStreamRow.hasNoSourceDefinedCursor();
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();
    });
  });

  describe("Full refresh | Append", () => {
    it("selects and saves", () => {
      usersStreamRow.selectSyncMode(SourceSyncMode.FullRefresh, DestinationSyncMode.Append);

      // Verify primary key and cursor
      usersStreamRow.hasNoSourceDefinedCursor();
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();

      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "users",
        config: {
          syncMode: SourceSyncMode.FullRefresh,
          destinationSyncMode: DestinationSyncMode.Append,
        },
      });

      // Verify changes after save
      usersStreamRow.hasNoSourceDefinedCursor();
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();
    });
  });

  describe("Incremental | Deduped + history", () => {
    it("selects and saves with source-defined primary keys", () => {
      const users2StreamRow = streamsTable.getRow("public", "users2");
      const cursor = "updated_at";
      const primaryKey = "id";

      users2StreamRow.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);

      // Select cursor mode
      users2StreamRow.hasEmptyCursorSelect();
      users2StreamRow.selectCursor(cursor);
      users2StreamRow.hasSelectedCursorField(cursor);

      // Check primary key
      users2StreamRow.hasSourceDefinedPrimaryKeys(primaryKey);

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
      users2StreamRow.hasSelectedCursorField(cursor);
      users2StreamRow.hasSourceDefinedPrimaryKeys(primaryKey);
    });

    it("selects and saves with source-defined cursor and primary keys", () => {
      const accountsStreamRow = streamsTable.getRow("public", "accounts");
      const cursor = "updated_at";
      const primaryKey = "id";

      accountsStreamRow.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);

      // Check cursor and primary key
      accountsStreamRow.hasSourceDefinedCursor(cursor);
      accountsStreamRow.hasSourceDefinedPrimaryKeys(primaryKey);

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
      accountsStreamRow.hasSourceDefinedCursor(cursor);
      accountsStreamRow.hasSourceDefinedPrimaryKeys(primaryKey);
    });

    it("selects and saves with selectable user-defined keys and cursors", () => {
      const userCarsStreamRow = streamsTable.getRow("public", "user_cars");
      const cursorValue = "created_at";
      const primaryKeyValue = ["car_id", "user_id"];

      userCarsStreamRow.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);

      // Check that cursor and primary key is required
      userCarsStreamRow.hasEmptyCursorSelect();
      userCarsStreamRow.hasEmptyPrimaryKeySelect();
      replicationPage.getSaveButton().should("be.disabled");

      // Can save when stream is disabled
      userCarsStreamRow.toggleStreamSync();
      replicationPage.getSaveButton().should("be.enabled");
      userCarsStreamRow.toggleStreamSync();

      // Can select cursor
      userCarsStreamRow.selectCursor(cursorValue);
      userCarsStreamRow.hasSelectedCursorField(cursorValue);

      // Can select single primary key
      const singlePrimaryKeyValue = [primaryKeyValue[0]];
      userCarsStreamRow.selectPrimaryKeys(singlePrimaryKeyValue);
      userCarsStreamRow.hasSelectedPrimaryKeys(singlePrimaryKeyValue);

      // Unchecks:
      userCarsStreamRow.selectPrimaryKeys(singlePrimaryKeyValue);

      // Can select multiple values
      userCarsStreamRow.selectPrimaryKeys(primaryKeyValue);
      userCarsStreamRow.hasSelectedPrimaryKeys(primaryKeyValue);

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
      userCarsStreamRow.hasSelectedCursorField(cursorValue);
      userCarsStreamRow.hasSelectedPrimaryKeys(primaryKeyValue);
    });
  });

  describe("Incremental | Append", () => {
    it("selects and saves", () => {
      const cursor = "updated_at";

      usersStreamRow.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.Append);

      // Cursor selection is required
      replicationPage.getSaveButton().should("be.disabled");
      usersStreamRow.hasEmptyCursorSelect();

      // No primary key required
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();

      // Can save if disabled
      usersStreamRow.toggleStreamSync();
      replicationPage.getSaveButton().should("be.enabled");
      usersStreamRow.toggleStreamSync();

      // Select cursor
      usersStreamRow.selectCursor(cursor);
      usersStreamRow.hasSelectedCursorField(cursor);

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
      usersStreamRow.hasSelectedCursorField(cursor);
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();
    });
  });
});
