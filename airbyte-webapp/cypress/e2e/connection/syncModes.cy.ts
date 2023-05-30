import {
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
  createNewConnectionViaApi,
} from "@cy/commands/connection";
import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "commands/api";
import {
  Connection,
  Destination,
  DestinationSyncMode,
  Source,
  SourceSyncMode,
  SyncCatalogStreamConfig,
} from "commands/api/types";
import { runDbQuery } from "commands/db/db";
import {
  createAccountsTableQuery,
  createUserCarsTableQuery,
  dropAccountsTableQuery,
  dropUserCarsTableQuery,
  getCreateUsersTableQuery,
  getDropUsersTableQuery,
} from "commands/db/queries";
import { RouteHandler } from "cypress/types/net-stubbing";

import * as connectionPage from "pages/connection/connectionPageObject";
import * as replicationPage from "pages/connection/connectionReplicationPageObject";
import { streamDetails } from "pages/connection/StreamDetailsPageObject";
import { streamsTable } from "pages/connection/StreamsTablePageObject";
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
        // TODO we really shouldn't be modifying the stream here, but it's the only way to get the tests to pass with this source
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
    .saveChangesAndHandleResetModal({ interceptUpdateHandler: modifyAccountsTableInterceptHandler })
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

const USER_FIELD_NAMES = ["email", "id", "name", "updated_at"];
const USER_FIELD_DATA_TYPES = ["String", "Integer", "String", "Datetime"];

const ACCOUNTS_FIELD_NAMES = ["id", "name", "updated_at"];
const ACCOUNTS_FIELD_DATA_TYPES = ["Integer", "String", "Datetime"];

const USER_CARS_FIELD_NAMES = ["car_id", "created_at", "user_id"];
const USER_CARS_FIELD_DATA_TYPES = ["Integer", "Datetime", "Integer"];

describe("Connection - sync modes", () => {
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

    createPostgresSourceViaApi().then((pgSource) => {
      source = pgSource;
      createPostgresDestinationViaApi().then((pgDestination) => {
        destination = pgDestination;
        createNewConnectionViaApi(source, destination).then((connectionResponse) => {
          connection = connectionResponse;
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

      // Check Stream details table
      usersStreamRow.showStreamDetails();
      streamDetails.areFieldsValid({
        names: USER_FIELD_NAMES,
        dataTypes: USER_FIELD_DATA_TYPES,
      });
      streamDetails.close();

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
      usersStreamRow.hasSelectedSyncMode(SourceSyncMode.FullRefresh, DestinationSyncMode.Overwrite);
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

      // Check Stream details table
      usersStreamRow.showStreamDetails();
      streamDetails.areFieldsValid({
        names: USER_FIELD_NAMES,
        dataTypes: USER_FIELD_DATA_TYPES,
      });
      streamDetails.close();

      saveConnectionAndAssertStreams({
        namespace: "public",
        name: "users",
        config: {
          syncMode: SourceSyncMode.FullRefresh,
          destinationSyncMode: DestinationSyncMode.Append,
        },
      });

      // Verify changes after save
      usersStreamRow.hasSelectedSyncMode(SourceSyncMode.FullRefresh, DestinationSyncMode.Append);
      usersStreamRow.hasNoSourceDefinedCursor();
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();
    });
  });

  describe("Incremental | Deduped + history", () => {
    it("selects and saves with source-defined primary keys", () => {
      const users2StreamRow = streamsTable.getRow("public", "users2");
      const cursor = "updated_at"; // todo: should we get this from syncCatalog.streams[??].config.defaultCursorField with sourceDefinedCursor === true? and/or
      const primaryKey = "id"; // todo: should we get this from syncCatalog.streams[??].config.primaryKeys?

      users2StreamRow.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);

      // Select cursor mode
      users2StreamRow.hasEmptyCursorSelect();
      users2StreamRow.selectCursor(cursor);
      users2StreamRow.hasSelectedCursorField(cursor);

      // Check primary key
      users2StreamRow.hasSourceDefinedPrimaryKeys(primaryKey);

      // Check Stream details table
      users2StreamRow.showStreamDetails();
      streamDetails.areFieldsValid({
        names: USER_FIELD_NAMES,
        dataTypes: USER_FIELD_DATA_TYPES,
        cursor,
        primaryKeys: [primaryKey],
        hasSourceDefinedPrimaryKeys: true,
      });
      streamDetails.close();

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
      users2StreamRow.hasSelectedSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);
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

      // Check Stream details table
      accountsStreamRow.showStreamDetails();
      streamDetails.areFieldsValid({
        names: ACCOUNTS_FIELD_NAMES,
        dataTypes: ACCOUNTS_FIELD_DATA_TYPES,
        cursor,
        hasSourceDefinedCursor: true,
        primaryKeys: [primaryKey],
        hasSourceDefinedPrimaryKeys: true,
      });
      streamDetails.close();

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
      accountsStreamRow.hasSelectedSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);
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

      // Check Stream details table
      userCarsStreamRow.showStreamDetails();
      streamDetails.areFieldsValid({
        names: USER_CARS_FIELD_NAMES,
        dataTypes: USER_CARS_FIELD_DATA_TYPES,
        cursor: cursorValue,
        primaryKeys: primaryKeyValue,
      });
      streamDetails.close();

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
      userCarsStreamRow.hasSelectedSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);
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

      // Check Stream details table
      usersStreamRow.showStreamDetails();
      streamDetails.areFieldsValid({
        names: USER_FIELD_NAMES,
        dataTypes: USER_FIELD_DATA_TYPES,
        cursor,
      });
      streamDetails.close();

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
      usersStreamRow.hasSelectedSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.Append);
      usersStreamRow.hasSelectedCursorField(cursor);
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();
    });
  });
});
