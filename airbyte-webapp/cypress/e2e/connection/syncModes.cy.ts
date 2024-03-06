import {
  createNewConnectionViaApi,
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
} from "@cy/commands/connection";
import {
  WebBackendConnectionRead,
  DestinationRead,
  DestinationSyncMode,
  SourceRead,
  SyncMode,
  AirbyteStreamConfiguration,
} from "@src/core/api/types/AirbyteClient";
import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "commands/api";
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
import { modifySyncCatalogStream } from "utils/connection";

import * as connectionPage from "pages/connection/connectionPageObject";
import * as replicationPage from "pages/connection/connectionReplicationPageObject";
import { streamDetails } from "pages/connection/StreamDetailsPageObject";
import { streamsTable } from "pages/connection/StreamsTablePageObject";

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
    const body: WebBackendConnectionRead = modifySyncCatalogStream({
      connection: response.body,
      namespace: "public",
      streamName: "accounts",
      modifyStream: (stream) => ({
        // TODO we really shouldn't be modifying the stream here, but it's the only way to get the tests to pass with this source
        ...stream,
        sourceDefinedCursor: true,
        defaultCursorField: ["updated_at"],
      }),
      modifyConfig: (config) => ({
        ...config,
        cursorField: ["updated_at"],
      }),
    });

    response.send(body);
  });
};

const saveConnectionAndAssertStreams = (
  expectedSyncMode: { namespace: string; name: string; config: Partial<AirbyteStreamConfiguration> },
  { expectModal = true }: { expectModal?: boolean } | undefined = {}
) => {
  replicationPage
    .saveChangesAndHandleResetModal({ interceptUpdateHandler: modifyAccountsTableInterceptHandler, expectModal })
    .then((connection) => {
      const stream = connection.syncCatalog.streams.find(
        ({ stream }) => stream?.namespace === expectedSyncMode.namespace && stream.name === expectedSyncMode.name
      );

      expect(stream).to.exist;
      expect(stream?.config).to.contain({
        syncMode: expectedSyncMode.config.syncMode,
        destinationSyncMode: expectedSyncMode.config.destinationSyncMode,
      });
      if (expectedSyncMode.config.cursorField) {
        expect(stream?.config?.cursorField).to.eql(expectedSyncMode.config.cursorField);
      }
      if (expectedSyncMode.config.primaryKey) {
        expect(stream?.config?.cursorField).to.eql(expectedSyncMode.config.cursorField);
      }
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

  let source: SourceRead;
  let destination: DestinationRead;
  let connection: WebBackendConnectionRead;

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
      requestDeleteConnection({ connectionId: connection.connectionId });
    }
    if (source) {
      requestDeleteSource({ sourceId: source.sourceId });
    }
    if (destination) {
      requestDeleteDestination({ destinationId: destination.destinationId });
    }

    dropTables();
  });

  beforeEach(() => {
    connectionPage.visit(connection, "replication", { interceptGetHandler: modifyAccountsTableInterceptHandler });
  });

  describe("Full refresh | Overwrite", () => {
    it("selects and saves", () => {
      usersStreamRow.toggleStreamSync();
      usersStreamRow.selectSyncMode(SyncMode.full_refresh, DestinationSyncMode.overwrite);

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
      saveConnectionAndAssertStreams(
        {
          namespace: "public",
          name: "users",
          config: {
            syncMode: SyncMode.full_refresh,
            destinationSyncMode: DestinationSyncMode.overwrite,
          },
        },
        { expectModal: false }
      );

      // Confirm after save
      usersStreamRow.hasSelectedSyncMode(SyncMode.full_refresh, DestinationSyncMode.overwrite);
      usersStreamRow.hasNoSourceDefinedCursor();
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();
    });
  });

  describe("Full refresh | Append", () => {
    it("selects and saves", () => {
      usersStreamRow.selectSyncMode(SyncMode.full_refresh, DestinationSyncMode.append);

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
          syncMode: SyncMode.full_refresh,
          destinationSyncMode: DestinationSyncMode.append,
        },
      });

      // Verify changes after save
      usersStreamRow.hasSelectedSyncMode(SyncMode.full_refresh, DestinationSyncMode.append);
      usersStreamRow.hasNoSourceDefinedCursor();
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();
    });
  });

  describe("Incremental | Append + Deduped", () => {
    it("selects and saves with source-defined primary keys", () => {
      const users2StreamRow = streamsTable.getRow("public", "users2");
      const cursor = "updated_at";
      const primaryKey = "id";

      users2StreamRow.toggleStreamSync();
      users2StreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);

      // Select cursor mode
      users2StreamRow.showStreamDetails();
      streamDetails.selectCursor(cursor);
      streamDetails.close();
      users2StreamRow.verifyCursor(cursor);

      // Check primary key
      users2StreamRow.verifyPrimaryKeys([primaryKey]);

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
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          cursorField: [cursor],
          primaryKey: [[primaryKey]],
        },
      });

      // Verify changes after save
      users2StreamRow.hasSelectedSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);
      users2StreamRow.verifyCursor(cursor);
      users2StreamRow.verifyPrimaryKeys([primaryKey]);
    });

    it("selects and saves with source-defined cursor and primary keys", () => {
      const accountsStreamRow = streamsTable.getRow("public", "accounts");
      const cursor = "updated_at";
      const primaryKey = "id";

      accountsStreamRow.toggleStreamSync();
      accountsStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);

      // Check cursor and primary key
      accountsStreamRow.verifyCursor(cursor);
      accountsStreamRow.verifyPrimaryKeys([primaryKey]);

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
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          cursorField: ["updated_at"],
          primaryKey: [["id"]],
        },
      });

      // Verify after save
      accountsStreamRow.hasSelectedSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);
      accountsStreamRow.verifyCursor(cursor);
      accountsStreamRow.verifyPrimaryKeys([primaryKey]);
    });

    it("selects and saves with selectable user-defined keys and cursors", () => {
      const userCarsStreamRow = streamsTable.getRow("public", "user_cars");
      const cursorValue = "created_at";
      const primaryKeyValue = ["car_id", "user_id"];

      userCarsStreamRow.toggleStreamSync();
      userCarsStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);

      // Check that cursor and primary key is required
      userCarsStreamRow.verifyCursor("missing");
      userCarsStreamRow.verifyPrimaryKeys(["missing"]);
      replicationPage.getSaveButton().should("be.disabled");

      // Can save if stream is disabled
      userCarsStreamRow.toggleStreamSync();
      const usersStreamRow = streamsTable.getRow("public", "users");
      usersStreamRow.toggleStreamSync();
      replicationPage.getSaveButton().should("be.enabled");
      userCarsStreamRow.toggleStreamSync();

      // Can select cursor
      userCarsStreamRow.showStreamDetails();
      streamDetails.selectCursor(cursorValue);
      streamDetails.close();
      userCarsStreamRow.verifyCursor(cursorValue);

      // Can select single primary key
      const singlePrimaryKeyValue = [primaryKeyValue[0]];
      userCarsStreamRow.showStreamDetails();
      streamDetails.selectPrimaryKeys(singlePrimaryKeyValue);
      streamDetails.close();
      userCarsStreamRow.verifyPrimaryKeys(singlePrimaryKeyValue);

      // Unchecks:
      userCarsStreamRow.showStreamDetails();
      streamDetails.deSelectPrimaryKeys(singlePrimaryKeyValue);
      streamDetails.close();
      userCarsStreamRow.verifyPrimaryKeys(["missing"]);

      // Can select multiple values
      userCarsStreamRow.showStreamDetails();
      streamDetails.selectPrimaryKeys(primaryKeyValue);
      streamDetails.close();
      userCarsStreamRow.verifyPrimaryKeys(primaryKeyValue);

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
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          cursorField: [cursorValue],
          primaryKey: [primaryKeyValue],
        },
      });

      // Verify save
      userCarsStreamRow.hasSelectedSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);
      userCarsStreamRow.verifyCursor(cursorValue);
      userCarsStreamRow.verifyPrimaryKeys(primaryKeyValue);
    });
  });

  describe("Incremental | Append", () => {
    it("selects and saves", () => {
      const cursor = "updated_at";

      usersStreamRow.toggleStreamSync();
      usersStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append);

      // Cursor selection is required
      replicationPage.getSaveButton().should("be.disabled");
      usersStreamRow.verifyCursor("missing");

      // Can save if disabled
      usersStreamRow.toggleStreamSync();

      const users2StreamRow = streamsTable.getRow("public", "users2");
      users2StreamRow.toggleStreamSync();
      replicationPage.getSaveButton().should("be.enabled"); // users 2 is enabled
      usersStreamRow.toggleStreamSync();

      // Select cursor
      usersStreamRow.showStreamDetails();
      streamDetails.selectCursor(cursor);
      streamDetails.close();
      usersStreamRow.verifyCursor(cursor);

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
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append,
          cursorField: ["updated_at"],
        },
      });

      // Verify save
      usersStreamRow.hasSelectedSyncMode(SyncMode.incremental, DestinationSyncMode.append);
      usersStreamRow.verifyCursor(cursor);
      usersStreamRow.hasNoSourceDefinedPrimaryKeys();
    });
  });
});
