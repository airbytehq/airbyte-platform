import {
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
  createNewConnectionViaApi,
} from "@cy/commands/connection";
import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "commands/api";
import { Connection, Destination, DestinationSyncMode, Source, SourceSyncMode } from "commands/api/types";
import { runDbQuery } from "commands/db/db";
import {
  createTableWithLotsOfColumnsQuery,
  createUserCarsTableQuery,
  dropTableWithLotsOfColumnsQuery,
  dropUserCarsTableQuery,
  getCreateUsersTableQuery,
  getDropUsersTableQuery,
} from "commands/db/queries";

import * as connectionPage from "pages/connection/connectionPageObject";
import { streamDetails } from "pages/connection/StreamDetailsPageObject";
import { StreamRowPageObject } from "pages/connection/StreamRowPageObject";

const dropTables = () => {
  runDbQuery(getDropUsersTableQuery("users"), dropUserCarsTableQuery, dropTableWithLotsOfColumnsQuery);
};

describe("Connection - Stream details", () => {
  const streamRow = new StreamRowPageObject("public", "users");

  let source: Source;
  let destination: Destination;
  let connection: Connection;

  before(() => {
    dropTables();

    runDbQuery(getCreateUsersTableQuery("users"), createUserCarsTableQuery, createTableWithLotsOfColumnsQuery);

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

  beforeEach(() => {
    connectionPage.visit(connection, "replication");
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

  describe("basics", () => {
    beforeEach(() => {
      streamRow.showStreamDetails();
      streamDetails.isOpen();
    });

    it("shows correct stream configuration", () => {
      const fieldNames = ["email", "id", "name", "updated_at"];
      const fieldTypes = ["String", "Integer", "String", "Datetime"];

      streamDetails.isSyncStreamEnabled();
      streamDetails.isNamespace("public");
      streamDetails.isStreamName("users");
      streamDetails.isSyncMode(SourceSyncMode.FullRefresh, DestinationSyncMode.Append);
      streamDetails.areFieldsValid({ names: fieldNames, dataTypes: fieldTypes });
    });

    it("closes", () => {
      streamDetails.close();
      streamDetails.isClosed();
    });
  });

  describe("enable / disable stream", () => {
    it("updates enabled state both in details panel and streams table", () => {
      streamRow.toggleStreamSync();
      streamRow.showStreamDetails();

      streamDetails.isSyncStreamDisabled();
      streamDetails.enableSyncStream();
      streamDetails.isSyncStreamEnabled();
      streamDetails.close();

      streamRow.isStreamSyncEnabled(true);
    });
  });

  describe("cursor / primary key", () => {
    const userCarsStreamRow = new StreamRowPageObject("public", "user_cars");

    it("can select cursor and primary key", () => {
      const cursor = "created_at";
      const primaryKeys = ["car_id", "user_id"];

      userCarsStreamRow.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);
      userCarsStreamRow.showStreamDetails();

      streamDetails.selectCursor(cursor);
      streamDetails.selectPrimaryKeys(primaryKeys);

      streamDetails.close();

      userCarsStreamRow.hasSelectedPrimaryKeys(primaryKeys);
      userCarsStreamRow.hasSelectedCursorField(cursor);
    });
  });

  describe("scrolling", () => {
    const columnsStreamRow = new StreamRowPageObject("public", "columns");

    it("selects cursors for stream with many fields", () => {
      columnsStreamRow.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.Append);
      columnsStreamRow.showStreamDetails();

      streamDetails.selectCursor("field_49");
      streamDetails.selectCursor("field_0"); // todo: is this correct?  there cannot be a composite cursor... so we end up with `field_` as the cursor?
      streamDetails.close();
      columnsStreamRow.hasSelectedCursorField("field_0");
    });
  });
});
