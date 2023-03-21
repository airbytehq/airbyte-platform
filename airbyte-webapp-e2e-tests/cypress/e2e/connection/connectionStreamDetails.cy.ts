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
import { Connection, Destination, DestinationSyncMode, Source, SourceSyncMode } from "commands/api/types";
import { appendRandomString } from "commands/common";
import { runDbQuery } from "commands/db/db";
import {
  createTableWithLotsOfColumnsQuery,
  createUserCarsTableQuery,
  dropTableWithLotsOfColumnsQuery,
  dropUserCarsTableQuery,
  getCreateUsersTableQuery,
  getDropUsersTableQuery,
} from "commands/db/queries";
import { initialSetupCompleted } from "commands/workspaces";
import * as connectionPage from "pages/connection/connectionPageObject";
import streamDetails from "pages/connection/streamDetailsPageObject";
import { StreamRowPageObject } from "pages/connection/streamsTablePageObject/StreamRowPageObject";

const dropTables = () => {
  runDbQuery(getDropUsersTableQuery("users"), dropUserCarsTableQuery, dropTableWithLotsOfColumnsQuery);
};

describe.skip("Connection - Stream details", () => {
  const streamRow = new StreamRowPageObject("public", "users");

  let source: Source;
  let destination: Destination;
  let connection: Connection;

  before(() => {
    dropTables();

    runDbQuery(getCreateUsersTableQuery("users"), createUserCarsTableQuery, createTableWithLotsOfColumnsQuery);

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
    connectionPage.visit(connection, "replication");
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
    });
  });

  describe("scrolling", () => {
    const columnsStreamRow = new StreamRowPageObject("public", "columns");

    it("selects cursors for stream with many fields", () => {
      columnsStreamRow.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.Append);
      columnsStreamRow.showStreamDetails();

      streamDetails.selectCursor("field_49");
      streamDetails.selectCursor("field_0");
    });
  });
});
