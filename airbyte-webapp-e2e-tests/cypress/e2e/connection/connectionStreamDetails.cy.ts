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
  createAccountsTableQuery,
  createUserCarsTableQuery,
  dropAccountsTableQuery,
  dropUserCarsTableQuery,
  getCreateUsersTableQuery,
  getDropUsersTableQuery,
} from "commands/db/queries";
import { initialSetupCompleted } from "commands/workspaces";
import * as connectionPage from "pages/connection/connectionPageObject";
import streamDetails from "pages/connection/streamDetailsPageObject";
import { StreamRowPageObject } from "pages/connection/streamsTablePageObject/StreamRowPageObject";

const dropTables = () => {
  runDbQuery(
    getDropUsersTableQuery("users"),
    getDropUsersTableQuery("users2"),
    dropAccountsTableQuery,
    dropUserCarsTableQuery
  );
};

describe.skip("Connection - Stream details", () => {
  const streamRow = new StreamRowPageObject("public", "users");

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
      streamDetails.areFieldsValid(fieldNames, fieldTypes);
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
});
