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
import * as replicationPage from "pages/connection/connectionReplicationPageObject";
import { StreamDetailsPageObject } from "pages/connection/streamDetailsPageObject";
import { NewStreamsTablePageObject } from "pages/connection/streamsTablePageObject/NewStreamsTablePageObject";

const dropTables = () => {
  runDbQuery(
    getDropUsersTableQuery("users"),
    getDropUsersTableQuery("users2"),
    dropAccountsTableQuery,
    dropUserCarsTableQuery
  );
};

describe.skip("Connection - Stream details", () => {
  const streamsTable = new NewStreamsTablePageObject();
  const streamDetails = new StreamDetailsPageObject();

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
    streamsTable.searchStream("users");
  });

  describe("basics", () => {
    beforeEach(() => {
      streamsTable.showStreamDetails("public", "users");
      streamDetails.checkIsOpen();
    });

    it("shows correct stream configuration", () => {
      const fieldNames = ["email", "id", "name", "updated_at"];
      const fieldTypes = ["String", "Integer", "String", "Datetime"];

      streamDetails.checkSyncStreamEnabled();
      streamDetails.checkNamespace("public");
      streamDetails.checkStreamName("users");
      streamDetails.checkSyncMode(SourceSyncMode.FullRefresh, DestinationSyncMode.Append);
      streamDetails.checkFields(fieldNames, fieldTypes);
    });

    it("closes", () => {
      streamDetails.close();
      streamDetails.checkIsClosed();
    });
  });

  describe("enable / disable stream", () => {
    it("updates enabled state both in details panel and streams table", () => {
      streamsTable.disableStream("public", "users");

      streamsTable.showStreamDetails("public", "users");

      streamDetails.checkSyncStreamDisabled();
      streamDetails.enableSyncStream();
      streamDetails.checkSyncStreamEnabled();
      streamDetails.close();

      streamsTable.checkEnabledStream("public", "users");
    });
  });
});
