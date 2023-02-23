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
import { Connection, Destination, Source } from "commands/api/types";
import { appendRandomString } from "commands/common";
import { runDbQuery } from "commands/db/db";
import {
  createUserCarsTableQuery,
  createUsersTableQuery,
  dropUserCarsTableQuery,
  dropUsersTableQuery,
} from "commands/db/queries";
import { initialSetupCompleted } from "commands/workspaces";
import * as connectionPage from "pages/connection/connectionPageObject";
import * as replicationPage from "pages/connection/connectionReplicationPageObject";
import { NewStreamsTablePageObject } from "pages/connection/streamsTablePageObject/NewStreamsTablePageObject";

describe.skip("Connection - sync modes", () => {
  const streamsTable = new NewStreamsTablePageObject();

  let source: Source;
  let destination: Destination;
  let connection: Connection;

  before(() => {
    initialSetupCompleted();
    runDbQuery(dropUsersTableQuery);
    runDbQuery(dropUserCarsTableQuery);
    runDbQuery(createUsersTableQuery);
    runDbQuery(createUserCarsTableQuery);

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
        connectionPage.visit(connection, "replication");
        streamsTable.searchStream("users");
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

    runDbQuery(dropUsersTableQuery);
    runDbQuery(dropUserCarsTableQuery);
  });

  describe("Incremental | Deduped + history", () => {
    describe("with source-defined primary keys", () => {
      before(() => {
        streamsTable.selectSyncMode("Incremental", "Deduped + history");
      });

      after(() => {
        replicationPage.clickCancelEditButton();
      });

      it("should be able to select cursor", () => {
        streamsTable.hasEmptyCursorSelect("public", "users");
        streamsTable.selectCursorField("users", "updated_at");
      });

      it("has source-defined primary key", () => {
        streamsTable.checkPreFilledPrimaryKeyField("users", "id");
      });
    });

    describe("with selectable primary keys", () => {
      before(() => {
        streamsTable.searchStream("user_cars");
        streamsTable.selectSyncMode("Incremental", "Deduped + history");
      });

      after(() => {
        replicationPage.clickCancelEditButton();
        streamsTable.searchStream("users");
      });

      it("has empty cursor and primary key selects", () => {
        streamsTable.hasEmptyCursorSelect("public", "user_cars");
        streamsTable.hasEmptyPrimaryKeySelect("public", "user_cars");
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
    });
  });

  describe("Incremental | Append", () => {
    after(() => {
      replicationPage.clickCancelEditButton();
    });

    it("selects sync mode", () => {
      streamsTable.selectSyncMode("Incremental", "Append");
    });

    it("selects cursor", () => {
      replicationPage.getSaveButton().should("be.disabled");
      streamsTable.hasEmptyCursorSelect("public", "users");
      streamsTable.selectCursorField("users", "updated_at");
    });

    it("does not require a primary key", () => {
      streamsTable.isPrimaryKeyNonExist("public", "users");
    });
  });

  describe("Full refresh | Overwrite", () => {
    after(() => {
      replicationPage.clickCancelEditButton();
    });

    it("selects sync mode", () => {
      streamsTable.selectSyncMode("Full refresh", "Overwrite");
    });

    it("does not require primary key or cursor", () => {
      streamsTable.isCursorNonExist("public", "users");
      streamsTable.isPrimaryKeyNonExist("public", "users");
    });
  });

  describe("Full refresh | Append", () => {
    after(() => {
      replicationPage.clickCancelEditButton();
    });

    it("selects sync mode", () => {
      streamsTable.selectSyncMode("Full refresh", "Append");
    });

    it("does not require primary key or cursor", () => {
      streamsTable.isCursorNonExist("public", "users");
      streamsTable.isPrimaryKeyNonExist("public", "users");
    });
  });
});
