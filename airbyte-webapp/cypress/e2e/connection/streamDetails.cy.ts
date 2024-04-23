import {
  createNewConnectionViaApi,
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
} from "@cy/commands/connection";
import {
  DestinationRead,
  DestinationSyncMode,
  SourceRead,
  SyncMode,
  WebBackendConnectionRead,
} from "@src/core/api/types/AirbyteClient";
import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "commands/api";
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

  let source: SourceRead;
  let destination: DestinationRead;
  let connection: WebBackendConnectionRead;

  // setup logic adapted from https://stackoverflow.com/questions/71285827/cypress-e2e-before-hook-not-working-on-retries/71377694#71377694
  // to allow retrying, as Cypress doesn't retry if `before` throws an error
  let isBackendSetup = false;
  let isError = false;
  const setup = () => {
    if (isBackendSetup === false) {
      dropTables();

      runDbQuery(getCreateUsersTableQuery("users"), createUserCarsTableQuery, createTableWithLotsOfColumnsQuery);

      return createPostgresSourceViaApi().then((pgSource) => {
        source = pgSource;
        createPostgresDestinationViaApi().then((pgDestination) => {
          destination = pgDestination;
          createNewConnectionViaApi(source, destination).then((connectionResponse) => {
            connection = connectionResponse;
            isBackendSetup = true;
          });
        });
      });
    }
    return cy.get("body"); // return a Cypress chainable so it can be 'then'ed
  };

  const cleanup = () => {
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
  };

  beforeEach(() => {
    cy.once("fail", (err) => {
      isError = true;
      throw err;
    });
    if (isError) {
      cleanup();
      isError = false;
    }

    // @ts-expect-error the .then() signature between the two possibilities don't exactly match
    setup().then(() => {
      connectionPage.visit(connection, "replication");
    });
  });

  after(cleanup);

  describe("basics", () => {
    beforeEach(() => {
      streamRow.showStreamDetails();
      streamDetails.isOpen();
    });

    it("shows correct stream configuration", () => {
      const fieldNames = ["email", "id", "name", "updated_at"];
      const fieldTypes = ["String", "Integer", "String", "Datetime"];

      streamDetails.isSyncStreamDisabled();
      streamDetails.isSelectSyncModeHidden();
      streamDetails.isNamespace("public");
      streamDetails.isStreamName("users");
      streamDetails.areFieldsValid({ names: fieldNames, dataTypes: fieldTypes });
    });

    it("show sync mode dropdown if stream is enabled", () => {
      streamDetails.enableSyncStream();
      streamDetails.isSelectSyncModeVisible();
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

      streamDetails.isSyncStreamEnabled();
      streamDetails.disableSyncStream();
      streamDetails.isSyncStreamDisabled();
      streamDetails.close();

      streamRow.isStreamSyncEnabled(false);
    });
  });

  describe("column selection", () => {
    it("disables column selection if a stream is disabled", () => {
      streamRow.showStreamDetails();
      streamDetails.disableSyncStream();
      streamDetails.isSyncStreamDisabled();
      streamDetails.areFieldsDeselected();
      streamDetails.close();

      streamRow.isStreamSyncEnabled(false);
    });
  });

  describe("cursor / primary key", () => {
    const userCarsStreamRow = new StreamRowPageObject("public", "user_cars");

    it("can select cursor and primary key", () => {
      const cursor = "created_at";
      const primaryKeys = ["car_id", "user_id"];

      userCarsStreamRow.toggleStreamSync();
      userCarsStreamRow.isStreamSyncEnabled(true);
      userCarsStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);
      userCarsStreamRow.showStreamDetails();

      streamDetails.selectCursor(cursor);
      streamDetails.selectPrimaryKeys(primaryKeys);

      streamDetails.close();

      userCarsStreamRow.verifyPrimaryKeys(primaryKeys);
      userCarsStreamRow.verifyCursor(cursor);
    });
  });

  describe("sync mode", () => {
    const userCarsStreamRow = new StreamRowPageObject("public", "user_cars");

    it("can select cursor and primary key", () => {
      const cursor = "created_at";
      const primaryKeys = ["car_id", "user_id"];

      userCarsStreamRow.toggleStreamSync();
      userCarsStreamRow.isStreamSyncEnabled(true);
      userCarsStreamRow.showStreamDetails();

      streamDetails.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);
      streamDetails.selectCursor(cursor);
      streamDetails.selectPrimaryKeys(primaryKeys);

      streamDetails.close();

      userCarsStreamRow.hasSelectedSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);
      userCarsStreamRow.verifyPrimaryKeys(primaryKeys);
      userCarsStreamRow.verifyCursor(cursor);
    });
  });

  describe("scrolling", () => {
    const columnsStreamRow = new StreamRowPageObject("public", "columns");

    it("selects cursors for stream with many fields", () => {
      columnsStreamRow.toggleStreamSync();
      columnsStreamRow.isStreamSyncEnabled(true);
      columnsStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append);
      columnsStreamRow.showStreamDetails();

      streamDetails.scrollToBottom().then(() => {
        streamDetails.selectCursor("field_49");
      });

      streamDetails.scrollToTop().then(() => {
        streamDetails.selectCursor("field_0"); // todo: is this correct?  there cannot be a composite cursor... so we end up with `field_` as the cursor?
      });

      streamDetails.close();
      columnsStreamRow.verifyCursor("field_0");
    });
  });
});
