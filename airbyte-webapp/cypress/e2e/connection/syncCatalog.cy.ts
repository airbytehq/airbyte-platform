import {
  getUpdateConnectionBody,
  requestDeleteConnection,
  requestDeleteDestination,
  requestDeleteSource,
  requestUpdateConnection,
} from "@cy/commands/api";
import {
  createJsonDestinationViaApi,
  createNewConnectionViaApi,
  createFakerSourceViaApi,
  createPostgresSourceViaApi,
  createPostgresDestinationViaApi,
} from "@cy/commands/connection";
import { runDbQuery } from "@cy/commands/db/db";
import { createUsersTableQuery, dropUsersTableQuery } from "@cy/commands/db/queries";
import { visit } from "@cy/pages/connection/connectionPageObject";
import { StreamRowPageObjectV2 } from "@cy/pages/connection/StreamRowPageObjectV2";
import { streamsTableV2 } from "@cy/pages/connection/StreamsTablePageObjectV2";
import { setFeatureFlags, setFeatureServiceFlags } from "@cy/support/e2e";
import {
  DestinationRead,
  DestinationSyncMode,
  SourceRead,
  SyncMode,
  WebBackendConnectionRead,
} from "@src/core/api/types/AirbyteClient";

const CATALOG_SEARCH_INPUT = '[data-testid="sync-catalog-search"]';

describe("Sync catalog", () => {
  let fakerSource: SourceRead;
  let jsonDestination: DestinationRead;
  let connection: WebBackendConnectionRead;

  before(() => {
    setFeatureFlags({ "connection.syncCatalogV2": true });
    setFeatureServiceFlags({ SYNC_CATALOG_V2: true });

    createFakerSourceViaApi()
      .then((source) => {
        fakerSource = source;
      })
      .then(() => createJsonDestinationViaApi())
      .then((destination) => {
        jsonDestination = destination;
      })
      .then(() => createNewConnectionViaApi(fakerSource, jsonDestination))
      .then((connectionResponse) => {
        connection = connectionResponse;
      });
  });

  after(() => {
    setFeatureFlags({});
  });

  describe("catalog search functionality", () => {
    before(() => {
      visit(connection, "replication");
    });

    it("Should find a nested field in the sync catalog", () => {
      // Intentionally search for a partial match of a nested field (address.city)
      cy.get(CATALOG_SEARCH_INPUT).type("address.cit");
      // Expect the parent field to exist
      cy.contains(/^address$/).should("exist");
      // Exppect the nested field to exist
      cy.contains(/^address\.city$/).should("exist");

      // Search for a stream
      cy.get(CATALOG_SEARCH_INPUT).clear();
      cy.get(CATALOG_SEARCH_INPUT).type("products");
      cy.contains(/^products$/).should("exist");
      cy.contains(/^users$/).should("not.exist");
    });
  });
});

describe("Sync Catalog - deleted connection", { testIsolation: false }, () => {
  let postgresSource: SourceRead;
  let postgresDestination: DestinationRead;
  let connection: WebBackendConnectionRead;

  before(() => {
    setFeatureFlags({ "connection.syncCatalogV2": true });
    setFeatureServiceFlags({ SYNC_CATALOG_V2: true });

    runDbQuery(dropUsersTableQuery);
    runDbQuery(createUsersTableQuery);

    createPostgresSourceViaApi()
      .then((source) => {
        postgresSource = source;
      })
      .then(() => createPostgresDestinationViaApi())
      .then((destination) => {
        postgresDestination = destination;
      })
      .then(() => createNewConnectionViaApi(postgresSource, postgresDestination))
      .then((connectionResponse) => {
        connection = connectionResponse;

        // change the sync mode and set the PK and cursor field
        const streamToUpdate = connection?.syncCatalog.streams.findIndex(
          (stream) => stream.stream?.name === "users" && stream.stream.namespace === "public"
        );
        const newSyncCatalog = {
          streams: [...connection?.syncCatalog?.streams],
        };
        newSyncCatalog.streams[streamToUpdate].config = {
          ...newSyncCatalog.streams[streamToUpdate].config,
          syncMode: SyncMode.incremental,
          destinationSyncMode: DestinationSyncMode.append_dedup,
          primaryKey: [["id"]],
          cursorField: ["email"],
          selected: true,
        };
        requestUpdateConnection(getUpdateConnectionBody(connection.connectionId, { syncCatalog: newSyncCatalog }));

        requestDeleteConnection({
          connectionId: connection.connectionId,
        });

        visit(connection, "replication");
      });
  });

  after(() => {
    setFeatureFlags({ "connection.syncCatalogV2": false });
    setFeatureServiceFlags({ SYNC_CATALOG_V2: false });

    // cleanup
    if (postgresSource) {
      requestDeleteSource({ sourceId: postgresSource.sourceId });
    }
    if (postgresDestination) {
      requestDeleteDestination({
        destinationId: postgresDestination.destinationId,
      });
    }
    if (connection) {
      requestDeleteConnection({ connectionId: connection.connectionId });
    }
    runDbQuery(dropUsersTableQuery);
  });

  it("should have stream filters still enabled", () => {
    const usersStreamRow = new StreamRowPageObjectV2("public", "users");
    // Filter input
    streamsTableV2.isFilterByStreamOrFieldNameInputEnabled(true);
    streamsTableV2.filterByStreamOrFieldName("users");
    usersStreamRow.isStreamExistInTable(true);

    streamsTableV2.filterByStreamOrFieldName("userss");
    streamsTableV2.isNoMatchingStreamsMsgDisplayed(true);
    streamsTableV2.clearFilterByStreamOrFieldNameInput();

    // Tab buttons
    streamsTableV2.isFilterTabEnabled("all", true);
    streamsTableV2.isFilterTabEnabled("enabledStreams", true);
    streamsTableV2.isFilterTabEnabled("disabledStreams", true);
    streamsTableV2.clickFilterTab("enabledStreams");
    usersStreamRow.isStreamExistInTable(true);
    usersStreamRow.isStreamSyncEnabled(true);
    streamsTableV2.clickFilterTab("disabledStreams");
    streamsTableV2.isNoStreamsMsgDisplayed(true);
    streamsTableV2.clickFilterTab("all");
  });

  it("should not allow refreshing the source schema", () => {
    streamsTableV2.isRefreshSourceSchemaBtnExist(true);
    streamsTableV2.isRefreshSourceSchemaBtnEnabled(false);
  });

  it("should allow expanding and collapsing all streams", () => {
    streamsTableV2.isToggleExpandCollapseAllStreamsBtnExist(true);
    streamsTableV2.isToggleExpandCollapseAllStreamsBtnEnabled(true);
  });

  it("should not allow enabling or disabling all streams in a namespace", () => {
    streamsTableV2.isNamespaceCheckboxEnabled(false);
  });

  describe("stream", () => {
    const usersStreamRow = new StreamRowPageObjectV2("public", "users");

    it("should not allow enabling or disabling individual streams", () => {
      usersStreamRow.isStreamSyncCheckboxDisabled(true);
    });

    it("should not allow enabling or disabling individual fields in a stream", () => {
      usersStreamRow.toggleExpandCollapseStream();
      usersStreamRow.isFieldSyncCheckboxDisabled("id", true);
    });

    it("should allow expanding and collapsing a single stream", () => {
      usersStreamRow.isStreamExpandBtnEnabled(true);
    });

    it("should not allow changing the stream sync mode", () => {
      usersStreamRow.isSyncModeDropdownDisabled(true);
    });

    it("should not allow changing the selected Primary Key and Cursor", () => {
      usersStreamRow.isPKComboboxBtnDisabled(true);
      usersStreamRow.isCursorComboboxBtnDisabled(true);
    });
  });
});
