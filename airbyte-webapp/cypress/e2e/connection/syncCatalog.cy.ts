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
import { cleanDBSource, runDbQuery } from "@cy/commands/db/db";
import {
  createCarsTableQuery,
  createCitiesTableQuery,
  createUsersTableQuery,
  dropUsersTableQuery,
} from "@cy/commands/db/queries";
import { interceptUpdateConnectionRequest, waitForUpdateConnectionRequest } from "@cy/commands/interceptors";
import { visit } from "@cy/pages/connection/connectionPageObject";
import { confirmStreamConfigurationChangedPopup } from "@cy/pages/connection/connectionReplicationPageObject";
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

describe("Stream", { testIsolation: false }, () => {
  let postgresSource: SourceRead;
  let postgresDestination: DestinationRead;
  let connection: WebBackendConnectionRead;

  before(() => {
    cleanDBSource();
    runDbQuery(createCarsTableQuery);

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

        visit(connection, "replication");
      });
  });

  after(() => {
    setFeatureFlags({});
    setFeatureServiceFlags({});

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
    cleanDBSource();
  });

  const carsStreamRow = new StreamRowPageObjectV2("public", "cars");

  describe("enabled state", () => {
    it("should have checked checkbox", () => {
      carsStreamRow.isStreamSyncEnabled(true);
    });
    it("should have all fields with checked checkbox", () => {
      carsStreamRow.toggleExpandCollapseStream();
      carsStreamRow.isFieldSyncEnabled("color", true);
      carsStreamRow.isFieldSyncEnabled("id", true);
      carsStreamRow.isFieldSyncEnabled("mark", true);
      carsStreamRow.isFieldSyncEnabled("model", true);
    });
    it("should show selected SyncMode dropdown", () => {
      carsStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);
      carsStreamRow.isSelectedSyncModeDisplayed(SyncMode.incremental, DestinationSyncMode.append_dedup);
    });
    it("should show selected PK", () => {
      carsStreamRow.isSelectedPKDisplayed("id");
    });
    it("should show selected Cursor", () => {
      carsStreamRow.isMissedCursorErrorDisplayed(true);
    });
  });

  describe("disabled state", () => {
    it("should have not checked checkbox", () => {
      carsStreamRow.toggleStreamSync(false);
      carsStreamRow.isStreamSyncEnabled(false);
    });
    it("should have all fields with unchecked checkbox", () => {
      carsStreamRow.isFieldSyncEnabled("color", false);
      carsStreamRow.isFieldSyncEnabled("id", false);
      carsStreamRow.isFieldSyncEnabled("mark", false);
      carsStreamRow.isFieldSyncEnabled("model", false);
    });
    it("should not show selected SyncMode", () => {
      carsStreamRow.isSyncModeDropdownDisplayed(false);
    });
    it("should not show selected PK", () => {
      carsStreamRow.isPKComboboxBtnDisplayed(false);
    });
    it("should not show selected Cursor", () => {
      carsStreamRow.isCursorComboboxBtnDisplayed(false);
      carsStreamRow.toggleExpandCollapseStream();
    });
  });

  describe("Field", () => {
    it("should show field checkbox by default", () => {
      carsStreamRow.toggleExpandCollapseStream();
      carsStreamRow.toggleStreamSync(true);
      carsStreamRow.isFieldSyncCheckboxDisplayed("color", true);
      carsStreamRow.toggleStreamSync(false);
      carsStreamRow.isFieldSyncCheckboxDisplayed("color", true);
      carsStreamRow.toggleStreamSync(true);
    });

    it("should show field type", () => {
      carsStreamRow.isFieldTypeDisplayed("id", "Integer", true);
      carsStreamRow.isFieldTypeDisplayed("color", "String", true);
    });

    it("should have disabled checkbox if field is a PK or Cursor", () => {
      carsStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);
      carsStreamRow.selectCursor("mark");

      carsStreamRow.isFieldSyncEnabled("id", true);
      carsStreamRow.isFieldSyncCheckboxDisabled("id", true);
      carsStreamRow.isFieldSyncEnabled("mark", true);
      carsStreamRow.isFieldSyncCheckboxDisabled("mark", true);
    });

    it("should show PK and Cursor labels", () => {
      carsStreamRow.isPKField("id", true);
      carsStreamRow.isCursorField("mark", true);
    });

    it("should enable the stream and required fields", () => {
      carsStreamRow.toggleStreamSync(false);
      carsStreamRow.isStreamSyncEnabled(false);
      carsStreamRow.toggleFieldSync("color", true);

      carsStreamRow.isFieldSyncEnabled("color", true);
      carsStreamRow.isFieldSyncEnabled("id", true);
      carsStreamRow.isFieldSyncCheckboxDisabled("id", true);
      carsStreamRow.isFieldSyncEnabled("mark", true);
      carsStreamRow.isFieldSyncCheckboxDisabled("mark", true);
    });
  });

  describe("Field - disabled columnSelection", () => {
    before(() => {
      setFeatureFlags({ "connection.columnSelection": false });
      visit(connection, "replication");
    });

    after(() => {
      setFeatureFlags({});
    });

    it("should not show field sync checkbox for default, PK and Cursor fields", () => {
      carsStreamRow.toggleStreamSync(true);
      carsStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);
      carsStreamRow.selectCursor("mark");
      carsStreamRow.toggleExpandCollapseStream();

      carsStreamRow.isFieldSyncCheckboxDisplayed("id", false);
      carsStreamRow.isFieldSyncCheckboxDisplayed("mark", false);
      carsStreamRow.isFieldSyncCheckboxDisplayed("color", false);
    });
  });
});

describe("Sync Modes", { testIsolation: false }, () => {
  let postgresSource: SourceRead;
  let postgresDestination: DestinationRead;
  let connection: WebBackendConnectionRead;

  before(() => {
    cleanDBSource();
    runDbQuery(createUsersTableQuery);
    runDbQuery(createCitiesTableQuery);

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
      });
  });

  beforeEach(() => {
    interceptUpdateConnectionRequest();
  });

  after(() => {
    setFeatureFlags({});
    setFeatureServiceFlags({});

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
    cleanDBSource();
  });

  const usersStreamRow = new StreamRowPageObjectV2("public", "users");
  const citiesStreamRow = new StreamRowPageObjectV2("public", "cities");

  describe("Full refresh | Append", { testIsolation: false }, () => {
    it("should select the sync mode", () => {
      visit(connection, "replication");
      usersStreamRow.toggleStreamSync(true);
      usersStreamRow.isStreamSyncEnabled(true);
      usersStreamRow.selectSyncMode(SyncMode.full_refresh, DestinationSyncMode.append);
      usersStreamRow.isSelectedSyncModeDisplayed(SyncMode.full_refresh, DestinationSyncMode.append);
    });

    it("should not display the PK and Cursor combobox buttons", () => {
      usersStreamRow.isPKComboboxBtnDisplayed(false);
      usersStreamRow.isMissedPKErrorDisplayed(false);
      usersStreamRow.isCursorComboboxBtnDisplayed(false);
      usersStreamRow.isMissedCursorErrorDisplayed(false);
    });

    it("should allow to save changes", () => {
      streamsTableV2.isNoStreamsSelectedErrorDisplayed(false);
      streamsTableV2.clickSaveChangesButton();
      waitForUpdateConnectionRequest();
    });

    it("should verify that changes are applied", () => {
      visit(connection, "replication");
      usersStreamRow.isStreamSyncEnabled(true);
      usersStreamRow.isSelectedSyncModeDisplayed(SyncMode.full_refresh, DestinationSyncMode.append);
    });
  });

  describe("Full refresh | Overwrite", { testIsolation: false }, () => {
    it("should select the sync mode", () => {
      visit(connection, "replication");
      usersStreamRow.toggleStreamSync(true);
      usersStreamRow.isStreamSyncEnabled(true);
      usersStreamRow.selectSyncMode(SyncMode.full_refresh, DestinationSyncMode.overwrite);
      usersStreamRow.isSelectedSyncModeDisplayed(SyncMode.full_refresh, DestinationSyncMode.overwrite);
    });

    it("should not display the PK and Cursor combobox buttons", () => {
      usersStreamRow.isPKComboboxBtnDisplayed(false);
      usersStreamRow.isMissedPKErrorDisplayed(false);
      usersStreamRow.isCursorComboboxBtnDisplayed(false);
      usersStreamRow.isMissedCursorErrorDisplayed(false);
    });

    it("should allow to save changes", () => {
      streamsTableV2.isNoStreamsSelectedErrorDisplayed(false);
      streamsTableV2.clickSaveChangesButton();
      waitForUpdateConnectionRequest();
    });

    it("should verify that changes are applied", () => {
      visit(connection, "replication");
      usersStreamRow.isStreamSyncEnabled(true);
      usersStreamRow.isSelectedSyncModeDisplayed(SyncMode.full_refresh, DestinationSyncMode.overwrite);
    });
  });

  describe("Full refresh | Overwrite + Deduped", { testIsolation: false }, () => {
    it("should select the sync mode", () => {
      visit(connection, "replication");
      citiesStreamRow.toggleStreamSync(true);
      citiesStreamRow.isStreamSyncEnabled(true);
      citiesStreamRow.selectSyncMode(SyncMode.full_refresh, DestinationSyncMode.overwrite_dedup);
      citiesStreamRow.isSelectedSyncModeDisplayed(SyncMode.full_refresh, DestinationSyncMode.overwrite_dedup);
    });

    it("should show missing PK error", () => {
      citiesStreamRow.isMissedPKErrorDisplayed(true);
      citiesStreamRow.isMissedCursorErrorDisplayed(false);
      streamsTableV2.isSaveChangesButtonEnabled(false);
    });

    it("should select PK", () => {
      citiesStreamRow.selectPKs(["city_code"]);
      citiesStreamRow.isSelectedPKDisplayed("city_code");
      citiesStreamRow.isMissedPKErrorDisplayed(false);
      citiesStreamRow.toggleExpandCollapseStream();
      citiesStreamRow.isPKField("city_code", true);
    });

    it("should allow to save changes", () => {
      streamsTableV2.clickSaveChangesButton();
      waitForUpdateConnectionRequest();
    });

    it("should select multiple PKs", () => {
      visit(connection, "replication");
      citiesStreamRow.selectPKs(["city"]);
      citiesStreamRow.isSelectedPKDisplayed("2 items selected");
      citiesStreamRow.toggleExpandCollapseStream();
      citiesStreamRow.isPKField("city_code", true);
      citiesStreamRow.isPKField("city", true);
      streamsTableV2.clickSaveChangesButton();
      waitForUpdateConnectionRequest();
    });

    it("should verify that changes are applied", () => {
      visit(connection, "replication");
      citiesStreamRow.isStreamSyncEnabled(true);
      citiesStreamRow.isSelectedPKDisplayed("2 items selected");
      citiesStreamRow.isSelectedSyncModeDisplayed(SyncMode.full_refresh, DestinationSyncMode.overwrite);
    });
  });

  describe("Full refresh | Overwrite + Deduped (source-defined PK)", { testIsolation: false }, () => {
    it("should select the sync mode", () => {
      visit(connection, "replication");
      usersStreamRow.toggleStreamSync(true);
      usersStreamRow.isStreamSyncEnabled(true);
      usersStreamRow.selectSyncMode(SyncMode.full_refresh, DestinationSyncMode.overwrite_dedup);
      usersStreamRow.isSelectedSyncModeDisplayed(SyncMode.full_refresh, DestinationSyncMode.overwrite_dedup);
    });

    it("should NOT show missing PK and Cursor error", () => {
      usersStreamRow.isMissedPKErrorDisplayed(false);
      usersStreamRow.isMissedCursorErrorDisplayed(false);
      streamsTableV2.isSaveChangesButtonEnabled(true);
    });

    it("should show non-editable selected PK", () => {
      usersStreamRow.isSelectedPKDisplayed("id");
      usersStreamRow.isPKComboboxBtnDisabled(true);
      usersStreamRow.toggleExpandCollapseStream();
      usersStreamRow.isPKField("id", true);
    });

    it("should allow to save changes", () => {
      streamsTableV2.clickSaveChangesButton();
      waitForUpdateConnectionRequest();
    });

    it("should verify that changes are applied", () => {
      visit(connection, "replication");
      usersStreamRow.isStreamSyncEnabled(true);
      usersStreamRow.isSelectedPKDisplayed("id");
      usersStreamRow.isSelectedSyncModeDisplayed(SyncMode.full_refresh, DestinationSyncMode.overwrite);
    });
  });

  describe("Incremental | Append", { testIsolation: false }, () => {
    it("should select the sync mode", () => {
      visit(connection, "replication");
      usersStreamRow.toggleStreamSync(true);
      usersStreamRow.isStreamSyncEnabled(true);
      usersStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append);
      usersStreamRow.isSelectedSyncModeDisplayed(SyncMode.incremental, DestinationSyncMode.append);
    });

    it("should show missing Cursor error", () => {
      usersStreamRow.isMissedPKErrorDisplayed(false);
      usersStreamRow.isMissedCursorErrorDisplayed(true);
      streamsTableV2.isSaveChangesButtonEnabled(false);
    });

    it("should select Cursor", () => {
      usersStreamRow.selectCursor("email");
      usersStreamRow.isSelectedCursorDisplayed("email");
      usersStreamRow.toggleExpandCollapseStream();
      usersStreamRow.isCursorField("email", true);
    });

    it("should allow to save changes", () => {
      streamsTableV2.clickSaveChangesButton();
      waitForUpdateConnectionRequest();
    });

    it("should verify that changes are applied", () => {
      visit(connection, "replication");
      usersStreamRow.isStreamSyncEnabled(true);
      usersStreamRow.isSelectedCursorDisplayed("email");
      usersStreamRow.isSelectedSyncModeDisplayed(SyncMode.incremental, DestinationSyncMode.append);
    });
  });

  describe("Incremental | Append + Deduped", { testIsolation: false }, () => {
    it("should select the sync mode", () => {
      // trick to unset PK from previous tests
      visit(connection, "replication");
      citiesStreamRow.selectPKs(["city_code", "city"]);
      citiesStreamRow.selectSyncMode(SyncMode.full_refresh, DestinationSyncMode.overwrite);
      streamsTableV2.clickSaveChangesButton();
      waitForUpdateConnectionRequest();

      visit(connection, "replication");
      citiesStreamRow.toggleStreamSync(true);
      citiesStreamRow.isStreamSyncEnabled(true);

      citiesStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);
      citiesStreamRow.isSelectedSyncModeDisplayed(SyncMode.incremental, DestinationSyncMode.append_dedup);
    });

    it("should show missing PK and Cursor errors", () => {
      citiesStreamRow.isMissedPKErrorDisplayed(true);
      citiesStreamRow.isMissedCursorErrorDisplayed(true);
      streamsTableV2.isSaveChangesButtonEnabled(false);
    });

    it("should select PK and Cursor", () => {
      citiesStreamRow.selectPKs(["city_code"]);
      citiesStreamRow.isSelectedPKDisplayed("city_code");
      citiesStreamRow.selectCursor("city");
      citiesStreamRow.isSelectedCursorDisplayed("city");
      citiesStreamRow.toggleExpandCollapseStream();
      citiesStreamRow.isPKField("city_code", true);
      citiesStreamRow.isCursorField("city", true);
    });

    it("should allow to save changes and discard refresh streams", () => {
      streamsTableV2.clickSaveChangesButton();
      confirmStreamConfigurationChangedPopup({ reset: false });
      waitForUpdateConnectionRequest();
    });

    it("should verify that changes are applied", () => {
      visit(connection, "replication");
      citiesStreamRow.isStreamSyncEnabled(true);
      citiesStreamRow.isSelectedPKDisplayed("city_code");
      citiesStreamRow.isSelectedCursorDisplayed("city");
      citiesStreamRow.isSelectedSyncModeDisplayed(SyncMode.incremental, DestinationSyncMode.append_dedup);
    });
  });
});

describe("Diff styles", { testIsolation: false }, () => {
  let postgresSource: SourceRead;
  let postgresDestination: DestinationRead;
  let connection: WebBackendConnectionRead;

  before(() => {
    cleanDBSource();
    runDbQuery(createUsersTableQuery);
    runDbQuery(createCitiesTableQuery);

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

        visit(connection, "replication");
      });
  });

  after(() => {
    setFeatureFlags({});
    setFeatureServiceFlags({});

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
    cleanDBSource();
  });

  const citiesStreamRow = new StreamRowPageObjectV2("public", "cities");
  const usersStreamRow = new StreamRowPageObjectV2("public", "users");

  describe("Stream", { testIsolation: false }, () => {
    it("should prepare streams for tests", () => {
      citiesStreamRow.toggleStreamSync(true);
      usersStreamRow.toggleStreamSync(true);
      streamsTableV2.clickSaveChangesButton();
    });

    it("should have `removed` style after changing state from `enabled` => `disabled`", () => {
      visit(connection, "replication");
      citiesStreamRow.toggleStreamSync(false);
      citiesStreamRow.streamHasRemovedStyle(true);
      citiesStreamRow.toggleExpandCollapseStream();
      citiesStreamRow.fieldHasDisabledStyle("city", true);
      citiesStreamRow.fieldHasDisabledStyle("city_code", true);
      streamsTableV2.clickSaveChangesButton();
      citiesStreamRow.streamHasRemovedStyle(false);
    });

    it("should have `disabled` style if stream is not enabled", () => {
      visit(connection, "replication");
      citiesStreamRow.isStreamSyncEnabled(false);
      citiesStreamRow.streamHasDisabledStyle(true);
      citiesStreamRow.toggleExpandCollapseStream();
      citiesStreamRow.fieldHasDisabledStyle("city", true);
      citiesStreamRow.fieldHasDisabledStyle("city_code", true);
    });

    it("should have `added` style after changing state from `disabled` => `enabled`", () => {
      citiesStreamRow.toggleStreamSync(true);
      citiesStreamRow.streamHasAddedStyle(true);
      citiesStreamRow.fieldHasDisabledStyle("city", false);
      citiesStreamRow.fieldHasDisabledStyle("city_code", false);
      streamsTableV2.clickSaveChangesButton();
      citiesStreamRow.streamHasAddedStyle(false);
    });

    it("should have `changed` style after changing the sync mode", () => {
      visit(connection, "replication");
      citiesStreamRow.isStreamSyncEnabled(true);
      citiesStreamRow.selectSyncMode(SyncMode.incremental, DestinationSyncMode.append_dedup);

      citiesStreamRow.streamHasChangedStyle(true);
      citiesStreamRow.selectPKs(["city_code"]);
      citiesStreamRow.selectCursor("city");
      streamsTableV2.clickSaveChangesButton();
      confirmStreamConfigurationChangedPopup({ reset: false });
      citiesStreamRow.streamHasChangedStyle(false);
    });

    it("should have `changed` style after changing the PK", () => {
      citiesStreamRow.selectPKs(["city"]);
      citiesStreamRow.streamHasChangedStyle(true);
      streamsTableV2.clickDiscardChangesButton();
      citiesStreamRow.streamHasChangedStyle(false);
    });

    it("should have `changed` style after changing the Cursor", () => {
      citiesStreamRow.selectCursor("city_code");
      citiesStreamRow.streamHasChangedStyle(true);
      streamsTableV2.clickDiscardChangesButton();
      citiesStreamRow.streamHasChangedStyle(false);
    });
  });

  describe("Field", { testIsolation: false }, () => {
    it("should prepare fields for tests", () => {
      citiesStreamRow.selectSyncMode(SyncMode.full_refresh, DestinationSyncMode.overwrite);
      streamsTableV2.clickSaveChangesButton();
    });

    it("should have field with `removed` and stream with `changed` styles after disabling the field", () => {
      visit(connection, "replication");
      citiesStreamRow.toggleExpandCollapseStream();
      citiesStreamRow.toggleFieldSync("city", false);

      citiesStreamRow.streamHasChangedStyle(true);
      citiesStreamRow.fieldHasRemovedStyle("city", true);
      streamsTableV2.clickSaveChangesButton();
      citiesStreamRow.streamHasChangedStyle(false);
      citiesStreamRow.fieldHasRemovedStyle("city", false);
    });
    it("should have field with `added` and stream with `changed` styles after enabling the field", () => {
      visit(connection, "replication");
      citiesStreamRow.toggleExpandCollapseStream();
      citiesStreamRow.toggleFieldSync("city", true);

      citiesStreamRow.streamHasChangedStyle(true);
      citiesStreamRow.fieldHasAddedStyle("city", true);
      streamsTableV2.clickSaveChangesButton();
      citiesStreamRow.streamHasChangedStyle(false);
      citiesStreamRow.fieldHasRemovedStyle("city", false);
    });
  });
});

describe("Sync Catalog - deleted connection", { testIsolation: false }, () => {
  let postgresSource: SourceRead;
  let postgresDestination: DestinationRead;
  let connection: WebBackendConnectionRead;

  before(() => {
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
    setFeatureFlags({});
    setFeatureServiceFlags({});

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

describe("Tab filter", { testIsolation: false }, () => {
  let postgresSource: SourceRead;
  let postgresDestination: DestinationRead;
  let connection: WebBackendConnectionRead;

  before(() => {
    cleanDBSource();
    runDbQuery(createUsersTableQuery);
    runDbQuery(createCitiesTableQuery);
    runDbQuery(createCarsTableQuery);

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

        visit(connection, "replication");
      });
  });

  after(() => {
    setFeatureFlags({});
    setFeatureServiceFlags({});

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
    cleanDBSource();
  });

  const usersStreamRow = new StreamRowPageObjectV2("public", "users");
  const citiesStreamRow = new StreamRowPageObjectV2("public", "cities");
  const carsStreamRow = new StreamRowPageObjectV2("public", "cars");

  describe("All", () => {
    it("should show all streams (enabled and disabled)", () => {
      streamsTableV2.clickFilterTab("all");

      usersStreamRow.isStreamExistInTable(true);
      citiesStreamRow.isStreamExistInTable(true);
      carsStreamRow.isStreamExistInTable(true);

      carsStreamRow.toggleStreamSync(true);
      carsStreamRow.isStreamExistInTable(true);
    });
    it("should show total count of streams in namespace row", () => {
      streamsTableV2.isTotalAmountOfStreamsDisplayed(3, true);
    });
  });

  describe("Enabled streams", () => {
    it("should show only enabled streams", () => {
      streamsTableV2.clickFilterTab("enabledStreams");

      usersStreamRow.isStreamExistInTable(false);
      citiesStreamRow.isStreamExistInTable(false);
      carsStreamRow.isStreamExistInTable(true);
      carsStreamRow.isStreamSyncEnabled(true);
    });
    it("should show only enabled streams filtered by name", () => {
      streamsTableV2.filterByStreamOrFieldName("cars");
      carsStreamRow.isStreamExistInTable(true);

      streamsTableV2.filterByStreamOrFieldName("carss");
      streamsTableV2.isNoMatchingStreamsMsgDisplayed(true);
      streamsTableV2.clearFilterByStreamOrFieldNameInput();
    });
    it("should show `{enabled} / {total} streams` count in namespace row if not all streams are enabled", () => {
      streamsTableV2.isAmountOfCountedStreamsOutOfTotalDisplayed("1 / 3", true);
    });
    it("should show empty table if there is no enabled streams", () => {
      carsStreamRow.toggleStreamSync(false);
      streamsTableV2.isNamespaceCellEmpty(true);
      streamsTableV2.isNoStreamsMsgDisplayed(true);
    });
  });

  describe("Disabled streams", () => {
    it("should show only disabled streams", () => {
      streamsTableV2.clickFilterTab("disabledStreams");

      usersStreamRow.isStreamExistInTable(true);
      citiesStreamRow.isStreamExistInTable(true);
      carsStreamRow.isStreamExistInTable(true);
    });

    it("should show only disabled streams filtered by name", () => {
      citiesStreamRow.toggleStreamSync(true);
      citiesStreamRow.isStreamExistInTable(false);
      streamsTableV2.filterByStreamOrFieldName("cities");
      streamsTableV2.isNamespaceCellEmpty(true);
      streamsTableV2.isNoMatchingStreamsMsgDisplayed(true);
      streamsTableV2.clearFilterByStreamOrFieldNameInput();
    });
    it("should show `{disabled} / {total} streams` count in namespace row if not all streams are disabled", () => {
      streamsTableV2.isAmountOfCountedStreamsOutOfTotalDisplayed("2 / 3", true);
    });
  });
});
