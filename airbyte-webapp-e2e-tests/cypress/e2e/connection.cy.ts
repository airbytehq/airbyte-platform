import { appendRandomString, deleteEntity, submitButtonClick } from "commands/common";
import { createTestConnection } from "commands/connection";
import { deleteDestination } from "commands/destination";
import { deleteSource } from "commands/source";
import * as connectionForm from "pages/connection/connectionFormPageObject";
import { goToSourcePage, openSourceOverview } from "pages/sourcePage";
import * as connectionSettings from "pages/connection/connectionSettingsPageObject";
import { cleanDBSource, makeChangesInDBSource, populateDBSource } from "commands/db";
import * as catalogDiffModal from "pages/connection/catalogDiffModalPageObject";
import {
  interceptGetConnectionRequest,
  interceptUpdateConnectionRequest,
  waitForGetConnectionRequest,
  waitForUpdateConnectionRequest,
} from "commands/interceptors";
import { goToReplicationTab } from "pages/connection/connectionPageObject";
import * as replicationPage from "pages/connection/connectionReplicationPageObject";
import { streamsTable } from "pages/connection/StreamsTablePageObject";
import { DestinationSyncMode, SourceSyncMode } from "commands/api/types";
import { runDbQuery } from "commands/db/db";
import { createUsersTableQuery, dropUsersTableQuery } from "commands/db/queries";
import { streamDetails } from "pages/connection/StreamDetailsPageObject";

const sourceNamespace = "public";

describe("Connection - creation, updating connection replication settings, deletion", () => {
  before(() => {
    runDbQuery(dropUsersTableQuery);
    runDbQuery(createUsersTableQuery);
  });

  beforeEach(() => {
    interceptGetConnectionRequest();
    interceptUpdateConnectionRequest();
  });

  after(() => {
    runDbQuery(dropUsersTableQuery);
  });

  it("Create Postgres <> LocalJSON connection, check its creation", () => {
    const sourceName = appendRandomString("Test connection source cypress");
    const destName = appendRandomString("Test connection destination cypress");

    createTestConnection(sourceName, destName);
    cy.get("div").contains(sourceName).should("exist");
    cy.get("div").contains(destName).should("exist");

    deleteSource(sourceName);
    deleteDestination(destName);
  });

  it("Create Postgres <> LocalJSON connection, update connection replication settings - select schedule and add destination prefix", () => {
    const sourceName = appendRandomString("Test update connection source cypress");
    const destName = appendRandomString("Test update connection destination cypress");

    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();
    connectionForm.expandConfigurationSection();

    connectionForm.selectSchedule("Every hour");
    connectionForm.fillOutDestinationPrefix("auto_test");

    submitButtonClick();

    waitForUpdateConnectionRequest().then((interception) => {
      assert.isNotNull(interception.response?.statusCode, "200");
    });

    replicationPage.checkSuccessResult();

    deleteSource(sourceName);
    deleteDestination(destName);
  });

  it(`Creates PokeAPI <> Local JSON connection, update connection replication settings - 
  select schedule, add destination prefix, set destination namespace custom format, change prefix and make sure that it's applied to all streams`, () => {
    const sourceName = appendRandomString("Test update connection PokeAPI source cypress");
    const destName = appendRandomString("Test update connection Local JSON destination cypress");

    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();
    connectionForm.expandConfigurationSection();

    connectionForm.selectSchedule("Every hour");

    const row = streamsTable.getRow("no-namespace", "pokemon");
    row.selectSyncMode(SourceSyncMode.FullRefresh, DestinationSyncMode.Append);

    const prefix = "auto_test";
    connectionForm.fillOutDestinationPrefix(prefix);
    connectionForm.setupDestinationNamespaceCustomFormat("_test");

    // Ensures the prefix is applied to the streams
    row.checkDestinationStreamName(`${prefix}pokemon`);

    submitButtonClick();
    replicationPage.confirmStreamConfigurationChangedPopup();

    waitForUpdateConnectionRequest().then((interception) => {
      assert.isNotNull(interception.response?.statusCode, "200");
      expect(interception.request.method).to.eq("POST");
      expect(interception.request)
        .property("body")
        .to.contain({
          name: `${sourceName} → ${destName}Connection name`,
          prefix: "auto_test",
          namespaceDefinition: "customformat",
          namespaceFormat: "${SOURCE_NAMESPACE}_test",
          status: "active",
        });
      expect(interception.request.body.scheduleData.basicSchedule).to.contain({
        units: 1,
        timeUnit: "hours",
      });

      const streamToUpdate = interception.request.body.syncCatalog.streams[0];

      expect(streamToUpdate.config).to.contain({
        aliasName: "pokemon",
        destinationSyncMode: "append",
        selected: true,
      });

      expect(streamToUpdate.stream).to.contain({
        name: "pokemon",
      });
      expect(streamToUpdate.stream.supportedSyncModes).to.contain("full_refresh");
    });
    replicationPage.checkSuccessResult();

    deleteSource(sourceName);
    deleteDestination(destName);
  });

  it("Create PokeAPI <> Local JSON connection, update connection replication settings - edit the schedule type one by one - cron, manual, every hour", () => {
    const sourceName = appendRandomString("Test connection source cypress PokeAPI");
    const destName = appendRandomString("Test connection destination cypress");

    createTestConnection(sourceName, destName);

    goToReplicationTab();
    connectionForm.expandConfigurationSection();

    connectionForm.selectSchedule("Cron");
    submitButtonClick();
    replicationPage.checkSuccessResult();

    connectionForm.selectSchedule("Manual");
    submitButtonClick();
    replicationPage.checkSuccessResult();

    connectionForm.selectSchedule("Every hour");
    submitButtonClick();
    replicationPage.checkSuccessResult();

    deleteSource(sourceName);
    deleteDestination(destName);
  });

  it("Create PokeAPI <> Local JSON connection, update connection replication settings - make sure that saving a connection's schedule type only changes expected values", () => {
    const sourceName = appendRandomString("Test update connection PokeAPI source cypress");
    const destName = appendRandomString("Test update connection Local JSON destination cypress");

    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    let loadedConnection: any = null; // Should be a WebBackendConnectionRead
    waitForGetConnectionRequest().then((interception) => {
      const {
        scheduleType: readScheduleType,
        scheduleData: readScheduleData,
        ...connectionRead
      } = interception.response?.body;
      loadedConnection = connectionRead;

      expect(loadedConnection).not.to.eq(null);
      expect(readScheduleType).to.eq("manual");
      expect(readScheduleData).to.eq(undefined);
    });

    goToReplicationTab();
    connectionForm.expandConfigurationSection();

    connectionForm.selectSchedule("Every hour");
    submitButtonClick();

    waitForUpdateConnectionRequest().then((interception) => {
      // Schedule is pulled out here, but we don't do anything with is as it's legacy
      const { scheduleType, scheduleData, schedule, ...connectionUpdate } = interception.response?.body;
      expect(scheduleType).to.eq("basic");
      expect(scheduleData.basicSchedule).to.deep.eq({
        timeUnit: "hours",
        units: 1,
      });

      expect(loadedConnection).to.deep.eq(connectionUpdate);
    });
    replicationPage.checkSuccessResult();

    deleteSource(sourceName);
    deleteDestination(destName);
  });

  it("Create PokeAPI <> Local JSON connection, and delete connection", () => {
    const sourceName = "Test delete connection source cypress";
    const destName = "Test delete connection destination cypress";
    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    connectionSettings.goToSettingsPage();

    deleteEntity();

    deleteSource(sourceName);
    deleteDestination(destName);
  });

  it("Create PokeAPI <> Local JSON connection, update connection replication settings - set destination namespace with 'Custom format' option", () => {
    const sourceName = appendRandomString("Test update connection PokeAPI source cypress");
    const destName = appendRandomString("Test update connection Local JSON destination cypress");

    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();
    connectionForm.expandConfigurationSection();

    const namespace = "_DestinationNamespaceCustomFormat";
    connectionForm.setupDestinationNamespaceCustomFormat(namespace);

    // Ensures the DestinationNamespace is applied to the streams
    const row = streamsTable.getRow("no-namespace", "pokemon");
    row.checkDestinationNamespace(`\${SOURCE_NAMESPACE}${namespace}`);

    submitButtonClick();

    waitForUpdateConnectionRequest().then((interception) => {
      assert.isNotNull(interception.response?.statusCode, "200");
      expect(interception.request.method).to.eq("POST");
      expect(interception.request)
        .property("body")
        .to.contain({
          name: `${sourceName} → ${destName}Connection name`,
          namespaceDefinition: "customformat",
          namespaceFormat: "${SOURCE_NAMESPACE}_DestinationNamespaceCustomFormat",
          status: "active",
        });

      const streamToUpdate = interception.request.body.syncCatalog.streams[0];

      expect(streamToUpdate.stream).to.contain({
        name: "pokemon",
      });
    });
    replicationPage.checkSuccessResult();

    deleteSource(sourceName);
    deleteDestination(destName);
  });

  it("Create PokeAPI <> Local JSON connection, update connection replication settings - set destination namespace with 'Mirror source structure' option", () => {
    const sourceName = appendRandomString("Test update connection PokeAPI source cypress");
    const destName = appendRandomString("Test update connection Local JSON destination cypress");

    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();

    const namespace = "<source schema>";

    // Ensures the DestinationNamespace is applied to the streams
    const row = streamsTable.getRow("no-namespace", "pokemon");
    row.checkDestinationNamespace(namespace);

    deleteSource(sourceName);
    deleteDestination(destName);
  });

  it("Create PokeAPI <> Local JSON connection, update connection replication settings - set destination namespace with 'Destination default' option", () => {
    const sourceName = appendRandomString("Test update connection PokeAPI source cypress");
    const destName = appendRandomString("Test update connection Local JSON destination cypress");

    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();
    connectionForm.expandConfigurationSection();

    connectionForm.setupDestinationNamespaceDefaultFormat();

    const namespace = "<destination schema>";

    // Ensures the DestinationNamespace is applied to the streams
    const row = streamsTable.getRow("no-namespace", "pokemon");
    row.checkDestinationNamespace(namespace);

    submitButtonClick();

    waitForUpdateConnectionRequest().then((interception) => {
      assert.isNotNull(interception.response?.statusCode, "200");
      expect(interception.request.method).to.eq("POST");
      expect(interception.request)
        .property("body")
        .to.contain({
          name: `${sourceName} → ${destName}Connection name`,
          namespaceDefinition: "destination",
          namespaceFormat: "${SOURCE_NAMESPACE}",
          status: "active",
        });

      const streamToUpdate = interception.request.body.syncCatalog.streams[0];

      expect(streamToUpdate.stream).to.contain({
        name: "pokemon",
      });
    });
    replicationPage.checkSuccessResult();

    deleteSource(sourceName);
    deleteDestination(destName);
  });
});

describe("Connection - stream details", () => {
  beforeEach(() => {
    cleanDBSource();
    populateDBSource();
  });

  afterEach(() => {
    cleanDBSource();
  });

  it("Create Postgres <> Postgres connection, connection replication settings, expand stream details", () => {
    const sourceName = appendRandomString("Test connection Postgres source cypress");
    const destName = appendRandomString("Test connection Postgres destination cypress");
    const streamName = "users";

    const names = ["email", "id", "name", "updated_at"];
    const dataTypes = ["String", "Integer", "String", "Datetime"];

    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();
    connectionForm.expandConfigurationSection();

    const row = streamsTable.getRow("public", streamName);
    row.showStreamDetails();
    streamDetails.areFieldsValid({ names, dataTypes });

    deleteSource(sourceName);
    deleteDestination(destName);
  });
});

describe("Connection sync modes", () => {
  beforeEach(() => {
    populateDBSource();

    interceptUpdateConnectionRequest();
  });

  afterEach(() => {
    cleanDBSource();
  });

  it("Create Postgres <> Postgres connection, update connection replication settings - select 'Incremental Append' sync mode, select required Cursor field, verify changes", () => {
    const sourceName = appendRandomString("Test connection Postgres source cypress");
    const destName = appendRandomString("Test connection Postgres destination cypress");
    const streamName = "users";

    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();
    connectionForm.expandConfigurationSection();

    let row = streamsTable.getRow("public", streamName);
    row.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.Append);
    row.selectCursor("updated_at");

    submitButtonClick();
    replicationPage.confirmStreamConfigurationChangedPopup();

    waitForUpdateConnectionRequest().then((interception) => {
      assert.isNotNull(interception.response?.statusCode, "200");
    });

    replicationPage.checkSuccessResult();

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();

    row = streamsTable.getRow("public", "users");
    row.hasSelectedCursorField("updated_at");

    deleteSource(sourceName);
    deleteDestination(destName);
  });

  it("Create Postgres <> Postgres connection, update connection replication settings - select 'Incremental Deduped History'(PK is defined), select Cursor field, verify changes", () => {
    const sourceName = appendRandomString("Test connection Postgres source cypress");
    const destName = appendRandomString("Test connection Postgres destination cypress");
    const streamName = "users";

    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();

    const row = streamsTable.getRow("public", streamName);
    row.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);
    row.selectCursor("updated_at");
    row.hasSourceDefinedPrimaryKeys("id");

    submitButtonClick();
    replicationPage.confirmStreamConfigurationChangedPopup();

    waitForUpdateConnectionRequest().then((interception) => {
      assert.isNotNull(interception.response?.statusCode, "200");
    });

    replicationPage.checkSuccessResult();

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();

    row.hasSelectedCursorField("updated_at");
    row.hasSourceDefinedPrimaryKeys("id");

    deleteSource(sourceName);
    deleteDestination(destName);
  });

  it("Create Postgres <> Postgres connection, update connection replication settings - select 'Incremental Deduped History'(PK is NOT defined), select Cursor field, select PK, verify changes", () => {
    const sourceName = appendRandomString("Test connection Postgres source cypress");
    const destName = appendRandomString("Test connection Postgres destination cypress");
    const streamName = "cities";

    createTestConnection(sourceName, destName);

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();

    const row = streamsTable.getRow("public", streamName);
    row.selectSyncMode(SourceSyncMode.Incremental, DestinationSyncMode.AppendDedup);
    row.selectCursor("city");
    row.hasNoSourceDefinedPrimaryKeys();
    row.selectPrimaryKeys(["city_code"]);

    submitButtonClick(true);
    replicationPage.confirmStreamConfigurationChangedPopup();

    waitForUpdateConnectionRequest().then((interception) => {
      assert.isNotNull(interception.response?.statusCode, "200");
    });

    replicationPage.checkSuccessResult();

    goToSourcePage();
    openSourceOverview(sourceName);
    connectionSettings.openConnectionOverviewByDestinationName(destName);

    goToReplicationTab();

    row.hasSelectedCursorField("city");
    row.hasSelectedPrimaryKeys(["city_code"]);

    deleteSource(sourceName);
    deleteDestination(destName);
  });
});

describe("Connection - detect source schema changes in source", () => {
  beforeEach(() => {
    populateDBSource();

    interceptUpdateConnectionRequest();
  });

  afterEach(() => {
    cleanDBSource();
  });

  it("Create Postgres <> Local JSON connection, update data in source (async), refresh source schema, check diff modal, reset streams", () => {
    const sourceName = appendRandomString(
      "Test refresh source schema with changed data - connection Postgres source cypress"
    );
    const destName = appendRandomString(
      "Test refresh source schema with changed data - connection Local JSON destination cypress"
    );

    createTestConnection(sourceName, destName);
    cy.get("div").contains(sourceName).should("exist");
    cy.get("div").contains(destName).should("exist");

    makeChangesInDBSource();
    goToReplicationTab();
    streamsTable.refreshSourceSchemaBtnClick();

    catalogDiffModal.shouldExist();

    cy.get(catalogDiffModal.removedStreamsTable).should("contain", "users");

    cy.get(catalogDiffModal.newStreamsTable).should("contain", "cars");

    catalogDiffModal.toggleStreamWithChangesAccordion("cities");
    cy.get(catalogDiffModal.removedFieldsTable).should("contain", "city_code");
    cy.get(catalogDiffModal.newFieldsTable).children().should("contain", "country").and("contain", "state");

    catalogDiffModal.clickCloseButton();

    const row = streamsTable.getRow(sourceNamespace, "cars");
    row.toggleStreamSync();

    submitButtonClick();
    replicationPage.resetModalSaveBtnClick();

    waitForUpdateConnectionRequest().then((interception) => {
      assert.isNotNull(interception.response?.statusCode, "200");
    });

    replicationPage.checkSuccessResult();

    deleteSource(sourceName);
    deleteDestination(destName);
  });
});
