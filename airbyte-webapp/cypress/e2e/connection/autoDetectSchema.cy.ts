import {
  createNewConnectionViaApi,
  createPostgresDestinationViaApi,
  createPostgresSourceViaApi,
} from "@cy/commands/connection";
import { StreamRowPageObject } from "@cy/pages/connection/StreamRowPageObject";
import { streamsTable } from "@cy/pages/connection/StreamsTablePageObject";
import { setFeatureFlags, setFeatureServiceFlags } from "@cy/support/e2e";
import {
  WebBackendConnectionRead,
  DestinationRead,
  DestinationSyncMode,
  SourceRead,
  SyncMode,
} from "@src/core/api/types/AirbyteClient";
import {
  getUpdateConnectionBody,
  requestDeleteConnection,
  requestDeleteDestination,
  requestDeleteSource,
  requestGetConnection,
  requestUpdateConnection,
} from "commands/api";
import {
  cleanDBSource,
  makeChangesInDBSource,
  populateDBSource,
  reverseChangesInDBSource,
  runDbQuery,
} from "commands/db/db";
import { alterTable } from "commands/db/queries";

import * as catalogDiffModal from "pages/connection/catalogDiffModalPageObject";
import * as connectionForm from "pages/connection/connectionFormPageObject";
import * as connectionListPage from "pages/connection/connectionListPageObject";
import * as connectionPage from "pages/connection/connectionPageObject";
import * as replicationPage from "pages/connection/connectionReplicationPageObject";
import * as settingsPage from "pages/connection/connectionSettingsPageObject";

// Note: this test is tagged as @sync-catalog to help even out the tests across four parallel github workflows, but it
// is not really related to the sync catalog functionality.
describe("Connection - Auto-detect schema changes", { tags: "@sync-catalog" }, () => {
  let source: SourceRead;
  let destination: DestinationRead;
  let connection: WebBackendConnectionRead;

  before(() => {
    createPostgresDestinationViaApi().then((pgDestination) => {
      destination = pgDestination;
    });
  });

  beforeEach(() => {
    populateDBSource();
    createPostgresSourceViaApi().then((pgSource) => {
      source = pgSource;
      createNewConnectionViaApi(source, destination, { enableAllStreams: true }).then((connectionResponse) => {
        connection = connectionResponse;
      });
    });
  });

  afterEach(() => {
    if (connection) {
      requestDeleteConnection({ connectionId: connection.connectionId });
    }
    if (source) {
      requestDeleteSource({ sourceId: source.sourceId });
    }
    cleanDBSource();
  });

  after(() => {
    setFeatureFlags({});
    setFeatureServiceFlags({});

    if (destination) {
      requestDeleteDestination({ destinationId: destination.destinationId });
    }
  });

  describe("non-breaking changes", () => {
    beforeEach(() => {
      makeChangesInDBSource();
      requestGetConnection({ connectionId: connection.connectionId, withRefreshedCatalog: true });
    });

    it("does not show non-breaking change on list page", () => {
      connectionListPage.visit();
      connectionListPage.getSchemaChangeIcon(connection, "warning").should("not.exist");
      connectionListPage.getConnectionStateSwitch(connection).should("be.checked").and("be.enabled");
    });

    it("shows non-breaking change that can be saved after refresh", () => {
      connectionPage.visit(connection, "replication");

      // check overlay panel
      replicationPage.checkSchemaChangesDetected({ breaking: false });
      replicationPage.clickSchemaChangesReviewButton();
      connectionPage.getSyncEnabledSwitch().should("be.enabled");

      // check modal content
      catalogDiffModal.shouldExist();
      cy.get(catalogDiffModal.removedStreamsTable).should("contain", "users");

      cy.get(catalogDiffModal.newStreamsTable).should("contain", "cars");

      catalogDiffModal.toggleStreamWithChangesAccordion("cities");
      cy.get(catalogDiffModal.removedFieldsTable).should("contain", "city_code");
      cy.get(catalogDiffModal.newFieldsTable).children().should("contain", "country").and("contain", "state");

      catalogDiffModal.clickCloseButton();

      replicationPage.checkSchemaChangesDetectedCleared();

      replicationPage.saveChangesAndHandleResetModal({ expectModal: false });
      connectionPage.getSyncEnabledSwitch().should("be.enabled");
    });

    it("clears non-breaking change when db changes are restored", () => {
      connectionPage.visit(connection, "replication");

      replicationPage.checkSchemaChangesDetected({ breaking: false });

      reverseChangesInDBSource();

      replicationPage.clickSchemaChangesReviewButton();

      replicationPage.checkSchemaChangesDetectedCleared();
      catalogDiffModal.shouldNotExist();
      replicationPage.checkNoDiffToast();
    });
  });

  describe("breaking changes", () => {
    beforeEach(() => {
      const streamToUpdate = connection.syncCatalog.streams.findIndex(
        (stream) => stream.stream?.name === "users" && stream.stream.namespace === "public"
      );
      const newSyncCatalog = { streams: [...connection.syncCatalog.streams] };

      newSyncCatalog.streams[streamToUpdate].config = {
        ...newSyncCatalog.streams[streamToUpdate].config,
        destinationSyncMode: DestinationSyncMode.append_dedup,
        syncMode: SyncMode.incremental,
        cursorField: ["updated_at"],
      };

      requestUpdateConnection(getUpdateConnectionBody(connection.connectionId, { syncCatalog: newSyncCatalog }));

      // Remove cursor from db and refreshes schema to force breaking change detection
      runDbQuery(alterTable("public.users", { drop: ["updated_at"] }));
      requestGetConnection({ connectionId: connection.connectionId, withRefreshedCatalog: true });
      cy.reload();
    });

    it("shows breaking change on list page", () => {
      connectionListPage.visit();
      connectionListPage.getSchemaChangeIcon(connection, "error").should("exist");
      connectionListPage.getConnectionStateSwitch(connection).should("not.be.checked").and("not.be.enabled");
    });

    it("shows breaking change that can be saved after refresh and fix", () => {
      connectionPage.visit(connection, "replication");

      // Confirm that breaking changes are there
      replicationPage.checkSchemaChangesDetected({ breaking: true });
      connectionPage.getSyncEnabledSwitch().should("be.disabled");
      replicationPage.clickSchemaChangesReviewButton();
      connectionPage.getSyncEnabledSwitch().should("be.disabled");

      // confirm contents of the catalog diff modal
      catalogDiffModal.shouldExist();
      cy.get(catalogDiffModal.removedStreamsTable).should("not.exist");
      cy.get(catalogDiffModal.newStreamsTable).should("not.exist");
      catalogDiffModal.toggleStreamWithChangesAccordion("users");
      cy.get(catalogDiffModal.removedFieldsTable).should("contain", "updated_at");
      cy.get(catalogDiffModal.newFieldsTable).should("not.exist");
      catalogDiffModal.clickCloseButton();

      replicationPage.checkSchemaChangesDetectedCleared();

      // Fix the conflict
      streamsTable.filterByStreamOrFieldName("users");
      const usersStreamRow = new StreamRowPageObject("public", "users");
      usersStreamRow.selectSyncMode(SyncMode.full_refresh, DestinationSyncMode.append);

      replicationPage.saveChangesAndHandleResetModal({ expectModal: false });
      connectionPage.getSyncEnabledSwitch().should("be.enabled");
    });

    it("clears breaking change if db changes are restored", () => {
      connectionPage.visit(connection, "replication");

      replicationPage.checkSchemaChangesDetected({ breaking: true });

      runDbQuery(alterTable("public.users", { add: ["updated_at TIMESTAMP"] }));
      replicationPage.clickSchemaChangesReviewButton();

      replicationPage.checkSchemaChangesDetectedCleared();
      replicationPage.checkNoDiffToast();
    });
  });

  describe("non-breaking schema update preference", () => {
    it("saves non-breaking schema update preference change", () => {
      connectionPage.visit(connection, "settings");
      connectionForm.toggleAdvancedSettingsSection();
      connectionForm.selectNonBreakingChangesPreference("disable");

      cy.intercept("/api/v1/web_backend/connections/update").as("updatesNonBreakingPreference");

      settingsPage.saveChanges();

      cy.wait("@updatesNonBreakingPreference").then((interception) => {
        assert.equal((interception.response?.body as WebBackendConnectionRead).nonBreakingChangesPreference, "disable");
      });
    });
  });

  it("shows no diff after refresh if there have been no changes", () => {
    connectionPage.visit(connection, "replication");

    streamsTable.clickRefreshSourceSchemaButton();

    replicationPage.checkNoDiffToast();
    catalogDiffModal.shouldNotExist();
  });
});
