import { AirbyteCatalog } from "@cy/../src/core/api/generated/AirbyteClient.schemas";
import { createNewConnectionViaApi } from "@cy/commands/connection";
import { createPostgresDestinationViaApi, createPostgresSourceViaApi } from "@cy/commands/connection";
import {
  getPostgresToPostgresUpdateConnectionBody,
  requestDeleteConnection,
  requestDeleteSource,
  requestGetConnection,
  requestUpdateConnection,
} from "commands/api";
import { Connection, Destination, DestinationSyncMode, Source, SourceSyncMode } from "commands/api/types";
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
import { streamsTable } from "pages/connection/StreamsTablePageObject";

describe("Connection - Auto-detect schema changes", () => {
  let source: Source;
  let destination: Destination;
  let connection: Connection;

  before(() => {
    createPostgresDestinationViaApi().then((pgDestination) => {
      destination = pgDestination;
    });
  });

  beforeEach(() => {
    populateDBSource();
    createPostgresSourceViaApi().then((pgSource) => {
      source = pgSource;
      createNewConnectionViaApi(source, destination).then((connectionResponse) => {
        connection = connectionResponse;
      });
    });
  });

  afterEach(() => {
    if (connection) {
      requestDeleteConnection(connection.connectionId);
    }
    if (source) {
      requestDeleteSource(source.sourceId);
    }
    cleanDBSource();
  });

  describe("non-breaking changes", () => {
    beforeEach(() => {
      makeChangesInDBSource();
      requestGetConnection({ connectionId: connection.connectionId, withRefreshedCatalog: true });
    });

    it("shows non-breaking change on list page", () => {
      connectionListPage.visit();
      connectionListPage.getSchemaChangeIcon(connection, "non_breaking").should("exist");
      connectionListPage.getManualSyncButton(connection).should("be.enabled");
    });

    it("shows non-breaking change that can be saved after refresh", () => {
      // Need to continue running but async breaks everything
      // todo: i am not sure what that comment means ^
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

      replicationPage.saveChangesAndHandleResetModal();
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
        (stream) => stream.stream.name === "users" && stream.stream.namespace === "public"
      );
      const newSyncCatalog = { streams: [...connection.syncCatalog.streams] } as AirbyteCatalog; // this is because we reinvented our type system for e2e :( TODO: don't

      newSyncCatalog.streams[streamToUpdate].config = {
        ...newSyncCatalog.streams[streamToUpdate].config,
        destinationSyncMode: DestinationSyncMode.AppendDedup,
        syncMode: SourceSyncMode.Incremental,
        cursorField: ["updated_at"],
      };

      requestUpdateConnection(
        getPostgresToPostgresUpdateConnectionBody(connection.connectionId, { syncCatalog: newSyncCatalog })
      );

      // Remove cursor from db and refreshes schema to force breaking change detection
      runDbQuery(alterTable("public.users", { drop: ["updated_at"] }));
      requestGetConnection({ connectionId: connection.connectionId, withRefreshedCatalog: true });
      cy.reload();
    });

    it("shows breaking change on list page", () => {
      connectionListPage.visit();
      connectionListPage.getSchemaChangeIcon(connection, "breaking").should("exist");
      connectionListPage.getManualSyncButton(connection).should("be.disabled");
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
      streamsTable.searchStream("users");
      const row = streamsTable.getRow("public", "users");
      row.selectSyncMode(SourceSyncMode.FullRefresh, DestinationSyncMode.Append);

      replicationPage.saveChangesAndHandleResetModal();
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
      connectionPage.visit(connection, "replication");
      connectionForm.expandConfigurationSection();
      replicationPage.selectNonBreakingChangesPreference("disable");

      cy.intercept("/api/v1/web_backend/connections/update").as("updatesNonBreakingPreference");

      replicationPage.saveChangesAndHandleResetModal({ expectModal: false });

      cy.wait("@updatesNonBreakingPreference").then((interception) => {
        assert.equal((interception.response?.body as Connection).nonBreakingChangesPreference, "disable");
      });
    });
  });

  it("shows no diff after refresh if there have been no changes", () => {
    connectionPage.visit(connection, "replication");

    replicationPage.clickRefreshSourceSchemaButton();

    replicationPage.checkNoDiffToast();
    catalogDiffModal.shouldNotExist();
  });
});
