import { getWorkspaceId } from "@cy/commands/api/workspace";
import { createPostgresDestinationViaApi, createPostgresSourceViaApi } from "@cy/commands/connection";
import { StreamRowPageObject } from "@cy/pages/connection/StreamRowPageObject";
import { streamsTable } from "@cy/pages/connection/StreamsTablePageObject";
import { setFeatureFlags, setFeatureServiceFlags } from "@cy/support/e2e";
import { WebBackendConnectionRead, DestinationRead, SourceRead } from "@src/core/api/types/AirbyteClient";
import { RoutePaths, ConnectionRoutePaths } from "@src/pages/routePaths";
import { requestDeleteConnection, requestDeleteDestination, requestDeleteSource } from "commands/api";
import { submitButtonClick } from "commands/common";
import { runDbQuery } from "commands/db/db";
import {
  createUsersTableQuery,
  dropUsersTableQuery,
  createDummyTablesQuery,
  dropDummyTablesQuery,
} from "commands/db/queries";
import {
  interceptCreateConnectionRequest,
  interceptDiscoverSchemaRequest,
  waitForCreateConnectionRequest,
  waitForDiscoverSchemaRequest,
} from "commands/interceptors";

import * as newConnectionPage from "pages/connection/createConnectionPageObject";
import { nextButton } from "pages/connection/createConnectionPageObject";

describe("Connection - Create new connection", { testIsolation: false }, () => {
  let source: SourceRead;
  let destination: DestinationRead;
  let connectionId: string;

  const dropTables = () => {
    runDbQuery(dropUsersTableQuery, dropDummyTablesQuery(20));
  };
  before(() => {
    dropTables();
    runDbQuery(createUsersTableQuery, createDummyTablesQuery(20));
    createPostgresSourceViaApi().then((pgSource) => {
      source = pgSource;
    });
    createPostgresDestinationViaApi().then((pgDestination) => {
      destination = pgDestination;
    });
  });

  after(() => {
    if (connectionId) {
      requestDeleteConnection({ connectionId });
    }
    if (source) {
      requestDeleteSource({ sourceId: source.sourceId });
    }
    if (destination) {
      requestDeleteDestination({ destinationId: destination.destinationId });
    }

    dropTables();
  });

  describe("Streams table", () => {
    before(() => {
      interceptDiscoverSchemaRequest();

      cy.visit(
        `/workspaces/${getWorkspaceId()}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${
          ConnectionRoutePaths.Configure
        }?sourceId=${source.sourceId}&destinationId=${destination.destinationId}`
      );
      waitForDiscoverSchemaRequest();
    });

    after(() => {
      setFeatureFlags({});
      setFeatureServiceFlags({});
    });

    it("should have no streams checked by default", () => {
      streamsTable.isNamespaceCheckboxChecked(false);
      streamsTable.areAllStreamsInNamespaceEnabled("public", false);
    });

    it("should verify namespace row", () => {
      streamsTable.isNamespaceCheckboxEnabled(true);
      streamsTable.isNamespaceCheckboxChecked(false);
      streamsTable.isNamespaceNameDisplayed("public", true);
      streamsTable.isTotalAmountOfStreamsDisplayed(21, true);
      streamsTable.isOpenNamespaceModalGearButtonDisplayed(true);
      streamsTable.isSyncModeColumnNameDisplayed();
      streamsTable.isFieldsColumnNameDisplayed();
    });

    it("should show 'no selected streams' error ", () => {
      streamsTable.isNoStreamsSelectedErrorDisplayed(true);
      streamsTable.areAllStreamsInNamespaceEnabled("public", false);
      newConnectionPage.isNextPageButtonEnabled(false);
    });

    it("should NOT show 'no selected streams' error", () => {
      streamsTable.toggleNamespaceCheckbox("public", true);
      streamsTable.areAllStreamsInNamespaceEnabled("public", true);

      streamsTable.isNoStreamsSelectedErrorDisplayed(false);
      newConnectionPage.isNextPageButtonEnabled(false);

      streamsTable.toggleNamespaceCheckbox("public", false);
    });

    it("should not replace refresh schema button with form controls", () => {
      streamsTable.isRefreshSourceSchemaBtnExist(true);
      streamsTable.isToggleExpandCollapseAllStreamsBtnExist(true);

      streamsTable.toggleNamespaceCheckbox("public", true);
      streamsTable.isRefreshSourceSchemaBtnExist(true);
      streamsTable.isToggleExpandCollapseAllStreamsBtnExist(true);

      streamsTable.toggleNamespaceCheckbox("public", false);
    });

    it("should enable all streams in namespace", () => {
      streamsTable.toggleNamespaceCheckbox("public", true);
      streamsTable.areAllStreamsInNamespaceEnabled("public", true);
      streamsTable.toggleNamespaceCheckbox("public", false);
      streamsTable.areAllStreamsInNamespaceEnabled("public", false);
    });
  });

  describe("Stream", () => {
    before(() => {
      interceptDiscoverSchemaRequest();

      cy.visit(
        `/workspaces/${getWorkspaceId()}/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}/${
          ConnectionRoutePaths.Configure
        }?sourceId=${source.sourceId}&destinationId=${destination.destinationId}`
      );
      waitForDiscoverSchemaRequest();
    });

    after(() => {
      setFeatureFlags({});
      setFeatureServiceFlags({});
    });

    const usersStreamRow = new StreamRowPageObject("public", "users");

    it("should enable and disable stream", () => {
      streamsTable.filterByStreamOrFieldName("users");
      streamsTable.isNamespaceCheckboxEnabled(false);
      usersStreamRow.streamHasDisabledStyle(true);
      usersStreamRow.toggleStreamSync(true);
      usersStreamRow.isStreamSyncEnabled(true);
      usersStreamRow.streamHasDisabledStyle(false);
      usersStreamRow.streamHasAddedStyle(false);
      usersStreamRow.isMissedCursorErrorDisplayed(true);

      usersStreamRow.toggleStreamSync(false);
      usersStreamRow.isStreamSyncEnabled(false);
    });

    it("should expand and collapse stream", () => {
      usersStreamRow.toggleExpandCollapseStream();
      usersStreamRow.isStreamExpanded(true);
      usersStreamRow.toggleExpandCollapseStream();
      usersStreamRow.isStreamExpanded(false);
    });

    it("should enable field", () => {
      usersStreamRow.isStreamSyncEnabled(false);
      usersStreamRow.toggleExpandCollapseStream();
      usersStreamRow.fieldHasDisabledStyle("email", true);

      usersStreamRow.toggleFieldSync("email", true);
      usersStreamRow.fieldHasDisabledStyle("email", false);
      usersStreamRow.fieldHasAddedStyle("email", false);
      usersStreamRow.isFieldSyncCheckboxDisabled("id", true);
      usersStreamRow.isPKField("id", true);

      usersStreamRow.isFieldSyncEnabled("email", true);
      usersStreamRow.isMissedCursorErrorDisplayed(true);
      usersStreamRow.isStreamSyncEnabled(true);
      streamsTable.isNamespaceCheckboxMixed(true);
    });

    it("should enable form submit after a stream is selected and configured", () => {
      usersStreamRow.selectSyncMode("full_refresh", "overwrite");
      streamsTable.isNoStreamsSelectedErrorDisplayed(false);
      newConnectionPage.isNextPageButtonEnabled(true);
    });
  });

  describe("Submit form", () => {
    it("should set up a connection", () => {
      interceptCreateConnectionRequest();
      cy.get(nextButton).click();
      submitButtonClick(true);

      waitForCreateConnectionRequest().then((interception) => {
        assert.isNotNull(interception.response?.statusCode, "200");
        expect(interception.request.method).to.eq("POST");

        const connection: Partial<WebBackendConnectionRead> = {
          name: `${source.name} → ${destination.name}`,
          scheduleType: "basic",
        };
        expect(interception.request.body).to.contain(connection);
        expect(interception.response?.body).to.contain(connection);

        connectionId = interception.response?.body?.connectionId;
      });
    });

    it("should redirect to connection overview page after connection set up", () => {
      newConnectionPage.isAtConnectionOverviewPage(connectionId);
    });
  });
});
