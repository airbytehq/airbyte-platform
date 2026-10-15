import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { useState } from "react";
import { Route, Routes, useParams } from "react-router-dom";

import { TestWrapper } from "test-utils";

import { useCurrentOrganizationId } from "area/organization/utils";
import {
  useAgentsProvisioningStatusQuery,
  useConnectionSemanticSearchEnablement,
  useListWorkspacesInOrganization,
  useOrganizationSemanticSearchEnablement,
  useRequestConnectionBackfill,
  useSemanticSearchConnections,
  useSourceSearchStatuses,
  useUpdateConnectionSemanticSearchEnablement,
} from "core/api";
import { FusionSearchStatusBackfillStatus, WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { Intent, useGeneratedIntent, useIntent } from "core/utils/rbac";

import { BACKFILL_STATUS_POLL_INTERVAL_MS, getDefaultBackfillStartTime } from "./connectionBackfillState";
import { SemanticSearchPage } from "./SemanticSearchPage";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatusQuery: jest.fn(),
  useConnectionSemanticSearchEnablement: jest.fn(),
  useListWorkspacesInOrganization: jest.fn(),
  useOrganizationSemanticSearchEnablement: jest.fn(),
  useRequestConnectionBackfill: jest.fn(),
  useSemanticSearchConnections: jest.fn(),
  useSourceSearchStatuses: jest.fn(),
  useUpdateConnectionSemanticSearchEnablement: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: () => true,
}));

jest.mock("core/utils/useAirbyteTheme", () => ({
  useAirbyteTheme: () => ({ theme: "airbyteThemeLight" }),
}));

jest.mock("./useShowAgentsOptIn", () => ({ useShowAgentsOptIn: jest.fn() }));

jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useGeneratedIntent: jest.fn(),
  useIntent: jest.fn(),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockUseAgentsProvisioningStatusQuery = useAgentsProvisioningStatusQuery as jest.MockedFunction<
  typeof useAgentsProvisioningStatusQuery
>;
const mockUseListWorkspacesInOrganization = useListWorkspacesInOrganization as jest.MockedFunction<
  typeof useListWorkspacesInOrganization
>;
const mockUseOrganizationSemanticSearchEnablement = useOrganizationSemanticSearchEnablement as jest.MockedFunction<
  typeof useOrganizationSemanticSearchEnablement
>;
const mockUseSemanticSearchConnections = useSemanticSearchConnections as jest.MockedFunction<
  typeof useSemanticSearchConnections
>;
const mockUseConnectionSemanticSearchEnablement = useConnectionSemanticSearchEnablement as jest.MockedFunction<
  typeof useConnectionSemanticSearchEnablement
>;
const mockUseUpdateConnectionSemanticSearchEnablement =
  useUpdateConnectionSemanticSearchEnablement as jest.MockedFunction<
    typeof useUpdateConnectionSemanticSearchEnablement
  >;
const mockUseSourceSearchStatuses = useSourceSearchStatuses as jest.MockedFunction<typeof useSourceSearchStatuses>;
const mockUseRequestConnectionBackfill = useRequestConnectionBackfill as jest.MockedFunction<
  typeof useRequestConnectionBackfill
>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;
const mockUseIntent = useIntent as jest.MockedFunction<typeof useIntent>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;

const connection = (connectionId: string, sourceName: string, destinationName: string, sourceId = "source-1") =>
  ({
    connectionId,
    name: `${sourceName} to ${destinationName}`,
    source: { name: sourceName, sourceId },
    destination: { name: destinationName },
  }) as WebBackendConnectionListItem;

const connectionsResult = (connections: WebBackendConnectionListItem[], overrides: Record<string, unknown> = {}) =>
  ({
    data: { pages: [{ connections }] },
    isLoading: false,
    isError: false,
    hasNextPage: false,
    isFetchingNextPage: false,
    fetchNextPage: jest.fn(),
    refetch: jest.fn(),
    ...overrides,
  }) as never;

const enablementResult = (enabled: boolean, overrides: Record<string, unknown> = {}) =>
  ({
    data: { connectionId: "connection", enable_indexing: enabled },
    isLoading: false,
    isError: false,
    ...overrides,
  }) as never;

const mockUpdateEnablement = (overrides: Record<string, unknown> = {}) => {
  const update = { mutateAsync: jest.fn().mockResolvedValue(undefined), isLoading: false, ...overrides };
  mockUseUpdateConnectionSemanticSearchEnablement.mockReturnValue(update as never);
  return update;
};

const backfillSource = (
  connectionId: string,
  statuses: FusionSearchStatusBackfillStatus[],
  indexedRecords = 0,
  expectedRecords = 0
) => ({
  source_id: "source-1",
  destination_id: "destination-1",
  connection_id: connectionId,
  streams: statuses.map((status, index) => ({
    name: `stream-${index}`,
    indexes: [],
    backfill: {
      status,
      indexed_records: indexedRecords,
      expected_total_records: expectedRecords,
      collected_records: 0,
      backfill_start_time: null,
      started_at: null,
      completed_at: null,
    },
  })),
});

const searchStatusResult = (sources: unknown[] = [], overrides: Record<string, unknown> = {}) => ({
  data: { sources },
  isError: false,
  dataUpdatedAt: 1,
  refetch: jest.fn(),
  ...overrides,
});

const mockSearchStatuses = (resultForSource: (sourceId: string) => ReturnType<typeof searchStatusResult>) =>
  mockUseSourceSearchStatuses.mockImplementation((sourceIds) => sourceIds.map(resultForSource) as never);

const mockRequestBackfill = (overrides: Record<string, unknown> = {}) => {
  const request = {
    mutateAsync: jest.fn().mockResolvedValue({ streams: [{ stream_name: "stream-0", outcome: "requested" }] }),
    isLoading: false,
    ...overrides,
  };
  mockUseRequestConnectionBackfill.mockReturnValue(request as never);
  return request;
};

const workspace = (index: number) => ({ workspaceId: `workspace-${index}`, name: `Workspace ${index}` });

const workspacesResult = (workspaces: Array<ReturnType<typeof workspace>>, overrides: Record<string, unknown> = {}) =>
  ({
    data: { pages: [{ workspaces }] },
    isLoading: false,
    isError: false,
    hasNextPage: false,
    isFetchingNextPage: false,
    fetchNextPage: jest.fn(),
    ...overrides,
  }) as never;

const NewConnectionPage: React.FC = () => {
  const { workspaceId } = useParams();
  return <div>New connection in {workspaceId}</div>;
};

const renderPage = () =>
  render(
    <TestWrapper route="/organization/test-org-123/context-layer/semantic-search">
      <Routes>
        <Route path="/organization/:organizationId/context-layer/semantic-search" element={<SemanticSearchPage />} />
        <Route path="/organization/:organizationId/context-layer" element={<div>Context layer settings page</div>} />
        <Route path="/workspaces/:workspaceId/connections/new-connection" element={<NewConnectionPage />} />
      </Routes>
    </TestWrapper>
  );

describe("SemanticSearchPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    window.localStorage.clear();
    mockUseCurrentOrganizationId.mockReturnValue("test-org-123");
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({
      data: { is_enrolled: true, external_cloud_eligible: true },
      isInitialLoading: false,
      isError: false,
      refetch: jest.fn(),
    } as never);
    mockUseOrganizationSemanticSearchEnablement.mockReturnValue({
      data: { organizationId: "test-org-123", semanticSearchEnabled: true },
      isInitialLoading: false,
      isError: false,
      refetch: jest.fn(),
    } as never);
    mockUseListWorkspacesInOrganization.mockReturnValue(workspacesResult([workspace(1)]));
    mockUseSemanticSearchConnections.mockReturnValue(connectionsResult([]));
    mockUseGeneratedIntent.mockReturnValue(true);
    mockUseIntent.mockReturnValue(true);
    mockUseConnectionSemanticSearchEnablement.mockReturnValue(enablementResult(false));
    mockUpdateEnablement();
    mockSearchStatuses(() => searchStatusResult());
    mockRequestBackfill();
  });

  describe("gating", () => {
    it.each([
      {
        name: "the organization is not eligible",
        status: { is_enrolled: false, external_cloud_eligible: false },
        showAgentsOptIn: true,
      },
      {
        name: "the opt-in is hidden for an unenrolled organization",
        status: { is_enrolled: false, external_cloud_eligible: true },
        showAgentsOptIn: false,
      },
    ])("shows the unavailable state when $name", ({ status, showAgentsOptIn }) => {
      mockUseShowAgentsOptIn.mockReturnValue(showAgentsOptIn);
      mockUseAgentsProvisioningStatusQuery.mockReturnValue({ data: status, isError: false } as never);

      renderPage();

      expect(screen.getByTestId("context-layer-unavailable")).toBeInTheDocument();
      expect(screen.queryByRole("heading", { name: "Semantic search" })).not.toBeInTheDocument();
      expect(mockUseListWorkspacesInOrganization).not.toHaveBeenCalled();
    });

    it("does not read the semantic search setting when the page is unavailable", () => {
      mockUseAgentsProvisioningStatusQuery.mockReturnValue({
        data: { is_enrolled: false, external_cloud_eligible: false },
        isError: false,
      } as never);

      renderPage();

      expect(screen.getByTestId("context-layer-unavailable")).toBeInTheDocument();
      expect(mockUseOrganizationSemanticSearchEnablement).not.toHaveBeenCalled();
    });

    it("shows the unavailable state when the provisioning status is empty", () => {
      mockUseAgentsProvisioningStatusQuery.mockReturnValue({ data: null, isError: false } as never);

      renderPage();

      expect(screen.getByTestId("context-layer-unavailable")).toBeInTheDocument();
    });

    it("waits for the provisioning status", () => {
      mockUseAgentsProvisioningStatusQuery.mockReturnValue({ isInitialLoading: true } as never);

      renderPage();

      expect(screen.queryByRole("heading", { name: "Semantic search" })).not.toBeInTheDocument();
      expect(screen.queryByTestId("context-layer-unavailable")).not.toBeInTheDocument();
    });

    it("shows a load error with retry when the provisioning status fails", () => {
      const refetch = jest.fn();
      mockUseAgentsProvisioningStatusQuery.mockReturnValue({ isError: true, refetch } as never);

      renderPage();

      expect(screen.getByTestId("context-layer-load-error")).toBeInTheDocument();
      fireEvent.click(screen.getByRole("button", { name: "Try again" }));
      expect(refetch).toHaveBeenCalledTimes(1);
    });
  });

  describe("organization semantic search setting", () => {
    it.each([["off", { organizationId: "test-org-123", semanticSearchEnabled: false }]])(
      "asks to enable semantic search when the setting is %s",
      (_name, data) => {
        mockUseOrganizationSemanticSearchEnablement.mockReturnValue({
          data,
          isInitialLoading: false,
          isError: false,
        } as never);

        renderPage();

        expect(screen.getByRole("heading", { name: "Semantic search" })).toBeInTheDocument();
        expect(
          screen.getByText("Choose which connections populate your context layer for semantic search.")
        ).toBeInTheDocument();
        expect(screen.getByText("Enable semantic search")).toBeInTheDocument();
        expect(screen.getByText("Before you can control semantic search, you need to enable it.")).toBeInTheDocument();
        expect(screen.queryByRole("table")).not.toBeInTheDocument();
        expect(mockUseSemanticSearchConnections).not.toHaveBeenCalled();
        expect(mockUseListWorkspacesInOrganization).not.toHaveBeenCalled();
      }
    );

    it("sends Go to settings to the Context layer settings page", () => {
      mockUseOrganizationSemanticSearchEnablement.mockReturnValue({
        data: { organizationId: "test-org-123", semanticSearchEnabled: false },
        isInitialLoading: false,
        isError: false,
      } as never);

      renderPage();

      fireEvent.click(screen.getByRole("button", { name: "Go to settings" }));
      expect(screen.getByText("Context layer settings page")).toBeInTheDocument();
    });

    it("waits for the setting", () => {
      mockUseOrganizationSemanticSearchEnablement.mockReturnValue({ isInitialLoading: true, isError: false } as never);

      renderPage();

      expect(screen.queryByRole("heading", { name: "Semantic search" })).not.toBeInTheDocument();
      expect(screen.queryByText("Enable semantic search")).not.toBeInTheDocument();
    });

    it("shows a load error with retry when the setting cannot be read", () => {
      const refetch = jest.fn();
      mockUseOrganizationSemanticSearchEnablement.mockReturnValue({
        isInitialLoading: false,
        isError: true,
        refetch,
      } as never);

      renderPage();

      expect(screen.getByTestId("context-layer-load-error")).toBeInTheDocument();
      expect(screen.queryByText("Enable semantic search")).not.toBeInTheDocument();
      fireEvent.click(screen.getByRole("button", { name: "Try again" }));
      expect(refetch).toHaveBeenCalledTimes(1);
    });
  });

  describe("workspaces", () => {
    it.each([
      ["while workspaces load", { data: undefined, isLoading: true }, "Loading workspaces..."],
      ["when the organization has no workspaces", {}, "No workspaces found in this organization."],
    ])("shows a message %s", (_name, overrides, message) => {
      mockUseListWorkspacesInOrganization.mockReturnValue(workspacesResult([], overrides));

      renderPage();

      expect(screen.getByText(message)).toBeInTheDocument();
    });

    it("shows a retry when the first workspace page fails", () => {
      const refetch = jest.fn();
      mockUseListWorkspacesInOrganization.mockReturnValue(
        workspacesResult([], { data: undefined, isError: true, refetch })
      );

      renderPage();

      expect(screen.getByText("Unable to load workspaces. Please try again.")).toBeInTheDocument();
      fireEvent.click(screen.getByRole("button", { name: "Try again" }));
      expect(refetch).toHaveBeenCalledTimes(1);
    });

    it("groups connections by workspace and hides workspaces without eligible connections", () => {
      mockUseListWorkspacesInOrganization.mockReturnValue(workspacesResult([workspace(1), workspace(2), workspace(3)]));
      const connectionsByWorkspace: Record<string, WebBackendConnectionListItem[]> = {
        "workspace-1": [
          connection("connection-1", "HubSpot", "Snowflake"),
          connection("connection-2", "HubSpot", "BigQuery"),
        ],
        "workspace-3": [connection("connection-3", "Gong", "Snowflake")],
      };
      mockUseSemanticSearchConnections.mockImplementation((workspaceId) =>
        connectionsResult(connectionsByWorkspace[workspaceId] ?? [])
      );

      renderPage();

      const firstWorkspace = screen.getByTestId("semantic-search-workspace-workspace-1");
      expect(within(firstWorkspace).getByRole("heading", { name: "Workspace 1" })).toBeVisible();
      expect(within(firstWorkspace).getAllByRole("row")).toHaveLength(3);
      expect(screen.getByTestId("semantic-search-workspace-workspace-2")).not.toBeVisible();
      expect(screen.queryByRole("heading", { name: "Workspace 2" })).not.toBeInTheDocument();
      expect(
        within(screen.getByTestId("semantic-search-workspace-workspace-3")).getByRole("heading", {
          name: "Workspace 3",
        })
      ).toBeVisible();
      expect(screen.getAllByRole("table")).toHaveLength(2);
      expect(screen.queryByText("No connections yet")).not.toBeInTheDocument();
    });

    it("shows a retry after a workspace page fails", async () => {
      const fetchNextPage = jest.fn().mockResolvedValue({ isError: true });
      mockUseListWorkspacesInOrganization.mockReturnValue(
        workspacesResult([workspace(1)], { hasNextPage: true, isError: true, fetchNextPage })
      );

      renderPage();

      expect(screen.getByText("Some workspaces could not be loaded.")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Load more workspaces" })).not.toBeInTheDocument();
      expect(screen.queryByText("No connections yet")).not.toBeInTheDocument();
      fireEvent.click(screen.getByRole("button", { name: "Try again" }));
      await waitFor(() => expect(fetchNextPage).toHaveBeenCalledTimes(1));
    });
  });

  describe("no connections", () => {
    beforeEach(() => {
      mockUseListWorkspacesInOrganization.mockReturnValue(workspacesResult([workspace(1), workspace(2)]));
    });

    it("shows the empty state and hides every workspace", () => {
      renderPage();

      expect(screen.getByText("No connections yet")).toBeVisible();
      expect(
        screen.getByText("Add connections to Airbyte to start populating your context layer for semantic search.")
      ).toBeVisible();
      expect(screen.getByTestId("semantic-search-workspace-workspace-1")).not.toBeVisible();
      expect(screen.getByTestId("semantic-search-workspace-workspace-2")).not.toBeVisible();
      expect(screen.queryByRole("table")).not.toBeInTheDocument();
    });

    it.each([
      ["the last-visited workspace when allowed", "workspace-2", ["workspace-1", "workspace-2"], "workspace-2"],
      [
        "the first allowed workspace when the last-visited one is not allowed",
        "workspace-1",
        ["workspace-2"],
        "workspace-2",
      ],
    ])("adds the first connection in %s", (_name, storedWorkspaceId, allowedWorkspaceIds, expectedWorkspaceId) => {
      if (storedWorkspaceId) {
        window.localStorage.setItem(
          "airbyte_organization-workspace-map",
          JSON.stringify({ "test-org-123": storedWorkspaceId })
        );
      }
      mockUseGeneratedIntent.mockImplementation(
        (intent, meta) =>
          intent === Intent.CreateOrEditConnection && allowedWorkspaceIds.includes(meta?.workspaceId ?? "")
      );

      renderPage();

      fireEvent.click(screen.getByRole("button", { name: "Add your first connection" }));
      expect(screen.getByText(`New connection in ${expectedWorkspaceId}`)).toBeInTheDocument();
    });

    it("hides the add button when no workspace allows creating connections", () => {
      mockUseGeneratedIntent.mockReturnValue(false);

      renderPage();

      expect(screen.getByText("No connections yet")).toBeVisible();
      expect(screen.queryByRole("button", { name: "Add your first connection" })).not.toBeInTheDocument();
    });

    it("waits for every workspace before showing the empty state", () => {
      mockUseSemanticSearchConnections.mockImplementation((workspaceId) =>
        workspaceId === "workspace-2"
          ? connectionsResult([], { data: undefined, isLoading: true })
          : connectionsResult([])
      );

      renderPage();

      expect(screen.queryByText("No connections yet")).not.toBeInTheDocument();
      expect(screen.getByText("Loading connections...")).toBeVisible();
    });

    it("waits for more workspaces before showing the empty state", () => {
      mockUseListWorkspacesInOrganization.mockReturnValue(workspacesResult([workspace(1)], { hasNextPage: true }));

      renderPage();

      expect(screen.queryByText("No connections yet")).not.toBeInTheDocument();
      expect(screen.getByText("No eligible connections in the loaded workspaces.")).toBeVisible();
      expect(screen.getByRole("button", { name: "Load more workspaces" })).toBeInTheDocument();
    });
  });

  describe("connection rows", () => {
    beforeEach(() => {
      mockUseSemanticSearchConnections.mockReturnValue(
        connectionsResult([
          connection("connection-1", "HubSpot", "Snowflake"),
          connection("connection-2", "HubSpot", "BigQuery"),
        ])
      );
    });

    it("renders the saved semantic search value and enables backfill only where semantic search is on", () => {
      mockUseConnectionSemanticSearchEnablement.mockImplementation((connectionId) =>
        enablementResult(connectionId === "connection-1")
      );

      renderPage();

      expect(screen.getByRole("checkbox", { name: "HubSpot to Snowflake semantic search" })).toBeChecked();
      expect(screen.getByRole("checkbox", { name: "HubSpot to BigQuery semantic search" })).not.toBeChecked();
      ["HubSpot to Snowflake", "HubSpot to BigQuery"].forEach((name) => {
        expect(screen.getByRole("checkbox", { name: `${name} semantic search` })).toBeEnabled();
      });
      expect(screen.getByRole("textbox", { name: "HubSpot to Snowflake backfill date" })).toBeEnabled();
      expect(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" })).toBeEnabled();
      expect(screen.getByRole("textbox", { name: "HubSpot to BigQuery backfill date" })).toBeDisabled();
      expect(screen.getByRole("button", { name: "Start backfill for HubSpot to BigQuery" })).toBeDisabled();
      expect(screen.getAllByText("Backfill off")).toHaveLength(2);
    });

    it("turns semantic search on without a confirmation", async () => {
      const update = mockUpdateEnablement();

      renderPage();
      fireEvent.click(screen.getByRole("checkbox", { name: "HubSpot to Snowflake semantic search" }));

      await waitFor(() => expect(update.mutateAsync).toHaveBeenCalledWith({ enabled: true, expectedEnabled: false }));
      expect(screen.queryByText("Turn off semantic search")).not.toBeInTheDocument();
    });

    it("turns semantic search off after the confirmation", async () => {
      mockUseConnectionSemanticSearchEnablement.mockReturnValue(enablementResult(true));
      const update = mockUpdateEnablement();

      renderPage();
      fireEvent.click(screen.getByRole("checkbox", { name: "HubSpot to Snowflake semantic search" }));

      expect(await screen.findByText("Turn off semantic search")).toBeInTheDocument();
      expect(update.mutateAsync).not.toHaveBeenCalled();
      fireEvent.click(screen.getByRole("button", { name: "Delete semantic index" }));

      await waitFor(() => expect(update.mutateAsync).toHaveBeenCalledWith({ enabled: false, expectedEnabled: true }));
      await waitFor(() => expect(screen.queryByText("Turn off semantic search")).not.toBeInTheDocument());
    });

    it("shows an error and closes the confirmation when turning off fails", async () => {
      mockUseConnectionSemanticSearchEnablement.mockReturnValue(enablementResult(true));
      mockUpdateEnablement({ mutateAsync: jest.fn().mockRejectedValue(new Error("conflict")) });

      renderPage();
      fireEvent.click(screen.getByRole("checkbox", { name: "HubSpot to Snowflake semantic search" }));
      fireEvent.click(await screen.findByRole("button", { name: "Delete semantic index" }));

      expect(
        await screen.findByText("Semantic search setting could not be updated. Please try again.")
      ).toBeInTheDocument();
      expect(screen.queryByText("Turn off semantic search")).not.toBeInTheDocument();
    });

    it.each([
      ["the viewer cannot edit semantic search", () => mockUseIntent.mockReturnValue(false)],
      [
        "the saved value is loading",
        () =>
          mockUseConnectionSemanticSearchEnablement.mockReturnValue(
            enablementResult(false, { data: undefined, isLoading: true })
          ),
      ],
      ["an update is saving", () => mockUpdateEnablement({ isLoading: true })],
    ])("disables the switch when %s", (_, arrange) => {
      arrange();

      renderPage();

      screen
        .getAllByRole("checkbox", { name: /semantic search$/ })
        .forEach((control) => expect(control).toBeDisabled());
    });

    it("checks the semantic search edit permission for the workspace of the row", () => {
      // Source and destination editors can edit connections, but the enablement endpoints require a workspace editor.
      mockUseGeneratedIntent.mockReturnValue(true);
      mockUseIntent.mockReturnValue(false);

      renderPage();

      expect(mockUseIntent).toHaveBeenCalledWith("EditConnectionSemanticSearch", { workspaceId: "workspace-1" });
      const control = screen.getByRole("checkbox", { name: "HubSpot to Snowflake semantic search" });
      expect(control).toBeDisabled();
      fireEvent.mouseOver(control);
      expect(screen.getByText("You need edit permission for this workspace to change semantic search.")).toBeVisible();
    });

    it("does not gate the switch on the connection edit intent", () => {
      mockUseGeneratedIntent.mockReturnValue(false);
      mockUseIntent.mockReturnValue(true);

      renderPage();

      expect(screen.getByRole("checkbox", { name: "HubSpot to Snowflake semantic search" })).toBeEnabled();
    });

    it("shows a retry in place of the switch when the saved value cannot be read", () => {
      const refetch = jest.fn();
      mockUseConnectionSemanticSearchEnablement.mockReturnValue(
        enablementResult(false, { data: undefined, isError: true, refetch })
      );

      renderPage();

      expect(screen.queryByRole("checkbox", { name: /semantic search$/ })).not.toBeInTheDocument();
      fireEvent.click(screen.getAllByRole("button", { name: "Try again" })[0]);
      expect(refetch).toHaveBeenCalledTimes(1);
    });

    it("loads more connections in a workspace on request", () => {
      const fetchNextPage = jest.fn();
      mockUseSemanticSearchConnections.mockReturnValue(
        connectionsResult([connection("connection-1", "HubSpot", "Snowflake")], { hasNextPage: true, fetchNextPage })
      );

      renderPage();

      fireEvent.click(screen.getByRole("button", { name: "Load more connections" }));
      expect(fetchNextPage).toHaveBeenCalledTimes(1);
    });
  });

  describe("backfill controls", () => {
    const snowflakeRow = () => screen.getByRole("row", { name: /HubSpot Snowflake/ });

    beforeEach(() => {
      mockUseSemanticSearchConnections.mockReturnValue(
        connectionsResult([
          connection("connection-1", "HubSpot", "Snowflake"),
          connection("connection-2", "HubSpot", "BigQuery"),
        ])
      );
      mockUseConnectionSemanticSearchEnablement.mockReturnValue(enablementResult(true));
    });

    it("reads the search status once for a source that several rows share", () => {
      renderPage();

      expect(mockUseSourceSearchStatuses).toHaveBeenCalled();
      mockUseSourceSearchStatuses.mock.calls.forEach(([sourceIds]) => expect(sourceIds).toEqual(["source-1"]));
    });

    it.each([
      [
        "semantic search is off for the connection",
        () => mockUseConnectionSemanticSearchEnablement.mockReturnValue(enablementResult(false)),
      ],
      ["the viewer cannot edit semantic search", () => mockUseIntent.mockReturnValue(false)],
      [
        "the semantic search value is loading",
        () =>
          mockUseConnectionSemanticSearchEnablement.mockReturnValue(
            enablementResult(false, { data: undefined, isLoading: true })
          ),
      ],
    ])("disables the backfill date and button when %s", (_name, arrange) => {
      arrange();

      renderPage();

      expect(screen.getByRole("textbox", { name: "HubSpot to Snowflake backfill date" })).toBeDisabled();
      expect(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" })).toBeDisabled();
    });

    it.each([
      ["Backfill off", "no backfill", [], true, true],
      ["Backfill off", "a cancelled backfill", [backfillSource("connection-1", ["cancelled"])], true, true],
      [
        "Running",
        "a running backfill",
        [backfillSource("connection-1", ["running", "complete"], 75, 100)],
        false,
        false,
      ],
      ["Complete", "a complete backfill", [backfillSource("connection-1", ["complete", "complete"])], true, true],
      [
        "Running",
        "a stream running next to a failed one",
        [backfillSource("connection-1", ["running", "failed"])],
        false,
        false,
      ],
      ["Failed", "a failed stream", [backfillSource("connection-1", ["complete", "failed"])], true, true],
    ])("shows %s for %s and sets the date and button from it", (label, _name, sources, dateEnabled, buttonEnabled) => {
      mockSearchStatuses(() => searchStatusResult(sources));

      renderPage();

      expect(within(snowflakeRow()).getByText(label)).toBeInTheDocument();
      expect(within(snowflakeRow()).getByRole("textbox", { name: /backfill date/ })).toHaveProperty(
        "disabled",
        !dateEnabled
      );
      expect(within(snowflakeRow()).getByRole("button", { name: /^Start backfill/ })).toHaveProperty(
        "disabled",
        !buttonEnabled
      );
    });

    it("shows the progress of a running backfill", () => {
      mockSearchStatuses(() => searchStatusResult([backfillSource("connection-1", ["running"], 75, 100)]));

      renderPage();

      expect(within(snowflakeRow()).getByText("75%")).toBeInTheDocument();
    });

    it("hides the progress until the backfiller has counted the records", () => {
      mockSearchStatuses(() => searchStatusResult([backfillSource("connection-1", ["starting"], 0, 0)]));

      renderPage();

      expect(within(snowflakeRow()).getByText("Running")).toBeInTheDocument();
      expect(within(snowflakeRow()).queryByText(/%$/)).not.toBeInTheDocument();
    });

    it("explains how to retry a failed backfill", () => {
      mockSearchStatuses(() => searchStatusResult([backfillSource("connection-1", ["failed"])]));

      renderPage();

      fireEvent.mouseOver(within(snowflakeRow()).getByText("Failed"));
      expect(screen.getByText("To try again, click Start backfill.")).toBeVisible();
    });

    it("keeps the date and button disabled while the status loads", () => {
      mockSearchStatuses(() => searchStatusResult([], { data: undefined }));

      renderPage();

      expect(screen.getByRole("textbox", { name: "HubSpot to Snowflake backfill date" })).toBeDisabled();
      expect(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" })).toBeDisabled();
    });

    it("shows an unknown status and locks the controls when the first read of a source fails, and keeps the other rows", () => {
      mockUseSemanticSearchConnections.mockReturnValue(
        connectionsResult([
          connection("connection-1", "HubSpot", "Snowflake", "source-1"),
          connection("connection-2", "Gong", "BigQuery", "source-2"),
        ])
      );
      mockSearchStatuses((sourceId) =>
        sourceId === "source-2"
          ? searchStatusResult([], { data: undefined, isError: true })
          : searchStatusResult([backfillSource("connection-1", ["complete"])])
      );

      renderPage();

      expect(within(snowflakeRow()).getByText("Complete")).toBeInTheDocument();
      const failedRow = screen.getByRole("row", { name: /Gong BigQuery/ });
      expect(within(failedRow).getByRole("button", { name: "Start backfill for Gong to BigQuery" })).toBeDisabled();
      expect(within(failedRow).getByRole("textbox", { name: "Gong to BigQuery backfill date" })).toBeDisabled();
      fireEvent.mouseOver(within(failedRow).getByText("Unknown"));
      expect(
        screen.getByText("We encountered an issue fetching the backfill status. We will keep trying in the background.")
      ).toBeVisible();
    });

    it.each([
      ["Running", ["running"]],
      ["Complete", ["complete"]],
      ["Failed", ["failed"]],
    ] as const)("keeps showing %s after a later read fails, and locks the controls", (label, statuses) => {
      mockSearchStatuses(() => searchStatusResult([backfillSource("connection-1", [...statuses])], { isError: true }));

      renderPage();

      expect(within(snowflakeRow()).getByText(label)).toBeInTheDocument();
      expect(within(snowflakeRow()).getByRole("textbox", { name: /backfill date/ })).toBeDisabled();
      expect(within(snowflakeRow()).getByRole("button", { name: /^Start backfill/ })).toBeDisabled();
    });

    it.each([
      ["a read failed", undefined, "error", BACKFILL_STATUS_POLL_INTERVAL_MS],
      ["a backfill is running", ["running"], "success", BACKFILL_STATUS_POLL_INTERVAL_MS],
      ["a stream runs next to a failed one", ["running", "failed"], "success", BACKFILL_STATUS_POLL_INTERVAL_MS],
      ["no backfill is running", ["complete"], "success", false],
    ] as const)("polls the status while %s", (_name, statuses, queryStatus, expected) => {
      renderPage();

      const [, { refetchInterval }] = mockUseSourceSearchStatuses.mock.calls[0];
      const data = statuses ? { sources: [backfillSource("connection-1", [...statuses])] } : undefined;
      expect(refetchInterval(data as never, { state: { status: queryStatus } } as never)).toBe(expected);
    });

    it("sends the backfill date with the request", async () => {
      const request = mockRequestBackfill();

      renderPage();
      fireEvent.change(screen.getByRole("textbox", { name: "HubSpot to Snowflake backfill date" }), {
        target: { value: "2025-09-19T00:00:00Z" },
      });
      fireEvent.click(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" }));

      await waitFor(() => expect(request.mutateAsync).toHaveBeenCalledWith({ startTime: "2025-09-19T00:00:00Z" }));
      expect(mockUseRequestConnectionBackfill).toHaveBeenCalledWith("connection-1", "source-1");
    });

    it("starts a new backfill for a connection whose backfill is complete", async () => {
      const request = mockRequestBackfill();
      mockSearchStatuses(() => searchStatusResult([backfillSource("connection-1", ["complete"])]));

      renderPage();
      fireEvent.change(screen.getByRole("textbox", { name: "HubSpot to Snowflake backfill date" }), {
        target: { value: "2024-01-01T00:00:00Z" },
      });
      fireEvent.click(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" }));

      await waitFor(() => expect(request.mutateAsync).toHaveBeenCalledWith({ startTime: "2024-01-01T00:00:00Z" }));
    });

    it("shows and sends the default backfill date when none is entered", async () => {
      const request = mockRequestBackfill();
      const defaultStartTime = getDefaultBackfillStartTime();

      renderPage();
      expect(screen.getByRole("textbox", { name: "HubSpot to Snowflake backfill date" })).toHaveAttribute(
        "placeholder",
        defaultStartTime
      );
      fireEvent.click(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" }));

      await waitFor(() => expect(request.mutateAsync).toHaveBeenCalledWith({ startTime: defaultStartTime }));
    });

    it("sends the default backfill date after the entered date is cleared", async () => {
      const request = mockRequestBackfill();
      const dateInput = () => screen.getByRole("textbox", { name: "HubSpot to Snowflake backfill date" });

      renderPage();
      fireEvent.change(dateInput(), { target: { value: "2025-09-19T00:00:00Z" } });
      fireEvent.change(dateInput(), { target: { value: "" } });
      fireEvent.click(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" }));

      await waitFor(() =>
        expect(request.mutateAsync).toHaveBeenCalledWith({ startTime: getDefaultBackfillStartTime() })
      );
    });

    it("shows a requested backfill as running until the status is read again", async () => {
      const { rerender } = renderPage();
      fireEvent.click(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" }));

      expect(await within(snowflakeRow()).findByText("Running")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" })).toBeDisabled();

      mockSearchStatuses(() =>
        searchStatusResult([backfillSource("connection-1", ["complete"])], { dataUpdatedAt: 2 })
      );
      rerender(
        <TestWrapper route="/organization/test-org-123/context-layer/semantic-search">
          <Routes>
            <Route
              path="/organization/:organizationId/context-layer/semantic-search"
              element={<SemanticSearchPage />}
            />
          </Routes>
        </TestWrapper>
      );

      expect(await within(snowflakeRow()).findByText("Complete")).toBeInTheDocument();
    });

    it("keeps a requested backfill running when the status is read again while the request is pending", async () => {
      let resolveRequest: (response: unknown) => void = () => undefined;
      mockRequestBackfill({
        mutateAsync: jest.fn().mockReturnValue(
          new Promise((resolve) => {
            resolveRequest = resolve;
          })
        ),
      });
      // A newer read re-renders the provider without re-rendering the page, as a poll result does.
      let setStatusRead: (read: ReturnType<typeof searchStatusResult>) => void = () => undefined;
      const useStatusReads = (sourceIds: string[]) => {
        const [read, setRead] = useState(searchStatusResult([backfillSource("connection-1", ["complete"])]));
        setStatusRead = setRead;
        return sourceIds.map(() => read) as never;
      };
      mockUseSourceSearchStatuses.mockImplementation(useStatusReads);

      renderPage();
      fireEvent.click(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" }));
      act(() =>
        setStatusRead(searchStatusResult([backfillSource("connection-1", ["complete"])], { dataUpdatedAt: 2 }))
      );
      await act(async () => resolveRequest({ streams: [{ stream_name: "stream-0", outcome: "requested" }] }));

      expect(within(snowflakeRow()).getByText("Running")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" })).toBeDisabled();
    });

    it.each([
      ["Running", "another stream started", "requested", false],
      ["Failed", "every stream failed", "failed", true],
    ] as const)("shows %s when a stream fails to start and %s", async (label, _name, otherOutcome, buttonEnabled) => {
      mockRequestBackfill({
        mutateAsync: jest.fn().mockResolvedValue({
          streams: [
            { stream_name: "stream-0", outcome: otherOutcome },
            { stream_name: "stream-1", outcome: "failed" },
          ],
        }),
      });

      renderPage();
      fireEvent.click(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" }));

      expect(await within(snowflakeRow()).findByText(label)).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" })).toHaveProperty(
        "disabled",
        !buttonEnabled
      );
    });

    it.each([
      ["free text", "foo"],
      ["a date without a time zone", "2025-10-01T00:00:00"],
      ["a future date", "2999-01-01T00:00:00Z"],
    ])("rejects %s as the backfill date", (_name, value) => {
      const request = mockRequestBackfill();

      renderPage();
      fireEvent.change(screen.getByRole("textbox", { name: "HubSpot to Snowflake backfill date" }), {
        target: { value },
      });
      fireEvent.click(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" }));

      expect(
        within(snowflakeRow()).getByText("Enter a past date and time in UTC, for example 2025-10-15T00:00:00Z.")
      ).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" })).toBeDisabled();
      expect(request.mutateAsync).not.toHaveBeenCalled();
    });

    it("shows an error when the backfill cannot be started", async () => {
      mockRequestBackfill({ mutateAsync: jest.fn().mockRejectedValue(new Error("conflict")) });

      renderPage();
      fireEvent.click(screen.getByRole("button", { name: "Start backfill for HubSpot to Snowflake" }));

      expect(await screen.findByText("The backfill could not be started. Please try again.")).toBeInTheDocument();
      expect(within(snowflakeRow()).getByText("Backfill off")).toBeInTheDocument();
    });
  });

  describe("workspace connection errors", () => {
    it("shows the workspace error with retry and does not treat it as empty", () => {
      const refetch = jest.fn();
      mockUseSemanticSearchConnections.mockReturnValue(
        connectionsResult([], { data: undefined, isError: true, refetch })
      );

      renderPage();

      const workspaceGroup = screen.getByTestId("semantic-search-workspace-workspace-1");
      expect(workspaceGroup).toBeVisible();
      expect(within(workspaceGroup).getByRole("heading", { name: "Workspace 1" })).toBeInTheDocument();
      expect(within(workspaceGroup).getByText("Unable to load connections. Please try again.")).toBeInTheDocument();
      expect(screen.queryByText("No connections yet")).not.toBeInTheDocument();
      expect(screen.queryByText("Loading connections...")).not.toBeInTheDocument();
      fireEvent.click(within(workspaceGroup).getByRole("button", { name: "Try again" }));
      expect(refetch).toHaveBeenCalledTimes(1);
    });

    it.each([
      ["Load more connections", true, "fetchNextPage"],
      ["Try again", false, "refetch"],
    ])("keeps loaded rows and retries a failed page with %s", (buttonName, hasNextPage, retryProperty) => {
      const retry = jest.fn();
      mockUseSemanticSearchConnections.mockReturnValue(
        connectionsResult([connection("connection-1", "HubSpot", "Snowflake")], {
          isError: true,
          hasNextPage,
          [retryProperty]: retry,
        })
      );

      renderPage();

      expect(screen.getByText("Unable to load connections. Please try again.")).toBeInTheDocument();
      expect(screen.getByRole("checkbox", { name: "HubSpot to Snowflake semantic search" })).toBeInTheDocument();
      expect(screen.getAllByRole("button", { name: /^(Load more connections|Try again)$/ })).toHaveLength(1);
      fireEvent.click(screen.getByRole("button", { name: buttonName }));
      expect(retry).toHaveBeenCalledTimes(1);
    });

    it("shows a ready workspace while another workspace is still loading", () => {
      mockUseListWorkspacesInOrganization.mockReturnValue(workspacesResult([workspace(1), workspace(2)]));
      mockUseSemanticSearchConnections.mockImplementation((workspaceId) =>
        workspaceId === "workspace-1"
          ? connectionsResult([connection("connection-1", "HubSpot", "Snowflake")])
          : connectionsResult([], { data: undefined, isLoading: true })
      );

      renderPage();

      expect(screen.getByRole("heading", { name: "Workspace 1" })).toBeVisible();
      expect(screen.getByText("Loading connections...")).toBeVisible();
      expect(screen.queryByText("No connections yet")).not.toBeInTheDocument();
    });
  });
});
