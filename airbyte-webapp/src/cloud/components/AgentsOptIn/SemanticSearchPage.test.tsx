import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { Route, Routes, useParams } from "react-router-dom";

import { TestWrapper } from "test-utils";

import { useCurrentOrganizationId } from "area/organization/utils";
import {
  useAgentsProvisioningStatusQuery,
  useListWorkspacesInOrganization,
  useOrganizationSemanticSearchEnablement,
  useSemanticSearchConnections,
} from "core/api";
import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { SemanticSearchPage } from "./SemanticSearchPage";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatusQuery: jest.fn(),
  useListWorkspacesInOrganization: jest.fn(),
  useOrganizationSemanticSearchEnablement: jest.fn(),
  useSemanticSearchConnections: jest.fn(),
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
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;

const connection = (connectionId: string, sourceName: string, destinationName: string) =>
  ({
    connectionId,
    name: `${sourceName} to ${destinationName}`,
    source: { name: sourceName },
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

    it("renders every control disabled and every status as Backfill off", () => {
      renderPage();

      ["HubSpot to Snowflake", "HubSpot to BigQuery"].forEach((name) => {
        const semanticSearchSwitch = screen.getByRole("checkbox", { name: `${name} semantic search` });
        expect(semanticSearchSwitch).toBeDisabled();
        expect(semanticSearchSwitch).not.toBeChecked();
        expect(screen.getByRole("textbox", { name: `${name} backfill date` })).toBeDisabled();
        expect(screen.getByRole("button", { name: `Start backfill for ${name}` })).toBeDisabled();
      });
      expect(screen.getAllByText("Backfill off")).toHaveLength(2);
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
