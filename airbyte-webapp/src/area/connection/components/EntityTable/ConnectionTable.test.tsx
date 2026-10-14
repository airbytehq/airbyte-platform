import { screen, within } from "@testing-library/react";

import { render } from "test-utils";

import { useConnectionSemanticSearchAvailability } from "cloud/components/AgentsOptIn";
import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

import { useOrganizationSemanticSearchState } from "./components/SemanticSearchCell";
import ConnectionTable from "./ConnectionTable";
import { ConnectionTableDataItem, Status } from "./types";

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceLink: () => (link: string) => `/workspaces/workspace-1${link}`,
}));
jest.mock("cloud/components/AgentsOptIn", () => ({ useConnectionSemanticSearchAvailability: jest.fn() }));
jest.mock("./components/SemanticSearchCell", () => ({
  useOrganizationSemanticSearchState: jest.fn(),
  SemanticSearchCell: ({
    connection,
    organizationSemanticSearch,
  }: {
    connection: WebBackendConnectionListItem;
    organizationSemanticSearch: string;
  }) => <span data-testid={`semantic-search-${connection.connectionId}`}>{organizationSemanticSearch}</span>,
}));
jest.mock("./components/ConnectorName", () => ({ ConnectorName: () => null }));
jest.mock("./components/EntityWarningsCell", () => ({ EntityWarningsCell: () => null }));
jest.mock("./components/FrequencyCell", () => ({ FrequencyCell: () => null }));
jest.mock("./components/StateSwitchCell", () => ({ StateSwitchCell: () => null }));
jest.mock("./components/TagsCell", () => ({ TagsCell: () => null }));

const mockAvailability = useConnectionSemanticSearchAvailability as jest.MockedFunction<
  typeof useConnectionSemanticSearchAvailability
>;
const mockOrganizationSemanticSearchState = useOrganizationSemanticSearchState as jest.MockedFunction<
  typeof useOrganizationSemanticSearchState
>;

const dataItem = {
  connectionId: "connection-1",
  name: "Linear → Snowflake",
  entityName: "Linear",
  connectorName: "Snowflake",
  enabled: true,
  lastSync: undefined,
  lastSyncStatus: Status.ACTIVE,
  connection: {
    connectionId: "connection-1",
    source: { sourceName: "Linear" },
    destination: { destinationName: "Snowflake" },
  },
} as unknown as ConnectionTableDataItem;

const columnHeaders = () => screen.getAllByRole("columnheader").map((header) => header.textContent);
const nameCell = () => screen.getAllByRole("cell")[0];

describe("ConnectionTable", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockAvailability.mockReturnValue("available");
    mockOrganizationSemanticSearchState.mockReturnValue("on");
  });

  it("adds the Sync status and Semantic search columns before Enabled on the Connections list", async () => {
    await render(<ConnectionTable data={[dataItem]} entity="connection" isConnectionsListPage />);

    expect(columnHeaders()).toEqual([
      "Name",
      "Source name",
      "Destination name",
      "Frequency",
      "Tags",
      "Last Sync",
      "Sync status",
      "Semantic search",
      "Enabled",
      "",
    ]);
    expect(mockOrganizationSemanticSearchState).toHaveBeenCalledWith({ enabled: true });
    expect(screen.getByTestId("semantic-search-connection-1")).toHaveTextContent("on");
  });

  it("shows the sync status icon in the Sync status cell, not in the Name cell", async () => {
    await render(<ConnectionTable data={[dataItem]} entity="connection" isConnectionsListPage />);

    expect(within(nameCell()).getByText("Linear → Snowflake")).toBeInTheDocument();
    expect(within(nameCell()).queryByTitle("Succeeded sync")).not.toBeInTheDocument();
    expect(within(screen.getAllByRole("cell")[6]).getByTitle("Succeeded sync")).toBeInTheDocument();
  });

  it("keeps the Sync status column but not the Semantic search column when semantic search is not available", async () => {
    mockAvailability.mockReturnValue("unavailable");

    await render(<ConnectionTable data={[dataItem]} entity="connection" isConnectionsListPage />);

    expect(columnHeaders()).toContain("Sync status");
    expect(columnHeaders()).not.toContain("Semantic search");
    expect(within(nameCell()).queryByTitle("Succeeded sync")).not.toBeInTheDocument();
    expect(screen.getByTestId("connectionsTable")).not.toHaveClass("nameWithStatusIcon");
    expect(mockOrganizationSemanticSearchState).toHaveBeenCalledWith({ enabled: false });
    expect(screen.queryByTestId("semantic-search-connection-1")).not.toBeInTheDocument();
  });

  it.each(["source", "destination"] as const)(
    "keeps the sync status icon in the Name cell and adds no columns on the %s Connections tab",
    async (entity) => {
      await render(<ConnectionTable data={[dataItem]} entity={entity} />);

      expect(columnHeaders()).not.toContain("Sync status");
      expect(columnHeaders()).not.toContain("Semantic search");
      expect(within(nameCell()).getByTitle("Succeeded sync")).toBeInTheDocument();
      expect(mockOrganizationSemanticSearchState).toHaveBeenCalledWith({ enabled: false });
      expect(screen.queryByTestId("semantic-search-connection-1")).not.toBeInTheDocument();
    }
  );
});
