import { renderHook, screen } from "@testing-library/react";

import { render } from "test-utils";

import {
  useAgentsSupportedDestinationDefinitionIds,
  useAgentsSupportedSourceDefinitionIds,
  useConnectionSemanticSearchEnablement,
  useOrganizationSemanticSearchEnablement,
} from "core/api";
import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

import {
  OrganizationSemanticSearchState,
  SemanticSearchCell,
  useOrganizationSemanticSearchState,
} from "./SemanticSearchCell";

jest.mock("core/api", () => ({
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
  useConnectionSemanticSearchEnablement: jest.fn(),
  useOrganizationSemanticSearchEnablement: jest.fn(),
}));

const mockSupportedSources = useAgentsSupportedSourceDefinitionIds as jest.MockedFunction<
  typeof useAgentsSupportedSourceDefinitionIds
>;
const mockSupportedDestinations = useAgentsSupportedDestinationDefinitionIds as jest.MockedFunction<
  typeof useAgentsSupportedDestinationDefinitionIds
>;
const mockEnablement = useConnectionSemanticSearchEnablement as jest.MockedFunction<
  typeof useConnectionSemanticSearchEnablement
>;
const mockOrganizationSetting = useOrganizationSemanticSearchEnablement as jest.MockedFunction<
  typeof useOrganizationSemanticSearchEnablement
>;

const SUPPORTED_SOURCE = "supported-source";
const SUPPORTED_DESTINATION = "supported-destination";

const listItem = (sourceDefinitionId = SUPPORTED_SOURCE, destinationDefinitionId = SUPPORTED_DESTINATION) =>
  ({
    connectionId: "connection-1",
    source: { sourceDefinitionId },
    destination: { destinationDefinitionId },
  }) as WebBackendConnectionListItem;

const enablement = (overrides: Record<string, unknown>) =>
  ({ data: undefined, isLoading: false, isError: false, ...overrides }) as never;

const renderCell = (connection = listItem(), organizationSemanticSearch: OrganizationSemanticSearchState = "on") =>
  render(<SemanticSearchCell connection={connection} organizationSemanticSearch={organizationSemanticSearch} />);

describe("SemanticSearchCell", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockSupportedSources.mockReturnValue(new Set([SUPPORTED_SOURCE]));
    mockSupportedDestinations.mockReturnValue(new Set([SUPPORTED_DESTINATION]));
    mockEnablement.mockReturnValue(enablement({ data: { enable_indexing: true } }));
  });

  it.each([
    ["source", listItem("other-source")],
    ["destination", listItem(SUPPORTED_SOURCE, "other-destination")],
  ])("shows a dash and reads nothing when the %s is not an agent connector", async (_, connection) => {
    await renderCell(connection);

    expect(screen.getByRole("img", { name: "Semantic search not applicable" })).toHaveTextContent("–");
    expect(mockEnablement).not.toHaveBeenCalled();
  });

  it("shows a grey cross and reads nothing when semantic search is off for the organization", async () => {
    await renderCell(listItem(), "off");

    const cell = screen.getByRole("img", { name: "Semantic search not configured" });
    expect(cell.querySelector('[data-icon="status-error"]')).toBeInTheDocument();
    expect(cell.querySelector(".icon--disabled")).toBeInTheDocument();
    expect(mockEnablement).not.toHaveBeenCalled();
  });

  it("shows a teal check when the connection value is on", async () => {
    await renderCell();

    const cell = screen.getByRole("img", { name: "Semantic search configured" });
    expect(cell.querySelector('[data-icon="status-success"]')).toBeInTheDocument();
    expect(cell.querySelector(".icon--success")).toBeInTheDocument();
    expect(mockEnablement).toHaveBeenCalledWith("connection-1");
  });

  it("shows a grey cross when the connection value is off", async () => {
    mockEnablement.mockReturnValue(enablement({ data: { enable_indexing: false } }));

    await renderCell();

    const cell = screen.getByRole("img", { name: "Semantic search not configured" });
    expect(cell.querySelector('[data-icon="status-error"]')).toBeInTheDocument();
    expect(cell.querySelector(".icon--disabled")).toBeInTheDocument();
  });

  it("shows a neutral loading state, not a cross, while the organization setting loads", async () => {
    await renderCell(listItem(), "loading");

    expect(screen.getByRole("img", { name: "Loading Semantic search status" })).toBeInTheDocument();
    expect(screen.queryByRole("img", { name: "Semantic search not configured" })).not.toBeInTheDocument();
    expect(mockEnablement).not.toHaveBeenCalled();
  });

  it("shows a neutral loading state, not a cross, while the connection value loads", async () => {
    mockEnablement.mockReturnValue(enablement({ isLoading: true }));

    await renderCell();

    expect(screen.getByRole("img", { name: "Loading Semantic search status" })).toBeInTheDocument();
    expect(screen.queryByRole("img", { name: "Semantic search not configured" })).not.toBeInTheDocument();
  });

  it("shows a neutral unavailable state when the connection value fails to load", async () => {
    mockEnablement.mockReturnValue(enablement({ isError: true }));

    await renderCell();

    const cell = screen.getByRole("img", { name: "Semantic search status unavailable" });
    expect(cell.querySelector('[data-icon="status-warning"]')).toBeInTheDocument();
    expect(cell.querySelector(".icon--disabled")).toBeInTheDocument();
  });

  it("shows a neutral unavailable state when the organization setting fails to load", async () => {
    await renderCell(listItem(), "unavailable");

    expect(screen.getByRole("img", { name: "Semantic search status unavailable" })).toBeInTheDocument();
    expect(mockEnablement).not.toHaveBeenCalled();
  });
});

describe("useOrganizationSemanticSearchState", () => {
  it.each([
    ["loading", { isInitialLoading: true }],
    ["unavailable", { isError: true, data: { semanticSearchEnabled: true } }],
    ["unavailable", {}],
    ["off", { data: { semanticSearchEnabled: false } }],
    ["on", { data: { semanticSearchEnabled: true } }],
  ])("is %s for %j", (expected, overrides) => {
    mockOrganizationSetting.mockReturnValue({
      data: undefined,
      isInitialLoading: false,
      isError: false,
      ...overrides,
    } as never);

    expect(renderHook(() => useOrganizationSemanticSearchState({ enabled: true })).result.current).toBe(expected);
    expect(mockOrganizationSetting).toHaveBeenCalledWith({ enabled: true });
  });
});
