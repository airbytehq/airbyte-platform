import { screen } from "@testing-library/react";

import { mockDestination, mockSource, render } from "test-utils";

import { useShowActorContextLayerToggles } from "cloud/components/AgentsOptIn/ActorContextLayerToggles";
import {
  useAgentsSupportedDestinationDefinitionIds,
  useAgentsSupportedSourceDefinitionIds,
  useFusionActorEnablement,
} from "core/api";
import { DestinationReadList, SourceReadList } from "core/api/types/AirbyteClient";

import { ActorTable } from "./ActorTable";

jest.mock("cloud/components/AgentsOptIn/ActorContextLayerToggles", () => ({
  useShowActorContextLayerToggles: jest.fn(),
  ActorSemanticSearchToggle: jest.fn(() => <input type="checkbox" aria-label="Semantic Search" />),
}));

jest.mock("core/api", () => ({
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
  useFusionActorEnablement: jest.fn(),
}));

jest.mock("cloud/components/AgentsOptIn/AgentsSourceCta", () => ({
  AgentsSourceCta: () => null,
}));

jest.mock("area/connection/components/EntityTable/components/AllConnectionsStatusCell", () => ({
  AllConnectionsStatusCell: () => null,
}));

jest.mock("area/connection/components/EntityTable/components/ConnectorName", () => ({
  ConnectorName: () => null,
}));

jest.mock("area/connection/components/EntityTable/components/EntityNameCell", () => ({
  EntityNameCell: () => null,
}));

jest.mock("area/connection/components/EntityTable/components/LastSyncCell", () => ({
  LastSyncCell: () => null,
}));

jest.mock("area/connection/components/EntityTable/components/NumberOfConnectionsCell", () => ({
  NumberOfConnectionsCell: () => null,
}));

const mockUseShowActorContextLayerToggles = useShowActorContextLayerToggles as jest.MockedFunction<
  typeof useShowActorContextLayerToggles
>;
const mockSupportedSources = useAgentsSupportedSourceDefinitionIds as jest.MockedFunction<
  typeof useAgentsSupportedSourceDefinitionIds
>;
const mockSupportedDestinations = useAgentsSupportedDestinationDefinitionIds as jest.MockedFunction<
  typeof useAgentsSupportedDestinationDefinitionIds
>;
const mockActorEnablement = useFusionActorEnablement as jest.MockedFunction<typeof useFusionActorEnablement>;

const sourceReadList: SourceReadList = {
  sources: [{ ...mockSource, status: "active" }],
};

const destinationReadList: DestinationReadList = {
  destinations: [{ ...mockDestination, status: "active" }],
};

const draftActorLists: Array<[string, SourceReadList | DestinationReadList, string]> = [
  [
    "source",
    { sources: [{ ...mockSource, name: "Draft source", status: "active", isDraft: true }] },
    mockSource.sourceId,
  ],
  [
    "destination",
    { destinations: [{ ...mockDestination, name: "Draft destination", status: "active", isDraft: true }] },
    mockDestination.destinationId,
  ],
];

const renderActorTable = (actorReadList: SourceReadList | DestinationReadList) =>
  render(<ActorTable actorReadList={actorReadList} hasNextPage={false} fetchNextPage={jest.fn()} />);

describe("ActorTable", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockSupportedSources.mockReturnValue(new Set([mockSource.sourceDefinitionId]));
    mockSupportedDestinations.mockReturnValue(new Set([mockDestination.destinationDefinitionId]));
    mockActorEnablement.mockReturnValue({
      data: { enable_agent_access: true, enable_indexing: true },
      isLoading: false,
      isError: false,
    } as ReturnType<typeof useFusionActorEnablement>);
  });

  it("renders read-only Agent Access and the existing Semantic Search toggle for sources", async () => {
    mockUseShowActorContextLayerToggles.mockReturnValue(true);

    await renderActorTable(sourceReadList);

    expect(screen.getByRole("columnheader", { name: "Agent Access" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Semantic Search" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Sync status" })).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Agent Access configured" })).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Semantic Search" })).toBeInTheDocument();
  });

  it("renders only Agent Access for destinations when toggles are enabled", async () => {
    mockUseShowActorContextLayerToggles.mockReturnValue(true);

    await renderActorTable(destinationReadList);

    expect(screen.getByRole("columnheader", { name: "Agent Access" })).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Semantic Search" })).not.toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Agent Access configured" })).toBeInTheDocument();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it("shows a gray unavailable state for disabled Agent Access", async () => {
    mockUseShowActorContextLayerToggles.mockReturnValue(true);
    mockActorEnablement.mockReturnValue({
      data: { enable_agent_access: false, enable_indexing: false },
      isLoading: false,
      isError: false,
    } as ReturnType<typeof useFusionActorEnablement>);

    await renderActorTable(sourceReadList);

    expect(screen.getByRole("img", { name: "Agent Access not configured" })).toBeInTheDocument();
  });

  it("shows a dash for non-agent connectors and disables the enablement query", async () => {
    mockUseShowActorContextLayerToggles.mockReturnValue(true);
    mockSupportedSources.mockReturnValue(new Set());

    await renderActorTable(sourceReadList);

    expect(screen.getByRole("img", { name: "Agent Access not applicable" })).toHaveTextContent("–");
    expect(mockActorEnablement).toHaveBeenCalledWith(
      { actorId: mockSource.sourceId, actorKind: "source", workspaceId: mockSource.workspaceId },
      false
    );
  });

  it("keeps failed enablement reads distinct from an unconfigured feature", async () => {
    mockUseShowActorContextLayerToggles.mockReturnValue(true);
    mockActorEnablement.mockReturnValue({
      isLoading: false,
      isError: true,
    } as ReturnType<typeof useFusionActorEnablement>);

    await renderActorTable(sourceReadList);

    expect(screen.getByRole("img", { name: "Agent Access status unavailable" })).toBeInTheDocument();
  });

  it("renders neither context layer column for sources when toggles are disabled", async () => {
    mockUseShowActorContextLayerToggles.mockReturnValue(false);

    await renderActorTable(sourceReadList);

    expect(screen.queryByRole("columnheader", { name: "Agent Access" })).not.toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Semantic Search" })).not.toBeInTheDocument();
  });

  it("renders neither context layer column for destinations when toggles are disabled", async () => {
    mockUseShowActorContextLayerToggles.mockReturnValue(false);

    await renderActorTable(destinationReadList);

    expect(screen.queryByRole("columnheader", { name: "Agent Access" })).not.toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Semantic Search" })).not.toBeInTheDocument();
  });

  it.each(draftActorLists)(
    "shows a resumable Draft state for a draft %s",
    async (_actorType, actorReadList, actorId) => {
      mockUseShowActorContextLayerToggles.mockReturnValue(false);

      await renderActorTable(actorReadList);

      expect(screen.getByText("Draft")).toBeInTheDocument();
      expect(screen.getAllByRole("link").some((link) => link.getAttribute("href")?.endsWith(`/${actorId}`))).toBe(true);
    }
  );
});
