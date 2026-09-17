import { screen } from "@testing-library/react";

import { mockDestination, mockSource, render } from "test-utils";

import { useShowActorContextLayerToggles } from "cloud/components/AgentsOptIn/ActorContextLayerToggles";
import { DestinationReadList, SourceReadList } from "core/api/types/AirbyteClient";

import { ActorTable } from "./ActorTable";

jest.mock("cloud/components/AgentsOptIn/ActorContextLayerToggles", () => ({
  useShowActorContextLayerToggles: jest.fn(),
  ActorAgentAccessToggle: () => null,
  ActorSemanticSearchToggle: () => null,
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

const sourceReadList: SourceReadList = {
  sources: [{ ...mockSource, status: "active" }],
};

const destinationReadList: DestinationReadList = {
  destinations: [{ ...mockDestination, status: "active" }],
};

const renderActorTable = (actorReadList: SourceReadList | DestinationReadList) =>
  render(<ActorTable actorReadList={actorReadList} hasNextPage={false} fetchNextPage={jest.fn()} />);

describe("ActorTable", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renders both context layer columns for sources when toggles are enabled", async () => {
    mockUseShowActorContextLayerToggles.mockReturnValue(true);

    await renderActorTable(sourceReadList);

    expect(screen.getByRole("columnheader", { name: "Agent Access" })).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: "Semantic Search" })).toBeInTheDocument();
  });

  it("renders only Agent Access for destinations when toggles are enabled", async () => {
    mockUseShowActorContextLayerToggles.mockReturnValue(true);

    await renderActorTable(destinationReadList);

    expect(screen.getByRole("columnheader", { name: "Agent Access" })).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Semantic Search" })).not.toBeInTheDocument();
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
});
