import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useAgentsProvisioningStatus, useExternalWorkspaceConnectors, useSetExternalActorEnabled } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { useGeneratedIntent } from "core/utils/rbac";

import { ActorContextLayerCard } from "./ActorContextLayerCard";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn(),
}));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
  useExternalWorkspaceConnectors: jest.fn(),
  useSetExternalActorEnabled: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useGeneratedIntent: jest.fn(),
}));

jest.mock("./useShowAgentsOptIn", () => ({
  useShowAgentsOptIn: jest.fn(),
}));

const mockUseCurrentWorkspaceId = useCurrentWorkspaceId as jest.MockedFunction<typeof useCurrentWorkspaceId>;
const mockUseAgentsProvisioningStatus = useAgentsProvisioningStatus as jest.MockedFunction<
  typeof useAgentsProvisioningStatus
>;
const mockUseExternalWorkspaceConnectors = useExternalWorkspaceConnectors as jest.MockedFunction<
  typeof useExternalWorkspaceConnectors
>;
const mockUseSetExternalActorEnabled = useSetExternalActorEnabled as jest.MockedFunction<
  typeof useSetExternalActorEnabled
>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;

const messages = {
  "cloud.contextLayer.actorCard.title": "Context layer",
  "cloud.contextLayer.agentAccess.title": "Agent Access",
  "cloud.contextLayer.agentAccess.description":
    "Allow AI agents with context layer access to query this {actorType, select, source {source} other {destination}} directly. This does not affect data replication.",
  "cloud.contextLayer.semanticSearch.title": "Semantic Search",
  "cloud.contextLayer.semanticSearch.description":
    "Data will be indexed when this source is synced to an enabled Context Layer destination.",
  "cloud.contextLayer.actor.notEnrolled":
    "An organization admin needs to enable the Context layer for this organization and workspace before agent access can be turned on.",
  "cloud.contextLayer.actor.status.saving": "Saving…",
  "cloud.contextLayer.actor.status.saved": "Agent access updated.",
  "cloud.contextLayer.actor.status.failed": "Could not update agent access. Please try again.",
};

const renderCard = (actorType: "source" | "destination") =>
  render(
    <IntlProvider locale="en" messages={messages}>
      <ActorContextLayerCard actorId="actor-id" actorType={actorType} />
    </IntlProvider>
  );

describe("ActorContextLayerCard", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentWorkspaceId.mockReturnValue("workspace-id");
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "org-id",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseExternalWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "actor-id", name: "GitHub", supported: true, enabled: true }],
      destinations: [{ id: "actor-id", name: "Snowflake", supported: true, enabled: true }],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    });
    mockUseSetExternalActorEnabled.mockReturnValue({ mutateAsync: jest.fn() } as never);
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(true);
  });

  it("renders nothing when context layer toggles are not visible", () => {
    mockUseShowAgentsOptIn.mockReturnValue(false);

    renderCard("source");

    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();
  });

  it("renders the card labels and both switches for sources", () => {
    renderCard("source");

    expect(screen.getByText("Context layer")).toBeInTheDocument();
    expect(screen.getByText("Agent Access")).toBeInTheDocument();
    expect(screen.getByText("Semantic Search")).toBeInTheDocument();
    expect(screen.getAllByRole("checkbox")).toHaveLength(2);
    expect(
      screen.getByText("Data will be indexed when this source is synced to an enabled Context Layer destination.")
    ).toBeInTheDocument();
  });

  it("nests the semantic search row under agent access", () => {
    renderCard("source");

    const semanticSearchRow = screen.getByText("Semantic Search").closest("div")?.parentElement;

    expect(semanticSearchRow).toHaveClass("tierTwo");
  });

  it("renders only the Agent Access switch for destinations", () => {
    renderCard("destination");

    expect(screen.getByText("Agent Access")).toBeInTheDocument();
    expect(
      screen.getByText(
        "Allow AI agents with context layer access to query this destination directly. This does not affect data replication."
      )
    ).toBeInTheDocument();
    expect(screen.queryByText("Semantic Search")).not.toBeInTheDocument();
    expect(screen.getAllByRole("checkbox")).toHaveLength(1);
  });

  it.each([
    ["source", "source"],
    ["destination", "destination"],
  ] as const)("interpolates the %s actor type in descriptions", (actorType, actorTypeText) => {
    renderCard(actorType);

    expect(
      screen.getByText(
        `Allow AI agents with context layer access to query this ${actorTypeText} directly. This does not affect data replication.`
      )
    ).toBeInTheDocument();
  });
});
