import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { mockExperiments } from "test-utils/mockExperiments";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  useAgentsProvisioningStatus,
  useAgentsSupportedSourceDefinitionIds,
  useAgentsSupportedDestinationDefinitionIds,
  useFusionWorkspaceConnectors,
  useSetFusionActorEnablement,
} from "core/api";
import { ConfirmationModalService } from "core/services/ConfirmationModal";
import { NotificationService } from "core/services/Notification";
import { useIsCloudApp } from "core/utils/app";
import { useGeneratedIntent } from "core/utils/rbac";

import { ActorContextLayerCard } from "./ActorContextLayerCard";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn(),
}));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
  useFusionWorkspaceConnectors: jest.fn(),
  useSetFusionActorEnablement: jest.fn(),
  useFusionActorEnablement: jest.fn(() => ({
    data: { enable_agent_access: false, enable_indexing: false },
    isLoading: false,
  })),
  useFusionActorSaving: jest.fn(() => false),
  fusionEnablementState: jest.fn((read) => read),
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
const mockUseFusionWorkspaceConnectors = useFusionWorkspaceConnectors as jest.MockedFunction<
  typeof useFusionWorkspaceConnectors
>;
const mockUseSetFusionActorEnablement = useSetFusionActorEnablement as jest.MockedFunction<
  typeof useSetFusionActorEnablement
>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;

const messages = {
  "cloud.contextLayer.agentAccess.title": "Agent access",
  "cloud.contextLayer.setup.agentAccess.title": "Agent access",
  "cloud.contextLayer.setup.agentAccess.description":
    "Agents can use the Airbyte MCP to read from and write to this {actorType, select, source {source} other {destination}} directly. No sync required.",
  "cloud.contextLayer.semanticSearch.title": "Semantic Search",
  "cloud.contextLayer.actor.notEnrolled":
    "An organization admin needs to enable the Context layer for this organization and workspace before agent access can be turned on.",
  "cloud.contextLayer.actor.status.saving": "Saving…",
  "cloud.contextLayer.actor.status.saved": "Agent access updated.",
  "cloud.contextLayer.actor.status.failed": "Could not update agent access. Please try again.",
};

const renderCard = (actorType: "source" | "destination", actorDefinitionId = `${actorType}-definition-id`) =>
  render(
    <MemoryRouter>
      <IntlProvider locale="en" messages={messages}>
        <NotificationService>
          <ConfirmationModalService>
            <ActorContextLayerCard actorId="actor-id" actorDefinitionId={actorDefinitionId} actorType={actorType} />
          </ConfirmationModalService>
        </NotificationService>
      </IntlProvider>
    </MemoryRouter>
  );

describe("ActorContextLayerCard", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.mocked(useAgentsSupportedSourceDefinitionIds).mockReturnValue(new Set(["source-definition-id"]));
    jest.mocked(useAgentsSupportedDestinationDefinitionIds).mockReturnValue(new Set(["destination-definition-id"]));
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
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "actor-id", name: "GitHub", supported: true, enabled: true }],
      destinations: [{ id: "actor-id", name: "Snowflake", supported: true, enabled: true }],
      isLoading: false,
      sourcesLoading: false,
      destinationsLoading: false,
      sourcesError: false,
      destinationsError: false,
    });
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync: jest.fn() } as never);
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(true);
    mockExperiments({ "platform.fusion-semantic-search-ui": true });
  });

  it("renders nothing when context layer toggles are not visible", () => {
    mockUseShowAgentsOptIn.mockReturnValue(false);

    renderCard("source");

    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it.each(["source", "destination"] as const)("hides the entire card for an unsupported %s", (actorType) => {
    renderCard(actorType, "unsupported-definition-id");

    expect(screen.queryByText("Agent access")).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    expect(mockUseSetFusionActorEnablement).not.toHaveBeenCalled();
  });

  it.each([
    ["source", "destination-definition-id"],
    ["destination", "source-definition-id"],
  ] as const)("uses the matching registry for %s support", (actorType, actorDefinitionId) => {
    renderCard(actorType, actorDefinitionId);

    expect(screen.queryByText("Agent access")).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it.each(["source", "destination"] as const)("hides the card with an empty %s support registry", (actorType) => {
    jest.mocked(useAgentsSupportedSourceDefinitionIds).mockReturnValue(new Set());
    jest.mocked(useAgentsSupportedDestinationDefinitionIds).mockReturnValue(new Set());

    renderCard(actorType);

    expect(screen.queryByText("Agent access")).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it.each(["source", "destination"] as const)("keeps the supported %s card visible before enrollment", (actorType) => {
    mockUseAgentsProvisioningStatus.mockReturnValue(null);

    renderCard(actorType);

    expect(screen.getByText("Agent access")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Agent access" })).toBeDisabled();
  });

  it.each(["source", "destination"] as const)(
    "keeps the supported %s card visible without edit permission",
    (actorType) => {
      mockUseGeneratedIntent.mockReturnValue(false);

      renderCard(actorType);

      expect(screen.getByText("Agent access")).toBeInTheDocument();
      expect(screen.getByRole("checkbox", { name: "Agent access" })).toBeDisabled();
    }
  );

  it.each([
    ["source", false],
    ["source", true],
    ["destination", false],
    ["destination", true],
  ] as const)("renders only agent access for %s when the semantic search flag is %s", (actorType, enabled) => {
    mockExperiments({ "platform.fusion-semantic-search-ui": enabled });
    renderCard(actorType);

    expect(screen.getByRole("checkbox", { name: "Agent access" })).toBeInTheDocument();
    expect(screen.getByText("Agent access")).toBeInTheDocument();
    expect(screen.getAllByRole("checkbox")).toHaveLength(1);
    expect(screen.queryByText(/semantic search/i)).not.toBeInTheDocument();
    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();
    expect(
      screen.getByText(
        `Agents can use the Airbyte MCP to read from and write to this ${actorType} directly. No sync required.`
      )
    ).toBeInTheDocument();
  });
});
