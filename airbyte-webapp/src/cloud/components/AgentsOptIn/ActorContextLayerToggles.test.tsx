import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { IntlProvider } from "react-intl";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useAgentsProvisioningStatus, useExternalWorkspaceConnectors, useSetExternalActorEnabled } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ActorAgentAccessToggle, ActorSemanticSearchToggle } from "./ActorContextLayerToggles";
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
  "cloud.contextLayer.actor.notSupported": "This connector is not yet supported by the context layer.",
  "cloud.contextLayer.actor.noPermission":
    "You need edit permission for this workspace's sources or destinations to change this.",
  "cloud.contextLayer.actor.notEnrolled":
    "An organization admin needs to enable the Context layer for this organization and workspace before agent access can be turned on.",
  "cloud.contextLayer.actor.loadFailed": "Context layer status could not be loaded for this workspace.",
  "cloud.contextLayer.actor.agentAccess": "Agent Access",
  "cloud.contextLayer.actor.semanticSearch": "Semantic Search",
  "cloud.contextLayer.actor.semanticSearch.comingSoon": "Semantic search is coming soon.",
  "cloud.contextLayer.actor.status.saving": "Saving…",
  "cloud.contextLayer.actor.status.saved": "Agent access updated.",
  "cloud.contextLayer.actor.status.failed": "Could not update agent access. Please try again.",
};

const renderToggle = (component: React.ReactNode) =>
  render(
    <IntlProvider locale="en" messages={messages}>
      {component}
    </IntlProvider>
  );

describe("ActorContextLayerToggles", () => {
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
      sources: [],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    });
    mockUseSetExternalActorEnabled.mockReturnValue({ mutateAsync: jest.fn() } as never);
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(true);
  });

  it("renders a disabled unchecked toggle with an enrollment tooltip before enrollment", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue(null);
    const mutateAsync = jest.fn();
    mockUseSetExternalActorEnabled.mockReturnValue({ mutateAsync } as never);
    renderToggle(<ActorAgentAccessToggle actorId="actor-id" actorType="source" />);

    const toggle = screen.getByRole("checkbox", { name: "Agent Access" });
    expect(toggle).not.toBeChecked();
    expect(toggle).toBeDisabled();
    expect(mockUseExternalWorkspaceConnectors).toHaveBeenCalledWith("workspace-id", { enabled: false });
    fireEvent.click(toggle);
    expect(mutateAsync).not.toHaveBeenCalled();
    fireEvent.mouseOver(toggle);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "An organization admin needs to enable the Context layer for this organization and workspace before agent access can be turned on."
    );
  });

  it("renders nothing outside the cloud app or when the gate is disabled", () => {
    mockUseIsCloudApp.mockReturnValue(false);
    renderToggle(<ActorAgentAccessToggle actorId="actor-id" actorType="source" />);
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();

    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(false);
    renderToggle(<ActorAgentAccessToggle actorId="actor-id" actorType="source" />);
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
  });

  it("renders Agent Access checked and enabled for a supported enabled actor", () => {
    mockUseExternalWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "actor-id", name: "GitHub", supported: true, enabled: true }],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    });

    renderToggle(<ActorAgentAccessToggle actorId="actor-id" actorType="source" />);

    const toggle = screen.getByRole("checkbox", { name: "Agent Access" });
    expect(toggle).toBeChecked();
    expect(toggle).toBeEnabled();
    expect(mockUseGeneratedIntent).toHaveBeenCalledWith(Intent.CreateOrEditSource);
  });

  it("renders an unsupported Agent Access toggle disabled and unchecked", () => {
    mockUseExternalWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "actor-id", name: "Unsupported", supported: false, enabled: true }],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    });

    renderToggle(<ActorAgentAccessToggle actorId="actor-id" actorType="source" />);

    const toggle = screen.getByRole("checkbox", { name: "Agent Access" });
    expect(toggle).not.toBeChecked();
    expect(toggle).toBeDisabled();
  });

  it("renders a workspace permission tooltip when the actor cannot be edited", async () => {
    mockUseGeneratedIntent.mockReturnValue(false);
    mockUseExternalWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "actor-id", name: "GitHub", supported: true, enabled: true }],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    });

    renderToggle(<ActorAgentAccessToggle actorId="actor-id" actorType="source" />);

    const toggle = screen.getByRole("checkbox", { name: "Agent Access" });
    expect(toggle).toBeDisabled();
    fireEvent.mouseOver(toggle);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "You need edit permission for this workspace's sources or destinations to change this."
    );
  });

  it("renders a disabled unchecked toggle with a load error tooltip when the connector query fails", async () => {
    mockUseExternalWorkspaceConnectors.mockReturnValue({
      sources: [],
      destinations: [],
      isLoading: false,
      sourcesError: true,
      destinationsError: false,
    });

    renderToggle(<ActorAgentAccessToggle actorId="actor-id" actorType="source" />);

    const toggle = screen.getByRole("checkbox", { name: "Agent Access" });
    expect(toggle).not.toBeChecked();
    expect(toggle).toBeDisabled();
    fireEvent.mouseOver(toggle);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "Context layer status could not be loaded for this workspace."
    );
    expect(
      screen.queryByText(
        "An organization admin needs to enable the Context layer for this organization and workspace before agent access can be turned on."
      )
    ).not.toBeInTheDocument();
  });

  it("updates actor access through the mutation", async () => {
    let resolveMutation: (value: { enabled: boolean }) => void = () => {};
    const mutateAsync = jest.fn().mockImplementation(
      () =>
        new Promise<{ enabled: boolean }>((resolve) => {
          resolveMutation = resolve;
        })
    );
    mockUseSetExternalActorEnabled.mockReturnValue({ mutateAsync } as never);
    mockUseExternalWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "actor-id", name: "GitHub", supported: true, enabled: true }],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    });

    renderToggle(<ActorAgentAccessToggle actorId="actor-id" actorType="source" />);
    fireEvent.click(screen.getByRole("checkbox", { name: "Agent Access" }));

    await waitFor(() => {
      expect(mutateAsync).toHaveBeenCalledWith({
        actorId: "actor-id",
        actorKind: "source",
        enabled: false,
      });
    });
    expect(screen.getByRole("checkbox", { name: "Agent Access" })).toBeDisabled();

    resolveMutation({ enabled: false });
    expect(await screen.findByTitle("Agent access updated.")).toBeInTheDocument();
  });

  it("reverts the toggle and shows the mutation error when updating actor access fails", async () => {
    const mutateAsync = jest.fn().mockRejectedValue(new Error("boom"));
    mockUseSetExternalActorEnabled.mockReturnValue({ mutateAsync } as never);
    mockUseExternalWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "actor-id", name: "GitHub", supported: true, enabled: true }],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    });

    renderToggle(<ActorAgentAccessToggle actorId="actor-id" actorType="source" />);
    fireEvent.click(screen.getByRole("checkbox", { name: "Agent Access" }));

    await waitFor(() => {
      expect(screen.getByRole("checkbox", { name: "Agent Access" })).toBeChecked();
    });
    expect(await screen.findByTitle("boom")).toBeInTheDocument();
  });

  it("renders Semantic Search disabled", () => {
    renderToggle(<ActorSemanticSearchToggle actorId="actor-id" actorType="destination" />);

    const toggle = screen.getByRole("checkbox", { name: "Semantic Search" });
    expect(toggle).not.toBeChecked();
    expect(toggle).toBeDisabled();
  });
});
