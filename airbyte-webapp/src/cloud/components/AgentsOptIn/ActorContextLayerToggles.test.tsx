import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useAgentsProvisioningStatus, useExternalWorkspaceConnectors, useSetExternalActorEnabled } from "core/api";
import { ConfirmationModalService } from "core/services/ConfirmationModal";
import { NotificationService } from "core/services/Notification";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ActorAgentAccessToggle, ActorSemanticSearchToggle } from "./ActorContextLayerToggles";
import { resetContextLayerDisableConfirmationState } from "./useConfirmContextLayerDisable";
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
  "cloud.contextLayer.agentAccess.title": "Agent Access",
  "cloud.contextLayer.semanticSearch.title": "Semantic Search",
  "cloud.contextLayer.actor.semanticSearch.comingSoon": "Semantic search is coming soon.",
  "cloud.contextLayer.actor.status.saving": "Saving…",
  "cloud.contextLayer.actor.status.saved": "Agent access updated.",
  "cloud.contextLayer.actor.status.failed": "Could not update agent access. Please try again.",
  "cloud.contextLayer.disableConfirm.title": "Disable Agents access?",
  "cloud.contextLayer.disableConfirm.text":
    "Agents will no longer be able to access {name}. You can re-enable it at any time.",
  "cloud.contextLayer.disableConfirm.submit": "Disable",
  "cloud.contextLayer.disableConfirm.dontAskAgain": "Don't ask again",
  "form.cancel": "Cancel",
};

const renderToggle = (component: React.ReactNode) =>
  render(
    <MemoryRouter>
      <IntlProvider locale="en" messages={messages}>
        <NotificationService>
          <ConfirmationModalService>{component}</ConfirmationModalService>
        </NotificationService>
      </IntlProvider>
    </MemoryRouter>
  );

describe("ActorContextLayerToggles", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    window.localStorage.clear();
    resetContextLayerDisableConfirmationState();
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

  it("updates actor access through the mutation after confirming the disable", async () => {
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

    expect(await screen.findByText("Disable Agents access?")).toBeInTheDocument();
    expect(mutateAsync).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Disable" }));

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
    fireEvent.click(await screen.findByRole("button", { name: "Disable" }));

    await waitFor(() => {
      expect(screen.getByRole("checkbox", { name: "Agent Access" })).toBeChecked();
    });
    expect(await screen.findByTitle("boom")).toBeInTheDocument();
  });

  it('persists "Don\'t ask again" and skips the modal for every toggle afterwards', async () => {
    const mutateAsync = jest.fn().mockResolvedValue({ enabled: false });
    mockUseSetExternalActorEnabled.mockReturnValue({ mutateAsync } as never);
    mockUseExternalWorkspaceConnectors.mockReturnValue({
      sources: [
        { id: "actor-id-1", name: "GitHub", supported: true, enabled: true },
        { id: "actor-id-2", name: "Stripe", supported: true, enabled: true },
      ],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    });

    renderToggle(
      <>
        <ActorAgentAccessToggle actorId="actor-id-1" actorType="source" />
        <ActorAgentAccessToggle actorId="actor-id-2" actorType="source" />
      </>
    );

    const toggles = screen.getAllByRole("checkbox", { name: "Agent Access" });
    fireEvent.click(toggles[0]);
    fireEvent.click(await screen.findByRole("checkbox", { name: "Don't ask again" }));
    fireEvent.click(screen.getByRole("button", { name: "Disable" }));

    await waitFor(() => {
      expect(mutateAsync).toHaveBeenCalledWith({
        actorId: "actor-id-1",
        actorKind: "source",
        enabled: false,
      });
    });
    expect(window.localStorage.getItem("airbyte_context-layer-skip-disable-confirmation")).toBe("true");

    // The second toggle's hook instance never saw setSkipConfirmation, so without the shared
    // flag it would still open the dialog here.
    fireEvent.click(toggles[1]);
    await waitFor(() => {
      expect(mutateAsync).toHaveBeenCalledWith({
        actorId: "actor-id-2",
        actorKind: "source",
        enabled: false,
      });
    });
    expect(screen.queryByTestId("confirmationModal")).not.toBeInTheDocument();

    fireEvent.click(toggles[0]);
    await waitFor(() => {
      expect(mutateAsync).toHaveBeenCalledTimes(3);
    });
    expect(screen.queryByTestId("confirmationModal")).not.toBeInTheDocument();
  });

  it("enables actor access without showing a confirmation modal", async () => {
    const mutateAsync = jest.fn().mockResolvedValue({ enabled: true });
    mockUseSetExternalActorEnabled.mockReturnValue({ mutateAsync } as never);
    mockUseExternalWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "actor-id", name: "GitHub", supported: true, enabled: false }],
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
        enabled: true,
      });
    });
    expect(screen.queryByTestId("confirmationModal")).not.toBeInTheDocument();
  });

  it("does not call the mutation when the disable confirmation is cancelled", async () => {
    const mutateAsync = jest.fn().mockResolvedValue({ enabled: false });
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

    expect(await screen.findByText("Disable Agents access?")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    await waitFor(() => {
      expect(screen.queryByTestId("confirmationModal")).not.toBeInTheDocument();
    });
    expect(mutateAsync).not.toHaveBeenCalled();
    expect(screen.getByRole("checkbox", { name: "Agent Access" })).toBeChecked();
  });

  it("resolves an earlier pending confirmation as cancelled when a second toggle opens the modal", async () => {
    const mutateAsync = jest.fn().mockResolvedValue({ enabled: false });
    mockUseSetExternalActorEnabled.mockReturnValue({ mutateAsync } as never);
    mockUseExternalWorkspaceConnectors.mockReturnValue({
      sources: [
        { id: "actor-id-1", name: "GitHub", supported: true, enabled: true },
        { id: "actor-id-2", name: "Stripe", supported: true, enabled: true },
      ],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    });

    renderToggle(
      <>
        <ActorAgentAccessToggle actorId="actor-id-1" actorType="source" />
        <ActorAgentAccessToggle actorId="actor-id-2" actorType="source" />
      </>
    );

    const toggles = screen.getAllByRole("checkbox", { name: "Agent Access" });
    fireEvent.click(toggles[0]);

    // Checking the box in the first dialog must not carry over to the replacement dialog.
    fireEvent.click(await screen.findByRole("checkbox", { name: "Don't ask again" }));
    fireEvent.click(toggles[1]);

    expect(await screen.findByRole("checkbox", { name: "Don't ask again" })).not.toBeChecked();
    fireEvent.click(screen.getByRole("button", { name: "Disable" }));

    await waitFor(() => {
      expect(mutateAsync).toHaveBeenCalledWith({
        actorId: "actor-id-2",
        actorKind: "source",
        enabled: false,
      });
    });
    expect(mutateAsync).toHaveBeenCalledTimes(1);
    expect(window.localStorage.getItem("airbyte_context-layer-skip-disable-confirmation")).toBeNull();
  });

  it("skips the confirmation modal when the skip preference is already set", async () => {
    window.localStorage.setItem("airbyte_context-layer-skip-disable-confirmation", "true");
    const mutateAsync = jest.fn().mockResolvedValue({ enabled: false });
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
    expect(screen.queryByTestId("confirmationModal")).not.toBeInTheDocument();
  });

  it("renders Semantic Search disabled", () => {
    renderToggle(<ActorSemanticSearchToggle actorId="actor-id" actorType="destination" />);

    const toggle = screen.getByRole("checkbox", { name: "Semantic Search" });
    expect(toggle).not.toBeChecked();
    expect(toggle).toBeDisabled();
  });
});
