import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils";
import {
  useAgentsProvisioningStatusQuery,
  useAgentsProvisioningStatus,
  useUnenrollOrganizationFromAgents,
  useEnrollOrganizationInAgents,
  useEnableFusionWorkspaceActors,
  useFusionWorkspaceConnectors,
  useListWorkspacesInOrganization,
  useSetFusionActorEnablement,
} from "core/api";
import { ConfirmationModalService } from "core/services/ConfirmationModal";
import { useModalService } from "core/services/Modal";
import { NotificationService, useNotificationService } from "core/services/Notification";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ContextLayerPage } from "./ContextLayerPage";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatusQuery: jest.fn(),
  useAgentsProvisioningStatus: jest.fn(),
  useUnenrollOrganizationFromAgents: jest.fn(),
  useEnrollOrganizationInAgents: jest.fn(),
  useFusionWorkspaceConnectors: jest.fn(),
  useListWorkspacesInOrganization: jest.fn(),
  useSetFusionActorEnablement: jest.fn(),
  useEnableFusionWorkspaceActors: jest.fn(() => ({ mutateAsync: jest.fn().mockResolvedValue([]) })),
}));

jest.mock("core/services/Modal", () => ({
  useModalService: jest.fn(),
}));

jest.mock("core/services/Notification", () => ({
  ...jest.requireActual("core/services/Notification"),
  useNotificationService: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: () => true,
}));

jest.mock("./useShowAgentsOptIn", () => ({
  useShowAgentsOptIn: jest.fn(),
}));

jest.mock("core/utils/links", () => ({
  links: {
    agentsDocs: "https://docs.airbyte.com/ai-agents/get-started",
  },
}));

jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useGeneratedIntent: jest.fn(),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockUseAgentsProvisioningStatus = useAgentsProvisioningStatus as jest.MockedFunction<
  typeof useAgentsProvisioningStatus
>;
const mockUseAgentsProvisioningStatusQuery = useAgentsProvisioningStatusQuery as jest.MockedFunction<
  typeof useAgentsProvisioningStatusQuery
>;
const mockUseEnrollOrganizationInAgents = useEnrollOrganizationInAgents as jest.MockedFunction<
  typeof useEnrollOrganizationInAgents
>;
const mockUseUnenrollOrganizationFromAgents = useUnenrollOrganizationFromAgents as jest.MockedFunction<
  typeof useUnenrollOrganizationFromAgents
>;
const mockUseListWorkspacesInOrganization = useListWorkspacesInOrganization as jest.MockedFunction<
  typeof useListWorkspacesInOrganization
>;
const mockUseFusionWorkspaceConnectors = useFusionWorkspaceConnectors as jest.MockedFunction<
  typeof useFusionWorkspaceConnectors
>;
const mockUseSetFusionActorEnablement = useSetFusionActorEnablement as jest.MockedFunction<
  typeof useSetFusionActorEnablement
>;
const mockUseModalService = useModalService as jest.MockedFunction<typeof useModalService>;
const mockUseNotificationService = useNotificationService as jest.MockedFunction<typeof useNotificationService>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;

const messages = {
  "cloud.contextLayer.title": "Context layer",
  "cloud.contextLayer.subtitle": "Configure metadata access and intelligence options for your organization.",
  "cloud.contextLayer.unavailable.title": "Context layer not available",
  "cloud.contextLayer.unavailable.description":
    "The context layer is not available for this organization yet. Contact Airbyte support if you'd like to learn more.",
  "cloud.contextLayer.loadError.title": "Couldn't load context layer status",
  "cloud.contextLayer.loadError.description":
    "We couldn't check whether the context layer is available for this organization. Please try again.",
  "cloud.contextLayer.enable.title": "Enable context layer",
  "cloud.contextLayer.enable.description":
    "The context layer allows AI agents to access and reason about your organization's data. Only organization admins can enable or disable this feature.",
  "cloud.contextLayer.enable.info":
    "When enabled, Airbyte constructs a semantic layer from your data. AI agents can read from and write to supported sources and destinations, search your data, and reason about it.",
  "cloud.contextLayer.enable.button": "Enable Context Layer",
  "cloud.contextLayer.enable.adminRequired": "Enabling the context layer requires an organization admin.",
  "cloud.contextLayer.enable.adminRequired.description":
    "Ask an organization admin of your Airbyte organization to enable this feature for your team.",
  "cloud.contextLayer.status.title": "Context layer status",
  "cloud.contextLayer.status.description":
    "AI agents can access and reason about your organization's data. Only organization admins can enable or disable this feature.",
  "cloud.contextLayer.toggle.label": "Context layer",
  "cloud.contextLayer.toggle.description": "Enable reasoning capabilities",
  "cloud.contextLayer.toggle.enabled": "Enabled",
  "cloud.contextLayer.toggle.disabled": "Disabled",
  "cloud.contextLayer.terms.title": "Terms of Service",
  "cloud.contextLayer.terms.subtitle": "Please review and accept the terms before enabling context layer.",
  "cloud.contextLayer.terms.intro": "By enabling Context layer, you agree to the following terms:",
  "cloud.contextLayer.terms.dataProcessing": "Data processing terms",
  "cloud.contextLayer.terms.privacyModeling": "Privacy and modeling terms",
  "cloud.contextLayer.terms.complianceResidency": "Compliance and data residency terms",
  "cloud.contextLayer.terms.liability": "Liability and indemnification terms",
  "cloud.contextLayer.terms.serviceLevel": "Service level and availability terms",
  "cloud.contextLayer.terms.intellectualProperty": "Intellectual property terms",
  "cloud.contextLayer.terms.acceptTerms": "I have read and agree to the Context layer Terms of Service",
  "cloud.contextLayer.terms.cancel": "Cancel",
  "cloud.contextLayer.terms.accept": "Accept and Enable",
  "cloud.contextLayer.terms.footnote": "* These terms are required for compliance and data processing purposes.",
  "cloud.contextLayer.terms.enrollError": "Context layer could not be enabled. Please try again.",
  "cloud.contextLayer.workspace.title": "Workspace connector access",
  "cloud.contextLayer.workspace.description":
    "Control which connectors AI agents can access within each workspace. Sources are enabled by default. Destinations must be explicitly enabled.",
  "cloud.contextLayer.workspace.loading": "Loading workspaces...",
  "cloud.contextLayer.workspace.loadingMore": "Loading more workspaces...",
  "cloud.contextLayer.workspace.empty": "No workspaces found in this organization.",
  "cloud.contextLayer.workspace.connectorsLoading": "Loading connectors...",
  "cloud.contextLayer.workspace.noConnectors": "No connectors found in this workspace.",
  "cloud.contextLayer.workspace.counts": "{sources} sources, {destinations} destinations",
  "cloud.contextLayer.workspace.sources": "Sources",
  "cloud.contextLayer.workspace.destinations": "Destinations",
  "cloud.contextLayer.workspace.enabledCount":
    "{enabled} of {supported} enabled{unsupported, plural, =0 {} other { (excludes {unsupported} not supported)}}",
  "cloud.contextLayer.connectors.error": "Unable to load connectors. Please try again.",
  "cloud.contextLayer.connectors.toggleError": "Could not update access for {name}. Please try again.",
  "cloud.contextLayer.docs": "Learn how to connect agents (SDK, API, MCP)",
  "cloud.contextLayer.disableConfirm.title": "Disable Agents access?",
  "cloud.contextLayer.disableConfirm.text":
    "Agents will no longer be able to access {name}. You can re-enable it at any time.",
  "cloud.contextLayer.disableConfirm.submit": "Disable",
  "cloud.contextLayer.disableConfirm.dontAskAgain": "Don't ask again",
  "cloud.contextLayer.disableOrg.title": "Disable the Context layer for this organization?",
  "cloud.contextLayer.disableOrg.text":
    "Agents will lose access to every connector in this organization. Your connector selections are kept and will be restored if you re-enable the Context layer.",
  "cloud.contextLayer.disableOrg.submit": "Disable",
  "cloud.contextLayer.disableOrg.error": "Could not disable the Context layer. Please try again.",
  "form.cancel": "Cancel",
  "form.tryAgain": "Try again",
  "ui.loading": "Loading …",
};

const renderWithIntl = () =>
  render(
    <MemoryRouter>
      <IntlProvider locale="en" messages={messages}>
        <NotificationService>
          <ConfirmationModalService>
            <ContextLayerPage />
          </ConfirmationModalService>
        </NotificationService>
      </IntlProvider>
    </MemoryRouter>
  );

describe("ContextLayerPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    window.localStorage.clear();
    mockUseCurrentOrganizationId.mockReturnValue("test-org-123");
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseAgentsProvisioningStatusQuery.mockImplementation(
      () =>
        ({
          data: mockUseAgentsProvisioningStatus(),
          isLoading: false,
          isError: false,
        }) as never
    );
    mockUseListWorkspacesInOrganization.mockReturnValue({ data: { pages: [] } } as never);
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    });
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync: jest.fn() } as never);
    mockUseGeneratedIntent.mockReturnValue(true);
    mockUseModalService.mockReturnValue({ openModal: jest.fn() } as never);
    mockUseNotificationService.mockReturnValue({ registerNotification: jest.fn() } as never);
    mockUseEnrollOrganizationInAgents.mockReturnValue({ mutateAsync: jest.fn() } as never);
    mockUseUnenrollOrganizationFromAgents.mockReturnValue({ mutateAsync: jest.fn() } as never);
  });

  it("renders the disabled state and opens the terms modal", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      is_instance_admin: false,
      provisioning_state: "not_provisioned",
      organization_id: "test-org-123",
      organization_kind: null,
      external_cloud_eligible: true,
      eligible_external_organization_id: "test-org-123",
    });
    const openModal = jest.fn();
    openModal.mockResolvedValue(undefined);
    mockUseModalService.mockReturnValue({ openModal } as never);

    renderWithIntl();

    expect(screen.getByRole("button", { name: "Enable Context Layer" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Enable Context Layer" }));
    expect(openModal).toHaveBeenCalled();
  });

  it("accepts enrollment only after both terms checkboxes are checked", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      is_instance_admin: false,
      provisioning_state: "not_provisioned",
      organization_id: "test-org-123",
      organization_kind: null,
      external_cloud_eligible: true,
      eligible_external_organization_id: "test-org-123",
    });
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      hasNextPage: true,
      fetchNextPage: jest.fn().mockResolvedValue({
        data: {
          pages: [
            { workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] },
            { workspaces: [{ workspaceId: "workspace-2", name: "Workspace 2" }] },
          ],
        },
        hasNextPage: false,
      }),
    } as never);
    const mutateAsync = jest.fn().mockResolvedValue(undefined);
    mockUseEnrollOrganizationInAgents.mockReturnValue({ mutateAsync } as never);
    mockUseModalService.mockReturnValue({
      openModal: jest.fn().mockImplementation(({ content }) => {
        const Content = content;
        return Promise.resolve(
          render(
            <IntlProvider locale="en" messages={messages}>
              <Content onCancel={jest.fn()} onComplete={jest.fn()} />
            </IntlProvider>
          )
        );
      }),
    } as never);

    renderWithIntl();
    fireEvent.click(screen.getByRole("button", { name: "Enable Context Layer" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Accept and Enable" })).toBeDisabled());
    fireEvent.click(screen.getByRole("checkbox", { name: messages["cloud.contextLayer.terms.acceptTerms"] }));
    expect(screen.getByRole("button", { name: "Accept and Enable" })).toBeEnabled();
    fireEvent.click(screen.getByRole("button", { name: "Accept and Enable" }));
    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        workspaceIds: ["workspace-1", "workspace-2"],
        addAllSupportedActors: false,
      })
    );
  });

  it.each(["inventory", "actor"])("retries %s failures in the existing enrollment modal", async (failure) => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      external_cloud_eligible: true,
    } as never);
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
    } as never);
    const failedActors = [{ actorId: "source-1", actorKind: "source", workspaceId: "workspace-1" }];
    const enableActors = jest
      .fn()
      .mockResolvedValueOnce([])
      .mockImplementationOnce(() =>
        failure === "inventory" ? Promise.reject(new Error("Inventory unavailable")) : Promise.resolve(failedActors)
      )
      .mockResolvedValueOnce([]);
    jest.mocked(useEnableFusionWorkspaceActors).mockReturnValue({ mutateAsync: enableActors } as never);
    mockUseModalService.mockReturnValue({
      openModal: jest.fn().mockImplementation(({ content }) => {
        const Content = content;
        const modal = render(
          <IntlProvider locale="en" messages={messages}>
            <Content onCancel={jest.fn()} onComplete={() => modal.unmount()} />
          </IntlProvider>
        );
        return Promise.resolve();
      }),
    } as never);

    renderWithIntl();
    const acceptEnrollment = () => {
      fireEvent.click(screen.getByRole("button", { name: "Enable Context Layer" }));
      fireEvent.click(screen.getByRole("checkbox", { name: messages["cloud.contextLayer.terms.acceptTerms"] }));
      fireEvent.click(screen.getByRole("button", { name: "Accept and Enable" }));
    };
    acceptEnrollment();
    await waitFor(() => expect(enableActors).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(screen.queryByRole("button", { name: "Accept and Enable" })).not.toBeInTheDocument());
    acceptEnrollment();
    await waitFor(() => expect(enableActors).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.getByRole("button", { name: "Accept and Enable" })).toBeEnabled());
    fireEvent.click(screen.getByRole("button", { name: "Accept and Enable" }));
    await waitFor(() =>
      expect(enableActors).toHaveBeenNthCalledWith(3, {
        workspaceIds: ["workspace-1"],
        retryActors: failure === "inventory" ? undefined : failedActors,
      })
    );
    await waitFor(() => expect(screen.queryByRole("button", { name: "Accept and Enable" })).not.toBeInTheDocument());
  });

  it("renders workspace connector access for an enrolled organization", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });

    renderWithIntl();

    expect(screen.getByText("Workspace connector access")).toBeInTheDocument();
    expect(screen.getByText("No workspaces found in this organization.")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Learn how to connect agents (SDK, API, MCP)" })).toHaveAttribute(
      "href",
      "https://docs.airbyte.com/ai-agents/get-started"
    );
    expect(screen.getByRole("link", { name: "Learn how to connect agents (SDK, API, MCP)" })).toHaveAttribute(
      "target",
      "_blank"
    );
  });

  it("hydrates connector enablement only for expanded workspaces", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: {
        pages: [
          {
            workspaces: [
              { workspaceId: "workspace-1", name: "Workspace 1" },
              { workspaceId: "workspace-2", name: "Workspace 2" },
            ],
          },
        ],
      },
    } as never);

    renderWithIntl();

    expect(screen.getByRole("button", { name: /Workspace 1/ })).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByRole("button", { name: /Workspace 2/ })).toHaveAttribute("aria-expanded", "false");
    expect(mockUseFusionWorkspaceConnectors).toHaveBeenCalledWith("workspace-1", { hydrate: false });
    expect(mockUseFusionWorkspaceConnectors).toHaveBeenCalledWith("workspace-2", { hydrate: false });
    expect(mockUseFusionWorkspaceConnectors).not.toHaveBeenCalledWith("workspace-1", { hydrate: true });
    expect(mockUseFusionWorkspaceConnectors).not.toHaveBeenCalledWith("workspace-2", { hydrate: true });

    fireEvent.click(screen.getByRole("button", { name: /Workspace 1/ }));

    expect(screen.getByRole("button", { name: /Workspace 1/ })).toHaveAttribute("aria-expanded", "true");
    expect(mockUseFusionWorkspaceConnectors).toHaveBeenCalledWith("workspace-1", { hydrate: true });
    expect(mockUseFusionWorkspaceConnectors).not.toHaveBeenCalledWith("workspace-2", { hydrate: true });
  });

  it("asks for confirmation before disabling the Context layer for an enrolled organization", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    const mutateAsync = jest.fn().mockResolvedValue(undefined);
    mockUseUnenrollOrganizationFromAgents.mockReturnValue({ mutateAsync } as never);

    renderWithIntl();

    const toggle = screen.getByRole("checkbox", { name: "Context layer" });
    expect(toggle).toBeEnabled();
    fireEvent.click(toggle);

    expect(await screen.findByText("Disable the Context layer for this organization?")).toBeInTheDocument();
    expect(mutateAsync).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Disable" }));
    await waitFor(() => expect(mutateAsync).toHaveBeenCalled());
  });

  it("keeps the confirmation modal open and shows an error notification when unenrollment fails", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    const mutateAsync = jest.fn().mockRejectedValue(new Error("failed"));
    mockUseUnenrollOrganizationFromAgents.mockReturnValue({ mutateAsync } as never);
    const registerNotification = jest.fn();
    mockUseNotificationService.mockReturnValue({ registerNotification } as never);

    renderWithIntl();

    fireEvent.click(screen.getByRole("checkbox", { name: "Context layer" }));
    expect(await screen.findByText("Disable the Context layer for this organization?")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Disable" }));

    await waitFor(() =>
      expect(registerNotification).toHaveBeenCalledWith({
        id: "context-layer-disable-org-error",
        text: "Could not disable the Context layer. Please try again.",
        type: "error",
      })
    );
    expect(screen.getByTestId("confirmationModal")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Disable" })).toBeInTheDocument();
  });

  it("does not disable the Context layer when the confirmation is cancelled", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    const mutateAsync = jest.fn().mockResolvedValue(undefined);
    mockUseUnenrollOrganizationFromAgents.mockReturnValue({ mutateAsync } as never);

    renderWithIntl();

    fireEvent.click(screen.getByRole("checkbox", { name: "Context layer" }));
    expect(await screen.findByText("Disable the Context layer for this organization?")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    await waitFor(() => expect(screen.queryByTestId("confirmationModal")).not.toBeInTheDocument());
    expect(mutateAsync).not.toHaveBeenCalled();
  });

  it("disables the organization toggle for enrolled non-admin viewers", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseGeneratedIntent.mockImplementation((intent) => intent !== Intent.UpdateOrganizationPermissions);

    renderWithIntl();

    expect(screen.getByRole("checkbox", { name: "Context layer" })).toBeDisabled();
  });

  it("renders real workspace connectors and the empty state", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: {
        pages: [
          {
            workspaces: [
              { workspaceId: "workspace-1", name: "Workspace 1" },
              { workspaceId: "workspace-2", name: "Workspace 2" },
            ],
          },
        ],
      },
    } as never);
    const mutateAsync = jest.fn().mockResolvedValue({ enabled: false });
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync } as never);
    mockUseFusionWorkspaceConnectors.mockImplementation((workspaceId) =>
      workspaceId === "workspace-1"
        ? {
            sources: [
              {
                id: "source-1",
                name: "GitHub account",
                supported: true,
                state: { enable_agent_access: true, enable_indexing: false },
                enabled: true,
              },
              {
                id: "source-2",
                name: "Stripe account",
                supported: true,
                enabled: false,
                state: { enable_agent_access: false, enable_indexing: false },
              },
            ],
            destinations: [
              {
                id: "destination-1",
                name: "BigQuery warehouse",
                supported: false,
                enabled: false,
                state: { enable_agent_access: false, enable_indexing: false },
              },
              {
                id: "destination-2",
                name: "Snowflake warehouse",
                supported: true,
                enabled: true,
                state: { enable_agent_access: false, enable_indexing: false },
              },
            ],
            isLoading: false,
            sourcesError: false,
            destinationsError: false,
          }
        : { sources: [], destinations: [], isLoading: false, sourcesError: false, destinationsError: false }
    );

    renderWithIntl();

    fireEvent.click(screen.getByRole("button", { name: /Workspace 1/ }));
    fireEvent.click(screen.getByRole("button", { name: /Workspace 2/ }));

    expect(screen.getByText("GitHub account")).toBeInTheDocument();
    expect(screen.getByText("Stripe account")).toBeInTheDocument();
    expect(screen.getByText("BigQuery warehouse")).toBeInTheDocument();
    expect(screen.getByText("Snowflake warehouse")).toBeInTheDocument();
    expect(screen.getByText("No connectors found in this workspace.")).toBeInTheDocument();
    expect(screen.getByText("1 of 2 enabled")).toBeInTheDocument();
    expect(screen.getByText("1 of 1 enabled (excludes 1 not supported)")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "GitHub account" })).toBeChecked();
    expect(screen.getByRole("checkbox", { name: "Stripe account" })).not.toBeChecked();
    expect(screen.getByRole("checkbox", { name: "BigQuery warehouse" })).toBeDisabled();
    fireEvent.click(screen.getByRole("checkbox", { name: "GitHub account" }));
    fireEvent.click(await screen.findByRole("button", { name: "Disable" }));
    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        actorId: "source-1",
        actorKind: "source",
        workspaceId: "workspace-1",
        expectedState: { enable_agent_access: true, enable_indexing: false },
        enabled: false,
      })
    );
  });

  it("shows a confirmation modal on disable but not on enable", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      isLoading: false,
    } as never);
    const mutateAsync = jest.fn().mockResolvedValue({ enabled: true });
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync } as never);
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [
        {
          id: "source-1",
          name: "GitHub account",
          supported: true,
          state: { enable_agent_access: true, enable_indexing: false },
          enabled: true,
        },
        {
          id: "source-2",
          name: "Stripe account",
          supported: true,
          state: { enable_agent_access: false, enable_indexing: false },
          enabled: false,
        },
      ],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    } as never);

    renderWithIntl();

    fireEvent.click(screen.getByRole("button", { name: /Workspace 1/ }));

    fireEvent.click(screen.getByRole("checkbox", { name: "GitHub account" }));
    expect(await screen.findByText("Disable Agents access?")).toBeInTheDocument();
    expect(mutateAsync).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Disable" }));
    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        actorId: "source-1",
        actorKind: "source",
        workspaceId: "workspace-1",
        expectedState: { enable_agent_access: true, enable_indexing: false },
        enabled: false,
      })
    );

    fireEvent.click(screen.getByRole("checkbox", { name: "Stripe account" }));
    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        actorId: "source-2",
        actorKind: "source",
        workspaceId: "workspace-1",
        expectedState: { enable_agent_access: false, enable_indexing: false },
        enabled: true,
      })
    );
    expect(screen.queryByTestId("confirmationModal")).not.toBeInTheDocument();
  });

  it("loads all workspace pages before rendering connector access", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    let workspacePages = [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }];
    let hasNextPage = true;
    const fetchNextPage = jest.fn().mockImplementation(async () => {
      workspacePages = [...workspacePages, { workspaces: [{ workspaceId: "workspace-2", name: "Workspace 2" }] }];
      hasNextPage = false;
      return { data: { pages: workspacePages }, hasNextPage: false };
    });
    mockUseListWorkspacesInOrganization.mockImplementation(
      () =>
        ({
          get data() {
            return { pages: workspacePages };
          },
          get hasNextPage() {
            return hasNextPage;
          },
          fetchNextPage,
          isFetchingNextPage: false,
          isLoading: false,
        }) as never
    );
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    } as never);

    const view = renderWithIntl();

    await waitFor(() => expect(fetchNextPage).toHaveBeenCalledTimes(1));
    view.rerender(
      <MemoryRouter>
        <IntlProvider locale="en" messages={messages}>
          <NotificationService>
            <ConfirmationModalService>
              <ContextLayerPage />
            </ConfirmationModalService>
          </NotificationService>
        </IntlProvider>
      </MemoryRouter>
    );
    expect(screen.getByText("Workspace 1")).toBeInTheDocument();
    expect(screen.getByText("Workspace 2")).toBeInTheDocument();
  });

  it("disables all connector switches and does not mutate for read-only viewers", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      isLoading: false,
    } as never);
    const mutateAsync = jest.fn();
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync } as never);
    mockUseGeneratedIntent.mockReturnValue(false);
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [
        {
          id: "source-1",
          name: "GitHub account",
          supported: true,
          state: { enable_agent_access: false, enable_indexing: false },
          enabled: true,
        },
      ],
      destinations: [
        {
          id: "destination-1",
          name: "BigQuery warehouse",
          supported: false,
          state: { enable_agent_access: false, enable_indexing: false },
          enabled: false,
        },
      ],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    } as never);

    renderWithIntl();

    fireEvent.click(screen.getByRole("button", { name: /Workspace 1/ }));

    expect(screen.getByRole("checkbox", { name: "GitHub account" })).toBeDisabled();
    expect(screen.getByRole("checkbox", { name: "BigQuery warehouse" })).toBeDisabled();
    expect(mutateAsync).not.toHaveBeenCalled();
  });

  it("renders a connector error for one category without hiding the other category", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      isLoading: false,
    } as never);
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [],
      destinations: [
        {
          id: "destination-1",
          name: "BigQuery warehouse",
          supported: true,
          state: { enable_agent_access: false, enable_indexing: false },
          enabled: true,
        },
      ],
      isLoading: false,
      sourcesError: true,
      destinationsError: false,
    } as never);

    renderWithIntl();

    fireEvent.click(screen.getByRole("button", { name: /Workspace 1/ }));

    expect(screen.getByTestId("context-layer-source-error")).toHaveTextContent(
      "Unable to load connectors. Please try again."
    );
    expect(screen.getByText("BigQuery warehouse")).toBeInTheDocument();
    expect(screen.queryByText("No connectors found in this workspace.")).not.toBeInTheDocument();
  });

  it("clears the optimistic connector state after a successful mutation", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      isLoading: false,
    } as never);
    const mutateAsync = jest.fn().mockResolvedValue({ enabled: false });
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync } as never);
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [
        {
          id: "source-1",
          name: "GitHub account",
          supported: true,
          state: { enable_agent_access: false, enable_indexing: false },
          enabled: true,
        },
      ],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    } as never);

    const view = renderWithIntl();
    fireEvent.click(screen.getByRole("button", { name: /Workspace 1/ }));
    const checkbox = screen.getByRole("checkbox", { name: "GitHub account" });
    fireEvent.click(checkbox);
    fireEvent.click(await screen.findByRole("button", { name: "Disable" }));
    await waitFor(() => expect(mutateAsync).toHaveBeenCalled());
    view.rerender(
      <MemoryRouter>
        <IntlProvider locale="en" messages={messages}>
          <NotificationService>
            <ConfirmationModalService>
              <ContextLayerPage />
            </ConfirmationModalService>
          </NotificationService>
        </IntlProvider>
      </MemoryRouter>
    );

    expect(screen.getByRole("checkbox", { name: "GitHub account" })).toBeChecked();
  });

  it("reverts the connector switch and shows an error notification when the mutation fails", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      isLoading: false,
    } as never);
    const mutateAsync = jest.fn().mockRejectedValue(new Error("failed"));
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync } as never);
    const registerNotification = jest.fn();
    mockUseNotificationService.mockReturnValue({ registerNotification } as never);
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [
        {
          id: "source-1",
          name: "GitHub account",
          supported: true,
          state: { enable_agent_access: false, enable_indexing: false },
          enabled: true,
        },
      ],
      destinations: [],
      isLoading: false,
      sourcesError: false,
      destinationsError: false,
    } as never);

    renderWithIntl();
    fireEvent.click(screen.getByRole("button", { name: /Workspace 1/ }));
    fireEvent.click(screen.getByRole("checkbox", { name: "GitHub account" }));
    fireEvent.click(await screen.findByRole("button", { name: "Disable" }));
    await waitFor(() => expect(mutateAsync).toHaveBeenCalled());
    await waitFor(() => expect(screen.getByRole("checkbox", { name: "GitHub account" })).toBeChecked());
    expect(registerNotification).toHaveBeenCalledWith({
      id: "context-layer-connector-toggle-error-workspace-1:source-1",
      text: "Could not update access for GitHub account. Please try again.",
      type: "error",
    });
  });

  it("shows a loading state while workspace access is loading", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseListWorkspacesInOrganization.mockReturnValue({ isLoading: true } as never);

    renderWithIntl();

    expect(screen.getByText("Loading workspaces...")).toBeInTheDocument();
    expect(screen.queryByText("No workspaces found in this organization.")).not.toBeInTheDocument();
  });

  it("shows the not-available state when the organization is not eligible", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      is_instance_admin: false,
      provisioning_state: null,
      organization_id: "test-org-123",
      organization_kind: null,
      external_cloud_eligible: false,
      eligible_external_organization_id: null,
    });

    renderWithIntl();

    expect(screen.getByTestId("context-layer-unavailable")).toHaveTextContent(
      "The context layer is not available for this organization yet. Contact Airbyte support if you'd like to learn more."
    );
    expect(screen.queryByRole("button", { name: "Enable Context Layer" })).not.toBeInTheDocument();
  });

  it("shows the not-available state when the provisioning status belongs to another organization", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue(null);

    renderWithIntl();

    expect(screen.getByTestId("context-layer-unavailable")).toHaveTextContent(
      "The context layer is not available for this organization yet. Contact Airbyte support if you'd like to learn more."
    );
  });

  it("shows a loading state while eligibility is resolving", () => {
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({ data: undefined, isInitialLoading: true } as never);

    renderWithIntl();

    expect(screen.getByTitle("Loading …")).toBeInTheDocument();
    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();
    expect(screen.queryByTestId("context-layer-unavailable")).not.toBeInTheDocument();
    expect(screen.queryByTestId("context-layer-card")).not.toBeInTheDocument();
  });

  it("renders an error state with retry instead of the not-available card when the provisioning request fails", () => {
    const mockRefetch = jest.fn();
    mockUseAgentsProvisioningStatusQuery.mockReturnValue({
      data: undefined,
      isInitialLoading: false,
      isError: true,
      refetch: mockRefetch,
    } as never);

    renderWithIntl();

    expect(screen.getByTestId("context-layer-load-error")).toBeInTheDocument();
    expect(screen.queryByTestId("context-layer-unavailable")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /try again/i }));
    expect(mockRefetch).toHaveBeenCalledTimes(1);
  });

  it("shows the not-available state when the opt-in flag is off and the org is not enrolled", () => {
    mockUseShowAgentsOptIn.mockReturnValue(false);
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      is_instance_admin: false,
      provisioning_state: "not_provisioned",
      organization_id: "test-org-123",
      organization_kind: null,
      external_cloud_eligible: true,
      eligible_external_organization_id: "test-org-123",
    });

    renderWithIntl();

    expect(screen.getByTestId("context-layer-unavailable")).toBeInTheDocument();
  });

  it("renders the enrolled page when the opt-in flag is off but the org is enrolled", () => {
    mockUseShowAgentsOptIn.mockReturnValue(false);
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: false,
      eligible_external_organization_id: null,
    });

    renderWithIntl();

    expect(screen.getByTestId("context-layer-card")).toBeInTheDocument();
  });

  it("keeps the terms modal open and shows an error when enrollment fails", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      is_instance_admin: false,
      provisioning_state: "not_provisioned",
      organization_id: "test-org-123",
      organization_kind: null,
      external_cloud_eligible: true,
      eligible_external_organization_id: "test-org-123",
    });
    const mutateAsync = jest.fn().mockRejectedValue(new Error("enrollment failed"));
    mockUseEnrollOrganizationInAgents.mockReturnValue({ mutateAsync } as never);
    const registerNotification = jest.fn();
    mockUseNotificationService.mockReturnValue({ registerNotification } as never);
    const onComplete = jest.fn();
    mockUseModalService.mockReturnValue({
      openModal: jest.fn().mockImplementation(({ content }) => {
        const Content = content;
        return Promise.resolve(
          render(
            <IntlProvider locale="en" messages={messages}>
              <Content onCancel={jest.fn()} onComplete={onComplete} />
            </IntlProvider>
          )
        );
      }),
    } as never);

    renderWithIntl();
    fireEvent.click(screen.getByRole("button", { name: "Enable Context Layer" }));
    fireEvent.click(screen.getByRole("checkbox", { name: messages["cloud.contextLayer.terms.acceptTerms"] }));
    fireEvent.click(screen.getByRole("button", { name: "Accept and Enable" }));

    await waitFor(() => {
      expect(registerNotification).toHaveBeenCalledWith({
        id: "context-layer-enrollment-error",
        text: messages["cloud.contextLayer.terms.enrollError"],
        type: "error",
      });
    });
    expect(onComplete).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Accept and Enable" })).toBeInTheDocument();
    expect(mutateAsync).not.toHaveBeenCalled();
  });

  it("shows the admin-only message instead of the enable button", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      is_instance_admin: false,
      provisioning_state: "not_provisioned",
      organization_id: "test-org-123",
      organization_kind: null,
      external_cloud_eligible: true,
      eligible_external_organization_id: "test-org-123",
    });
    mockUseGeneratedIntent.mockImplementation((intent) => intent !== Intent.UpdateOrganizationPermissions);

    renderWithIntl();

    expect(screen.queryByRole("button", { name: "Enable Context Layer" })).not.toBeInTheDocument();
    expect(screen.getByTestId("context-layer-admin-required")).toHaveTextContent(
      "Enabling the context layer requires an organization admin."
    );
    expect(screen.getByTestId("context-layer-admin-required")).toHaveTextContent(
      "Ask an organization admin of your Airbyte organization to enable this feature for your team."
    );
  });
});
