import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import { mockExperiments } from "test-utils/mockExperiments";

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
import { NotificationService, useNotificationService } from "core/services/Notification";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ContextLayerConnectorsPage, ContextLayerPage } from "./ContextLayerPage";
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

jest.mock("core/services/Notification", () => ({
  ...jest.requireActual("core/services/Notification"),
  useNotificationService: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: () => true,
}));

jest.mock("core/utils/useAirbyteTheme", () => ({
  useAirbyteTheme: () => ({ theme: "airbyteThemeLight" }),
}));

jest.mock("./useShowAgentsOptIn", () => ({ useShowAgentsOptIn: jest.fn() }));

jest.mock("core/utils/links", () => ({
  links: {
    contextLayerDocs: "https://docs.airbyte.com/platform/context-layer",
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
const mockUseNotificationService = useNotificationService as jest.MockedFunction<typeof useNotificationService>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;

const messages = {
  "cloud.contextLayer.title": "Context layer settings",
  "cloud.contextLayer.subtitle":
    "AI agents can use the Airbyte MCP to access and reason about your organization's data. Only organization admins can enable or disable context layer features.",
  "cloud.contextLayer.subtitle.enabled":
    "The context layer is how AI agents access and reason about your organization's data. Only organization admins can enable or disable this feature. The context layer is currently free to use. Limits apply.",
  "cloud.contextLayer.subtitle.viewUsage": "View usage →",
  "cloud.contextLayer.unavailable.title": "Context layer not available",
  "cloud.contextLayer.unavailable.description":
    "The context layer is not available for this organization yet. Contact Airbyte support if you'd like to learn more.",
  "cloud.contextLayer.loadError.title": "Couldn't load context layer status",
  "cloud.contextLayer.loadError.description":
    "We couldn't check whether the context layer is available for this organization. Please try again.",
  "cloud.contextLayer.enable.error": "Agent access could not be enabled. Please try again.",
  "cloud.contextLayer.enable.sourcesError":
    "Agent access is on, but some sources could not be enabled. Enable them individually under Agent access.",
  "cloud.contextLayer.enable.adminRequired": "Enabling the context layer requires an organization admin.",
  "cloud.contextLayer.enable.adminRequired.description":
    "Ask an organization admin of your Airbyte organization to enable this feature for your team.",
  "cloud.contextLayer.organizationAgentAccess.title": "Agent access",
  "cloud.contextLayer.organizationAgentAccess.description":
    "Agents can use the Airbyte MCP to query and write to supported sources, and query supported destinations. Agent access is controlled by the credentials used to authenticate the connector in that workspace. All supported sources are enabled when you turn this on. Destinations must be enabled individually.",
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
  "cloud.contextLayer.agentAccess.pageTitle": "Agent access",
  "cloud.contextLayer.agentAccess.description.enabled":
    "Your agents can use the Airbyte MCP to read data directly from these sources and destinations. Only agent connectors are supported.",
  "cloud.contextLayer.agentAccess.description.disabled":
    "Choose which sources and destinations agents can access. Only Snowflake and BigQuery destinations are supported.",
  "cloud.contextLayer.agentAccess.nameColumn": "Name",
  "cloud.contextLayer.agentAccess.typeColumn": "Type",
  "cloud.contextLayer.agentAccess.tooltip":
    "Agents can use the Airbyte MCP to access supported sources and destinations directly. No sync required.",
  "cloud.contextLayer.agentAccess.noAccess.title": "Enable agent access",
  "cloud.contextLayer.agentAccess.noAccess.description":
    "Before you can control agent access to sources and destinations, you need to enable agent access.",
  "cloud.contextLayer.agentAccess.empty.title": "No connectors yet",
  "cloud.contextLayer.agentAccess.empty.description":
    "Add sources or destinations to Airbyte to make them available to agents.",
  "cloud.contextLayer.agentAccess.sourcesError": "Unable to load sources. Please try again.",
  "cloud.contextLayer.agentAccess.destinationsError": "Unable to load destinations. Please try again.",
  "connector.source": "Source",
  "connector.destination": "Destination",
  "cloud.contextLayer.setup.agentAccess.title": "Agent access",
  "cloud.contextLayer.agentAccess.controlLabel": "{name} Agent access",
  "cloud.contextLayer.noAccess.settings": "Go to settings",
  "cloud.contextLayer.sources.empty.add": "Add your first source",
  "cloud.contextLayer.destinations.empty.add": "Add your first destination",
  "cloud.contextLayer.workspace.loading": "Loading workspaces...",
  "cloud.contextLayer.workspace.loadMore": "Load more workspaces",
  "cloud.contextLayer.workspace.loadingMore": "Loading more workspaces...",
  "cloud.contextLayer.workspace.loadMoreError": "Some workspaces could not be loaded.",
  "cloud.contextLayer.workspace.empty": "No workspaces found in this organization.",
  "cloud.contextLayer.workspace.connectorsLoading": "Loading connectors...",
  "cloud.contextLayer.workspace.noKindConnectors":
    "No {actorKind, select, source {sources} other {destinations}} found in this workspace.",
  "cloud.contextLayer.workspace.kindCount": "{count} {actorKind, select, source {sources} other {destinations}}",
  "cloud.contextLayer.workspace.sources": "Sources",
  "cloud.contextLayer.workspace.destinations": "Destinations",
  "cloud.contextLayer.workspace.enabledCount":
    "{enabled} of {supported} enabled{unsupported, plural, =0 {} other { (excludes {unsupported} not supported)}}",
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

const renderConnectorsWithIntl = () =>
  render(
    <MemoryRouter>
      <IntlProvider locale="en" messages={messages}>
        <NotificationService>
          <ConfirmationModalService>
            <ContextLayerConnectorsPage />
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
    mockExperiments({ "platform.fusion-semantic-search-ui": true });
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
      sourcesLoading: false,
      destinationsLoading: false,
      sourcesError: false,
      destinationsError: false,
    });
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync: jest.fn() } as never);
    mockUseGeneratedIntent.mockReturnValue(true);
    mockUseNotificationService.mockReturnValue({ registerNotification: jest.fn() } as never);
    mockUseEnrollOrganizationInAgents.mockReturnValue({ mutateAsync: jest.fn() } as never);
    jest
      .mocked(useEnableFusionWorkspaceActors)
      .mockReturnValue({ mutateAsync: jest.fn().mockResolvedValue([]) } as never);
    mockUseUnenrollOrganizationFromAgents.mockReturnValue({ mutateAsync: jest.fn() } as never);
  });

  it("renders the agent access copy with only a switch", () => {
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

    expect(screen.getByRole("heading", { name: "Context layer settings" })).toBeInTheDocument();
    expect(screen.getByText(messages["cloud.contextLayer.subtitle"])).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Agent access" })).not.toBeChecked();
    expect(screen.getByText(messages["cloud.contextLayer.organizationAgentAccess.description"])).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /enable context layer/i })).not.toBeInTheDocument();
    expect(screen.queryByText(/terms of service/i)).not.toBeInTheDocument();
  });

  it("enables agent access from the switch across all workspace pages", async () => {
    let isEnrolled = false;
    mockUseAgentsProvisioningStatus.mockImplementation(() => ({
      is_enrolled: isEnrolled,
      is_instance_admin: false,
      provisioning_state: isEnrolled ? "provisioned" : "not_provisioned",
      organization_id: "test-org-123",
      organization_kind: null,
      external_cloud_eligible: true,
      eligible_external_organization_id: "test-org-123",
    }));
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
    const mutateAsync = jest.fn().mockImplementation(async () => {
      isEnrolled = true;
    });
    mockUseEnrollOrganizationInAgents.mockReturnValue({ mutateAsync } as never);
    const enableActors = jest.fn().mockResolvedValue([]);
    jest.mocked(useEnableFusionWorkspaceActors).mockReturnValue({ mutateAsync: enableActors } as never);

    renderWithIntl();
    const toggle = screen.getByRole("checkbox", { name: "Agent access" });
    expect(toggle).not.toBeChecked();
    fireEvent.click(toggle);
    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        workspaceIds: ["workspace-1", "workspace-2"],
        addAllSupportedActors: false,
      })
    );
    await waitFor(() => expect(enableActors).toHaveBeenCalledWith({ workspaceIds: ["workspace-1", "workspace-2"] }));
    await waitFor(() => expect(toggle).toBeChecked());
  });

  it.each(["inventory", "actor"])("shows an error when %s enablement fails", async (failure) => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      external_cloud_eligible: true,
    } as never);
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
    } as never);
    const enableActors = jest
      .fn()
      .mockImplementation(() =>
        failure === "inventory"
          ? Promise.reject(new Error("Inventory unavailable"))
          : Promise.resolve([{ actorId: "source-1", actorKind: "source", workspaceId: "workspace-1" }])
      );
    jest.mocked(useEnableFusionWorkspaceActors).mockReturnValue({ mutateAsync: enableActors } as never);
    const registerNotification = jest.fn();
    mockUseNotificationService.mockReturnValue({ registerNotification } as never);

    renderWithIntl();
    fireEvent.click(screen.getByRole("checkbox", { name: "Agent access" }));
    await waitFor(() => expect(enableActors).toHaveBeenCalledTimes(1));
    await waitFor(() =>
      expect(registerNotification).toHaveBeenCalledWith({
        id: "context-layer-enrollment-error",
        text: messages["cloud.contextLayer.enable.sourcesError"],
        type: "error",
      })
    );
  });

  it("keeps connector access off Settings for an enrolled organization", () => {
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

    expect(screen.getByRole("heading", { name: "Context layer settings" })).toBeInTheDocument();
    expect(screen.getByText(messages["cloud.contextLayer.subtitle.enabled"])).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "View usage →" })).toHaveAttribute(
      "href",
      "/organization/test-org-123/settings/organization-usage"
    );
    expect(screen.queryByText("Context layer sources")).not.toBeInTheDocument();
    expect(screen.queryByText("Context layer destinations")).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Learn how to connect agents (SDK, API, MCP)" })).not.toBeInTheDocument();
  });

  it("omits the usage link when organization usage is not available", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true, external_cloud_eligible: true } as never);
    mockUseGeneratedIntent.mockImplementation((intent) => intent !== Intent.ViewOrganizationUsage);

    renderWithIntl();

    expect(screen.getByText(messages["cloud.contextLayer.subtitle.enabled"])).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "View usage →" })).not.toBeInTheDocument();
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

    const toggle = screen.getByRole("checkbox", { name: "Agent access" });
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

    fireEvent.click(screen.getByRole("checkbox", { name: "Agent access" }));
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

    fireEvent.click(screen.getByRole("checkbox", { name: "Agent access" }));
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

    expect(screen.getByRole("checkbox", { name: "Agent access" })).toBeDisabled();
  });

  it("renders a mixed Name, Type, Agent access table per workspace", async () => {
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
                icon: "https://example.com/github.svg",
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
            sourcesLoading: false,
            destinationsLoading: false,
            sourcesError: false,
            destinationsError: false,
          }
        : {
            sources: [
              {
                id: "source-3",
                name: "Gong account",
                supported: true,
                enabled: false,
                state: { enable_agent_access: false, enable_indexing: false },
              },
            ],
            destinations: [],
            isLoading: false,
            sourcesLoading: false,
            destinationsLoading: false,
            sourcesError: false,
            destinationsError: false,
          }
    );

    renderConnectorsWithIntl();

    expect(mockUseFusionWorkspaceConnectors).toHaveBeenCalledWith("workspace-1", { hydrate: true });
    expect(mockUseFusionWorkspaceConnectors).toHaveBeenCalledWith("workspace-2", { hydrate: true });
    expect(screen.getAllByRole("columnheader", { name: "Name" })).toHaveLength(2);
    expect(screen.getAllByRole("columnheader", { name: "Type" })).toHaveLength(2);
    expect(screen.getByText("GitHub account").closest("tr")).toHaveTextContent("Source");
    expect(screen.getByText("Snowflake warehouse").closest("tr")).toHaveTextContent("Destination");
    expect(screen.getAllByRole("columnheader", { name: /Agent access/ })).toHaveLength(2);
    expect(screen.queryByRole("columnheader", { name: /Semantic search/ })).not.toBeInTheDocument();
    expect(screen.getByText("GitHub account")).toBeInTheDocument();
    expect(screen.getByText("Stripe account")).toBeInTheDocument();
    expect(screen.getByText("Gong account")).toBeInTheDocument();
    expect(screen.getByText("GitHub account").closest("tr")?.querySelector("img")).toHaveAttribute(
      "src",
      "https://example.com/github.svg"
    );
    expect(screen.getByText("BigQuery warehouse")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "GitHub account Agent access" })).toBeChecked();
    expect(screen.getByRole("checkbox", { name: "Stripe account Agent access" })).not.toBeChecked();
    fireEvent.click(screen.getByRole("checkbox", { name: "GitHub account Agent access" }));
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

    expect(screen.getByRole("checkbox", { name: "BigQuery warehouse Agent access" })).toBeDisabled();
  });

  it("shows source connectors while destination inventory is loading", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      hasNextPage: false,
      isFetchingNextPage: false,
      isLoading: false,
    } as never);
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "source-1", name: "GitHub account", supported: true, enabled: false }],
      destinations: [],
      isLoading: true,
      sourcesLoading: false,
      destinationsLoading: true,
      sourcesError: false,
      destinationsError: false,
    });

    renderConnectorsWithIntl();

    expect(screen.getByTestId("context-layer-workspace-workspace-1")).toBeVisible();
    expect(screen.getByText("GitHub account")).toBeVisible();
    expect(screen.getByText("Loading connectors...")).toBeVisible();
  });

  it("shows a ready workspace while another workspace inventory is pending", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
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
      hasNextPage: false,
      isFetchingNextPage: false,
      isLoading: false,
    } as never);
    mockUseFusionWorkspaceConnectors.mockImplementation((workspaceId) => ({
      sources:
        workspaceId === "workspace-1"
          ? [{ id: "source-1", name: "GitHub account", supported: true, enabled: false }]
          : [],
      destinations: [],
      isLoading: workspaceId === "workspace-2",
      sourcesLoading: workspaceId === "workspace-2",
      destinationsLoading: false,
      sourcesError: false,
      destinationsError: false,
    }));

    renderConnectorsWithIntl();

    await waitFor(() => expect(screen.getByTestId("context-layer-workspace-workspace-1")).toBeVisible());
    expect(screen.getByText("Loading connectors...")).toBeVisible();
    expect(screen.getByText("GitHub account")).toBeVisible();
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

    renderConnectorsWithIntl();

    fireEvent.click(screen.getByRole("checkbox", { name: "GitHub account Agent access" }));
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

    fireEvent.click(screen.getByRole("checkbox", { name: "Stripe account Agent access" }));
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

  it("loads another workspace page only when requested", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-org-123",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    let workspacePages = [
      {
        workspaces: Array.from({ length: 25 }, (_, index) => ({
          workspaceId: `workspace-${index + 1}`,
          name: `Workspace ${index + 1}`,
        })),
      },
    ];
    let hasNextPage = true;
    const fetchNextPage = jest.fn().mockImplementation(async () => {
      workspacePages = [...workspacePages, { workspaces: [{ workspaceId: "workspace-26", name: "Workspace 26" }] }];
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

    const view = renderConnectorsWithIntl();

    expect(mockUseListWorkspacesInOrganization).toHaveBeenCalledWith(
      expect.objectContaining({ pagination: { pageSize: 25, rowOffset: 0 } })
    );
    expect(fetchNextPage).not.toHaveBeenCalled();
    expect(screen.getByTestId("context-layer-workspace-workspace-1")).toBeInTheDocument();
    expect(screen.getByTestId("context-layer-workspace-workspace-25")).toBeInTheDocument();
    expect(screen.queryByTestId("context-layer-workspace-workspace-26")).not.toBeInTheDocument();
    expect(mockUseFusionWorkspaceConnectors).not.toHaveBeenCalledWith("workspace-26", expect.anything());
    expect(screen.queryByText("No connectors yet")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Load more workspaces" }));
    await waitFor(() => expect(fetchNextPage).toHaveBeenCalledTimes(1));
    view.rerender(
      <MemoryRouter>
        <IntlProvider locale="en" messages={messages}>
          <NotificationService>
            <ConfirmationModalService>
              <ContextLayerConnectorsPage />
            </ConfirmationModalService>
          </NotificationService>
        </IntlProvider>
      </MemoryRouter>
    );
    expect(screen.getByTestId("context-layer-workspace-workspace-1")).toBeInTheDocument();
    expect(screen.getByTestId("context-layer-workspace-workspace-26")).toBeInTheDocument();
    expect(await screen.findByText("No connectors yet")).toBeInTheDocument();
  });

  it("shows only 25 cached workspaces until more are requested", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
    const fetchNextPage = jest.fn();
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: {
        pages: [
          {
            workspaces: Array.from({ length: 25 }, (_, index) => ({
              workspaceId: `workspace-${index + 1}`,
              name: `Workspace ${index + 1}`,
            })),
          },
          { workspaces: [{ workspaceId: "workspace-26", name: "Workspace 26" }] },
        ],
      },
      hasNextPage: false,
      fetchNextPage,
      isFetchingNextPage: false,
      isLoading: false,
    } as never);

    renderConnectorsWithIntl();

    expect(screen.queryByTestId("context-layer-workspace-workspace-26")).not.toBeInTheDocument();
    expect(mockUseFusionWorkspaceConnectors).not.toHaveBeenCalledWith("workspace-26", expect.anything());
    fireEvent.click(screen.getByRole("button", { name: "Load more workspaces" }));
    expect(screen.getByTestId("context-layer-workspace-workspace-26")).toBeInTheDocument();
    expect(fetchNextPage).not.toHaveBeenCalled();
  });

  it("resets the visible workspace count when the organization changes", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
    let organizationId = "organization-1";
    mockUseCurrentOrganizationId.mockImplementation(() => organizationId);
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: {
        pages: [
          {
            workspaces: Array.from({ length: 26 }, (_, index) => ({
              workspaceId: `workspace-${index + 1}`,
              name: `Workspace ${index + 1}`,
            })),
          },
        ],
      },
      hasNextPage: false,
      fetchNextPage: jest.fn(),
      isFetchingNextPage: false,
      isLoading: false,
    } as never);

    const view = renderConnectorsWithIntl();
    fireEvent.click(screen.getByRole("button", { name: "Load more workspaces" }));
    expect(screen.getByTestId("context-layer-workspace-workspace-26")).toBeInTheDocument();

    organizationId = "organization-2";
    view.rerender(
      <MemoryRouter>
        <IntlProvider locale="en" messages={messages}>
          <NotificationService>
            <ConfirmationModalService>
              <ContextLayerConnectorsPage />
            </ConfirmationModalService>
          </NotificationService>
        </IntlProvider>
      </MemoryRouter>
    );

    expect(screen.queryByTestId("context-layer-workspace-workspace-26")).not.toBeInTheDocument();
  });

  it("shows a retry after a workspace page fails without fetching again automatically", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
    let workspacePages = [
      {
        workspaces: Array.from({ length: 25 }, (_, index) => ({
          workspaceId: `workspace-${index + 1}`,
          name: `Workspace ${index + 1}`,
        })),
      },
    ];
    let isError = false;
    let hasNextPage = true;
    const fetchNextPage = jest.fn().mockImplementation(async () => {
      if (fetchNextPage.mock.calls.length === 1) {
        isError = true;
        return { data: { pages: workspacePages }, hasNextPage: true, isError: true };
      }
      isError = false;
      hasNextPage = false;
      workspacePages = [...workspacePages, { workspaces: [{ workspaceId: "workspace-26", name: "Workspace 26" }] }];
      return { data: { pages: workspacePages }, hasNextPage: false, isError: false };
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
          get isError() {
            return isError;
          },
          isLoading: false,
        }) as never
    );

    const view = renderConnectorsWithIntl();
    expect(fetchNextPage).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Load more workspaces" }));
    await waitFor(() => expect(fetchNextPage).toHaveBeenCalledTimes(1));
    view.rerender(
      <MemoryRouter>
        <IntlProvider locale="en" messages={messages}>
          <NotificationService>
            <ConfirmationModalService>
              <ContextLayerConnectorsPage />
            </ConfirmationModalService>
          </NotificationService>
        </IntlProvider>
      </MemoryRouter>
    );

    expect(fetchNextPage).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole("button", { name: "Load more workspaces" })).not.toBeInTheDocument();
    expect(screen.getByText("Some workspaces could not be loaded.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(fetchNextPage).toHaveBeenCalledTimes(2);
    expect(await screen.findByTestId("context-layer-workspace-workspace-26")).toBeInTheDocument();
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

    const view = renderConnectorsWithIntl();

    expect(screen.getByRole("checkbox", { name: "GitHub account Agent access" })).toBeDisabled();
    view.unmount();
    renderConnectorsWithIntl();
    expect(screen.getByRole("checkbox", { name: "BigQuery warehouse Agent access" })).toBeDisabled();
    expect(mutateAsync).not.toHaveBeenCalled();
  });

  it("enables only the matching workspace editor's switch", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      isLoading: false,
    } as never);
    mockUseGeneratedIntent.mockImplementation(
      (intent, meta) => intent === Intent.CreateOrEditDestination && meta?.workspaceId === "workspace-1"
    );
    const mutateAsync = jest.fn().mockResolvedValue({ enabled: true });
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync } as never);
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "source-1", name: "GitHub account", supported: true, enabled: false }],
      destinations: [{ id: "destination-1", name: "Snowflake warehouse", supported: true, enabled: false }],
      isLoading: false,
      sourcesLoading: false,
      destinationsLoading: false,
      sourcesError: false,
      destinationsError: false,
    });

    renderConnectorsWithIntl();
    expect(screen.getByRole("checkbox", { name: "GitHub account Agent access" })).toBeDisabled();
    const destinationSwitch = screen.getByRole("checkbox", { name: "Snowflake warehouse Agent access" });
    expect(destinationSwitch).toBeEnabled();
    fireEvent.click(destinationSwitch);
    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        actorId: "destination-1",
        actorKind: "destination",
        workspaceId: "workspace-1",
        expectedState: undefined,
        enabled: true,
      })
    );
    expect(mockUseGeneratedIntent).toHaveBeenCalledWith(Intent.CreateOrEditDestination, { workspaceId: "workspace-1" });
  });

  it.each([true, false])("shows the shared pre-enrollment state with semantic search flag %s", (showSemanticSearch) => {
    mockExperiments({ "platform.fusion-semantic-search-ui": showSemanticSearch });
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: false, external_cloud_eligible: true } as never);
    renderConnectorsWithIntl();
    expect(screen.getByRole("heading", { name: "Agent access" })).toBeInTheDocument();
    expect(screen.getByText(messages["cloud.contextLayer.agentAccess.description.disabled"])).toBeInTheDocument();
    expect(screen.getByText("Enable agent access")).toBeInTheDocument();
    expect(screen.getByText(messages["cloud.contextLayer.agentAccess.noAccess.description"])).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Go to settings" })).toBeInTheDocument();
    expect(screen.queryByText(/semantic search/i)).not.toBeInTheDocument();
    expect(mockUseListWorkspacesInOrganization).toHaveBeenCalledWith(expect.objectContaining({ enabled: false }));
  });

  it("sends the pre-enrollment action to Context Layer Settings", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: false, external_cloud_eligible: true } as never);

    render(
      <MemoryRouter initialEntries={["/organization/test-org-123/context-layer/agent-access"]}>
        <IntlProvider locale="en" messages={messages}>
          <Routes>
            <Route
              path="/organization/:organizationId/context-layer/agent-access"
              element={<ContextLayerConnectorsPage />}
            />
            <Route path="/organization/:organizationId/context-layer" element={<div>Settings destination</div>} />
          </Routes>
        </IntlProvider>
      </MemoryRouter>
    );

    fireEvent.click(screen.getByRole("button", { name: "Go to settings" }));
    expect(screen.getByText("Settings destination")).toBeInTheDocument();
  });

  it.each([
    ["Add your first source", "/workspaces/workspace-1/source/new-source"],
    ["Add your first destination", "/workspaces/workspace-1/destination/new-destination"],
  ])("shows the shared empty state and opens %s", async (action, path) => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      hasNextPage: false,
      isFetchingNextPage: false,
      isLoading: false,
    } as never);
    render(
      <MemoryRouter initialEntries={["/organization/test-org-123/context-layer/agent-access"]}>
        <IntlProvider locale="en" messages={messages}>
          <NotificationService>
            <ConfirmationModalService>
              <Routes>
                <Route
                  path="/organization/:organizationId/context-layer/agent-access"
                  element={<ContextLayerConnectorsPage />}
                />
                <Route path={path} element={<div>Connector setup</div>} />
              </Routes>
            </ConfirmationModalService>
          </NotificationService>
        </IntlProvider>
      </MemoryRouter>
    );
    expect(await screen.findByText("No connectors yet")).toBeInTheDocument();
    expect(screen.getByText(messages["cloud.contextLayer.agentAccess.description.enabled"])).toBeInTheDocument();
    expect(screen.getByText(messages["cloud.contextLayer.agentAccess.empty.description"])).toBeInTheDocument();
    expect(screen.getByTestId("context-layer-workspace-workspace-1")).not.toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: action }));
    expect(screen.getByText("Connector setup")).toBeInTheDocument();
  });

  it("waits for both inventories before showing the empty state", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      hasNextPage: false,
      isFetchingNextPage: false,
      isLoading: false,
    } as never);
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [],
      destinations: [],
      isLoading: true,
      sourcesLoading: false,
      destinationsLoading: true,
      sourcesError: false,
      destinationsError: false,
    });

    renderConnectorsWithIntl();
    expect(screen.queryByText("No connectors yet")).not.toBeInTheDocument();
    expect(screen.getByText("Loading connectors...")).toBeVisible();
  });

  it.each(["source", "destination"] as const)(
    "hides the %s empty-state action from read-only viewers",
    async (actorKind) => {
      mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
      mockUseListWorkspacesInOrganization.mockReturnValue({
        data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
        hasNextPage: false,
        isFetchingNextPage: false,
        isLoading: false,
      } as never);
      mockUseGeneratedIntent.mockReturnValue(false);

      renderConnectorsWithIntl();

      expect(await screen.findByText("No connectors yet")).toBeVisible();
      expect(
        screen.queryByRole("button", {
          name: actorKind === "source" ? "Add your first source" : "Add your first destination",
        })
      ).not.toBeInTheDocument();
    }
  );

  it("uses an authorized workspace when the stored workspace cannot create sources", async () => {
    window.localStorage.setItem(
      "airbyte_organization-workspace-map",
      JSON.stringify({ "test-org-123": "workspace-1" })
    );
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
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
      hasNextPage: false,
      isFetchingNextPage: false,
      isLoading: false,
    } as never);
    mockUseGeneratedIntent.mockImplementation(
      (intent, meta) => intent === Intent.CreateOrEditSource && meta?.workspaceId === "workspace-2"
    );

    render(
      <MemoryRouter initialEntries={["/organization/test-org-123/context-layer/agent-access"]}>
        <IntlProvider locale="en" messages={messages}>
          <NotificationService>
            <ConfirmationModalService>
              <Routes>
                <Route
                  path="/organization/:organizationId/context-layer/agent-access"
                  element={<ContextLayerConnectorsPage />}
                />
                <Route
                  path="/workspaces/workspace-2/source/new-source"
                  element={<div>Authorized connector setup</div>}
                />
              </Routes>
            </ConfirmationModalService>
          </NotificationService>
        </IntlProvider>
      </MemoryRouter>
    );

    expect(await screen.findByText("No connectors yet")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Add your first source" }));
    expect(screen.getByText("Authorized connector setup")).toBeInTheDocument();
  });

  it.each(["source", "destination"] as const)("shows successful rows when %s inventory fails", (kind) => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      isLoading: false,
    } as never);
    const connector = { id: "connector-1", name: "Ready connector", supported: true, enabled: false };
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: kind === "destination" ? [connector] : [],
      destinations: kind === "source" ? [connector] : [],
      isLoading: false,
      sourcesLoading: false,
      destinationsLoading: false,
      sourcesError: kind === "source",
      destinationsError: kind === "destination",
    });
    renderConnectorsWithIntl();
    expect(screen.getByTestId(`context-layer-${kind}-error`)).toHaveTextContent(
      `Unable to load ${kind}s. Please try again.`
    );
    expect(screen.getByText("Ready connector")).toBeVisible();
    expect(screen.getByRole("checkbox", { name: "Ready connector Agent access" })).toBeEnabled();
    expect(screen.queryByText("No connectors yet")).not.toBeInTheDocument();
    expect(screen.queryByText("Loading connectors...")).not.toBeInTheDocument();
  });

  it("does not treat failed empty inventories as an empty organization", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      isLoading: false,
    } as never);
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [],
      destinations: [],
      isLoading: false,
      sourcesLoading: false,
      destinationsLoading: false,
      sourcesError: true,
      destinationsError: true,
    });
    renderConnectorsWithIntl();
    expect(screen.getByTestId("context-layer-source-error")).toBeVisible();
    expect(screen.getByTestId("context-layer-destination-error")).toBeVisible();
    expect(screen.queryByText("No connectors yet")).not.toBeInTheDocument();
  });

  it("keeps source and destination rows with the same ID independent while saving", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      isLoading: false,
    } as never);
    const state = { enable_agent_access: false, enable_indexing: false };
    mockUseFusionWorkspaceConnectors.mockReturnValue({
      sources: [{ id: "same-id", name: "Source account", supported: true, enabled: false, state }],
      destinations: [{ id: "same-id", name: "Destination account", supported: true, enabled: false, state }],
      isLoading: false,
      sourcesLoading: false,
      destinationsLoading: false,
      sourcesError: false,
      destinationsError: false,
    });
    let resolveSource!: (value: { enabled: boolean }) => void;
    const mutateAsync = jest.fn().mockImplementation(({ actorKind }) =>
      actorKind === "source"
        ? new Promise((resolve) => {
            resolveSource = resolve;
          })
        : Promise.resolve({ enabled: true })
    );
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync } as never);
    renderConnectorsWithIntl();
    const sourceSwitch = () => screen.getByRole("checkbox", { name: "Source account Agent access" });
    const destinationSwitch = () => screen.getByRole("checkbox", { name: "Destination account Agent access" });
    fireEvent.click(sourceSwitch());
    expect(sourceSwitch()).toBeChecked();
    expect(sourceSwitch()).toBeDisabled();
    expect(destinationSwitch()).not.toBeChecked();
    expect(destinationSwitch()).toBeEnabled();
    fireEvent.click(destinationSwitch());
    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        actorId: "same-id",
        actorKind: "destination",
        workspaceId: "workspace-1",
        expectedState: state,
        enabled: true,
      })
    );
    expect(mutateAsync).toHaveBeenCalledWith({
      actorId: "same-id",
      actorKind: "source",
      workspaceId: "workspace-1",
      expectedState: state,
      enabled: true,
    });
    expect(sourceSwitch()).toBeDisabled();
    resolveSource({ enabled: true });
    await waitFor(() => expect(sourceSwitch()).toBeEnabled());
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

    const view = renderConnectorsWithIntl();
    const checkbox = screen.getByRole("checkbox", { name: "GitHub account Agent access" });
    fireEvent.click(checkbox);
    fireEvent.click(await screen.findByRole("button", { name: "Disable" }));
    await waitFor(() => expect(mutateAsync).toHaveBeenCalled());
    view.rerender(
      <MemoryRouter>
        <IntlProvider locale="en" messages={messages}>
          <NotificationService>
            <ConfirmationModalService>
              <ContextLayerConnectorsPage />
            </ConfirmationModalService>
          </NotificationService>
        </IntlProvider>
      </MemoryRouter>
    );

    expect(screen.getByRole("checkbox", { name: "GitHub account Agent access" })).toBeChecked();
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

    renderConnectorsWithIntl();
    fireEvent.click(screen.getByRole("checkbox", { name: "GitHub account Agent access" }));
    fireEvent.click(await screen.findByRole("button", { name: "Disable" }));
    await waitFor(() => expect(mutateAsync).toHaveBeenCalled());
    await waitFor(() => expect(screen.getByRole("checkbox", { name: "GitHub account Agent access" })).toBeChecked());
    expect(registerNotification).toHaveBeenCalledWith({
      id: "context-layer-connector-toggle-error-workspace-1:source:source-1",
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

    renderConnectorsWithIntl();

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
    expect(screen.queryByRole("checkbox", { name: "Agent access" })).not.toBeInTheDocument();
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

  it("shows the enable card when the API reports an eligible unenrolled organization", () => {
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

    expect(screen.getByTestId("context-layer-card")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Agent access" })).not.toBeChecked();
  });

  it("hides enrollment and connector access for an unenrolled organization when the opt-in is disabled", () => {
    mockUseShowAgentsOptIn.mockReturnValue(false);
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      external_cloud_eligible: true,
    } as never);

    const settingsView = renderWithIntl();
    expect(screen.getByTestId("context-layer-unavailable")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Enable Context Layer" })).not.toBeInTheDocument();

    settingsView.unmount();
    renderConnectorsWithIntl();
    expect(screen.getByTestId("context-layer-unavailable")).toBeInTheDocument();
    expect(screen.queryByText("Go to settings")).not.toBeInTheDocument();
  });

  it("shows the unavailable page instead of connector access for an ineligible organization", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      external_cloud_eligible: false,
    } as never);

    renderConnectorsWithIntl();

    expect(screen.getByTestId("context-layer-unavailable")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Add your first source" })).not.toBeInTheDocument();
    expect(mockUseListWorkspacesInOrganization).toHaveBeenCalledWith(expect.objectContaining({ enabled: false }));
    expect(mockUseFusionWorkspaceConnectors).not.toHaveBeenCalled();
  });

  it("renders the enrolled page when external eligibility is false", () => {
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

  it("shows an error and leaves the switch off when enrollment fails", async () => {
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
    } as never);
    const mutateAsync = jest.fn().mockRejectedValue(new Error("enrollment failed"));
    mockUseEnrollOrganizationInAgents.mockReturnValue({ mutateAsync } as never);
    const registerNotification = jest.fn();
    mockUseNotificationService.mockReturnValue({ registerNotification } as never);
    renderWithIntl();
    fireEvent.click(screen.getByRole("checkbox", { name: "Agent access" }));

    await waitFor(() => {
      expect(registerNotification).toHaveBeenCalledWith({
        id: "context-layer-enrollment-error",
        text: messages["cloud.contextLayer.enable.error"],
        type: "error",
      });
    });
    expect(mutateAsync).toHaveBeenCalledWith({ workspaceIds: ["workspace-1"], addAllSupportedActors: false });
    expect(screen.getByRole("checkbox", { name: "Agent access" })).not.toBeChecked();
  });

  it("stops paging workspaces before enrollment when a workspace page fails", async () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      is_instance_admin: false,
      provisioning_state: "not_provisioned",
      organization_id: "test-org-123",
      organization_kind: null,
      external_cloud_eligible: true,
      eligible_external_organization_id: "test-org-123",
    });
    const fetchNextPage = jest.fn().mockResolvedValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      hasNextPage: true,
      isError: true,
      error: new Error("Too many requests"),
    });
    mockUseListWorkspacesInOrganization.mockReturnValue({
      data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
      hasNextPage: true,
      fetchNextPage,
    } as never);
    const mutateAsync = jest.fn().mockResolvedValue(undefined);
    mockUseEnrollOrganizationInAgents.mockReturnValue({ mutateAsync } as never);
    const registerNotification = jest.fn();
    mockUseNotificationService.mockReturnValue({ registerNotification } as never);
    renderWithIntl();
    fireEvent.click(screen.getByRole("checkbox", { name: "Agent access" }));

    await waitFor(() => {
      expect(registerNotification).toHaveBeenCalledWith({
        id: "context-layer-enrollment-error",
        text: messages["cloud.contextLayer.enable.error"],
        type: "error",
      });
    });
    expect(fetchNextPage).toHaveBeenCalledTimes(1);
    expect(mutateAsync).not.toHaveBeenCalled();
  });

  it("disables the switch and shows the admin-only message for non-admin viewers", () => {
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

    expect(screen.getByRole("checkbox", { name: "Agent access" })).toBeDisabled();
    expect(screen.getByTestId("context-layer-admin-required")).toHaveTextContent(
      "Enabling the context layer requires an organization admin."
    );
    expect(screen.getByTestId("context-layer-admin-required")).toHaveTextContent(
      "Ask an organization admin of your Airbyte organization to enable this feature for your team."
    );
  });
});
