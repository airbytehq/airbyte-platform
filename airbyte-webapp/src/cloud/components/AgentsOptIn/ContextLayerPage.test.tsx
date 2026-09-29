import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter, Route, Routes } from "react-router-dom";

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
    "Agent access is on, but some sources could not be enabled. Enable them individually under Context layer sources.",
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
  "cloud.contextLayer.sources.title": "Context layer sources",
  "cloud.contextLayer.setup.agentAccess.title": "Agent access",
  "cloud.contextLayer.agentAccess.controlLabel": "{name} Agent access",
  "cloud.contextLayer.setup.semanticSearch.title": "Semantic search",
  "cloud.contextLayer.sources.semanticSearch.tooltip":
    "Builds a semantic index from your data each time you sync. Then, you can use the Airbyte MCP to search your data. Requires a connection to a supported destination.",
  "cloud.contextLayer.sources.semanticSearch.controlLabel": "{name} Semantic search",
  "cloud.contextLayer.sources.semanticSearch.disableTitle": "Turn off semantic search",
  "cloud.contextLayer.sources.semanticSearch.disableText":
    "Turning off semantic search also deletes your semantic index. If you enable this again, you will need to backfill your data again, which will incur additional compute costs on your destination.",
  "cloud.contextLayer.sources.semanticSearch.disableSubmit": "Delete semantic index",
  "cloud.contextLayer.sources.semanticSearch.toggleError":
    "Could not update semantic search for {name}. Please try again.",
  "cloud.contextLayer.sources.column": "Source",
  "cloud.contextLayer.sources.agentAccess.tooltip":
    "Use the Airbyte MCP to query the source directly for up-to-date direct API results. You don’t need to create a connection in Airbyte.",
  "cloud.contextLayer.sources.description":
    "Choose which sources populate your context layer. Backfills are free in Airbyte but incur compute costs from your destination.",
  "cloud.contextLayer.destinations.title": "Context layer destinations",
  "cloud.contextLayer.destinations.column": "Destination",
  "cloud.contextLayer.destinations.agentAccess.tooltip": "Query the destination directly, no sync required.",
  "cloud.contextLayer.destinations.description":
    "Choose which destinations populate your context layer. Only Snowflake and BigQuery are supported.",
  "cloud.contextLayer.destinations.noAccess.pageDescription":
    "Choose which destinations populate your context layer. Backfills are free in Airbyte but incur compute costs from your destination.",
  "cloud.contextLayer.sources.noAccess.title": "Enable agent access or semantic search",
  "cloud.contextLayer.sources.noAccess.description":
    "Before you can control agent access to sources, you need to enable agent access or semantic search",
  "cloud.contextLayer.destinations.noAccess.title": "Enable agent access",
  "cloud.contextLayer.destinations.noAccess.description":
    "Before you can control agent access to destinations, you need to enable agent access",
  "cloud.contextLayer.noAccess.settings": "Go to settings",
  "cloud.contextLayer.sources.empty.title": "No sources yet",
  "cloud.contextLayer.sources.empty.description": "Add sources to Airbyte to start populating your context layer.",
  "cloud.contextLayer.sources.empty.add": "Add your first source",
  "cloud.contextLayer.destinations.empty.title": "No destinations yet",
  "cloud.contextLayer.destinations.empty.description":
    "Add destinations to Airbyte to start populating your context layer.",
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

const renderConnectorsWithIntl = (actorKind: "source" | "destination" = "source") =>
  render(
    <MemoryRouter>
      <IntlProvider locale="en" messages={messages}>
        <NotificationService>
          <ConfirmationModalService>
            <ContextLayerConnectorsPage actorKind={actorKind} />
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

    const view = renderConnectorsWithIntl();

    expect(mockUseFusionWorkspaceConnectors).toHaveBeenCalledWith("workspace-1", { hydrate: true });
    expect(mockUseFusionWorkspaceConnectors).toHaveBeenCalledWith("workspace-2", { hydrate: true });
    expect(screen.getAllByRole("columnheader", { name: "Source" })).toHaveLength(2);
    expect(screen.getAllByRole("columnheader", { name: /Agent access/ })).toHaveLength(2);
    expect(screen.getAllByRole("columnheader", { name: /Semantic search/ })).toHaveLength(2);
    expect(screen.getByText("GitHub account")).toBeInTheDocument();
    expect(screen.getByText("Stripe account")).toBeInTheDocument();
    expect(screen.getByText("Gong account")).toBeInTheDocument();
    expect(screen.getByText("GitHub account").closest("tr")?.querySelector("img")).toHaveAttribute(
      "src",
      "https://example.com/github.svg"
    );
    expect(screen.queryByText("BigQuery warehouse")).not.toBeInTheDocument();
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

    view.unmount();
    renderConnectorsWithIntl("destination");
    expect(screen.getAllByRole("columnheader", { name: "Destination" })).toHaveLength(1);
    expect(screen.queryByRole("columnheader", { name: /Semantic search/ })).not.toBeInTheDocument();
    expect(screen.queryByText("GitHub account")).not.toBeInTheDocument();
    expect(screen.getByText("BigQuery warehouse")).toBeInTheDocument();
    expect(screen.getByText("Snowflake warehouse")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "BigQuery warehouse Agent access" })).toBeDisabled();
  });

  describe("source semantic search", () => {
    let sourceState: { enable_agent_access: boolean; enable_indexing: boolean };
    let sourceSupported: boolean;
    let sourceLoading: boolean;
    let sourceError: boolean;

    beforeEach(() => {
      mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true } as never);
      mockUseListWorkspacesInOrganization.mockReturnValue({
        data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
        hasNextPage: false,
        isLoading: false,
      } as never);
      sourceState = { enable_agent_access: true, enable_indexing: false };
      sourceSupported = true;
      sourceLoading = false;
      sourceError = false;
      mockUseFusionWorkspaceConnectors.mockImplementation(() => ({
        sources: [
          {
            id: "source-1",
            name: "GitHub account",
            supported: sourceSupported,
            enabled: sourceState.enable_agent_access,
            state: sourceState,
            loading: sourceLoading,
            error: sourceError,
          },
        ],
        destinations: [],
        isLoading: false,
        sourcesLoading: false,
        destinationsLoading: false,
        sourcesError: false,
        destinationsError: false,
      }));
    });

    it("shows the Figma semantic-search tooltip", async () => {
      renderConnectorsWithIntl();

      fireEvent.mouseOver(screen.getByRole("columnheader", { name: /Semantic search/ }).querySelector("span")!);
      expect(await screen.findByRole("tooltip")).toHaveTextContent(
        messages["cloud.contextLayer.sources.semanticSearch.tooltip"]
      );
    });

    it("shows source help when its info controls receive keyboard focus", async () => {
      renderConnectorsWithIntl();

      const semanticHelp = screen.getByRole("button", {
        name: messages["cloud.contextLayer.sources.semanticSearch.tooltip"],
      });
      fireEvent.focus(semanticHelp);
      expect(await screen.findByRole("tooltip")).toHaveTextContent(
        messages["cloud.contextLayer.sources.semanticSearch.tooltip"]
      );
      fireEvent.blur(semanticHelp);
      await waitFor(() => expect(screen.queryByRole("tooltip")).not.toBeInTheDocument());

      fireEvent.focus(screen.getByRole("button", { name: messages["cloud.contextLayer.sources.agentAccess.tooltip"] }));
      expect(await screen.findByRole("tooltip")).toHaveTextContent(
        messages["cloud.contextLayer.sources.agentAccess.tooltip"]
      );
    });

    it.each([
      { access: true, indexing: true, checked: true, disabled: false },
      { access: true, indexing: false, checked: false, disabled: false },
      { access: false, indexing: false, checked: false, disabled: true },
    ])(
      "shows independent source controls for access=$access and indexing=$indexing",
      ({ access, indexing, checked, disabled }) => {
        sourceState = { enable_agent_access: access, enable_indexing: indexing };
        renderConnectorsWithIntl();

        const accessSwitch = screen.getByRole("checkbox", { name: "GitHub account Agent access" });
        const semanticSwitch = screen.getByRole("checkbox", { name: "GitHub account Semantic search" });
        expect(accessSwitch).toHaveProperty("checked", access);
        expect(semanticSwitch).toHaveProperty("checked", checked);
        expect(semanticSwitch).toHaveProperty("disabled", disabled);
      }
    );

    it.each(["unsupported", "loading", "error", "read-only"] as const)(
      "disables semantic search when the source is %s",
      (condition) => {
        sourceSupported = condition !== "unsupported";
        sourceLoading = condition === "loading";
        sourceError = condition === "error";
        if (condition === "read-only") {
          mockUseGeneratedIntent.mockReturnValue(false);
        }
        renderConnectorsWithIntl();

        expect(screen.getByRole("checkbox", { name: "GitHub account Semantic search" })).toBeDisabled();
      }
    );

    it("writes full state with expected state and locks both controls during a pending update", async () => {
      let finishMutation: (state: { enable_agent_access: boolean; enable_indexing: boolean }) => void = () => {};
      const mutateAsync = jest.fn().mockImplementation(
        () =>
          new Promise((resolve) => {
            finishMutation = resolve;
          })
      );
      mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync } as never);
      renderConnectorsWithIntl();

      fireEvent.click(screen.getByRole("checkbox", { name: "GitHub account Semantic search" }));
      await waitFor(() =>
        expect(mutateAsync).toHaveBeenCalledWith({
          actorId: "source-1",
          workspaceId: "workspace-1",
          actorKind: "source",
          expectedState: { enable_agent_access: true, enable_indexing: false },
          state: { enable_agent_access: true, enable_indexing: true },
        })
      );
      expect(screen.getByRole("checkbox", { name: "GitHub account Semantic search" })).toBeChecked();
      expect(screen.getByRole("checkbox", { name: "GitHub account Semantic search" })).toBeDisabled();
      expect(screen.getByRole("checkbox", { name: "GitHub account Agent access" })).toBeDisabled();

      sourceState = { enable_agent_access: true, enable_indexing: true };
      finishMutation(sourceState);
      await waitFor(() => expect(screen.getByRole("checkbox", { name: "GitHub account Agent access" })).toBeEnabled());
      expect(screen.getByRole("checkbox", { name: "GitHub account Semantic search" })).toBeChecked();
    });

    it("confirms index deletion and leaves state unchanged on cancel", async () => {
      sourceState = { enable_agent_access: true, enable_indexing: true };
      const mutateAsync = jest.fn().mockResolvedValue({ enable_agent_access: true, enable_indexing: false });
      mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync } as never);
      renderConnectorsWithIntl();

      fireEvent.click(screen.getByRole("checkbox", { name: "GitHub account Semantic search" }));
      expect(screen.getByText("Turn off semantic search")).toBeInTheDocument();
      expect(screen.getByText(messages["cloud.contextLayer.sources.semanticSearch.disableText"])).toBeInTheDocument();
      expect(mutateAsync).not.toHaveBeenCalled();
      fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
      expect(screen.getByRole("checkbox", { name: "GitHub account Semantic search" })).toBeChecked();
      expect(mutateAsync).not.toHaveBeenCalled();

      fireEvent.click(screen.getByRole("checkbox", { name: "GitHub account Semantic search" }));
      fireEvent.click(screen.getByRole("button", { name: "Delete semantic index" }));
      await waitFor(() =>
        expect(mutateAsync).toHaveBeenCalledWith({
          actorId: "source-1",
          workspaceId: "workspace-1",
          actorKind: "source",
          expectedState: { enable_agent_access: true, enable_indexing: true },
          state: { enable_agent_access: true, enable_indexing: false },
        })
      );
    });

    it("reverts a failed update and reports a semantic-search error", async () => {
      const mutateAsync = jest.fn().mockRejectedValue(new Error("Forbidden"));
      const registerNotification = jest.fn();
      mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync } as never);
      mockUseNotificationService.mockReturnValue({ registerNotification } as never);
      renderConnectorsWithIntl();

      fireEvent.click(screen.getByRole("checkbox", { name: "GitHub account Semantic search" }));
      await waitFor(() =>
        expect(screen.getByRole("checkbox", { name: "GitHub account Semantic search" })).not.toBeChecked()
      );
      expect(registerNotification).toHaveBeenCalledWith(
        expect.objectContaining({
          text: "Could not update semantic search for GitHub account. Please try again.",
          type: "error",
        })
      );
    });
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
    expect(screen.queryByText("Loading connectors...")).not.toBeInTheDocument();
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
    expect(screen.getByTestId("context-layer-workspace-workspace-2")).not.toBeVisible();
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

  it.each(["source", "destination"] as const)(
    "loads another %s workspace page only when requested",
    async (actorKind) => {
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

      const view = renderConnectorsWithIntl(actorKind);

      expect(mockUseListWorkspacesInOrganization).toHaveBeenCalledWith(
        expect.objectContaining({ pagination: { pageSize: 25, rowOffset: 0 } })
      );
      expect(fetchNextPage).not.toHaveBeenCalled();
      expect(screen.getByTestId("context-layer-workspace-workspace-1")).toBeInTheDocument();
      expect(screen.getByTestId("context-layer-workspace-workspace-25")).toBeInTheDocument();
      expect(screen.queryByTestId("context-layer-workspace-workspace-26")).not.toBeInTheDocument();
      expect(mockUseFusionWorkspaceConnectors).not.toHaveBeenCalledWith("workspace-26", expect.anything());
      expect(
        screen.queryByText(actorKind === "source" ? "No sources yet" : "No destinations yet")
      ).not.toBeInTheDocument();

      fireEvent.click(screen.getByRole("button", { name: "Load more workspaces" }));
      await waitFor(() => expect(fetchNextPage).toHaveBeenCalledTimes(1));
      view.rerender(
        <MemoryRouter>
          <IntlProvider locale="en" messages={messages}>
            <NotificationService>
              <ConfirmationModalService>
                <ContextLayerConnectorsPage actorKind={actorKind} />
              </ConfirmationModalService>
            </NotificationService>
          </IntlProvider>
        </MemoryRouter>
      );
      expect(screen.getByTestId("context-layer-workspace-workspace-1")).toBeInTheDocument();
      expect(screen.getByTestId("context-layer-workspace-workspace-26")).toBeInTheDocument();
      expect(
        await screen.findByText(actorKind === "source" ? "No sources yet" : "No destinations yet")
      ).toBeInTheDocument();
    }
  );

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
              <ContextLayerConnectorsPage actorKind="source" />
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
              <ContextLayerConnectorsPage actorKind="source" />
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
    renderConnectorsWithIntl("destination");
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

    const sourceView = renderConnectorsWithIntl();
    expect(screen.getByRole("checkbox", { name: "GitHub account Agent access" })).toBeDisabled();
    sourceView.unmount();
    renderConnectorsWithIntl("destination");
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

  it.each([
    [
      "source",
      "Context layer sources",
      "Choose which sources populate your context layer. Backfills are free in Airbyte but incur compute costs from your destination.",
      "Enable agent access or semantic search",
      "Before you can control agent access to sources, you need to enable agent access or semantic search",
    ],
    [
      "destination",
      "Context layer destinations",
      "Choose which destinations populate your context layer. Backfills are free in Airbyte but incur compute costs from your destination.",
      "Enable agent access",
      "Before you can control agent access to destinations, you need to enable agent access",
    ],
  ] as const)("shows the %s pre-enrollment settings action", (actorKind, pageTitle, subtitle, title, description) => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: false, external_cloud_eligible: true } as never);

    renderConnectorsWithIntl(actorKind);

    expect(screen.getByRole("heading", { name: pageTitle })).toBeInTheDocument();
    expect(screen.getByText(subtitle)).toBeInTheDocument();
    expect(screen.getByText(title)).toBeInTheDocument();
    expect(screen.getByText(description)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Go to settings" })).toBeInTheDocument();
    expect(mockUseListWorkspacesInOrganization).toHaveBeenCalledWith(expect.objectContaining({ enabled: false }));
  });

  it("sends the pre-enrollment action to Context Layer Settings", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: false, external_cloud_eligible: true } as never);

    render(
      <MemoryRouter initialEntries={["/organization/test-org-123/context-layer/sources"]}>
        <IntlProvider locale="en" messages={messages}>
          <Routes>
            <Route
              path="/organization/:organizationId/context-layer/sources"
              element={<ContextLayerConnectorsPage actorKind="source" />}
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
    [
      "source",
      "Choose which sources populate your context layer. Backfills are free in Airbyte but incur compute costs from your destination.",
      "No sources yet",
      "Add sources to Airbyte to start populating your context layer.",
      "Add your first source",
      "/workspaces/workspace-1/source/new-source",
    ],
    [
      "destination",
      "Choose which destinations populate your context layer. Only Snowflake and BigQuery are supported.",
      "No destinations yet",
      "Add destinations to Airbyte to start populating your context layer.",
      "Add your first destination",
      "/workspaces/workspace-1/destination/new-destination",
    ],
  ] as const)(
    "shows the %s empty state and opens connector setup",
    async (actorKind, subtitle, title, description, action, path) => {
      mockUseAgentsProvisioningStatus.mockReturnValue({ is_enrolled: true, external_cloud_eligible: true } as never);
      mockUseListWorkspacesInOrganization.mockReturnValue({
        data: { pages: [{ workspaces: [{ workspaceId: "workspace-1", name: "Workspace 1" }] }] },
        hasNextPage: false,
        isFetchingNextPage: false,
        isLoading: false,
      } as never);

      render(
        <MemoryRouter initialEntries={[`/organization/test-org-123/context-layer/${actorKind}s`]}>
          <IntlProvider locale="en" messages={messages}>
            <NotificationService>
              <ConfirmationModalService>
                <Routes>
                  <Route
                    path="/organization/:organizationId/context-layer/:kind"
                    element={<ContextLayerConnectorsPage actorKind={actorKind} />}
                  />
                  <Route path={path} element={<div>Connector setup</div>} />
                </Routes>
              </ConfirmationModalService>
            </NotificationService>
          </IntlProvider>
        </MemoryRouter>
      );

      expect(await screen.findByText(title)).toBeInTheDocument();
      expect(screen.getByText(subtitle)).toBeInTheDocument();
      expect(screen.getByText(description)).toBeInTheDocument();
      expect(screen.getByTestId("context-layer-workspace-workspace-1")).not.toBeVisible();
      fireEvent.click(screen.getByRole("button", { name: action }));
      expect(screen.getByText("Connector setup")).toBeInTheDocument();
    }
  );

  it("waits for destination inventory after switching from an empty Sources page", async () => {
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

    const view = renderConnectorsWithIntl();
    expect(await screen.findByText("No sources yet")).toBeVisible();

    view.rerender(
      <MemoryRouter>
        <IntlProvider locale="en" messages={messages}>
          <NotificationService>
            <ConfirmationModalService>
              <ContextLayerConnectorsPage actorKind="destination" />
            </ConfirmationModalService>
          </NotificationService>
        </IntlProvider>
      </MemoryRouter>
    );

    expect(screen.queryByText("No destinations yet")).not.toBeInTheDocument();
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

      renderConnectorsWithIntl(actorKind);

      expect(await screen.findByText(actorKind === "source" ? "No sources yet" : "No destinations yet")).toBeVisible();
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
      <MemoryRouter initialEntries={["/organization/test-org-123/context-layer/sources"]}>
        <IntlProvider locale="en" messages={messages}>
          <NotificationService>
            <ConfirmationModalService>
              <Routes>
                <Route
                  path="/organization/:organizationId/context-layer/sources"
                  element={<ContextLayerConnectorsPage actorKind="source" />}
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

    expect(await screen.findByText("No sources yet")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Add your first source" }));
    expect(screen.getByText("Authorized connector setup")).toBeInTheDocument();
  });

  it("shows errors only for the selected connector kind", () => {
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

    const view = renderConnectorsWithIntl();

    expect(screen.getByTestId("context-layer-source-error")).toHaveTextContent(
      "Unable to load connectors. Please try again."
    );
    expect(screen.queryByText("BigQuery warehouse")).not.toBeInTheDocument();
    view.unmount();
    renderConnectorsWithIntl("destination");
    expect(screen.queryByTestId("context-layer-source-error")).not.toBeInTheDocument();
    expect(screen.getByText("BigQuery warehouse")).toBeInTheDocument();
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
              <ContextLayerConnectorsPage actorKind="source" />
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
