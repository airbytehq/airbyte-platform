import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { IntlProvider } from "react-intl";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useAgentsProvisioningStatus, useEnrollOrganizationInAgents, useListWorkspacesInOrganization } from "core/api";
import { useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ContextLayerPage } from "./ContextLayerPage";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
  useEnrollOrganizationInAgents: jest.fn(),
  useListWorkspacesInOrganization: jest.fn(),
}));

jest.mock("core/services/Modal", () => ({
  useModalService: jest.fn(),
}));

jest.mock("core/services/Notification", () => ({
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
    agentEngineApp: "https://app.airbyte.ai",
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
const mockUseEnrollOrganizationInAgents = useEnrollOrganizationInAgents as jest.MockedFunction<
  typeof useEnrollOrganizationInAgents
>;
const mockUseListWorkspacesInOrganization = useListWorkspacesInOrganization as jest.MockedFunction<
  typeof useListWorkspacesInOrganization
>;
const mockUseModalService = useModalService as jest.MockedFunction<typeof useModalService>;
const mockUseNotificationService = useNotificationService as jest.MockedFunction<typeof useNotificationService>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;

const messages = {
  "cloud.contextLayer.title": "Context layer",
  "cloud.contextLayer.subtitle": "Configure metadata access and intelligence options for your organization.",
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
  "cloud.contextLayer.billing":
    "Context layer is billed separately from your current plan. Usage is metered based on AI agent activity.",
  "cloud.contextLayer.pricing": "View pricing details →",
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
  "cloud.contextLayer.terms.acceptCharges":
    "I understand that using the context layer may incur additional charges, and I am authorized to approve those charges",
  "cloud.contextLayer.terms.cancel": "Cancel",
  "cloud.contextLayer.terms.accept": "Accept and Enable",
  "cloud.contextLayer.terms.footnote": "* These terms are required for compliance and data processing purposes.",
  "cloud.contextLayer.terms.enrollError": "Context layer could not be enabled. Please try again.",
  "cloud.contextLayer.workspace.title": "Workspace connector access",
  "cloud.contextLayer.workspace.description":
    "Control which connectors AI agents can access within each workspace. Sources are enabled by default. Destinations must be explicitly enabled.",
  "cloud.contextLayer.workspace.loading": "Loading workspaces...",
  "cloud.contextLayer.workspace.empty": "No workspaces found in this organization.",
  "cloud.contextLayer.workspace.counts": "{sources} sources, {destinations} destinations",
  "cloud.contextLayer.workspace.sources": "Sources",
  "cloud.contextLayer.workspace.destinations": "Destinations",
  "cloud.contextLayer.workspace.enabledCount": "{enabled} of {total} enabled",
  "cloud.contextLayer.openAgents": "Open Airbyte Agents",
};

const renderWithIntl = () =>
  render(
    <IntlProvider locale="en" messages={messages}>
      <ContextLayerPage />
    </IntlProvider>
  );

describe("ContextLayerPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("test-org-123");
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseListWorkspacesInOrganization.mockReturnValue({ data: { pages: [] } } as never);
    mockUseGeneratedIntent.mockReturnValue(true);
    mockUseModalService.mockReturnValue({ openModal: jest.fn() } as never);
    mockUseNotificationService.mockReturnValue({ registerNotification: jest.fn() } as never);
    mockUseEnrollOrganizationInAgents.mockReturnValue({ mutateAsync: jest.fn() } as never);
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
    fireEvent.click(screen.getByRole("checkbox", { name: messages["cloud.contextLayer.terms.acceptCharges"] }));
    expect(screen.getByRole("button", { name: "Accept and Enable" })).toBeEnabled();
    fireEvent.click(screen.getByRole("button", { name: "Accept and Enable" }));
    await waitFor(() =>
      expect(mutateAsync).toHaveBeenCalledWith({
        workspaceIds: ["workspace-1", "workspace-2"],
        addAllSupportedActors: true,
      })
    );
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
    expect(screen.getByRole("link", { name: "Open Airbyte Agents" })).toHaveAttribute("target", "_blank");
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

  it("renders nothing when the organization is not eligible", () => {
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

    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();
  });

  it("renders nothing when the provisioning status belongs to another organization", () => {
    mockUseAgentsProvisioningStatus.mockReturnValue(null);

    renderWithIntl();

    expect(screen.queryByText("Context layer")).not.toBeInTheDocument();
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
    fireEvent.click(screen.getByRole("checkbox", { name: messages["cloud.contextLayer.terms.acceptCharges"] }));
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
