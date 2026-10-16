import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { HelmetProvider } from "react-helmet-async";
import { IntlProvider } from "react-intl";
import { MemoryRouter, Route, Routes, useParams } from "react-router-dom";

import { ConnectorNavigationTabs } from "area/connector/components/ConnectorNavigationTabs";
import { useGetDestinationFromParams, useGetSourceFromParams } from "area/connector/utils";
import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  useCurrentOrganizationInfo,
  useEnrollOrganizationInAgents,
  useFusionActorEnablement,
  useSetFusionActorEnablement,
} from "core/api";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { useShowActorContextLayerToggles } from "./ActorContextLayerToggles";
import { EnableDirectAccessPage } from "./EnableDirectAccessPage";
import { useContextLayerEligibility } from "./useContextLayerEligibility";

jest.mock("area/connector/utils", () => ({
  useGetDestinationFromParams: jest.fn(),
  useGetSourceFromParams: jest.fn(),
}));

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn(),
}));

jest.mock("core/api", () => ({
  fusionEnablementState: jest.fn((read: { enable_agent_access: boolean; enable_indexing: boolean }) => ({
    enable_agent_access: read.enable_agent_access,
    enable_indexing: read.enable_indexing,
  })),
  useCurrentOrganizationInfo: jest.fn(),
  useEnrollOrganizationInAgents: jest.fn(),
  useFusionActorEnablement: jest.fn(),
  useSetFusionActorEnablement: jest.fn(),
}));

jest.mock("core/services/auth", () => ({
  useAuthService: jest.fn(() => ({ user: null })),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: {
    CreateOrEditSource: "CreateOrEditSource",
    CreateOrEditDestination: "CreateOrEditDestination",
    UpdateOrganizationPermissions: "UpdateOrganizationPermissions",
  },
  useGeneratedIntent: jest.fn(),
}));

jest.mock("./ActorContextLayerToggles", () => ({
  useShowActorContextLayerToggles: jest.fn(),
}));

jest.mock("./useContextLayerEligibility", () => ({
  useContextLayerEligibility: jest.fn(),
}));

const messages = {
  "ui.loading": "Loading",
  "admin.sources": "Sources",
  "admin.destinations": "Destinations",
  "connector.settings": "Settings",
  "connector.connections": "Connections",
  "cloud.contextLayer.actor.loadFailed": "Context layer status could not be loaded for this workspace.",
  "cloud.contextLayer.docs": "Learn how to connect agents (SDK, API, MCP)",
  "cloud.contextLayer.enable.error": "Agent access could not be enabled. Please try again.",
  "cloud.contextLayer.enableDirectAccess.title": "Enabling direct access for {name}",
  "cloud.contextLayer.enableDirectAccess.description":
    "Agents can use Airbyte's MCP server to query <b>{definitionName}</b> directly using credentials from \"<lnk>{name}</lnk>\". You can turn off direct access anytime from the connector's <settingsLnk>settings page</settingsLnk>.",
  "cloud.contextLayer.enableDirectAccess.organization":
    "This also enables direct access for your organization {orgName}.",
  "cloud.contextLayer.enableDirectAccess.understand": "I understand",
  "cloud.contextLayer.enableDirectAccess.confirm": "Confirm",
  "cloud.contextLayer.enableDirectAccess.error.title": "An error occurred when enabling direct access for {name}",
  "cloud.contextLayer.enableDirectAccess.success.title": "Connector {name} is now enabled for direct access",
  "cloud.contextLayer.enableDirectAccess.success.description":
    "You can change this anytime in the Agent Access setting on the <lnk>connector's settings page</lnk>.",
  "cloud.contextLayer.enable.adminRequired": "Enabling the context layer requires an organization admin.",
  "cloud.contextLayer.enableDirectAccess.settingsLink": "Go to connector settings",
  "cloud.contextLayer.loadError.description":
    "We couldn't check whether the context layer is available for this organization. Please try again.",
  "cloud.contextLayer.semanticSearch.terms.title": "Terms of Service",
  "cloud.contextLayer.semanticSearch.terms.link": "Read terms of service",
  "cloud.contextLayer.sourceOptIn.noPermission":
    "You need edit permission for this workspace's sources to change this.",
  "cloud.contextLayer.destinationOptIn.noPermission":
    "You need edit permission for this workspace's destinations to change this.",
  "cloud.contextLayer.terms.subtitle": "Please review and accept the terms before enabling context layer.",
  "cloud.contextLayer.terms.intro": "By enabling Context layer, you agree to the following terms:",
  "cloud.contextLayer.terms.dataProcessing": "Data processing terms",
  "cloud.contextLayer.terms.privacyModeling": "Privacy and modeling terms",
  "cloud.contextLayer.terms.complianceResidency": "Compliance and data residency terms",
  "cloud.contextLayer.terms.liability": "Liability and indemnification terms",
  "cloud.contextLayer.terms.serviceLevel": "Service level and availability terms",
  "cloud.contextLayer.terms.intellectualProperty": "Intellectual property terms",
  "cloud.contextLayer.terms.footnote": "* These terms are required for compliance and data processing purposes.",
  "cloud.contextLayer.unavailable.description":
    "The context layer is not available for this organization yet. Contact Airbyte support if you'd like to learn more.",
  "form.cancel": "Cancel",
};

const mockUseGetDestinationFromParams = useGetDestinationFromParams as jest.MockedFunction<
  typeof useGetDestinationFromParams
>;
const mockUseGetSourceFromParams = useGetSourceFromParams as jest.MockedFunction<typeof useGetSourceFromParams>;
const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockUseCurrentWorkspaceId = useCurrentWorkspaceId as jest.MockedFunction<typeof useCurrentWorkspaceId>;
const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.MockedFunction<
  typeof useCurrentOrganizationInfo
>;
const mockUseEnrollOrganizationInAgents = useEnrollOrganizationInAgents as jest.MockedFunction<
  typeof useEnrollOrganizationInAgents
>;
const mockUseFusionActorEnablement = useFusionActorEnablement as jest.MockedFunction<typeof useFusionActorEnablement>;
const mockUseSetFusionActorEnablement = useSetFusionActorEnablement as jest.MockedFunction<
  typeof useSetFusionActorEnablement
>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;
const mockUseShowActorContextLayerToggles = useShowActorContextLayerToggles as jest.MockedFunction<
  typeof useShowActorContextLayerToggles
>;
const mockUseContextLayerEligibility = useContextLayerEligibility as jest.MockedFunction<
  typeof useContextLayerEligibility
>;
const mockSetEnablement = jest.fn();
const mockEnrollOrganization = jest.fn();

const sourceEnablement = (enabled: boolean) => ({
  actorId: "source-1",
  actorType: "source",
  workspaceId: "workspace-1",
  enable_agent_access: enabled,
  enable_indexing: false,
});

const setEligibility = (isEnrolled = true, isEligible = true) => {
  mockUseContextLayerEligibility.mockReturnValue({
    statusQuery: { isInitialLoading: false, isError: false },
    status: { is_enrolled: isEnrolled },
    isEligible,
  } as never);
};

const SourceItemPageRoute: React.FC = () => {
  const { sourceId = "" } = useParams<{ sourceId: string }>();

  return <ConnectorNavigationTabs connectorType="source" connector={{} as never} id={sourceId} />;
};

const renderPageAt = (initialPath: string) =>
  render(
    <HelmetProvider>
      <IntlProvider locale="en" messages={messages}>
        <MemoryRouter initialEntries={[initialPath]}>
          <React.Suspense fallback="Loading">
            <Routes>
              <Route path="/workspaces/:workspaceId/source">
                <Route path=":sourceId/enable-direct-access" element={<EnableDirectAccessPage actorType="source" />} />
                <Route path=":sourceId/*" element={<SourceItemPageRoute />} />
                <Route path=":sourceId" element={<div>Connector settings</div>} />
              </Route>
            </Routes>
          </React.Suspense>
        </MemoryRouter>
      </IntlProvider>
    </HelmetProvider>
  );

const renderPage = (query = "") => renderPageAt(`/workspaces/workspace-1/source/source-1/enable-direct-access${query}`);

describe("EnableDirectAccessPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseGetSourceFromParams.mockReturnValue({
      sourceId: "source-1",
      name: "GitHub Issues",
      sourceName: "GitHub",
    } as never);
    mockUseGetDestinationFromParams.mockReturnValue({
      destinationId: "destination-1",
      name: "BigQuery",
      destinationName: "BigQuery",
    } as never);
    mockUseCurrentOrganizationId.mockReturnValue("organization-1");
    mockUseCurrentWorkspaceId.mockReturnValue("workspace-1");
    mockUseCurrentOrganizationInfo.mockReturnValue({ organizationName: "Example Org" } as never);
    mockUseShowActorContextLayerToggles.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(true);
    mockSetEnablement.mockResolvedValue({ enabled: true });
    mockEnrollOrganization.mockResolvedValue(undefined);
    mockUseSetFusionActorEnablement.mockReturnValue({ mutateAsync: mockSetEnablement } as never);
    mockUseEnrollOrganizationInAgents.mockReturnValue({ mutateAsync: mockEnrollOrganization } as never);
    mockUseFusionActorEnablement.mockReturnValue({
      data: sourceEnablement(false),
      isInitialLoading: false,
      isError: false,
    } as never);
    setEligibility();
  });

  it("requires confirmation before enabling an enrolled connector", async () => {
    renderPage();

    expect(screen.getByRole("heading", { name: "Enabling direct access for GitHub Issues" })).toBeInTheDocument();
    expect(screen.getByText("GitHub", { selector: "strong" })).toBeInTheDocument();
    expect(screen.queryByText("cloud.contextLayer.agentAccess.description.enabled")).not.toBeInTheDocument();
    for (const name of ["GitHub Issues", "settings page"]) {
      const settingsLink = screen.getByRole("link", { name });
      expect(settingsLink).toHaveAttribute("href", "/workspaces/workspace-1/source/source-1");
      expect(settingsLink).toHaveAttribute("target", "_blank");
      expect(settingsLink).toHaveAttribute("rel", "noopener noreferrer");
    }
    expect(screen.queryByRole("link", { name: "Learn how to connect agents (SDK, API, MCP)" })).not.toBeInTheDocument();
    const confirm = screen.getByRole("button", { name: "Confirm" });
    expect(confirm).toBeDisabled();

    fireEvent.click(screen.getByRole("checkbox", { name: "I understand" }));
    expect(confirm).toBeEnabled();
    fireEvent.click(confirm);

    expect(
      await screen.findByRole("heading", { name: "Connector GitHub Issues is now enabled for direct access" })
    ).toBeInTheDocument();
    expect(mockSetEnablement).toHaveBeenCalledWith(
      expect.objectContaining({
        actorId: "source-1",
        actorKind: "source",
        workspaceId: "workspace-1",
        enabled: true,
        expectedState: { enable_agent_access: false, enable_indexing: false },
      })
    );
  });

  it("renders without connector tabs at the standalone direct-access route", () => {
    mockUseGetSourceFromParams.mockReturnValue({ sourceId: "s", name: "GitHub Issues", sourceName: "GitHub" } as never);
    mockUseCurrentWorkspaceId.mockReturnValue("w");

    renderPageAt("/workspaces/w/source/s/enable-direct-access");

    expect(screen.getByRole("heading", { name: "Enabling direct access for GitHub Issues" })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Settings" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Connections" })).not.toBeInTheDocument();
  });

  it("shows consent when skip-consent-screen is false", () => {
    renderPage("?skip-consent-screen=false");

    expect(screen.getByRole("heading", { name: "Enabling direct access for GitHub Issues" })).toBeInTheDocument();
    expect(mockSetEnablement).not.toHaveBeenCalled();
  });

  it("automatically enables once when consent is skipped for an enrolled connector", async () => {
    renderPage("?skip-consent-screen=true");

    expect(
      await screen.findByRole("heading", { name: "Connector GitHub Issues is now enabled for direct access" })
    ).toBeInTheDocument();
    expect(mockSetEnablement).toHaveBeenCalledTimes(1);
  });

  it("requires consent and enrolls only the current workspace when the organization is not enrolled", async () => {
    setEligibility(false, true);
    mockUseFusionActorEnablement.mockReturnValue({
      data: undefined,
      isInitialLoading: false,
      isError: false,
    } as never);
    renderPage("?skip-consent-screen=true");

    expect(screen.getByRole("heading", { name: "Enabling direct access for GitHub Issues" })).toBeInTheDocument();
    expect(screen.getByText("This also enables direct access for your organization Example Org.")).toBeInTheDocument();
    expect(mockEnrollOrganization).not.toHaveBeenCalled();
    expect(mockSetEnablement).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("checkbox", { name: "I understand" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm" }));

    await waitFor(() => {
      expect(mockEnrollOrganization).toHaveBeenCalledWith({
        workspaceIds: ["workspace-1"],
        addAllSupportedActors: false,
      });
      expect(mockSetEnablement).toHaveBeenCalledWith(
        expect.objectContaining({
          actorId: "source-1",
          actorKind: "source",
          workspaceId: "workspace-1",
          enabled: true,
        })
      );
    });
    expect(mockEnrollOrganization.mock.invocationCallOrder[0]).toBeLessThan(
      mockSetEnablement.mock.invocationCallOrder[0]
    );
    expect(
      await screen.findByRole("heading", { name: "Connector GitHub Issues is now enabled for direct access" })
    ).toBeInTheDocument();
  });

  it("shows success to read-only users without mutations when direct access is already enabled", () => {
    mockUseGeneratedIntent.mockReturnValue(false);
    mockUseFusionActorEnablement.mockReturnValue({
      data: sourceEnablement(true),
      isInitialLoading: false,
      isError: false,
    } as never);
    renderPage();

    expect(
      screen.getByRole("heading", { name: "Connector GitHub Issues is now enabled for direct access" })
    ).toBeInTheDocument();
    const docsLink = screen.getByRole("link", { name: "Learn how to connect agents (SDK, API, MCP)" });
    expect(docsLink).toHaveAttribute("href", links.contextLayerDocs);
    expect(docsLink).toHaveAttribute("target", "_blank");
    const settingsLink = screen.getByRole("link", { name: "connector's settings page" });
    expect(settingsLink).toHaveAttribute("href", "/workspaces/workspace-1/source/source-1");
    expect(settingsLink).toHaveAttribute("target", "_blank");
    expect(settingsLink).toHaveAttribute("rel", "noopener noreferrer");
    expect(mockUseFusionActorEnablement).toHaveBeenCalledWith(
      { actorId: "source-1", actorKind: "source", workspaceId: "workspace-1" },
      true
    );
    expect(mockEnrollOrganization).not.toHaveBeenCalled();
    expect(mockSetEnablement).not.toHaveBeenCalled();
  });

  it("requires organization admin to enroll even when consent is skipped", () => {
    setEligibility(false, true);
    mockUseFusionActorEnablement.mockReturnValue({
      data: undefined,
      isInitialLoading: false,
      isError: false,
    } as never);
    mockUseGeneratedIntent.mockImplementation((intent) => intent !== Intent.UpdateOrganizationPermissions);
    renderPage("?skip-consent-screen=true");

    expect(mockUseGeneratedIntent).toHaveBeenCalledWith(Intent.UpdateOrganizationPermissions, {
      organizationId: "organization-1",
    });
    expect(screen.getByText("Enabling the context layer requires an organization admin.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Confirm" })).not.toBeInTheDocument();
    expect(mockEnrollOrganization).not.toHaveBeenCalled();
    expect(mockSetEnablement).not.toHaveBeenCalled();
  });

  it("shows the unavailable reason when the status query is disabled without data", () => {
    mockUseContextLayerEligibility.mockReturnValue({
      statusQuery: { isInitialLoading: false, isError: false },
      status: undefined,
      isEligible: false,
    } as never);
    mockUseFusionActorEnablement.mockReturnValue({
      data: undefined,
      isInitialLoading: false,
      isError: false,
    } as never);
    renderPage();

    expect(
      screen.getByText(
        "The context layer is not available for this organization yet. Contact Airbyte support if you'd like to learn more."
      )
    ).toBeInTheDocument();
    expect(screen.queryByText("Loading")).not.toBeInTheDocument();
  });

  it("shows a generic error when enabling fails", async () => {
    mockSetEnablement.mockRejectedValueOnce(new Error("backend response body"));
    renderPage();

    fireEvent.click(screen.getByRole("checkbox", { name: "I understand" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm" }));

    expect(
      await screen.findByRole("heading", { name: "An error occurred when enabling direct access for GitHub Issues" })
    ).toBeInTheDocument();
    expect(screen.getByText("Agent access could not be enabled. Please try again.")).toBeInTheDocument();
    expect(screen.queryByText("backend response body")).not.toBeInTheDocument();
  });

  it("shows the permission reason without a Confirm button", () => {
    mockUseGeneratedIntent.mockReturnValue(false);
    renderPage();

    expect(
      screen.getByText("You need edit permission for this workspace's sources to change this.")
    ).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Confirm" })).not.toBeInTheDocument();
  });

  it("navigates to connector settings when canceled", () => {
    renderPage();

    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    expect(screen.getByText("Connector settings")).toBeInTheDocument();
  });
});
