import { render, screen, waitFor } from "@testing-library/react";
import { Suspense } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";

import { useShowAgentsOptIn } from "cloud/components/AgentsOptIn/useShowAgentsOptIn";
import { useCurrentWorkspace } from "core/api";
import { useExperiment } from "core/services/Experiment";
import { useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { useGeneratedIntent, useIntent } from "core/utils/rbac";

import { WorkspacesRoutes } from "./WorkspacesRoutes";

jest.mock("area/connector/components/EnterpriseStubConnectorPage/EnterpriseStubConnectorPage", () => ({
  EnterpriseStubConnectorPage: () => null,
}));

jest.mock("area/settings/UserSettingsRoutes", () => ({
  UserSettingsRoutes: () => null,
}));

jest.mock("cloud/components/AgentsOptIn/useShowAgentsOptIn", () => ({
  useShowAgentsOptIn: jest.fn(),
}));

jest.mock("cloud/views/billing/OrganizationBillingPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("cloud/views/billing/OrganizationUsagePage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("cloud/views/settings/CloudSettingsPage", () => ({
  CloudSettingsPage: () => {
    const { Outlet: MockOutlet } = jest.requireActual("react-router-dom");
    return <MockOutlet />;
  },
}));

jest.mock("cloud/views/settings/integrations/DbtCloudSettingsView", () => ({
  DbtCloudSettingsView: () => null,
}));

jest.mock("cloud/views/settings/privateLinks/PrivateLinksSettingsPage", () => ({
  PrivateLinksSettingsPage: () => null,
}));

jest.mock("cloud/views/workspaces/WorkspaceSettingsView", () => ({
  WorkspaceSettingsView: () => null,
}));

jest.mock("cloud/views/workspaces/WorkspaceUsagePage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: jest.fn(),
}));

jest.mock("core/api/cloud", () => ({
  usePrefetchWorkspaceData: jest.fn(),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: jest.fn(),
  useExperimentContext: jest.fn(),
}));

jest.mock("core/services/analytics/useAnalyticsService", () => ({
  useAnalyticsRegisterValues: jest.fn(),
}));

jest.mock("core/services/features", () => ({
  FeatureItem: {
    AllowDBTCloudIntegration: "AllowDBTCloudIntegration",
    PrivateLinks: "PrivateLinks",
  },
  useFeature: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: {
    ManageOrganizationBilling: "ManageOrganizationBilling",
    ViewOrganizationUsage: "ViewOrganizationUsage",
  },
  useGeneratedIntent: jest.fn(),
  useIntent: jest.fn(),
}));

jest.mock("pages/OnboardingPage/OnboardingPage", () => ({
  OnboardingPage: () => null,
}));

jest.mock("pages/SettingsPage/pages/AdvancedSettingsPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/SettingsPage/pages/ConnectorsPage", () => ({
  DestinationsPage: () => null,
  SourcesPage: () => null,
}));

jest.mock("pages/SettingsPage/pages/NotificationPage", () => ({
  NotificationPage: () => null,
}));

jest.mock("pages/SettingsPage/pages/Organization/GeneralOrganizationSettingsPage", () => ({
  GeneralOrganizationSettingsPage: () => null,
}));

jest.mock("pages/SettingsPage/pages/Organization/OrganizationMembersPage", () => ({
  OrganizationMembersPage: () => null,
}));

jest.mock("pages/SettingsPage/Workspace/WorkspaceMembersPage", () => ({
  WorkspaceMembersPage: () => null,
}));

jest.mock("pages/destination/AllDestinationsPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/destination/CreateDestinationPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/destination/SelectDestinationPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/destination/DestinationItemPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/destination/DestinationConnectionsPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/destination/DestinationSettingsPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/source/AllSourcesPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/source/CreateSourcePage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/source/SelectSourcePage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/source/SourceItemPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/source/SourceConnectionsPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/source/SourceSettingsPage", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/connections/ConnectionsRoutes", () => ({
  __esModule: true,
  default: () => null,
}));

jest.mock("pages/connectorBuilder/ConnectorBuilderRoutes", () => ({
  __esModule: true,
  default: () => null,
}));

const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;
const mockUseCurrentWorkspace = useCurrentWorkspace as jest.MockedFunction<typeof useCurrentWorkspace>;
const mockUseExperiment = useExperiment as jest.MockedFunction<typeof useExperiment>;
const mockUseFeature = useFeature as jest.MockedFunction<typeof useFeature>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;
const mockUseIntent = useIntent as jest.MockedFunction<typeof useIntent>;

const LocationDisplay = () => {
  const location = useLocation();
  return <div data-testid="location">{location.pathname}</div>;
};

describe("WorkspacesRoutes", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseCurrentWorkspace.mockReturnValue({
      organizationId: "test-org",
      workspaceId: "test-ws",
      customerId: "test-customer",
    } as never);
    mockUseExperiment.mockReturnValue(false);
    mockUseFeature.mockReturnValue(false);
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(false);
    mockUseIntent.mockReturnValue(false);
  });

  it("redirects the legacy Context Layer route to the organization page when the opt-in flag is off", async () => {
    mockUseShowAgentsOptIn.mockReturnValue(false);

    render(
      <MemoryRouter initialEntries={["/workspaces/test-ws/settings/context-layer"]}>
        <Suspense fallback={<div>Loading...</div>}>
          <Routes>
            <Route path="/workspaces/:workspaceId/*" element={<WorkspacesRoutes />} />
          </Routes>
          <LocationDisplay />
        </Suspense>
      </MemoryRouter>
    );

    await waitFor(() =>
      expect(screen.getByTestId("location")).toHaveTextContent("/organization/test-org/context-layer")
    );
  });

  it("redirects the legacy Context Layer route to the organization page when the opt-in flag is on", async () => {
    mockUseShowAgentsOptIn.mockReturnValue(true);

    render(
      <MemoryRouter initialEntries={["/workspaces/test-ws/settings/context-layer"]}>
        <Suspense fallback={<div>Loading...</div>}>
          <Routes>
            <Route path="/workspaces/:workspaceId/*" element={<WorkspacesRoutes />} />
          </Routes>
          <LocationDisplay />
        </Suspense>
      </MemoryRouter>
    );

    await waitFor(() =>
      expect(screen.getByTestId("location")).toHaveTextContent("/organization/test-org/context-layer")
    );
  });

  it("does not register the context layer route outside cloud", async () => {
    mockUseIsCloudApp.mockReturnValue(false);

    render(
      <MemoryRouter initialEntries={["/workspaces/test-ws/settings/context-layer"]}>
        <Suspense fallback={<div>Loading...</div>}>
          <Routes>
            <Route path="/workspaces/:workspaceId/*" element={<WorkspacesRoutes />} />
          </Routes>
          <LocationDisplay />
        </Suspense>
      </MemoryRouter>
    );

    await waitFor(() =>
      expect(screen.getByTestId("location")).not.toHaveTextContent("/workspaces/test-ws/settings/context-layer")
    );
  });
});
