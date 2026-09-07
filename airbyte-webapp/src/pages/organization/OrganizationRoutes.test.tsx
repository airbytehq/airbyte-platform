import { render, screen, waitFor } from "@testing-library/react";
import { Suspense } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useShowAgentsOptIn } from "cloud/components/AgentsOptIn/useShowAgentsOptIn";
import { useExperiment } from "core/services/Experiment";
import { useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { useGeneratedIntent } from "core/utils/rbac";

import { OrganizationRoutes } from "./OrganizationRoutes";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("area/organization/OrganizationSettingsLayout", () => ({
  __esModule: true,
  default: () => {
    const { Outlet: MockOutlet } = jest.requireActual("react-router-dom");
    return <MockOutlet />;
  },
}));

jest.mock("area/settings/UserSettingsRoutes", () => ({
  UserSettingsRoutes: () => null,
}));

jest.mock("cloud/components/AgentsOptIn/useShowAgentsOptIn", () => ({
  useShowAgentsOptIn: jest.fn(),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: jest.fn(),
}));

jest.mock("core/services/features", () => ({
  FeatureItem: {
    AllowAuditLogs: "AllowAuditLogs",
    AllowUpdateSSOConfig: "AllowUpdateSSOConfig",
    EnterpriseLicenseChecking: "EnterpriseLicenseChecking",
  },
  useFeature: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: {
    ViewOrganizationSettings: "ViewOrganizationSettings",
    ManageOrganizationBilling: "ManageOrganizationBilling",
    ViewOrganizationUsage: "ViewOrganizationUsage",
    UpdateOrganizationPermissions: "UpdateOrganizationPermissions",
  },
  useGeneratedIntent: jest.fn(),
}));

jest.mock("pages/SettingsPage/OrganizationSettingsPage", () => ({
  OrganizationSettingsPage: () => {
    const { Outlet: MockOutlet } = jest.requireActual("react-router-dom");
    return <MockOutlet />;
  },
}));

jest.mock("pages/SettingsPage/pages/ConnectorsPage", () => ({
  DestinationsPage: () => null,
  SourcesPage: () => null,
}));

jest.mock("pages/SettingsPage/pages/LicenseDetailsPage/LicenseSettingsPage", () => ({
  LicenseSettingsPage: () => null,
}));

jest.mock("pages/SettingsPage/pages/Organization/GeneralOrganizationSettingsPage", () => ({
  GeneralOrganizationSettingsPage: () => null,
}));

jest.mock("pages/SettingsPage/pages/Organization/OrganizationAuditLogsPage", () => ({
  OrganizationAuditLogsPage: () => null,
}));

jest.mock("pages/SettingsPage/pages/Organization/OrganizationGroupsPage", () => ({
  OrganizationGroupsPage: () => null,
}));

jest.mock("pages/SettingsPage/pages/Organization/OrganizationMembersPage", () => ({
  OrganizationMembersPage: () => null,
}));

jest.mock("pages/SettingsPage/pages/Organization/SSOAndScimOrganizationSettingsPage", () => ({
  SSOAndScimOrganizationSettingsPage: () => null,
}));

jest.mock("pages/workspaces/OrganizationWorkspacesPage", () => ({
  __esModule: true,
  default: () => <div data-testid="organization-workspaces-page" />,
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;
const mockUseExperiment = useExperiment as jest.MockedFunction<typeof useExperiment>;
const mockUseFeature = useFeature as jest.MockedFunction<typeof useFeature>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;

const LocationDisplay = () => {
  const location = useLocation();
  return <div data-testid="location">{location.pathname}</div>;
};

describe("OrganizationRoutes", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("test-org");
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseExperiment.mockReturnValue(false);
    mockUseFeature.mockReturnValue(false);
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(false);
  });

  it("redirects non-admin organization settings wildcard routes to organization workspaces", async () => {
    render(
      <MemoryRouter initialEntries={["/organizations/test-org/settings/unknown"]}>
        <Suspense fallback={<div>Loading...</div>}>
          <Routes>
            <Route path="/organizations/:organizationId/*" element={<OrganizationRoutes />} />
          </Routes>
          <LocationDisplay />
        </Suspense>
      </MemoryRouter>
    );

    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent("/organizations/test-org/workspaces"));
  });
});
