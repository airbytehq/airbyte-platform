import { render, screen } from "@testing-library/react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";

import { useGetConnectorsOutOfDate } from "area/connector/utils/useConnector";
import { useCurrentOrganizationId } from "area/organization/utils";
import { useShowAgentsOptIn } from "cloud/components/AgentsOptIn/useShowAgentsOptIn";
import {
  useAgentsProvisioningStatus,
  useCurrentWorkspaceOrUndefined,
  useDefaultWorkspaceInOrganization,
  useGetWorkspaceIgnoreErrors,
} from "core/api";
import { useAuthService } from "core/services/auth";
import { useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { useGeneratedIntent } from "core/utils/rbac";
import { useLocalStorage } from "core/utils/useLocalStorage";

import { SideBar } from "./SideBar";

jest.mock("area/connector/utils/useConnector", () => ({
  useGetConnectorsOutOfDate: jest.fn(),
}));

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
  useTrackLastOrganization: jest.fn(),
}));

jest.mock("area/organization/OrganizationPicker/OrganizationPicker", () => ({
  OrganizationPicker: () => null,
}));

jest.mock("area/layout/SideBar/AirbyteHomeLink", () => ({
  AirbyteHomeLink: () => null,
}));

jest.mock("area/workspace/components/WorkspacesPickerNext", () => ({
  WorkspacesPickerNext: () => null,
}));

jest.mock("cloud/components/CloudHelpDropdown", () => ({
  CloudHelpDropdown: () => null,
}));

jest.mock("core/api", () => ({
  useAgentsProvisioningStatus: jest.fn(),
  useCurrentWorkspaceOrUndefined: jest.fn(),
  useDefaultWorkspaceInOrganization: jest.fn(),
  useGetWorkspaceIgnoreErrors: jest.fn(),
}));

jest.mock("core/services/auth", () => ({
  useAuthService: jest.fn(),
}));

jest.mock("core/services/features", () => ({
  FeatureItem: {
    OrganizationUI: "OrganizationUI",
    ShowAdminWarningInWorkspace: "ShowAdminWarningInWorkspace",
    ShowWorkspacePicker: "ShowWorkspacePicker",
  },
  IfFeatureEnabled: ({ children }: React.PropsWithChildren) => children,
  useFeature: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: {
    ViewOrganizationSettings: "ViewOrganizationSettings",
  },
  useGeneratedIntent: jest.fn(),
}));

jest.mock("core/utils/useLocalStorage", () => ({
  useLocalStorage: jest.fn(),
}));

jest.mock("cloud/components/AgentsOptIn/useShowAgentsOptIn", () => ({
  useShowAgentsOptIn: jest.fn(),
}));

jest.mock("components/ui/AdminWorkspaceWarning", () => ({
  AdminWorkspaceWarning: () => null,
}));

jest.mock("components/ui/ThemeToggle", () => ({
  ThemeToggle: () => null,
}));

const mockUseAgentsProvisioningStatus = useAgentsProvisioningStatus as jest.MockedFunction<
  typeof useAgentsProvisioningStatus
>;
const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockUseCurrentWorkspaceOrUndefined = useCurrentWorkspaceOrUndefined as jest.MockedFunction<
  typeof useCurrentWorkspaceOrUndefined
>;
const mockUseDefaultWorkspaceInOrganization = useDefaultWorkspaceInOrganization as jest.MockedFunction<
  typeof useDefaultWorkspaceInOrganization
>;
const mockUseGetConnectorsOutOfDate = useGetConnectorsOutOfDate as jest.MockedFunction<
  typeof useGetConnectorsOutOfDate
>;
const mockUseGetWorkspaceIgnoreErrors = useGetWorkspaceIgnoreErrors as jest.MockedFunction<
  typeof useGetWorkspaceIgnoreErrors
>;
const mockUseAuthService = useAuthService as jest.MockedFunction<typeof useAuthService>;
const mockUseFeature = useFeature as jest.MockedFunction<typeof useFeature>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;
const mockUseIsCloudApp = useIsCloudApp as jest.MockedFunction<typeof useIsCloudApp>;
const mockUseLocalStorage = useLocalStorage as jest.MockedFunction<typeof useLocalStorage>;
const mockUseShowAgentsOptIn = useShowAgentsOptIn as jest.MockedFunction<typeof useShowAgentsOptIn>;
const renderSidebar = (pathname: string) =>
  render(
    <IntlProvider
      locale="en"
      messages={{
        "cloud.contextLayer.sidebar": "Context layer",
        "cloud.installMcp.sidebar": "Install MCP",
        "settings.organization": "Organization",
        "settings.organizationSettings": "Organization settings",
        "sidebar.defaultUsername": "User",
        "sidebar.home": "Home",
      }}
    >
      <MemoryRouter initialEntries={[pathname]}>
        <SideBar />
      </MemoryRouter>
    </IntlProvider>
  );

describe("SideBar organization navigation", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      external_cloud_eligible: true,
    } as never);
    mockUseCurrentOrganizationId.mockReturnValue("test-org");
    mockUseCurrentWorkspaceOrUndefined.mockReturnValue(undefined);
    mockUseDefaultWorkspaceInOrganization.mockReturnValue(undefined);
    mockUseGetConnectorsOutOfDate.mockReturnValue({ hasNewVersions: false } as never);
    mockUseGetWorkspaceIgnoreErrors.mockReturnValue(undefined);
    mockUseAuthService.mockReturnValue({ authType: "simple", logout: undefined, user: undefined } as never);
    mockUseFeature.mockReturnValue(true);
    mockUseGeneratedIntent.mockReturnValue(true);
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseLocalStorage.mockReturnValue([{}, jest.fn()] as never);
    mockUseShowAgentsOptIn.mockReturnValue(true);
  });

  it("only highlights Install MCP on the Install MCP route", () => {
    renderSidebar("/organization/test-org/settings/install-mcp");

    expect(screen.getByTestId("orgSettingsLink")).not.toHaveClass("active");
    expect(screen.getByTestId("orgSettingsLink")).not.toHaveAttribute("aria-current", "page");
    expect(screen.getByTestId("installMcpSidebarLink")).toHaveClass("active");
    expect(screen.getByTestId("installMcpSidebarLink")).toHaveAttribute("aria-current", "page");
  });

  it("highlights Organization settings on organization settings routes", () => {
    renderSidebar("/organization/test-org/settings");

    expect(screen.getByTestId("orgSettingsLink")).toHaveClass("active");
  });
});
