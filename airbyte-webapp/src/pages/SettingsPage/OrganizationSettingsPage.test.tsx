import { screen } from "@testing-library/react";

import { mocked, render } from "test-utils";

import { useGetConnectorsOutOfDate } from "area/connector/utils/useConnector";
import { isOrganizationSubscribed, useCurrentOrganizationId } from "area/organization/utils";
import { useDefaultWorkspaceInOrganization, useOrgInfo } from "core/api";
import { useExperiment } from "core/services/Experiment";
import { FeatureItem, useFeature } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { OrganizationSettingsPage } from "./OrganizationSettingsPage";

jest.mock("area/connector/utils/useConnector", () => ({
  useGetConnectorsOutOfDate: jest.fn(),
}));

jest.mock("area/organization/utils", () => ({
  isOrganizationSubscribed: jest.fn(),
  useCurrentOrganizationId: jest.fn(),
}));

// The core/api barrel cannot be spread from jest.requireActual (its import graph is circular and
// fails at module evaluation), so this factory must explicitly list every export the page tree uses.
jest.mock("core/api", () => ({
  useDefaultWorkspaceInOrganization: jest.fn(),
  useOrgInfo: jest.fn(),
}));

jest.mock("core/services/Experiment", () => ({
  useExperiment: jest.fn(),
}));

jest.mock("core/services/features", () => ({
  ...jest.requireActual("core/services/features"),
  useFeature: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useGeneratedIntent: jest.fn(),
}));

// SettingsLayout pulls in HeadTitle -> useAuthService/react-helmet-async, neither of which
// this nav-focused test wires up a provider for. It's a plain structural shell, so stub it
// down to its children and keep the test scoped to the nav block this page renders.
jest.mock("area/settings/components/SettingsLayout", () => ({
  SettingsLayout: ({ children }: React.PropsWithChildren) => <>{children}</>,
  SettingsLayoutContent: ({ children }: React.PropsWithChildren) => <>{children}</>,
}));

const mockUseGetConnectorsOutOfDate = mocked(useGetConnectorsOutOfDate);
const mockIsOrganizationSubscribed = mocked(isOrganizationSubscribed);
const mockUseCurrentOrganizationId = mocked(useCurrentOrganizationId);
const mockUseDefaultWorkspaceInOrganization = mocked(useDefaultWorkspaceInOrganization);
const mockUseOrgInfo = mocked(useOrgInfo);
const mockUseExperiment = mocked(useExperiment);
const mockUseFeature = mocked(useFeature);
const mockUseIsCloudApp = mocked(useIsCloudApp);
const mockUseGeneratedIntent = mocked(useGeneratedIntent);

describe("OrganizationSettingsPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("test-organization-id");
    mockUseGetConnectorsOutOfDate.mockReturnValue({
      countNewSourceVersion: 0,
      countNewDestinationVersion: 0,
    } as unknown as ReturnType<typeof useGetConnectorsOutOfDate>);
    mockIsOrganizationSubscribed.mockReturnValue(false);
    mockUseDefaultWorkspaceInOrganization.mockReturnValue(undefined);
    mockUseOrgInfo.mockReturnValue(undefined);
    mockUseIsCloudApp.mockReturnValue(false);
    mockUseGeneratedIntent.mockImplementation((intent) => intent === Intent.ViewOrganizationSettings);
    mockUseFeature.mockImplementation((feature) => feature === FeatureItem.AllowUpdateSSOConfig);
    mockUseExperiment.mockReturnValue(false);
  });

  it("shows the plain SSO nav label when settings.scimProvisioning is off", async () => {
    await render(<OrganizationSettingsPage />);

    expect(screen.getByText("SSO")).toBeInTheDocument();
    expect(screen.queryByText("SSO and SCIM")).not.toBeInTheDocument();
  });

  it("shows the SSO and SCIM nav label when settings.scimProvisioning is on", async () => {
    mockUseExperiment.mockImplementation((key) => key === "settings.scimProvisioning");

    await render(<OrganizationSettingsPage />);

    expect(screen.getByText("SSO and SCIM")).toBeInTheDocument();
    expect(screen.queryByText("SSO")).not.toBeInTheDocument();
  });

  it("hides the User Groups nav link when settings.scimProvisioning is off", async () => {
    mockUseGeneratedIntent.mockImplementation(
      (intent) => intent === Intent.ViewOrganizationSettings || intent === Intent.UpdateOrganizationPermissions
    );

    await render(<OrganizationSettingsPage />);

    expect(screen.queryByText("User Groups")).not.toBeInTheDocument();
  });

  it("hides the User Groups nav link for a non-admin when settings.scimProvisioning is on", async () => {
    mockUseExperiment.mockImplementation((key) => key === "settings.scimProvisioning");
    // Default mockUseGeneratedIntent from beforeEach only allows ViewOrganizationSettings,
    // so UpdateOrganizationPermissions stays false here, simulating a non-admin.

    await render(<OrganizationSettingsPage />);

    expect(screen.queryByText("User Groups")).not.toBeInTheDocument();
  });

  it("shows the User Groups nav link for an admin when settings.scimProvisioning is on", async () => {
    mockUseExperiment.mockImplementation((key) => key === "settings.scimProvisioning");
    mockUseGeneratedIntent.mockImplementation(
      (intent) => intent === Intent.ViewOrganizationSettings || intent === Intent.UpdateOrganizationPermissions
    );

    await render(<OrganizationSettingsPage />);

    expect(screen.getByText("User Groups")).toBeInTheDocument();
  });
});
