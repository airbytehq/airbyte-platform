import { screen } from "@testing-library/react";

import { mocked, render } from "test-utils";

import { useGetConnectorsOutOfDate } from "area/connector/utils/useConnector";
import { isOrganizationSubscribed, useCurrentOrganizationId } from "area/organization/utils";
import { useShowAgentsOptIn } from "cloud/components/AgentsOptIn/useShowAgentsOptIn";
import { useAgentsProvisioningStatus, useDefaultWorkspaceInOrganization, useOrgInfo } from "core/api";
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
  useAgentsProvisioningStatus: jest.fn(),
  useDefaultWorkspaceInOrganization: jest.fn(),
  useOrgInfo: jest.fn(),
}));

jest.mock("cloud/components/AgentsOptIn/useShowAgentsOptIn", () => ({
  useShowAgentsOptIn: jest.fn(() => false),
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
const mockUseAgentsProvisioningStatus = mocked(useAgentsProvisioningStatus);
const mockUseDefaultWorkspaceInOrganization = mocked(useDefaultWorkspaceInOrganization);
const mockUseOrgInfo = mocked(useOrgInfo);
const mockUseShowAgentsOptIn = mocked(useShowAgentsOptIn);
const mockUseExperiment = mocked(useExperiment);
const mockUseFeature = mocked(useFeature);
const mockUseIsCloudApp = mocked(useIsCloudApp);
const mockUseGeneratedIntent = mocked(useGeneratedIntent);

describe("OrganizationSettingsPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("test-organization-id");
    mockUseAgentsProvisioningStatus.mockReturnValue(null);
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

  it("hides the Audit Logs nav link for an admin without the audit logging entitlement", async () => {
    mockUseGeneratedIntent.mockImplementation(
      (intent) => intent === Intent.ViewOrganizationSettings || intent === Intent.UpdateOrganizationPermissions
    );
    // The beforeEach default leaves AllowAuditLogs false, standing in for an unentitled org.

    await render(<OrganizationSettingsPage />);

    expect(screen.queryByText("Audit Logs")).not.toBeInTheDocument();
  });

  it("hides the Audit Logs nav link for a non-admin with the audit logging entitlement", async () => {
    mockUseFeature.mockImplementation((feature) => feature === FeatureItem.AllowAuditLogs);
    // The beforeEach default leaves UpdateOrganizationPermissions false, standing in for a non-admin.

    await render(<OrganizationSettingsPage />);

    expect(screen.queryByText("Audit Logs")).not.toBeInTheDocument();
  });

  it("hides the Audit Logs nav link for an admin with the audit logging entitlement when audit-log-ui is off", async () => {
    mockUseFeature.mockImplementation((feature) => feature === FeatureItem.AllowAuditLogs);
    mockUseGeneratedIntent.mockImplementation(
      (intent) => intent === Intent.ViewOrganizationSettings || intent === Intent.UpdateOrganizationPermissions
    );
    // The beforeEach default leaves audit-log-ui off.

    await render(<OrganizationSettingsPage />);

    expect(screen.queryByText("Audit Logs")).not.toBeInTheDocument();
  });

  it("shows the Audit Logs nav link for an admin with the audit logging entitlement when audit-log-ui is on", async () => {
    mockUseFeature.mockImplementation((feature) => feature === FeatureItem.AllowAuditLogs);
    mockUseGeneratedIntent.mockImplementation(
      (intent) => intent === Intent.ViewOrganizationSettings || intent === Intent.UpdateOrganizationPermissions
    );
    mockUseExperiment.mockImplementation((key) => key === "audit-log-ui");

    await render(<OrganizationSettingsPage />);

    expect(screen.getByText("Audit Logs")).toBeInTheDocument();
  });

  it("shows the Agents nav link when the opt-in is enabled for an eligible organization", async () => {
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: false,
      is_instance_admin: false,
      provisioning_state: "not_provisioned",
      organization_id: "test-organization-id",
      organization_kind: null,
      external_cloud_eligible: true,
      eligible_external_organization_id: "test-organization-id",
    });

    await render(<OrganizationSettingsPage />);

    expect(screen.getByText("Context layer")).toBeInTheDocument();
  });

  it("shows only the Context layer nav link to eligible organization members", async () => {
    mockUseIsCloudApp.mockReturnValue(true);
    mockUseShowAgentsOptIn.mockReturnValue(true);
    mockUseAgentsProvisioningStatus.mockReturnValue({
      is_enrolled: true,
      is_instance_admin: false,
      provisioning_state: "provisioned",
      organization_id: "test-organization-id",
      organization_kind: "external_cloud",
      external_cloud_eligible: true,
      eligible_external_organization_id: null,
    });
    mockUseGeneratedIntent.mockReturnValue(false);

    await render(<OrganizationSettingsPage />);

    expect(screen.getByText("Context layer")).toBeInTheDocument();
    expect(screen.queryByText("SSO")).not.toBeInTheDocument();
    expect(screen.queryByText("General")).not.toBeInTheDocument();
  });
});
