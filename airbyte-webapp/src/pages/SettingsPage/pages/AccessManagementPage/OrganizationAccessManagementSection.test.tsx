import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { useCurrentOrganizationInfo, useListUserInvitations, useListUsersInOrganization } from "core/api";
import { FeatureItem } from "core/services/features";
import { useIsCloudApp } from "core/utils/app";
import { useGeneratedIntent } from "core/utils/rbac";

import { OrganizationAccessManagementSection } from "./OrganizationAccessManagementSection";

// core/api's import graph is circular, so a jest.requireActual spread fails at module evaluation
// (same rationale as ScimSettingsCard.test.tsx) - list the exports this component uses explicitly.
jest.mock("core/api", () => ({
  useCurrentOrganizationInfo: jest.fn(),
  useListUsersInOrganization: jest.fn(),
  useListUserInvitations: jest.fn(),
}));

// Intent is re-declared rather than spread from the real module: core/utils/rbac reaches core/api,
// so requireActual reintroduces the circular import this file is avoiding.
jest.mock("core/utils/rbac", () => ({
  Intent: { UpdateOrganizationPermissions: "UpdateOrganizationPermissions" },
  useGeneratedIntent: jest.fn(),
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: jest.fn(),
}));

// Stubbed so the section's own SCIM branches are what these tests exercise, rather than the table's
// per-row rendering (covered in RoleManagementCell.test.tsx).
jest.mock("./OrganizationUsersTable", () => ({
  OrganizationUsersTable: () => <div data-testid="organization-users-table" />,
}));

const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.Mock;
const mockUseListUsersInOrganization = useListUsersInOrganization as jest.Mock;
const mockUseListUserInvitations = useListUserInvitations as jest.Mock;
const mockUseGeneratedIntent = useGeneratedIntent as jest.Mock;
const mockUseIsCloudApp = useIsCloudApp as jest.Mock;

const SCIM_BANNER =
  "Organization membership is controlled by your identity provider. Edit membership in your identity provider.";

const setScim = (scim: boolean) => {
  mockUseCurrentOrganizationInfo.mockReturnValue({
    organizationId: "org-id",
    organizationName: "Org",
    sso: false,
    scim,
  });
};

const renderSection = () =>
  render(<OrganizationAccessManagementSection />, undefined, [FeatureItem.ExternalInvitations]);

describe(`${OrganizationAccessManagementSection.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseListUsersInOrganization.mockReturnValue({ users: [] });
    mockUseListUserInvitations.mockReturnValue([]);
    mockUseGeneratedIntent.mockReturnValue(true);
    mockUseIsCloudApp.mockReturnValue(false);
  });

  describe("when SCIM is enabled", () => {
    beforeEach(() => setScim(true));

    it("renders the provider-managed banner", async () => {
      await renderSection();

      expect(screen.getByText(SCIM_BANNER)).toBeInTheDocument();
    });

    it("renders the SCIM badge", async () => {
      await renderSection();

      expect(screen.getByText("SCIM enabled")).toBeInTheDocument();
    });

    it("hides the invite action", async () => {
      await renderSection();

      expect(screen.queryByRole("button", { name: /new member/i })).not.toBeInTheDocument();
    });
  });

  describe("when SCIM is disabled", () => {
    beforeEach(() => setScim(false));

    it("does not render the provider-managed banner", async () => {
      await renderSection();

      expect(screen.queryByText(SCIM_BANNER)).not.toBeInTheDocument();
    });

    it("does not render the SCIM badge", async () => {
      await renderSection();

      expect(screen.queryByText("SCIM enabled")).not.toBeInTheDocument();
    });

    it("shows the invite action", async () => {
      await renderSection();

      expect(screen.getByRole("button", { name: /new member/i })).toBeInTheDocument();
    });
  });
});
