import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { useCurrentOrganizationInfo } from "core/api";
import { useFeature } from "core/services/features";

import { RoleManagementMenuBody } from "./RoleManagementMenuBody";
import { ResourceType, UnifiedUserModel } from "./util";

// core/api's import graph is circular, so a jest.requireActual spread fails at module evaluation
// (same rationale as RoleManagementCell.test.tsx) - list the exports this component uses explicitly.
jest.mock("core/api", () => ({
  useCurrentOrganizationInfo: jest.fn(),
}));

// useFeature must return true here, otherwise rolesToAllow is [] (RoleManagementMenuBody.tsx:21)
// and the role-option assertions would be vacuous. FeatureService/defaultOssFeatures from this same
// module are needed by test-utils' TestWrapper, so spread the actual module rather than replacing it
// wholesale (core/services/features has no circular import problem, unlike core/api above).
jest.mock("core/services/features", () => ({
  ...jest.requireActual("core/services/features"),
  useFeature: jest.fn(),
}));

// Stubbed so each menu item mounts a sentinel rather than its own query stack.
jest.mock("./ChangeRoleMenuItem", () => ({
  ChangeRoleMenuItem: () => <div data-testid="change-role-menu-item" />,
}));
jest.mock("./RemoveRoleMenuItem", () => ({
  RemoveRoleMenuItem: () => <div data-testid="remove-role-menu-item" />,
}));
jest.mock("./CancelInvitationMenuItem", () => ({
  CancelInvitationMenuItem: () => <div data-testid="cancel-invitation-menu-item" />,
}));

const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.Mock;
const mockUseFeature = useFeature as jest.Mock;

const setScim = (scim: boolean) => {
  mockUseCurrentOrganizationInfo.mockReturnValue({
    organizationId: "org-id",
    organizationName: "Org",
    sso: false,
    scim,
  });
};

const MEMBER: UnifiedUserModel = {
  id: "other-user-id",
  userEmail: "member@airbyte.io",
  userName: "Member",
  organizationPermission: {
    permissionId: "permission-id",
    permissionType: "organization_member",
    userId: "other-user-id",
    organizationId: "org-id",
  },
};

const renderMenuBody = (resourceType: ResourceType) =>
  render(<RoleManagementMenuBody user={MEMBER} resourceType={resourceType} close={jest.fn()} />);

describe(`${RoleManagementMenuBody.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseFeature.mockReturnValue(true);
  });

  it("keeps role options and hides removal for an organization row when SCIM is enabled", async () => {
    setScim(true);

    await renderMenuBody("organization");

    expect(screen.getAllByTestId("change-role-menu-item").length).toBeGreaterThan(0);
    expect(screen.queryByTestId("remove-role-menu-item")).not.toBeInTheDocument();
  });

  it("keeps role options and removal for an organization row when SCIM is disabled", async () => {
    setScim(false);

    await renderMenuBody("organization");

    expect(screen.getAllByTestId("change-role-menu-item").length).toBeGreaterThan(0);
    expect(screen.getByTestId("remove-role-menu-item")).toBeInTheDocument();
  });

  it("keeps removal for a workspace row when SCIM is enabled", async () => {
    setScim(true);

    await renderMenuBody("workspace");

    expect(screen.getByTestId("remove-role-menu-item")).toBeInTheDocument();
  });
});
