import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { useCurrentOrganizationInfo } from "core/api";
import { useCurrentUser } from "core/services/auth";
import { useGeneratedIntent, useIntent } from "core/utils/rbac";

import { RoleManagementCell } from "./RoleManagementCell";
import { UnifiedUserModel } from "./util";

// core/api's import graph is circular, so a jest.requireActual spread fails at module evaluation
// (same rationale as ScimSettingsCard.test.tsx) - list the exports this component uses explicitly.
jest.mock("core/api", () => ({
  useCurrentOrganizationInfo: jest.fn(),
}));

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: () => "org-id",
}));

jest.mock("core/services/auth", () => ({
  useCurrentUser: jest.fn(),
}));

// Intent is re-declared rather than spread from the real module: core/utils/rbac reaches core/api,
// so requireActual reintroduces the circular import this file is avoiding.
jest.mock("core/utils/rbac", () => ({
  Intent: {
    UpdateWorkspacePermissions: "UpdateWorkspacePermissions",
    UpdateOrganizationPermissions: "UpdateOrganizationPermissions",
  },
  useGeneratedIntent: jest.fn(),
  useIntent: jest.fn(),
}));

// Stubbed so each assertion reads as "editable control" vs "static role text" without mounting the
// menu's own query stack.
jest.mock("./RoleManagementMenu", () => ({
  RoleManagementMenu: () => <div data-testid="role-management-menu" />,
}));

const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.Mock;
const mockUseCurrentUser = useCurrentUser as jest.Mock;
const mockUseGeneratedIntent = useGeneratedIntent as jest.Mock;
const mockUseIntent = useIntent as jest.Mock;

const ORG_MEMBER: UnifiedUserModel = {
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

const WORKSPACE_MEMBER: UnifiedUserModel = {
  id: "other-user-id",
  userEmail: "member@airbyte.io",
  userName: "Member",
  workspacePermission: {
    permissionId: "permission-id",
    permissionType: "workspace_editor",
    userId: "other-user-id",
    workspaceId: "workspace-id",
  },
};

const setScim = (scim: boolean) => {
  mockUseCurrentOrganizationInfo.mockReturnValue({
    organizationId: "org-id",
    organizationName: "Org",
    sso: false,
    scim,
  });
};

describe(`${RoleManagementCell.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    // Current user is someone else, so the "cannot edit your own permissions" branch stays out of
    // the way and SCIM is the only thing that can force the view-only box.
    mockUseCurrentUser.mockReturnValue({ userId: "current-user-id" });
    mockUseGeneratedIntent.mockReturnValue(true);
    mockUseIntent.mockReturnValue(false);
  });

  it("leaves the organization role editable when SCIM is enabled", async () => {
    setScim(true);

    await render(<RoleManagementCell user={ORG_MEMBER} resourceType="organization" />);

    expect(screen.getByTestId("role-management-menu")).toBeInTheDocument();
  });

  it("renders a pending organization invitation as static text when SCIM is enabled", async () => {
    setScim(true);

    const PENDING_INVITATION: UnifiedUserModel = {
      id: "invitation-id",
      userEmail: "invited@airbyte.io",
      invitationStatus: "pending",
      invitationPermissionType: "organization_member",
    };

    await render(<RoleManagementCell user={PENDING_INVITATION} resourceType="organization" />);

    expect(screen.queryByTestId("role-management-menu")).not.toBeInTheDocument();
  });

  it("leaves the workspace role editable when SCIM is enabled", async () => {
    setScim(true);

    await render(<RoleManagementCell user={WORKSPACE_MEMBER} resourceType="workspace" />);

    expect(screen.getByTestId("role-management-menu")).toBeInTheDocument();
  });

  it("leaves the organization role editable when SCIM is disabled", async () => {
    setScim(false);

    await render(<RoleManagementCell user={ORG_MEMBER} resourceType="organization" />);

    expect(screen.getByTestId("role-management-menu")).toBeInTheDocument();
  });

  it("renders the organization role as static text when the organization info is unavailable", async () => {
    mockUseCurrentOrganizationInfo.mockReturnValue(undefined);
    mockUseGeneratedIntent.mockReturnValue(false);

    await render(<RoleManagementCell user={ORG_MEMBER} resourceType="organization" />);

    expect(screen.queryByTestId("role-management-menu")).not.toBeInTheDocument();
  });
});
