import {
  mockOrganizationUserInvitations,
  mockOrganizationUsers,
  mockWorkspaceAccessUsers,
  mockWorkspaceUserInvitations,
} from "test-utils/mock-data/mockUsersList";

import { PermissionType } from "core/api/types/AirbyteClient";

import {
  unifyWorkspaceUserData,
  unifyOrganizationUserData,
  getWorkspaceAccessLevel,
  getOrganizationAccessLevel,
} from "./util";

describe("unifyWorkspaceUserData", () => {
  it("should merge workspace users and invitations correctly", () => {
    const result = unifyWorkspaceUserData(mockWorkspaceAccessUsers, mockWorkspaceUserInvitations);
    expect(result).toHaveLength(4);
    expect(result).toEqual([
      {
        id: "workspaceUser1",
        userEmail: "wsuser1@test.com",
        userName: "Ws User 1",
        organizationPermission: undefined,
        workspacePermission: {
          permissionId: "permission4",
          permissionType: "workspace_admin",
          userId: "workspaceUser1",
        },
      },
      {
        id: "orgUser2",
        organizationPermission: {
          organizationId: "org-id",
          permissionId: "permission2",
          permissionType: "organization_reader",
          userId: "orgUser2",
        },
        workspacePermission: undefined,
        userEmail: "orguser2@test.com",
        userName: "Org User 2",
      },
      {
        id: "orgAndWorkspaceUser",
        organizationPermission: {
          permissionId: "permission3",
          permissionType: "organization_member",
          userId: "orgAndWorkspaceUser",
        },
        userEmail: "organdworkspaceuser@test.com",
        userName: "Org And Workspace User",
        workspacePermission: {
          permissionId: "permission5",
          permissionType: "workspace_editor",
          userId: "orgAndWorkspaceUser",
        },
      },
      {
        id: "code1",
        invitationPermissionType: "workspace_reader",
        invitationStatus: "pending",
        userEmail: "userInvite1@example.com",
      },
    ]);
  });

  it("should handle empty input arrays", () => {
    const result = unifyWorkspaceUserData([], []);
    expect(result).toHaveLength(0);
  });
});

describe("unifyOrganizationUserData", () => {
  it("should merge organization users and invitations correctly", () => {
    const result = unifyOrganizationUserData(mockOrganizationUsers, mockOrganizationUserInvitations);
    expect(result).toHaveLength(5);
    expect(result).toEqual([
      {
        id: "orgUser1",
        organizationPermission: {
          organizationId: "org-id",
          permissionId: "permission1",
          permissionType: "organization_member",
          userId: "orgUser1",
        },
        userEmail: "orguser1@test.com",
        userName: "Org User 1",
      },
      {
        id: "orgUser2",
        organizationPermission: {
          organizationId: "org-id",
          permissionId: "permission2",
          permissionType: "organization_reader",
          userId: "orgUser2",
        },
        userEmail: "orguser2@test.com",
        userName: "Org User 2",
      },
      {
        id: "orgUser3",
        organizationPermission: {
          organizationId: "org-id",
          permissionId: "permission6",
          permissionType: "organization_admin",
          userId: "orgUser3",
        },
        userEmail: "orguser3@test.com",
        userName: "Org User 3",
      },
      {
        id: "orgAndWorkspaceUser",
        organizationPermission: {
          organizationId: "org-id",
          permissionId: "permission3",
          permissionType: "organization_member",
          userId: "orgAndWorkspaceUser",
        },
        userEmail: "organdworkspaceuser@test.com",
        userName: "Org And Workspace User",
      },
      {
        id: "code1",
        invitationPermissionType: "organization_reader",
        invitationStatus: "pending",
        userEmail: "userInvite1@example.com",
      },
    ]);
  });
  it("should handle empty input arrays", () => {
    const result = unifyOrganizationUserData([], []);
    expect(result).toHaveLength(0);
  });
});

describe("getWorkspaceAccessLevel", () => {
  it("should return highest permission from workspace and organization", () => {
    const unifiedUser = {
      workspacePermission: { permissionType: PermissionType.workspace_admin, permissionId: "1", userId: "user1" },
      organizationPermission: {
        permissionType: PermissionType.organization_editor,
        permissionId: "2",
        userId: "user2",
      },
    };
    const result = getWorkspaceAccessLevel(unifiedUser);
    expect(result).toBe("ADMIN");
  });

  it("should return highest available role when one is missing", () => {
    const unifiedUser = {
      workspacePermission: { permissionType: PermissionType.workspace_reader, permissionId: "1", userId: "user1" },
    };
    const result = getWorkspaceAccessLevel(unifiedUser);
    expect(result).toBe("READER");
  });

  it("should handle missing permissions gracefully", () => {
    const unifiedUser = {};
    const result = getWorkspaceAccessLevel(unifiedUser);
    expect(result).toBeUndefined();
  });
});

describe("getOrganizationAccessLevel", () => {
  it("should return highest organization permission", () => {
    const unifiedUser = {
      organizationPermission: { permissionType: PermissionType.organization_admin, permissionId: "1", userId: "user1" },
    };
    const result = getOrganizationAccessLevel(unifiedUser);
    expect(result).toBe("ADMIN");
  });

  it("should handle missing permissions gracefully", () => {
    const unifiedUser = {};
    const result = getOrganizationAccessLevel(unifiedUser);
    expect(result).toBeUndefined();
  });
});
