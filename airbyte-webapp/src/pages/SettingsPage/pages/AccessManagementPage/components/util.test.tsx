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
  isTeamsFeaturePermissionType,
  permissionsByResourceType,
  permissionStringDictionary,
  permissionDescriptionDictionary,
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
        id: "invitation1",
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
        id: "invitation1",
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

describe("isTeamsFeaturePermissionType", () => {
  it("should return true for workspace_reader", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.workspace_reader)).toBe(true);
  });

  it("should return true for organization_reader", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.organization_reader)).toBe(true);
  });

  it("should return true for workspace_editor", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.workspace_editor)).toBe(true);
  });

  it("should return true for workspace_runner", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.workspace_runner)).toBe(true);
  });

  it("should return true for organization_editor", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.organization_editor)).toBe(true);
  });

  it("should return true for organization_runner", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.organization_runner)).toBe(true);
  });

  it("should return false for workspace_admin", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.workspace_admin)).toBe(false);
  });

  it("should return false for organization_admin", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.organization_admin)).toBe(false);
  });

  it("should return false for organization_member", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.organization_member)).toBe(false);
  });

  it("should return false for instance_admin", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.instance_admin)).toBe(false);
  });

  it("should return true for the actor-scoped workspace editors", () => {
    expect(isTeamsFeaturePermissionType(PermissionType.workspace_source_editor)).toBe(true);
    expect(isTeamsFeaturePermissionType(PermissionType.workspace_destination_editor)).toBe(true);
  });
});

describe("actor-scoped workspace editor roles", () => {
  it.each([PermissionType.workspace_source_editor, PermissionType.workspace_destination_editor])(
    "%s is assignable at the workspace scope",
    (permissionType) => {
      expect(permissionsByResourceType.workspace).toContain(permissionType);
      expect(permissionsByResourceType.organization).not.toContain(permissionType);
      expect(permissionsByResourceType.instance).not.toContain(permissionType);
    }
  );

  it.each([PermissionType.workspace_source_editor, PermissionType.workspace_destination_editor])(
    "%s has a label and a description",
    (permissionType) => {
      expect(permissionStringDictionary[permissionType].resource).toBe("resource.workspace");
      expect(permissionStringDictionary[permissionType].role).toBeTruthy();
      expect(permissionDescriptionDictionary[permissionType].id).toBeTruthy();
      expect(permissionDescriptionDictionary[permissionType].values.resourceType).toBe("workspace");
    }
  );

  it("resolves to its own access level rather than falling back to a lesser role", () => {
    expect(
      getWorkspaceAccessLevel({
        workspacePermission: {
          permissionType: PermissionType.workspace_source_editor,
          permissionId: "1",
          userId: "user1",
        },
      })
    ).toBe("SOURCE_EDITOR");

    expect(
      getWorkspaceAccessLevel({
        workspacePermission: {
          permissionType: PermissionType.workspace_destination_editor,
          permissionId: "1",
          userId: "user1",
        },
      })
    ).toBe("DESTINATION_EDITOR");
  });

  it("is outranked by a workspace admin and outranks a runner", () => {
    expect(
      getWorkspaceAccessLevel({
        workspacePermission: {
          permissionType: PermissionType.workspace_source_editor,
          permissionId: "1",
          userId: "user1",
        },
        organizationPermission: { permissionType: PermissionType.organization_admin, permissionId: "2", userId: "u2" },
      })
    ).toBe("ADMIN");

    expect(
      getWorkspaceAccessLevel({
        workspacePermission: {
          permissionType: PermissionType.workspace_source_editor,
          permissionId: "1",
          userId: "user1",
        },
        organizationPermission: { permissionType: PermissionType.organization_runner, permissionId: "2", userId: "u2" },
      })
    ).toBe("SOURCE_EDITOR");
  });
});
