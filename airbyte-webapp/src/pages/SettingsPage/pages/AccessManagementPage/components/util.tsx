import {
  OrganizationUserRead,
  PermissionType,
  ScopeType,
  UserInvitationRead,
  WorkspaceUserAccessInfoRead,
} from "core/api/types/AirbyteClient";
import { RbacRole, RbacRoleHierarchy, partitionPermissionType } from "core/utils/rbac/rbacPermissionsQuery";
export type ResourceType = "workspace" | "organization" | "instance";

export const permissionStringDictionary: Record<PermissionType, Record<string, string>> = {
  instance_admin: { resource: "resource.instance", role: "role.admin" },
  organization_admin: { resource: "resource.organization", role: "role.admin" },
  organization_editor: { resource: "resource.organization", role: "role.editor" },
  organization_runner: { resource: "resource.organization", role: "role.runner" },
  organization_reader: { resource: "resource.organization", role: "role.reader" },
  organization_member: { resource: "resource.organization", role: "role.member" },
  workspace_admin: { resource: "resource.workspace", role: "role.admin" },
  workspace_owner: { resource: "resource.workspace", role: "role.admin" },
  workspace_editor: { resource: "resource.workspace", role: "role.editor" },
  workspace_runner: { resource: "resource.workspace", role: "role.runner" },
  workspace_reader: { resource: "resource.workspace", role: "role.reader" },
};

interface PermissionDescription {
  id: string;
  values: Record<"resourceType", ResourceType>;
}
export const permissionDescriptionDictionary: Record<PermissionType, PermissionDescription> = {
  instance_admin: { id: "role.admin.description", values: { resourceType: "instance" } },
  organization_admin: { id: "role.admin.description", values: { resourceType: "organization" } },
  organization_editor: { id: "role.editor.description", values: { resourceType: "organization" } },
  organization_runner: { id: "role.runner.description", values: { resourceType: "organization" } },
  organization_reader: { id: "role.reader.description", values: { resourceType: "organization" } },
  organization_member: { id: "role.member.description", values: { resourceType: "organization" } },
  workspace_admin: { id: "role.admin.description", values: { resourceType: "workspace" } },
  workspace_owner: { id: "role.admin.description", values: { resourceType: "workspace" } },
  workspace_editor: { id: "role.editor.description", values: { resourceType: "workspace" } },
  workspace_runner: { id: "role.runner.description", values: { resourceType: "workspace" } },
  workspace_reader: { id: "role.reader.description", values: { resourceType: "workspace" } },
};
export const permissionsByResourceType: Record<ResourceType, PermissionType[]> = {
  workspace: [
    PermissionType.workspace_admin,
    PermissionType.workspace_editor,
    PermissionType.workspace_runner,
    PermissionType.workspace_reader,
  ],
  organization: [
    PermissionType.organization_admin,
    PermissionType.organization_editor,
    PermissionType.organization_runner,
    PermissionType.organization_reader,
    PermissionType.organization_member,
  ],
  instance: [PermissionType.instance_admin],
};

/**
 * a unified typing to allow listing invited and current users together in WorkspaceUsersTable
 * using this custom union rather than a union of WorkspaceUserAccessInfoRead | UserInvitationRead
 * allows us to handle intentionally missing properties more gracefully.
 */
export type UnifiedUserModel =
  | {
      id: string;
      userEmail: string;
      userName?: string;
      organizationPermission?: WorkspaceUserAccessInfoRead["organizationPermission"];
      workspacePermission?: WorkspaceUserAccessInfoRead["workspacePermission"];
      invitationStatus?: never; // Explicitly marking these as never when permissions are present
      invitationPermissionType?: never;
    }
  | {
      id: string;
      userEmail: string;
      userName?: string;
      organizationPermission?: never; // Explicitly marking these as never when invitation fields are present
      workspacePermission?: never;
      invitationStatus: UserInvitationRead["status"];
      invitationPermissionType: UserInvitationRead["permissionType"];
    };

export const unifyWorkspaceUserData = (
  workspaceAccessUsers: WorkspaceUserAccessInfoRead[],
  workspaceInvitations: UserInvitationRead[]
): UnifiedUserModel[] => {
  const normalizedUsers = workspaceAccessUsers.map((user) => {
    return {
      id: user.userId,
      userEmail: user.userEmail,
      userName: user.userName,
      organizationPermission: user.organizationPermission,
      workspacePermission: user.workspacePermission,
    };
  });

  const normalizedInvitations = workspaceInvitations.map((invitation) => {
    return {
      id: invitation.inviteCode,
      userEmail: invitation.invitedEmail,
      invitationStatus: invitation.status,
      invitationPermissionType: invitation.permissionType,
    };
  });

  return [...normalizedUsers, ...normalizedInvitations];
};

export const unifyOrganizationUserData = (
  organizationUsers: OrganizationUserRead[],
  organizationInvitations: UserInvitationRead[]
): UnifiedUserModel[] => {
  const normalizedUsers = organizationUsers.map((user) => {
    return {
      id: user.userId,
      userEmail: user.email,
      userName: user.name,
      organizationPermission: {
        permissionType: user.permissionType,
        permissionId: user.permissionId,
        organizationId: user.organizationId,
        userId: user.userId,
      },
    };
  });

  const normalizedInvitations = organizationInvitations.map((invitation) => {
    return {
      id: invitation.inviteCode,
      userEmail: invitation.invitedEmail,
      invitationStatus: invitation.status,
      invitationPermissionType: invitation.permissionType,
    };
  });

  return [...normalizedUsers, ...normalizedInvitations];
};

export const getWorkspaceAccessLevel = (
  unifiedWorkspaceUser: Pick<
    UnifiedUserModel,
    "workspacePermission" | "organizationPermission" | "invitationPermissionType"
  >
): RbacRole => {
  const workspacePermissionType =
    unifiedWorkspaceUser.workspacePermission?.permissionType ?? unifiedWorkspaceUser.invitationPermissionType;

  const organizationPermissionType = unifiedWorkspaceUser.organizationPermission?.permissionType;

  const orgRole = organizationPermissionType ? partitionPermissionType(organizationPermissionType)[1] : undefined;
  const workspaceRole = workspacePermissionType ? partitionPermissionType(workspacePermissionType)[1] : undefined;

  // return whatever is the "highest" role ie the lowest index greater than -1.
  // the reason we set the index to the length of the array is so that if there is not a given type of role, it will not be the lowest index.
  return RbacRoleHierarchy[
    Math.min(
      orgRole ? RbacRoleHierarchy.indexOf(orgRole) : RbacRoleHierarchy.length,
      workspaceRole ? RbacRoleHierarchy.indexOf(workspaceRole) : RbacRoleHierarchy.length
    )
  ];
};

export const getOrganizationAccessLevel = (
  unifiedOrganizationUser: Pick<UnifiedUserModel, "organizationPermission" | "invitationPermissionType">
): RbacRole => {
  const orgPermissionType =
    unifiedOrganizationUser.organizationPermission?.permissionType ?? unifiedOrganizationUser.invitationPermissionType;

  const orgRole = orgPermissionType ? partitionPermissionType(orgPermissionType)[1] : undefined;

  // return whatever is the "highest" role ie the lowest index greater than -1.
  // the reason we set the index to the length of the array is so that if there is not a given type of role, it will not be the lowest index.
  return RbacRoleHierarchy[Math.min(orgRole ? RbacRoleHierarchy.indexOf(orgRole) : RbacRoleHierarchy.length)];
};

/**
 * This is used to check if the permission type is a teams feature permission type
 * @returns true if the permission type is a teams feature permission type
 */
export const isTeamsFeaturePermissionType = (permission: PermissionType): boolean => {
  const warningPermissions: PermissionType[] = [
    PermissionType.workspace_editor,
    PermissionType.workspace_runner,
    // TBD: ask do we need to handle organization level permissions
    // PermissionType.organization_editor,
    // PermissionType.organization_runner,
  ];
  return warningPermissions.includes(permission);
};

/**
 * This is used to set the initial permission type based on the scope for the AddUserModal form
 * @returns the initial permission type based on the scope
 */
export const getInitialPermissionType = (scope: ScopeType): PermissionType =>
  scope === ScopeType.workspace ? PermissionType.workspace_admin : PermissionType.organization_admin;
