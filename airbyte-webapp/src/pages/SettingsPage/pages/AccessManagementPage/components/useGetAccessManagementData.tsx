import { useCurrentWorkspace, useListUsersInOrganization } from "core/api";
import {
  OrganizationUserRead,
  PermissionRead,
  PermissionType,
  WorkspaceUserAccessInfoRead,
  WorkspaceUserRead,
} from "core/api/types/AirbyteClient";
import { RbacRole, RbacRoleHierarchy, partitionPermissionType } from "core/utils/rbac/rbacPermissionsQuery";
export type ResourceType = "workspace" | "organization" | "instance";

export const permissionStringDictionary: Record<PermissionType, Record<string, string>> = {
  instance_admin: { resource: "resource.instance", role: "role.admin" },
  organization_admin: { resource: "resource.organization", role: "role.admin" },
  organization_editor: { resource: "resource.organization", role: "role.editor" },
  organization_reader: { resource: "resource.organization", role: "role.reader" },
  organization_member: { resource: "resource.organization", role: "role.member" },
  workspace_admin: { resource: "resource.workspace", role: "role.admin" },
  workspace_owner: { resource: "resource.workspace", role: "role.admin" },
  workspace_editor: { resource: "resource.workspace", role: "role.editor" },
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
  organization_reader: { id: "role.reader.description", values: { resourceType: "organization" } },
  organization_member: { id: "role.member.description", values: { resourceType: "organization" } },
  workspace_admin: { id: "role.admin.description", values: { resourceType: "workspace" } },
  workspace_owner: { id: "role.admin.description", values: { resourceType: "workspace" } }, // is not and should not be referenced in code.  required by types but will be deprecated soon.
  workspace_editor: { id: "role.editor.description", values: { resourceType: "workspace" } },
  workspace_reader: { id: "role.reader.description", values: { resourceType: "workspace" } },
};
export const permissionsByResourceType: Record<ResourceType, PermissionType[]> = {
  workspace: [
    PermissionType.workspace_admin,
    // PermissionType.workspace_editor,
    PermissionType.workspace_reader,
  ],
  organization: [
    PermissionType.organization_admin,
    // PermissionType.organization_editor,
    // PermissionType.organization_reader,
    PermissionType.organization_member,
  ],
  instance: [PermissionType.instance_admin],
};

export interface NextAccessUserRead {
  userId: string;
  email: string;
  name?: string;
  workspacePermission?: PermissionRead;
  organizationPermission?: PermissionRead;
}

export interface AccessUsers {
  workspace?: { users: WorkspaceUserRead[]; usersToAdd: OrganizationUserRead[] };
  organization?: { users: OrganizationUserRead[]; usersToAdd: [] };
}

export interface NextAccessUsers {
  workspace?: { users: NextAccessUserRead[]; usersToAdd: OrganizationUserRead[] };
}

export const useGetOrganizationAccessUsers = (): AccessUsers => {
  const workspace = useCurrentWorkspace();
  const organizationUsers = useListUsersInOrganization(workspace.organizationId ?? "").users;
  return {
    organization: { users: organizationUsers, usersToAdd: [] },
  };
};

export const getWorkspaceAccessLevel = (user: WorkspaceUserAccessInfoRead): RbacRole => {
  const orgPermissionType = user.organizationPermission?.permissionType;
  const workspacePermissionType = user.workspacePermission?.permissionType;

  const orgRole = orgPermissionType ? partitionPermissionType(orgPermissionType)[1] : undefined;
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
