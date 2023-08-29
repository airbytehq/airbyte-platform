import { useCurrentWorkspace, useListUsersInOrganization, useListUsersInWorkspace } from "core/api";
import { OrganizationUserRead, PermissionType, WorkspaceUserRead } from "core/request/AirbyteClient";

export type ResourceType = "workspace" | "organization" | "instance";

export const permissionStringDictionary: Record<PermissionType, string> = {
  instance_admin: "role.admin",
  organization_admin: "role.admin",
  organization_editor: "role.editor",
  organization_reader: "role.reader",
  workspace_admin: "role.admin",
  workspace_owner: "role.admin",
  workspace_editor: "role.editor",
  workspace_reader: "role.reader",
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
  workspace_admin: { id: "role.admin.description", values: { resourceType: "workspace" } },
  workspace_owner: { id: "role.admin.description", values: { resourceType: "workspace" } }, // is not and should not be referenced in code.  required by types but will be deprecated soon.
  workspace_editor: { id: "role.editor.description", values: { resourceType: "workspace" } },
  workspace_reader: { id: "role.reader.description", values: { resourceType: "workspace" } },
};

export const tableTitleDictionary: Record<ResourceType, string> = {
  workspace: "settings.accessManagement.workspace",
  organization: "settings.accessManagement.organization",
  instance: "settings.accessManagement.instance",
};

export const permissionsByResourceType: Record<ResourceType, PermissionType[]> = {
  workspace: [PermissionType.workspace_admin, PermissionType.workspace_editor, PermissionType.workspace_reader],
  organization: [
    PermissionType.organization_admin,
    PermissionType.organization_editor,
    PermissionType.organization_reader,
  ],
  instance: [PermissionType.instance_admin],
};

export interface AccessUsers {
  workspace?: WorkspaceUserRead[];
  organization?: OrganizationUserRead[];
  instance?: WorkspaceUserRead[];
}

export const useGetWorkspaceAccessUsers = (): AccessUsers => {
  const workspace = useCurrentWorkspace();
  const workspaceUsers = useListUsersInWorkspace(workspace.workspaceId).users;
  const organizationUsers = useListUsersInOrganization(workspace.organizationId ?? "").users;
  const instanceUsers: WorkspaceUserRead[] = []; // todo: temporary hacky workaround until we have an endpoint

  return {
    workspace: workspaceUsers,
    organization: organizationUsers,
    instance: instanceUsers,
  };
};

export const useGetOrganizationAccessUsers = (): AccessUsers => {
  const workspace = useCurrentWorkspace();
  const organizationUsers = useListUsersInOrganization(workspace.organizationId ?? "").users;
  const instanceUsers: WorkspaceUserRead[] = []; // todo: temporary hacky workaround until we have an endpoint

  return {
    organization: organizationUsers,
    instance: instanceUsers,
  };
};
