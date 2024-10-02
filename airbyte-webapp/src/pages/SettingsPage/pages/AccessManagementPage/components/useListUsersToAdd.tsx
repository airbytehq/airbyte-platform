import { useCurrentWorkspace, useListUsersInOrganization, useListWorkspaceAccessUsers } from "core/api";
import { ScopeType } from "core/api/types/AirbyteClient";
import { useIntent } from "core/utils/rbac";

export const useListUsersToAdd = (scope: ScopeType, deferredSearchValue: string) => {
  const { workspaceId, organizationId } = useCurrentWorkspace();

  const canListUsersInOrganization = useIntent("ListOrganizationMembers", {
    organizationId,
  });
  const { users } = useListUsersInOrganization(canListUsersInOrganization ? organizationId : undefined);
  const { usersWithAccess } = useListWorkspaceAccessUsers(workspaceId);

  const userMap = new Map();

  if (scope === "workspace") {
    /*    Before the user begins typing an email address, the list of users should only be users
          who can be added to the workspace (organization users who aren't org_admin + don't have a workspace permission).  
      
          When they begin typing, we filter a list that is a superset of workspaceAccessUsers + organization users.  We want to prefer the workspaceAccessUsers
          object for a given user (if present) because it contains all relevant permissions for the user.  
          
          Then, we enrich that from the list of organization_members who don't have a permission to this workspace.
      */
    usersWithAccess
      .filter((user) => {
        return deferredSearchValue.length > 0
          ? true // include all workspaceAccessUsers if there _is_ a search value
          : !user.workspacePermission && !(user.organizationPermission?.permissionType === "organization_admin"); // otherwise, show only those who can be "upgraded" by creating a permission
      })
      .forEach((user) => {
        userMap.set(user.userId, {
          userId: user.userId,
          userName: user.userName,
          userEmail: user.userEmail,
          organizationPermission: user.organizationPermission,
          workspacePermission: user.workspacePermission,
        });
      });

    users.forEach((user) => {
      if (
        user.permissionType === "organization_member" && // they are an organization_member
        !usersWithAccess.some((u) => u.userId === user.userId) // they don't have a workspace permission (they may not be listed)
      ) {
        userMap.set(user.userId, {
          userId: user.userId,
          userName: user.name,
          userEmail: user.email,
          organizationPermission: {
            permissionId: user.permissionId,
            permissionType: user.permissionType,
            organizationId: user.organizationId,
            userId: user.userId,
          },
        });
      }
    });
  } else if (scope === "organization") {
    /*    Before the user begins typing an email address, do not list any users 
      
          When they begin typing, we show any existing organization members that match (and their permissions).
          These users cannot be invited (because they're already there), but helps give clarity as to why they can't be invited.
          
      */
    users
      .filter((_user) => (deferredSearchValue.length > 0 ? true : false))
      .forEach((user) => {
        userMap.set(user.userId, {
          userId: user.userId,
          userName: user.name,
          userEmail: user.email,
          organizationPermission: {
            permissionId: user.permissionId,
            permissionType: user.permissionType,
            organizationId: user.organizationId,
            userId: user.userId,
          },
        });
      });
  }

  return Array.from(userMap.values()).filter((user) => {
    return (
      user.userName?.toLowerCase().includes(deferredSearchValue.toLowerCase()) ||
      user.userEmail?.toLowerCase().includes(deferredSearchValue.toLowerCase())
    );
  });
};
