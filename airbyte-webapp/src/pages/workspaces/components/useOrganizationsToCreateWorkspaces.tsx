import { useListOrganizationsById, useListPermissions } from "core/api";
import { useCurrentUser } from "core/services/auth";

/**
 * This hook is dependent upon the configdb Permissions table and thus should only be utilized in OSS/Enterprise for the time being.
 *
 * May be utilized in cloud once:
 * - Cloud uses configdb permissions
 * - Cloud uses organizations
 * - All users in Cloud have an organization (their own default one + any they might have from SSO)
 */
export const useOrganizationsToCreateWorkspaces = () => {
  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);
  const rolesToFilterBy = ["organization_admin", "organization_editor"];

  const organizationIds = [];

  for (const permission of permissions) {
    if (rolesToFilterBy.includes(permission.permissionType) && permission.organizationId) {
      organizationIds.push(permission.organizationId);
    }
  }

  return useListOrganizationsById(organizationIds);
};
