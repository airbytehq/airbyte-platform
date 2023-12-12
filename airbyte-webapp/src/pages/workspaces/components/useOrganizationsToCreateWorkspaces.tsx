import { useListOrganizationsById, useListPermissions } from "core/api";
import { useCurrentUser } from "core/services/auth";

export const useOrganizationsToCreateWorkspaces = () => {
  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);
  const creatableOrganizationIds: string[] = [];
  const memberOrganizationIds: string[] = [];

  // this variable is to support the interim state where not all users/workspaces in cloud are within an organization.
  // once that migration is complete, we can remove this and its accompanying checks, as workspace creation will be solely dependent
  // upon having adequate permissions in 1+ organization.
  let hasOrganization = false;

  for (const permission of permissions) {
    if (permission.organizationId) {
      hasOrganization = true;
      if (permission.permissionType === "organization_admin" || permission.permissionType === "organization_editor") {
        creatableOrganizationIds.push(permission.organizationId);
      }

      if (permission.permissionType === "organization_member") {
        memberOrganizationIds.push(permission.organizationId);
      }
    }
  }

  return {
    hasOrganization,
    organizationsToCreateIn: useListOrganizationsById(creatableOrganizationIds),
    organizationsMemberOnly: useListOrganizationsById(memberOrganizationIds),
  };
};
