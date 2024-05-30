import { useListOrganizationsById, useListPermissions } from "core/api";
import { useCurrentUser } from "core/services/auth";

export const useOrganizationsToCreateWorkspaces = () => {
  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);
  const creatableOrganizationIds: string[] = [];
  const memberOrganizationIds: string[] = [];

  for (const permission of permissions) {
    if (permission.organizationId) {
      if (permission.permissionType === "organization_admin") {
        creatableOrganizationIds.push(permission.organizationId);
      }

      if (permission.permissionType === "organization_member") {
        memberOrganizationIds.push(permission.organizationId);
      }
    }
  }

  return {
    organizationsToCreateIn: useListOrganizationsById(creatableOrganizationIds),
    organizationsMemberOnly: useListOrganizationsById(memberOrganizationIds),
  };
};
