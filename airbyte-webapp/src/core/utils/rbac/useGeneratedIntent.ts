import { useCurrentWorkspace, useListPermissions } from "core/api";
import { useCurrentUser } from "core/services/auth";

import { INTENTS } from "./generated-intents";

interface MetaOverride {
  organizationId?: string;
  workspaceId?: string;
}

export const useGeneratedIntent = (intentName: keyof typeof INTENTS, metaOverride?: MetaOverride) => {
  const { workspaceId: currentWorkspaceId, organizationId: currentOrganizationId } = useCurrentWorkspace();
  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);
  const intent = INTENTS[intentName];

  const organizationId = metaOverride?.organizationId || currentOrganizationId;
  const workspaceId = metaOverride?.workspaceId || currentWorkspaceId;

  const hasPermission = intent.roles.some((role) => {
    return permissions.some((permission) => {
      if (
        permission.permissionType.indexOf("organization") !== -1 &&
        permission.organizationId === organizationId &&
        permission.permissionType === role
      ) {
        return true;
      }
      if (
        permission.permissionType.indexOf("workspace") !== -1 &&
        permission.workspaceId === workspaceId &&
        permission.permissionType === role
      ) {
        return true;
      }
      return false;
    });
  });

  return hasPermission;
};
