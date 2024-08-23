import { useCurrentWorkspace, useListPermissions } from "core/api";
import { useCurrentUser } from "core/services/auth";
import { assertNever } from "core/utils/asserts";

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
      switch (permission.permissionType) {
        case "organization_admin":
        case "organization_editor":
        case "organization_reader":
        case "organization_member":
          return permission.permissionType === role && permission.organizationId === organizationId;
        case "workspace_owner":
        case "workspace_admin":
        case "workspace_editor":
        case "workspace_reader":
          return permission.permissionType === role && permission.workspaceId === workspaceId;
        case "instance_admin":
          return permission.permissionType === role;
        default:
          return assertNever(permission.permissionType);
      }
    });
  });

  return hasPermission;
};
