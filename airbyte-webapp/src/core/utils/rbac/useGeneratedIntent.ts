import { useCurrentWorkspaceOrUndefined, useListPermissions } from "core/api";
import { PermissionType } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";
import { assertNever } from "core/utils/asserts";

import { INTENTS, Intent } from "./generated-intents";

interface MetaOverride {
  organizationId?: string;
  workspaceId?: string;
}

export const useGeneratedIntent = (intentName: Intent, metaOverride?: MetaOverride) => {
  const workspace = useCurrentWorkspaceOrUndefined();
  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);
  const intent = INTENTS[intentName];

  // If metaOverride is provided, use those values instead of workspace values
  const organizationId = metaOverride?.organizationId || workspace?.organizationId;
  const workspaceId = metaOverride?.workspaceId || workspace?.workspaceId;

  // If we don't have either an organization ID or workspace ID, return false
  if (!organizationId && !workspaceId) {
    return false;
  }

  const hasPermission = intent.roles.some((role) => {
    return permissions.some((permission) => {
      switch (permission.permissionType) {
        case "organization_admin":
        case "organization_editor":
        case "organization_runner":
        case "organization_reader":
        case "organization_member":
          return permission.permissionType === role && permission.organizationId === organizationId;
        case "workspace_owner":
        case "workspace_admin":
        case "workspace_editor":
        case "workspace_runner":
        case "workspace_reader":
          return permission.permissionType === role && permission.workspaceId === workspaceId;
        // instance_reader is a frontend-only role that is used to support the admin "viewing/editing" feature. We will need to re-engineer that feature now that we are moving away from hierarchical permissions. But for now, this is here to avoid a runtime error when a useGeneratedIntent() encounters an instance_reader permission.
        case "instance_reader" as PermissionType:
        case "instance_admin":
          return permission.permissionType === role;
        default:
          return assertNever(permission.permissionType);
      }
    });
  });

  return hasPermission;
};
