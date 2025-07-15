import classNames from "classnames";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCreatePermission, useCurrentWorkspaceOrUndefined, useUpdatePermissions } from "core/api";
import { PermissionType } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";

import styles from "./ChangeRoleMenuItem.module.scss";
import { ChangeRoleMenuItemContent } from "./ChangeRoleMenuItemContent";
import { ResourceType, UnifiedUserModel } from "./util";

const useCreateOrUpdateRole = (user: UnifiedUserModel, resourceType: ResourceType, permissionType: PermissionType) => {
  const { mutateAsync: createPermission } = useCreatePermission();
  const { mutateAsync: updatePermission } = useUpdatePermissions();
  const workspaceId = useCurrentWorkspaceOrUndefined()?.workspaceId;
  const organizationId = useCurrentOrganizationId();

  const existingPermissionIdForResourceType =
    resourceType === "organization"
      ? user.organizationPermission?.permissionId
      : resourceType === "workspace"
      ? user.workspacePermission?.permissionId
      : undefined;

  return async () => {
    if (!existingPermissionIdForResourceType) {
      if (resourceType === "organization") {
        return createPermission({
          userId: user.id,
          permissionType,
          organizationId,
        });
      }
      return createPermission({ userId: user.id, permissionType, workspaceId });
    }

    return updatePermission({ permissionId: existingPermissionIdForResourceType, permissionType });
  };
};

export const disallowedRoles = (
  user: UnifiedUserModel | null,
  targetResourceType: ResourceType,
  isCurrentUser: boolean
): PermissionType[] => {
  if (isCurrentUser) {
    return [
      "organization_admin",
      "organization_editor",
      "organization_runner",
      "organization_reader",
      "organization_member",
      "workspace_admin",
      "workspace_editor",
      "workspace_runner",
      "workspace_reader",
    ];
  }

  if (targetResourceType === "organization") {
    return [];
  }

  const organizationRole = user?.organizationPermission?.permissionType;
  const workspaceRole = user?.workspacePermission?.permissionType;

  if (organizationRole === "organization_reader") {
    if (!workspaceRole) {
      return ["workspace_reader"];
    }
    return [];
  }

  if (organizationRole === "organization_runner") {
    if (!workspaceRole) {
      return ["workspace_runner", "workspace_reader"];
    }
    return ["workspace_reader"];
  }

  if (organizationRole === "organization_editor") {
    if (!workspaceRole) {
      return ["workspace_editor", "workspace_runner", "workspace_reader"];
    }
    return ["workspace_runner", "workspace_reader"];
  }

  if (organizationRole === "organization_admin") {
    return ["workspace_admin", "workspace_editor", "workspace_runner", "workspace_reader"];
  }

  return [];
};

interface RoleMenuItemProps {
  user: UnifiedUserModel;
  permissionType: PermissionType;
  resourceType: ResourceType;
  onClose: () => void;
}

export const ChangeRoleMenuItem: React.FC<RoleMenuItemProps> = ({ user, permissionType, resourceType, onClose }) => {
  const createOrUpdateRole = useCreateOrUpdateRole(user, resourceType, permissionType);
  const currentUser = useCurrentUser();
  const isCurrentUser = currentUser.userId === user.id;

  const roleIsActive =
    permissionType === user.workspacePermission?.permissionType ||
    permissionType === user.organizationPermission?.permissionType;

  const roleIsInvalid = disallowedRoles(user, resourceType, isCurrentUser).includes(permissionType);

  return (
    <button
      disabled={roleIsInvalid || roleIsActive}
      onClick={async () => {
        onClose();
        await createOrUpdateRole();
      }}
      className={classNames(styles.changeRoleMenuItem__button, {
        [styles["changeRoleMenuItem__button--active"]]: roleIsActive,
      })}
    >
      <ChangeRoleMenuItemContent
        roleIsActive={roleIsActive}
        roleIsInvalid={roleIsInvalid}
        permissionType={permissionType}
      />
    </button>
  );
};
