import classNames from "classnames";

import { useCreatePermission, useCurrentWorkspace, useUpdatePermissions } from "core/api";
import { PermissionType } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";

import styles from "./ChangeRoleMenuItem.module.scss";
import { ChangeRoleMenuItemContent } from "./ChangeRoleMenuItemContent";
import { ResourceType, UnifiedWorkspaceUserModel } from "./useGetAccessManagementData";

const useCreateOrUpdateRole = (
  user: UnifiedWorkspaceUserModel,
  resourceType: ResourceType,
  permissionType: PermissionType
) => {
  const { mutateAsync: createPermission } = useCreatePermission();
  const { mutateAsync: updatePermission } = useUpdatePermissions();
  const { workspaceId, organizationId } = useCurrentWorkspace();

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
  user: UnifiedWorkspaceUserModel | null,
  targetResourceType: ResourceType,
  isCurrentUser: boolean
): PermissionType[] => {
  if (isCurrentUser) {
    return [
      "organization_admin",
      "organization_editor",
      "organization_reader",
      "organization_member",
      "workspace_admin",
      "workspace_editor",
      "workspace_reader",
    ];
  }

  if (targetResourceType === "organization") {
    return [];
  }

  const organizationRole = user?.organizationPermission?.permissionType;

  if (organizationRole === "organization_reader") {
    return ["workspace_reader"];
  }
  if (organizationRole === "organization_editor") {
    return ["workspace_editor", "workspace_reader"];
  }
  if (organizationRole === "organization_admin") {
    return ["workspace_admin", "workspace_editor", "workspace_reader"];
  }

  return [];
};

interface RoleMenuItemProps {
  user: UnifiedWorkspaceUserModel;
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
