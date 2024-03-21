import classNames from "classnames";

import { useCreatePermission, useCurrentOrganizationInfo, useCurrentWorkspace, useUpdatePermissions } from "core/api";
import { PermissionType, WorkspaceUserAccessInfoRead } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";

import styles from "./ChangeRoleMenuItem.module.scss";
import { ChangeRoleMenuItemContent } from "./ChangeRoleMenuItemContent";
import { ResourceType } from "../components/useGetAccessManagementData";

const useCreateOrUpdateRole = (
  user: WorkspaceUserAccessInfoRead,
  resourceType: ResourceType,
  permissionType: PermissionType
) => {
  const { mutateAsync: createPermission } = useCreatePermission();
  const { mutateAsync: updatePermission } = useUpdatePermissions();
  const { workspaceId } = useCurrentWorkspace();
  const organizationInfo = useCurrentOrganizationInfo();

  const existingPermissionIdForResourceType =
    resourceType === "organization"
      ? user.organizationPermission?.permissionId
      : resourceType === "workspace"
      ? user.workspacePermission?.permissionId
      : undefined;

  return async () => {
    if (!existingPermissionIdForResourceType) {
      if (resourceType === "organization") {
        if (!organizationInfo) {
          throw new Error("Organization info not found");
        }
        return createPermission({
          userId: user.userId,
          permissionType,
          organizationId: organizationInfo.organizationId,
        });
      }
      return createPermission({ userId: user.userId, permissionType, workspaceId });
    }

    return updatePermission({ permissionId: existingPermissionIdForResourceType, permissionType });
  };
};

export const disallowedRoles = (
  user: WorkspaceUserAccessInfoRead,
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

  const organizationRole = user.organizationPermission?.permissionType;

  if (organizationRole === "organization_editor") {
    return ["workspace_reader"];
  }
  if (organizationRole === "organization_admin") {
    return ["workspace_editor", "workspace_reader"];
  }

  return [];
};

interface RoleMenuItemProps {
  user: WorkspaceUserAccessInfoRead;
  permissionType: PermissionType;
  resourceType: ResourceType;
  onClose: () => void;
}

export const ChangeRoleMenuItem: React.FC<RoleMenuItemProps> = ({ user, permissionType, resourceType, onClose }) => {
  const createOrUpdateRole = useCreateOrUpdateRole(user, resourceType, permissionType);
  const currentUser = useCurrentUser();
  const isCurrentUser = currentUser.userId === user.userId;

  const roleIsActive =
    permissionType === user.workspacePermission?.permissionType ||
    permissionType === user.organizationPermission?.permissionType;

  const roleIsInvalid = disallowedRoles(user, resourceType, isCurrentUser).includes(permissionType);

  return (
    <button
      disabled={roleIsInvalid || roleIsActive}
      onClick={async () => {
        await createOrUpdateRole();
        onClose();
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
