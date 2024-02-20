import { autoUpdate, flip, offset, useFloating } from "@floating-ui/react-dom";
import { Popover } from "@headlessui/react";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentOrganizationInfo, useCurrentWorkspace } from "core/api";
import { WorkspaceUserAccessInfoRead } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";
import { useIntent } from "core/utils/rbac";

import { GuestBadge } from "./GuestBadge";
import styles from "./RoleManagementMenu.module.scss";
import { RoleManagementMenuBody } from "./RoleManagementMenuBody";
import { getWorkspaceAccessLevel } from "../components/useGetAccessManagementData";
import { UserRoleText } from "../components/UserRoleText";

type ResourceType = "workspace" | "organization" | "instance";

export interface RoleManagementMenuProps {
  user: WorkspaceUserAccessInfoRead;
  resourceType: ResourceType;
}

const RoleManagementButton = React.forwardRef<HTMLButtonElement | null, React.ButtonHTMLAttributes<HTMLButtonElement>>(
  ({ children, ...props }, ref) => {
    return (
      <button className={styles.roleManagementMenu__popoverButton} ref={ref} {...props}>
        {children}
      </button>
    );
  }
);

RoleManagementButton.displayName = "RoleManagementButton";

const ViewOnlyRoleBox: React.FC<{ highestPermissionType: "MEMBER" | "ADMIN" | "READER" | "EDITOR" }> = ({
  highestPermissionType,
}) => {
  return (
    <Box pl="md" pr="sm">
      <FlexContainer gap="md" alignItems="center" justifyContent="flex-start">
        <UserRoleText highestPermissionType={highestPermissionType} />
      </FlexContainer>
    </Box>
  );
};

export const RoleManagementMenu: React.FC<RoleManagementMenuProps> = ({ user, resourceType }) => {
  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset(5), flip()],
    whileElementsMounted: autoUpdate,
    placement: "bottom-start",
  });
  const { workspaceId } = useCurrentWorkspace();
  const organizationInfo = useCurrentOrganizationInfo();
  const highestPermissionType = getWorkspaceAccessLevel(user);
  const currentUser = useCurrentUser();
  const orgPermissionType = user.organizationPermission ? user.organizationPermission.permissionType : undefined;
  const canEditPermissions = useIntent(
    resourceType === "workspace" ? "UpdateWorkspacePermissions" : "UpdateOrganizationPermissions",
    { workspaceId, organizationId: organizationInfo?.organizationId }
  );
  const canListOrganizationUsers = useIntent("ListOrganizationMembers", {
    organizationId: organizationInfo?.organizationId,
  });
  const cannotDemoteUser = resourceType === "workspace" && orgPermissionType === "organization_admin";
  const shouldHidePopover = cannotDemoteUser || !canEditPermissions || user.userId === currentUser.userId;

  const tooltipContent =
    cannotDemoteUser && canEditPermissions
      ? "settings.accessManagement.cannotDemoteOrgAdmin"
      : user.userId === currentUser.userId && canEditPermissions
      ? "settings.accessManagement.cannotEditOwnPermissions"
      : undefined;

  return (
    <FlexContainer gap="xs" alignItems="center">
      {shouldHidePopover ? (
        tooltipContent ? (
          <Tooltip control={<ViewOnlyRoleBox highestPermissionType={highestPermissionType} />}>
            <FormattedMessage id={tooltipContent} />
          </Tooltip>
        ) : (
          <ViewOnlyRoleBox highestPermissionType={highestPermissionType} />
        )
      ) : (
        <Popover>
          {({ close }) => (
            <>
              <Popover.Button ref={reference} as={RoleManagementButton}>
                <Box py="sm" px="xs">
                  <FlexContainer direction="row" alignItems="center" gap="xs">
                    <UserRoleText highestPermissionType={highestPermissionType} />
                    <FlexItem>
                      <Icon type="chevronDown" color="disabled" size="sm" />
                    </FlexItem>
                  </FlexContainer>
                </Box>
              </Popover.Button>
              <Popover.Panel
                ref={floating}
                style={{
                  position: strategy,
                  top: y ?? 0,
                  left: x ?? 0,
                }}
                className={styles.roleManagementMenu__popoverPanel}
              >
                <RoleManagementMenuBody user={user} resourceType={resourceType} close={close} />
              </Popover.Panel>
            </>
          )}
        </Popover>
      )}
      {user.organizationPermission?.permissionType === "organization_admin" && resourceType === "workspace" && (
        <Badge variant="grey">
          <FormattedMessage id="role.organizationAdmin" />
        </Badge>
      )}
      {canListOrganizationUsers && organizationInfo?.organizationId && (
        <GuestBadge userId={user.userId} organizationId={organizationInfo.organizationId} />
      )}
    </FlexContainer>
  );
};
