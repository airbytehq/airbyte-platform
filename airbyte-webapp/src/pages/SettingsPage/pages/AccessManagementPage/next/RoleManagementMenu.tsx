import { autoUpdate, flip, offset, useFloating } from "@floating-ui/react-dom";
import { Popover } from "@headlessui/react";
import React from "react";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { useCurrentOrganizationInfo, useCurrentWorkspace } from "core/api";
import { WorkspaceUserAccessInfoRead } from "core/api/types/AirbyteClient";
import { useIntent } from "core/utils/rbac";

import styles from "./RoleManagementMenu.module.scss";
import { RoleManagementMenuBody } from "./RoleManagementMenuBody";
import { RoleIndicator } from "../components/RoleIndicator";
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

export const RoleManagementMenu: React.FC<RoleManagementMenuProps> = ({ user, resourceType }) => {
  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset(5), flip()],
    whileElementsMounted: autoUpdate,
    placement: "bottom-start",
  });
  const { workspaceId } = useCurrentWorkspace();
  const organizationInfo = useCurrentOrganizationInfo();
  const highestPermissionType = getWorkspaceAccessLevel(user);
  const orgPermissionType = user.organizationPermission ? user.organizationPermission.permissionType : undefined;
  const canEditPermissions = useIntent(
    resourceType === "workspace" ? "UpdateWorkspacePermissions" : "UpdateOrganizationPermissions",
    { workspaceId, organizationId: organizationInfo?.organizationId }
  );
  const cannotEditUser = resourceType === "workspace" && orgPermissionType === "organization_admin";
  const shouldHidePopover = cannotEditUser || !canEditPermissions;

  return shouldHidePopover ? (
    <Box pl="md" pr="sm">
      <FlexContainer gap="md" alignItems="center" justifyContent="flex-start">
        <UserRoleText highestPermissionType={highestPermissionType} />
        {canEditPermissions && orgPermissionType && <RoleIndicator permissionType={orgPermissionType} />}
      </FlexContainer>
    </Box>
  ) : (
    <FlexContainer gap="xs" alignItems="center">
      <Popover>
        {({ close }) => (
          <>
            <Popover.Button ref={reference} as={RoleManagementButton}>
              <FlexContainer direction="row" alignItems="center" gap="xs">
                <Box p="sm">
                  <UserRoleText highestPermissionType={highestPermissionType} />
                </Box>
                <Icon type="chevronDown" color="disabled" />
              </FlexContainer>
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
      {!orgPermissionType && <RoleIndicator />}
    </FlexContainer>
  );
};
