import { autoUpdate, flip, offset, useFloating } from "@floating-ui/react-dom";
import { Popover, PopoverButton, PopoverPanel } from "@headlessui/react";
import React from "react";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { RbacRole } from "core/utils/rbac/rbacPermissionsQuery";

import { RoleManagementButton } from "./RoleManagementButton";
import styles from "./RoleManagementMenu.module.scss";
import { RoleManagementMenuBody } from "./RoleManagementMenuBody";
import { UserRoleText } from "./UserRoleText";
import { UnifiedUserModel } from "./util";

type ResourceType = "workspace" | "organization" | "instance";

export interface RoleManagementMenuProps {
  user: UnifiedUserModel;
  resourceType: ResourceType;
  highestPermissionType: RbacRole;
}

export const RoleManagementMenu: React.FC<RoleManagementMenuProps> = ({
  user,
  resourceType,
  highestPermissionType,
}) => {
  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset(5), flip()],
    whileElementsMounted: autoUpdate,
    placement: "bottom-start",
  });

  return (
    <Popover>
      {({ close }) => (
        <>
          <PopoverButton ref={reference} as={RoleManagementButton}>
            <Box py="sm" px="xs">
              <FlexContainer direction="row" alignItems="center" gap="xs">
                <UserRoleText highestPermissionType={highestPermissionType} />
                <FlexItem>
                  <Icon type="chevronDown" color="disabled" size="sm" />
                </FlexItem>
              </FlexContainer>
            </Box>
          </PopoverButton>
          <PopoverPanel
            ref={floating}
            style={{
              position: strategy,
              top: y ?? 0,
              left: x ?? 0,
            }}
            className={styles.roleManagementMenu__popoverPanel}
          >
            <RoleManagementMenuBody user={user} resourceType={resourceType} close={close} />
          </PopoverPanel>
        </>
      )}
    </Popover>
  );
};
