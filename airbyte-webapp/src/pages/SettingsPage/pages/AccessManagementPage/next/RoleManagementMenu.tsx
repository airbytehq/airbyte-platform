import { autoUpdate, flip, offset, useFloating } from "@floating-ui/react-dom";
import { Popover } from "@headlessui/react";
import React from "react";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { WorkspaceUserAccessInfoRead } from "core/api/types/AirbyteClient";

import { RoleManagementButton } from "./RoleManagementButton";
import styles from "./RoleManagementMenu.module.scss";
import { RoleManagementMenuBody } from "./RoleManagementMenuBody";
import { getWorkspaceAccessLevel } from "../components/useGetAccessManagementData";
import { UserRoleText } from "../components/UserRoleText";

type ResourceType = "workspace" | "organization" | "instance";

export interface RoleManagementMenuProps {
  user: WorkspaceUserAccessInfoRead;
  resourceType: ResourceType;
}

export const RoleManagementMenu: React.FC<RoleManagementMenuProps> = ({ user, resourceType }) => {
  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset(5), flip()],
    whileElementsMounted: autoUpdate,
    placement: "bottom-start",
  });
  const highestPermissionType = getWorkspaceAccessLevel(user);

  return (
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
  );
};
