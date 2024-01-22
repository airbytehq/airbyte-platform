import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { ChangeRoleMenuItem } from "./ChangeRoleMenuItem";
import { RemoveRoleMenuItem } from "./RemoveRoleMenuItem";
import styles from "./RoleManagementMenuBody.module.scss";
import {
  NextAccessUserRead,
  ResourceType,
  permissionStringDictionary,
  permissionsByResourceType,
} from "../components/useGetAccessManagementData";
interface RoleManagementMenuBodyProps {
  user: NextAccessUserRead;
  resourceType: ResourceType;
  close: () => void;
}
export const RoleManagementMenuBody: React.FC<RoleManagementMenuBodyProps> = ({ user, resourceType, close }) => {
  return (
    <ul className={styles.roleManagementMenu__rolesList}>
      {resourceType === "workspace" &&
        user.organizationPermission?.permissionType &&
        user.organizationPermission?.permissionType !== "organization_member" && (
          <li className={styles.roleManagementMenu__listItem}>
            <Box pt="xl" pb="xl" px="lg" className={styles.roleManagementMenu__menuTitle}>
              <Text
                color="grey"
                align="center"
                size="sm"
                italicized
                bold={!user.organizationPermission?.permissionType}
              >
                <FormattedMessage
                  id="role.organization.userDescription"
                  values={{
                    name: user.name,
                    role: (
                      <FormattedMessage
                        id={permissionStringDictionary[user.organizationPermission.permissionType].role}
                      />
                    ),
                  }}
                />
              </Text>
            </Box>
          </li>
        )}
      {permissionsByResourceType[resourceType].map((permissionOption) => {
        return (
          <li key={permissionOption} className={styles.roleManagementMenu__listItem}>
            <ChangeRoleMenuItem
              permissionType={permissionOption}
              resourceType={resourceType}
              user={user}
              onClose={close}
            />
          </li>
        );
      })}

      {resourceType === "workspace" && (
        <li className={styles.roleManagementMenu__listItem}>
          <RemoveRoleMenuItem user={user} resourceType={resourceType} />
        </li>
      )}
    </ul>
  );
};
