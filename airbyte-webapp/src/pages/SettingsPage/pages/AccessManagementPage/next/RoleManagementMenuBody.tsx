import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { FeatureItem, useFeature } from "core/services/features";

import { CancelInvitationMenuItem } from "./CancelInvitationMenuItem";
import { ChangeRoleMenuItem } from "./ChangeRoleMenuItem";
import { RemoveRoleMenuItem } from "./RemoveRoleMenuItem";
import styles from "./RoleManagementMenuBody.module.scss";
import {
  ResourceType,
  UnifiedWorkspaceUserModel,
  permissionStringDictionary,
  permissionsByResourceType,
} from "../components/useGetAccessManagementData";
interface RoleManagementMenuBodyProps {
  user: UnifiedWorkspaceUserModel;
  resourceType: ResourceType;
  close: () => void;
}
export const RoleManagementMenuBody: React.FC<RoleManagementMenuBodyProps> = ({ user, resourceType, close }) => {
  const areAllRbacRolesEnabled = useFeature(FeatureItem.AllowAllRBACRoles);
  /** this will break when we add additional organization role types.
      it would allow users an organization, but without the AllowAllRbacRoles feature to assign any
      organization role. 
      */

  const rolesToAllow =
    !user.invitationStatus && (areAllRbacRolesEnabled || resourceType === "organization")
      ? permissionsByResourceType[resourceType]
      : [];

  return (
    <ul className={styles.roleManagementMenu__rolesList}>
      {resourceType === "workspace" &&
        user?.organizationPermission?.permissionType &&
        user?.organizationPermission?.permissionType !== "organization_member" && (
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
                    name: user.userName || user.userEmail,
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
      {rolesToAllow.map((permissionOption) => {
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
      {resourceType === "workspace" && !!user.invitationStatus && (
        <li className={styles.roleManagementMenu__listItem}>
          <CancelInvitationMenuItem user={user} />
        </li>
      )}
      {resourceType === "workspace" && !user.invitationStatus && (
        <li className={styles.roleManagementMenu__listItem}>
          <RemoveRoleMenuItem user={user} resourceType={resourceType} />
        </li>
      )}
    </ul>
  );
};
