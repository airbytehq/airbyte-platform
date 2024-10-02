import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { FeatureItem, useFeature } from "core/services/features";
import { useExperiment } from "hooks/services/Experiment";

import { CancelInvitationMenuItem } from "./CancelInvitationMenuItem";
import { ChangeRoleMenuItem } from "./ChangeRoleMenuItem";
import { RemoveRoleMenuItem } from "./RemoveRoleMenuItem";
import styles from "./RoleManagementMenuBody.module.scss";
import { ResourceType, UnifiedUserModel, permissionStringDictionary, permissionsByResourceType } from "./util";
interface RoleManagementMenuBodyProps {
  user: UnifiedUserModel;
  resourceType: ResourceType;
  close: () => void;
}
export const RoleManagementMenuBody: React.FC<RoleManagementMenuBodyProps> = ({ user, resourceType, close }) => {
  const areAllRbacRolesEnabled = useFeature(FeatureItem.AllowAllRBACRoles);
  const improvedOrganizationRbac = useExperiment("settings.organizationRbacImprovements");
  const rolesToAllow = !user.invitationStatus && areAllRbacRolesEnabled ? permissionsByResourceType[resourceType] : [];

  const showOrgRoleInWorkspaceMenu =
    resourceType === "workspace" &&
    user?.organizationPermission?.permissionType &&
    user?.organizationPermission?.permissionType !== "organization_member";

  // user is invited but not yet accepted
  const showCancelInvite = !!user.invitationStatus;
  // user is not invited (so has a relevant permission) and we're in a workspace OR the new UI is enabled
  // whether or not the button is disabled (due to, for instance, the user being the current user or having an org permission in the workspace table) is handled in the component
  const showRemoveUser = !user.invitationStatus && (resourceType === "workspace" || improvedOrganizationRbac);

  return (
    <ul className={styles.roleManagementMenu__rolesList}>
      {showOrgRoleInWorkspaceMenu && user?.organizationPermission?.permissionType && (
        <li className={styles.roleManagementMenu__listItem}>
          <Box pt="xl" pb="xl" px="lg" className={styles.roleManagementMenu__menuTitle}>
            <Text color="grey" align="center" size="sm" italicized bold={!user.organizationPermission?.permissionType}>
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
      {showCancelInvite && (
        <li className={styles.roleManagementMenu__listItem}>
          <CancelInvitationMenuItem user={user} />
        </li>
      )}
      {showRemoveUser && (
        <li className={styles.roleManagementMenu__listItem}>
          <RemoveRoleMenuItem user={user} resourceType={resourceType} />
        </li>
      )}
    </ul>
  );
};
