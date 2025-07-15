import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceOrUndefined, useDeletePermissions, useOrganization } from "core/api";
import { useCurrentUser } from "core/services/auth";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";

import styles from "./RemoveRoleMenuItem.module.scss";
import { ResourceType, UnifiedUserModel } from "./util";

interface RemoveRoleMenuItemProps {
  user: UnifiedUserModel;
  resourceType: ResourceType;
}

export const RemoveRoleMenuItem: React.FC<RemoveRoleMenuItemProps> = ({ user, resourceType }) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  let permissionToRemove = "";

  if (resourceType === "organization") {
    permissionToRemove = user.organizationPermission?.permissionId ?? "";
  } else if (!user.organizationPermission || user.organizationPermission?.permissionType === "organization_member") {
    permissionToRemove = user.workspacePermission?.permissionId ?? "";
  }

  const organizationId = useCurrentOrganizationId();
  const { organizationName } = useOrganization(organizationId);
  const workspaceName = useCurrentWorkspaceOrUndefined()?.name;
  const { formatMessage } = useIntl();
  const { userId: currentUserId } = useCurrentUser();
  const resourceName = resourceType === "organization" ? organizationName : workspaceName;
  const nameToDisplay = user.userName || user.userEmail;
  const { mutateAsync: removePermission } = useDeletePermissions();

  const openRemoveUserModal = () =>
    openConfirmationModal({
      text: (
        <>
          <FormattedMessage
            id="settings.accessManagement.removePermissions"
            values={{
              user: (
                <Text as="span" italicized>
                  {nameToDisplay}
                </Text>
              ),
              resource: (
                <Text as="span" italicized>
                  {resourceName}
                </Text>
              ),
            }}
          />
          {resourceType === "organization" && (
            <Box pt="md">
              <Text color="grey400">
                <FormattedMessage
                  id="settings.accessManagement.removePermissions.orgWarning"
                  values={{
                    resource: (
                      <Text as="span" color="grey400" italicized>
                        {resourceName}
                      </Text>
                    ),
                  }}
                />
              </Text>
            </Box>
          )}
        </>
      ),
      title: formatMessage({ id: "settings.accessManagement.removeUser" }),
      submitButtonText: formatMessage({ id: "settings.accessManagement.removeUser" }),
      onSubmit: async () => {
        await removePermission(permissionToRemove);
        closeConfirmationModal();
      },
      submitButtonDataId: "remove",
    });

  return (
    <button
      onClick={openRemoveUserModal}
      disabled={currentUserId === user.id || permissionToRemove.length === 0}
      className={styles.removeRoleMenuItem__button}
    >
      <Box py="lg" px="md">
        <Text color={currentUserId === user.id || permissionToRemove.length === 0 ? "red200" : "red"}>
          <FormattedMessage
            id="settings.accessManagement.removeUserFrom"
            values={{
              scope: resourceType,
            }}
          />
        </Text>
      </Box>
    </button>
  );
};
