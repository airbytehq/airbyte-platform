import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useDeletePermissions, useUpdatePermissions } from "core/api";
import { PermissionRead, PermissionType, PermissionUpdate } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";
import { useIntent } from "core/utils/rbac";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";

import styles from "./RoleManagementControl.module.scss";
import { ResourceType, permissionStringDictionary, permissionsByResourceType } from "./useGetAccessManagementData";

interface RoleManagementControlProps {
  userName?: string;
  resourceName: string;
  pageResourceType: ResourceType;
  tableResourceType: ResourceType;
  activeEditRow?: string;
  setActiveEditRow: (permissionId?: string) => void;
  permission: PermissionRead;
}

const roleManagementFormSchema = yup.object().shape({
  permissionType: yup.mixed<PermissionType>().oneOf(Object.values(PermissionType)).required(),
  permissionId: yup.string().required(),
});

export const RoleManagementControl: React.FC<RoleManagementControlProps> = ({
  permission,
  pageResourceType,
  tableResourceType,
  userName,
  resourceName,
  activeEditRow,
  setActiveEditRow,
}) => {
  const { mutateAsync: updatePermissions } = useUpdatePermissions();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { mutateAsync: deletePermissions } = useDeletePermissions();
  const { formatMessage } = useIntl();
  const { permissionId, permissionType, userId } = permission;

  const isEditMode = activeEditRow === permissionId;
  const currentUser = useCurrentUser();

  const isCurrentUsersPermission = userId === currentUser.userId;

  const intentKey =
    tableResourceType === "organization" ? "UpdateOrganizationPermissions" : "UpdateWorkspacePermissions";

  const intentMeta = permission.hasOwnProperty("organizationId")
    ? { organizationId: permission.organizationId }
    : { workspaceId: permission.workspaceId };

  const canUpdateUserPermissions = useIntent(intentKey, intentMeta);

  if (!permissionType) {
    return null;
  }

  if (pageResourceType !== tableResourceType || !canUpdateUserPermissions || isCurrentUsersPermission) {
    return (
      <Box py="sm">
        <FormattedMessage id={`${permissionStringDictionary[permissionType]}`} />
      </Box>
    );
  }

  const onSubmitRoleChangeClick = async (values: PermissionUpdate) => {
    const permission = await updatePermissions(values);
    if (permission) {
      setActiveEditRow(undefined);
    }
  };

  const onRemoveUserClick = () => {
    openConfirmationModal({
      title: "Remove User",
      text: formatMessage(
        {
          id: "settings.accessManagement.removePermissions",
        },
        { user: userName, resource: resourceName }
      ),
      additionalContent: (
        <Box pt="md">
          <Text>
            <FormattedMessage id="settings.accessManagement.removePermissions.additionalContent" />
          </Text>
        </Box>
      ),
      onSubmit: async () => {
        await deletePermissions(permissionId);
        closeConfirmationModal();
      },
      submitButtonText: "settings.accessManagement.confirmRemovePermissions",
    });
  };

  return (
    <>
      {isEditMode ? (
        <Form<PermissionUpdate>
          schema={roleManagementFormSchema}
          defaultValues={{ permissionType, permissionId }}
          onSubmit={onSubmitRoleChangeClick}
        >
          <FlexContainer alignItems="center" gap="2xl">
            <FormControl<PermissionUpdate>
              containerControlClassName={styles.roleManagementControl__input}
              name="permissionType"
              fieldType="dropdown"
              options={permissionsByResourceType[tableResourceType].map((permission) => {
                return {
                  value: permission,
                  label: (
                    <Box px="sm">
                      <FormattedMessage id={permissionStringDictionary[permission]} />
                    </Box>
                  ),
                  disabled: permission === permissionType,
                };
              })}
            />
            <FormSubmissionButtons
              submitKey="form.saveChanges"
              onCancelClickCallback={() => setActiveEditRow(undefined)}
              allowNonDirtyCancel
            />
          </FlexContainer>
        </Form>
      ) : (
        <FlexContainer alignItems="center" gap="2xl">
          <Text className={styles.roleManagementControl__roleLabel_view}>
            <FormattedMessage id={`${permissionStringDictionary[permissionType]}`} />
          </Text>
          <FlexContainer>
            {/* for initial release, there is only a single workspace-level role: workspace_admin */}
            {tableResourceType !== "workspace" && (
              <Button
                variant="secondary"
                onClick={() => setActiveEditRow(permissionId)}
                className={styles.roleManagementControl__button}
              >
                <FormattedMessage id="settings.accessManagement.changeRole" />
              </Button>
            )}
            {/* for initial release, organization-level permission creation + deletion are to be handled in a user's SSO provider */}
            {tableResourceType !== "organization" && (
              <Button variant="danger" onClick={onRemoveUserClick} className={styles.roleManagementControl__button}>
                <FormattedMessage id="settings.accessManagement.user.remove" />
              </Button>
            )}
          </FlexContainer>
        </FlexContainer>
      )}
    </>
  );
};
