import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useDeletePermissions, useUpdatePermissions } from "core/api";
import { PermissionType, PermissionUpdate } from "core/request/AirbyteClient";
import { useIntent } from "core/utils/rbac/intent";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";

import styles from "./RoleManagementControl.module.scss";
import { ResourceType, permissionStringDictionary, permissionsByResourceType } from "./useGetAccessManagementData";

interface RoleManagementControlProps extends PermissionUpdate {
  userName?: string;
  resourceName: string;
  pageResourceType: ResourceType;
  tableResourceType: ResourceType;
  activeEditRow?: string;
  setActiveEditRow: (permissionId?: string) => void;
}

const roleManagementFormSchema = yup.object().shape({
  userId: yup.string().required(),
  permissionType: yup.mixed<PermissionType>().oneOf(Object.values(PermissionType)).required(),
  permissionId: yup.string().required(),
  workspaceId: yup.string(),
  organizationId: yup.string(),
});

export const RoleManagementControl: React.FC<RoleManagementControlProps> = ({
  userId,
  permissionType,
  workspaceId,
  organizationId,
  pageResourceType,
  tableResourceType,
  permissionId,
  userName,
  resourceName,
  activeEditRow,
  setActiveEditRow,
}) => {
  const { mutateAsync: updatePermissions } = useUpdatePermissions();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { mutateAsync: deletePermissions } = useDeletePermissions();
  const { formatMessage } = useIntl();
  const isEditMode = activeEditRow === permissionId;

  const intentKey =
    tableResourceType === "organization" ? "UpdateOrganizationPermissions" : "UpdateWorkspacePermissions";

  const intentMeta = tableResourceType === "organization" ? { organizationId } : { workspaceId };

  const canUpdateUserPermissions = useIntent(intentKey, intentMeta);

  if (!permissionType) {
    return null;
  }

  if (pageResourceType !== tableResourceType || !canUpdateUserPermissions) {
    return (
      <Box py="sm">
        <FormattedMessage id={`${permissionStringDictionary[permissionType]}`} />
      </Box>
    );
  }

  const onSubmitRoleChangeClick = async (values: PermissionUpdate) => {
    await updatePermissions(values);
    setActiveEditRow(undefined);
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
          defaultValues={{ userId, permissionType, workspaceId, organizationId, permissionId }}
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
            <Button
              variant="secondary"
              onClick={() => setActiveEditRow(permissionId)}
              className={styles.roleManagementControl__button}
            >
              <FormattedMessage id="settings.accessManagement.changeRole" />
            </Button>
            <Button variant="danger" onClick={onRemoveUserClick} className={styles.roleManagementControl__button}>
              <FormattedMessage id="settings.accessManagement.user.remove" />
            </Button>
          </FlexContainer>
        </FlexContainer>
      )}
    </>
  );
};
