import { useState } from "react";
import { FormattedMessage } from "react-intl";
import * as yup from "yup";

import { Form, FormControl } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCreatePermission, useCurrentOrganizationInfo, useListUsersInOrganization } from "core/api";
import { OrganizationUserRead, PermissionCreate, PermissionType } from "core/api/types/AirbyteClient";
import { useIntent } from "core/utils/rbac";

import styles from "./AddUserControl.module.scss";

/**
 * The name of this component is based on what a user sees... not so much what it does.
 * This button will NOT create a user, it will create a permission for an existing organization member to access a given workspace.
 */

const createPermissionControlSchema = yup.object().shape({
  userId: yup.string().required(),
  permissionType: yup.mixed<PermissionType>().oneOf(Object.values(PermissionType)).required(),
  workspaceId: yup.string(),
  permissionId: yup.string().strip(), // this property is defined on the type solely for migration purposes
  organizationId: yup.string().strip(), // we do not have a mechanism for creating an organization permission with this control as of yet
});

const AddUserForm: React.FC<{
  usersToAdd: OrganizationUserRead[];
  workspaceId: string;
  setIsEditMode: (mode: boolean) => void;
}> = ({ usersToAdd, workspaceId, setIsEditMode }) => {
  const { mutateAsync: createPermission } = useCreatePermission();
  const canUpdateWorkspacePermissions = useIntent("UpdateWorkspacePermissions", { workspaceId });

  const onSubmitClick = async (values: PermissionCreate) => {
    await createPermission(values).then(() => setIsEditMode(false));
  };

  const AddUserListBoxControl = <T,>({ selectedOption }: ListBoxControlButtonProps<T>) => {
    const value = selectedOption?.value;
    const userToAdd = usersToAdd.find((user) => user.userId === value);
    const nameToDisplay = userToAdd?.name ? userToAdd.name : userToAdd?.email;

    if (!userToAdd) {
      return null;
    }

    return (
      <>
        <Text as="span" className={styles.addUserControl__buttonName}>
          {nameToDisplay}
        </Text>
        <Icon type="caretDown" color="action" />
      </>
    );
  };

  return (
    <Form<PermissionCreate>
      schema={createPermissionControlSchema}
      defaultValues={{
        userId: usersToAdd[0].userId,
        permissionType: PermissionType.workspace_admin,
        workspaceId,
      }}
      onSubmit={onSubmitClick}
      disabled={!canUpdateWorkspacePermissions}
    >
      <FlexContainer alignItems="baseline">
        <FormControl<PermissionCreate>
          containerControlClassName={styles.addUserControl__dropdown}
          optionsMenuClassName={styles.addUserControl__dropdownMenu}
          controlButton={AddUserListBoxControl}
          name="userId"
          fieldType="dropdown"
          options={usersToAdd.map((user) => {
            return {
              value: user.userId,
              label: (
                <FlexContainer as="span" direction="column" gap="xs">
                  <Text as="span" size="sm" bold>
                    {user.name ? user.name : user.email}
                  </Text>
                  <Text as="span" size="sm" color="grey">
                    {user.email}
                  </Text>
                </FlexContainer>
              ),
            };
          })}
        />
        <FormSubmissionButtons
          submitKey="form.add"
          onCancelClickCallback={() => setIsEditMode(false)}
          allowNonDirtyCancel
        />
      </FlexContainer>
    </Form>
  );
};
export const AddUserControl: React.FC = () => {
  const [isEditMode, setIsEditMode] = useState(false);
  const workspaceId = useCurrentWorkspaceId();
  const organizationInfo = useCurrentOrganizationInfo();

  const usersToAdd = useListUsersInOrganization(organizationInfo?.organizationId ?? "").users ?? [];
  if (!usersToAdd || usersToAdd.length === 0) {
    return null;
  }

  return !isEditMode ? (
    <Button onClick={() => setIsEditMode(true)} icon={<Icon type="plus" />}>
      <FormattedMessage id="role.addUser" />
    </Button>
  ) : (
    <AddUserForm usersToAdd={usersToAdd} workspaceId={workspaceId} setIsEditMode={setIsEditMode} />
  );
};
