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
import { useCreatePermission } from "core/api";
import { OrganizationUserRead, PermissionCreate, PermissionType } from "core/api/types/AirbyteClient";

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

  const onSubmitClick = async (values: PermissionCreate) => {
    await createPermission(values).then(() => setIsEditMode(false));
  };

  const AddUserListBoxControl = <T,>({ selectedOption }: ListBoxControlButtonProps<T>) => {
    const value = selectedOption?.value;
    const userName = usersToAdd.find((user) => user.userId === value)?.name;
    return (
      <>
        <Text as="span" className={styles.addUserControl__buttonName}>
          {userName}
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
    >
      <FlexContainer alignItems="baseline">
        <FormControl<PermissionCreate>
          containerControlClassName={styles.addUserControl__dropdown}
          controlButton={AddUserListBoxControl}
          name="userId"
          fieldType="dropdown"
          options={usersToAdd.map((user) => {
            return {
              value: user.userId,
              label: (
                <FlexContainer as="span" direction="column" gap="xs">
                  <Text as="span" size="sm" bold className={styles.addUserControl__userName}>
                    {user.name}
                  </Text>
                  <Text as="span" size="sm" color="grey" className={styles.addUserControl__userEmail}>
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
export const AddUserControl: React.FC<{ usersToAdd: OrganizationUserRead[] }> = ({ usersToAdd }) => {
  const [isEditMode, setIsEditMode] = useState(false);
  const workspaceId = useCurrentWorkspaceId();

  return !isEditMode ? (
    <Button onClick={() => setIsEditMode(true)} icon={<Icon type="plus" />}>
      <FormattedMessage id="role.addUser" />
    </Button>
  ) : (
    <AddUserForm usersToAdd={usersToAdd} workspaceId={workspaceId} setIsEditMode={setIsEditMode} />
  );
};
