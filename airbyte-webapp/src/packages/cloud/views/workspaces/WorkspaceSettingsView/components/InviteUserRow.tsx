import React, { useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { SelectedIndicatorDot } from "components/connection/CreateConnection/SelectedIndicatorDot";
import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { PermissionType, WorkspaceUserAccessInfoRead } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { partitionPermissionType } from "core/utils/rbac/rbacPermissionsQuery";
import {
  getWorkspaceAccessLevel,
  permissionsByResourceType,
  unifyWorkspaceUserData,
} from "pages/SettingsPage/pages/AccessManagementPage/components/useGetAccessManagementData";
import { UserRoleText } from "pages/SettingsPage/pages/AccessManagementPage/components/UserRoleText";
import { disallowedRoles } from "pages/SettingsPage/pages/AccessManagementPage/next/ChangeRoleMenuItem";
import { ChangeRoleMenuItemContent } from "pages/SettingsPage/pages/AccessManagementPage/next/ChangeRoleMenuItemContent";

import { AddUserFormValues } from "./AddUserModal";
import styles from "./InviteUserRow.module.scss";
import { ViewOnlyUserRow } from "./ViewOnlyUserRow";

interface InviteUserRowProps {
  id: string;
  name?: string;
  email: string;
  selectedRow: string | null;
  setSelectedRow: (value: string | null) => void;
  user?: WorkspaceUserAccessInfoRead;
}

export const InviteUserRow: React.FC<InviteUserRowProps> = ({ id, name, email, selectedRow, setSelectedRow, user }) => {
  const transformedUser = !!user ? unifyWorkspaceUserData([user], [])[0] : null;
  const allowAllRBACRoles = useFeature(FeatureItem.AllowAllRBACRoles);

  const [selectedPermissionType, setPermissionType] = useState<PermissionType>(PermissionType.workspace_admin);
  const { setValue } = useFormContext<AddUserFormValues>();
  const { formatMessage } = useIntl();
  const { userId: currentUserId } = useCurrentUser();
  const isCurrentUser = user?.userId === currentUserId;
  const isOrgAdmin = user?.organizationPermission?.permissionType === PermissionType.organization_admin;

  const onSelectRow = () => {
    setSelectedRow(id);
    setValue("permission", selectedPermissionType, { shouldValidate: true });
    setValue("email", email, { shouldValidate: true });
  };

  const onSelectPermission = (selectedValue: PermissionType) => {
    setPermissionType(selectedValue);
    setValue("permission", selectedValue, { shouldValidate: true });
  };

  const shouldDisableRow = useMemo(() => {
    return id === "inviteNewUser" ? false : isOrgAdmin || !!user?.workspacePermission?.permissionType || isCurrentUser;
  }, [id, isOrgAdmin, isCurrentUser, user]);

  const highestPermissionType = user ? getWorkspaceAccessLevel(user) : undefined;

  const selectedPermissionTypeString = partitionPermissionType(selectedPermissionType)[1];

  if (shouldDisableRow) {
    return (
      <ViewOnlyUserRow
        name={name}
        email={email}
        isCurrentUser={isCurrentUser}
        isOrgAdmin={isOrgAdmin}
        highestPermissionType={highestPermissionType}
      />
    );
  }

  return (
    <div className={styles.inviteUserRow}>
      <input
        type="radio"
        name={`radio-button-${id}`}
        id={id}
        value={id}
        checked={selectedRow === id}
        onChange={() => onSelectRow()}
        className={styles.inviteUserRow__hiddenInput}
        data-testid={`radio-button-new-user-${id}`}
      />
      {/* the linter cannot seem to keep track of the input + label here */}
      {/* eslint-disable-next-line jsx-a11y/label-has-associated-control */}
      <label className={styles.inviteUserRow__label} htmlFor={id}>
        <FlexContainer
          justifyContent="space-between"
          alignItems="center"
          className={styles.inviteUserRow__labelContent}
        >
          <FlexContainer direction="column" gap="none" justifyContent="center">
            <Text>{id === "inviteNewUser" ? formatMessage({ id: "userInvitations.create.modal.addNew" }) : name}</Text>
            <Text color="grey400" italicized>
              {email}
            </Text>
          </FlexContainer>
          <FlexContainer alignItems="center">
            {allowAllRBACRoles && selectedRow === id && (
              <ListBox<PermissionType>
                buttonClassName={styles.inviteUserRow__listBoxButton}
                selectedValue={selectedPermissionType}
                controlButton={() => (
                  <Box py="sm" px="xs">
                    <FlexContainer direction="row" alignItems="center" gap="xs">
                      <Text size="md" color="grey" as="span">
                        <FormattedMessage
                          id="userInvitations.create.modal.asRole"
                          values={{ role: <UserRoleText highestPermissionType={selectedPermissionTypeString} /> }}
                        />
                      </Text>
                      <FlexItem>
                        <Icon type="chevronDown" color="disabled" size="sm" />
                      </FlexItem>
                    </FlexContainer>
                  </Box>
                )}
                options={permissionsByResourceType.workspace.map((optionPermissionType) => {
                  return {
                    label: (
                      <ChangeRoleMenuItemContent
                        permissionType={optionPermissionType}
                        roleIsInvalid={
                          !!user
                            ? disallowedRoles(transformedUser, "workspace", isCurrentUser).includes(
                                optionPermissionType
                              )
                            : false
                        }
                        roleIsActive={optionPermissionType === selectedPermissionType}
                      />
                    ),
                    value: optionPermissionType,
                  };
                })}
                onSelect={onSelectPermission}
              />
            )}
            <div className={styles.inviteUserRow__dot}>
              <SelectedIndicatorDot selected={selectedRow === id} />
            </div>
          </FlexContainer>
        </FlexContainer>
      </label>
    </div>
  );
};
