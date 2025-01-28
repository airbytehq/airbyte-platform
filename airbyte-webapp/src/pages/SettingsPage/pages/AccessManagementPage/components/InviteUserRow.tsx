import React, { useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { SelectedIndicatorDot } from "components/connection/CreateConnection/SelectedIndicatorDot";
import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { PermissionType, ScopeType } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { partitionPermissionType } from "core/utils/rbac/rbacPermissionsQuery";

import { AddUserFormValues } from "./AddUserModal";
import { disallowedRoles } from "./ChangeRoleMenuItem";
import { ChangeRoleMenuItemContent } from "./ChangeRoleMenuItemContent";
import styles from "./InviteUserRow.module.scss";
import { UserRoleText } from "./UserRoleText";
import {
  getOrganizationAccessLevel,
  getWorkspaceAccessLevel,
  permissionsByResourceType,
  UnifiedUserModel,
} from "./util";
import { ViewOnlyUserRow } from "./ViewOnlyUserRow";

interface InviteUserRowProps {
  id: string;
  name?: string;
  email: string;
  selectedRow: string | null;
  setSelectedRow: (value: string | null) => void;
  user?: UnifiedUserModel;
  scope: ScopeType;
}

export const InviteUserRow: React.FC<InviteUserRowProps> = ({
  id,
  name,
  email,
  selectedRow,
  setSelectedRow,
  user,
  scope,
}) => {
  const allowAllRBACRoles = useFeature(FeatureItem.AllowAllRBACRoles);

  const [selectedPermissionType, setPermissionType] = useState<PermissionType>(
    scope === ScopeType.workspace ? PermissionType.workspace_admin : PermissionType.organization_admin
  );

  const { setValue } = useFormContext<AddUserFormValues>();
  const { formatMessage } = useIntl();
  const { userId: currentUserId } = useCurrentUser();
  const isCurrentUser = user?.id === currentUserId;

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
    if (isCurrentUser) {
      return true;
    }

    // in a workspace, only allow inviting users with a lower-than-admin permission level in the org and no workspace permission
    if (scope === "workspace") {
      const isUserOrgAdmin = isOrgAdmin;
      const hasWorkspacePermission = user?.workspacePermission?.permissionType !== undefined;
      const isInvitedUser = user?.invitationStatus !== undefined;
      if (isUserOrgAdmin || hasWorkspacePermission || isInvitedUser) {
        return true;
      }
      // in an organization, never allow inviting existing org members
    } else if (scope === "organization" && id !== "inviteNewUser") {
      return true;
    }
    // allow all other cases (new user, or existing user that didn't trigger the workspace check)
    return false;
  }, [isCurrentUser, scope, id, isOrgAdmin, user?.workspacePermission?.permissionType, user?.invitationStatus]);

  const highestPermissionType = useMemo(() => {
    if (user) {
      if (scope === ScopeType.workspace) {
        return getWorkspaceAccessLevel(user);
      } else if (user.organizationPermission) {
        return getOrganizationAccessLevel(user);
      }
    }

    return undefined;
  }, [scope, user]);

  const selectedPermissionTypeString = partitionPermissionType(selectedPermissionType)[1];

  if (shouldDisableRow) {
    return (
      <ViewOnlyUserRow
        name={name}
        email={email}
        isCurrentUser={isCurrentUser}
        isOrgAdmin={isOrgAdmin}
        scope={scope}
        highestPermissionType={highestPermissionType}
        isPending={user?.invitationStatus !== undefined}
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
                options={permissionsByResourceType[`${scope}`].map((optionPermissionType) => {
                  return {
                    label: (
                      <ChangeRoleMenuItemContent
                        permissionType={optionPermissionType}
                        roleIsInvalid={
                          !!user ? disallowedRoles(user, scope, isCurrentUser).includes(optionPermissionType) : false
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
