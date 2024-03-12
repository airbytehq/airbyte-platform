import React, { useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { SelectedIndicatorDot } from "components/connection/CreateConnection/SelectedIndicatorDot";
import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { PermissionType, WorkspaceUserAccessInfoRead } from "core/api/types/AirbyteClient";
import { useCurrentUser } from "core/services/auth";
import { getWorkspaceAccessLevel } from "pages/SettingsPage/pages/AccessManagementPage/components/useGetAccessManagementData";

import { AddUserFormValues } from "./AddUserModal";
import { ExistingUserIndicator } from "./ExistingUserIndicator";
import styles from "./InviteUserRow.module.scss";

interface InviteUserRowProps {
  id: string;
  name?: string;
  email: string;
  selectedRow: string | null;
  setSelectedRow: (value: string | null) => void;
  user?: WorkspaceUserAccessInfoRead;
}

export const InviteUserRow: React.FC<InviteUserRowProps> = ({ id, name, email, selectedRow, setSelectedRow, user }) => {
  const [permissionType] = useState<PermissionType>(PermissionType.workspace_admin);
  const { setValue } = useFormContext<AddUserFormValues>();
  const { formatMessage } = useIntl();
  const { userId: currentUserId } = useCurrentUser();

  const onSelectRow = () => {
    setSelectedRow(id);
    setValue("permission", permissionType, { shouldValidate: true });
    setValue("email", email, { shouldValidate: true });
  };

  const shouldDisableRow = useMemo(() => {
    return id === "inviteNewUser"
      ? false
      : user?.organizationPermission?.permissionType === PermissionType.organization_admin ||
          !!user?.workspacePermission?.permissionType ||
          user?.userId === currentUserId;
  }, [currentUserId, id, user]);

  const highestPermissionType = user ? getWorkspaceAccessLevel(user) : undefined;

  if (shouldDisableRow && highestPermissionType) {
    return (
      <Box py="md" className={styles.inviteUserRow}>
        <FlexContainer justifyContent="space-between" alignItems="center">
          <FlexContainer direction="column" gap="none" justifyContent="center">
            <Text>
              {name}
              {user?.userId === currentUserId && (
                <Box as="span" px="sm">
                  <Badge variant="grey">
                    <FormattedMessage id="settings.accessManagement.youHint" />
                  </Badge>
                </Box>
              )}
            </Text>
            <Text color="grey400" italicized>
              {email}
            </Text>
          </FlexContainer>
          {user?.organizationPermission?.permissionType === PermissionType.organization_admin ? (
            <Tooltip
              control={
                <Badge variant="grey">
                  <FormattedMessage id="role.organizationAdmin" />
                </Badge>
              }
              placement="top-start"
            >
              <FormattedMessage id="userInvitations.create.modal.organizationAdminTooltip" />
            </Tooltip>
          ) : (
            <ExistingUserIndicator highestPermissionType={highestPermissionType} />
          )}
        </FlexContainer>
      </Box>
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
        <Box py="md">
          <FlexContainer justifyContent="space-between" alignItems="center">
            <FlexContainer direction="column" gap="none" justifyContent="center">
              <Text>
                {id === "inviteNewUser" ? formatMessage({ id: "userInvitations.create.modal.addNew" }) : name}
              </Text>
              <Text color="grey400" italicized>
                {email}
              </Text>
            </FlexContainer>
            <div className={styles.inviteUserRow__dot}>
              <SelectedIndicatorDot selected={selectedRow === id} />
            </div>
          </FlexContainer>
        </Box>
      </label>
    </div>
  );
};
