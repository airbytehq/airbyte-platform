import React, { useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { SelectedIndicatorDot } from "components/connection/CreateConnection/SelectedIndicatorDot";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { PermissionType, WorkspaceUserAccessInfoRead } from "core/api/types/AirbyteClient";

import { AddUserFormValues } from "./AddUserModal";
import styles from "./InviteUserRow.module.scss";

interface InviteUserRowProps {
  id: string;
  name?: string;
  email: string;
  permissions?: Pick<WorkspaceUserAccessInfoRead, "workspacePermission" | "organizationPermission">;
  selectedRow: string | null;
  setSelectedRow: (value: string | null) => void;
}

export const InviteUserRow: React.FC<InviteUserRowProps> = ({
  id,
  name,
  email,
  permissions,
  selectedRow,
  setSelectedRow,
}) => {
  const [permissionType] = useState<PermissionType>(PermissionType.workspace_admin);
  const { setValue } = useFormContext<AddUserFormValues>();
  const { formatMessage } = useIntl();

  const onSelectRow = () => {
    setSelectedRow(id);
    setValue("permission", permissionType, { shouldValidate: true });
    setValue("email", email, { shouldValidate: true });
  };

  const shouldDisableRow = useMemo(() => {
    return id === "inviteNewUser"
      ? false
      : permissions?.organizationPermission?.permissionType === PermissionType.organization_admin ||
          !!permissions?.workspacePermission?.permissionType;
  }, [permissions, id]);

  if (shouldDisableRow) {
    return (
      <Box py="md" className={styles.inviteUserRow}>
        <FlexContainer>
          <FlexContainer direction="column" gap="none" justifyContent="center">
            <Text>{id === "inviteNewUser" ? formatMessage({ id: "userInvitations.create.modal.addNew" }) : name}</Text>
            <Text color="grey400" italicized>
              {email}
            </Text>
          </FlexContainer>
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
