import { useEffect } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ModalBody } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationInfo } from "core/api";
import { WorkspaceUserAccessInfoRead } from "core/api/types/AirbyteClient";

import { AddUserFormValues } from "./AddUserModal";
import styles from "./AddUserModalBody.module.scss";
import { InviteUserRow } from "./InviteUserRow";
interface AddUserModalBodyProps {
  usersToList: WorkspaceUserAccessInfoRead[];
  showInviteNewUser: boolean;
  selectedRow: string | null;
  setSelectedRow: (value: string | null) => void;
  deferredSearchValue: string;
  canInviteExternalUsers: boolean;
}

export const AddUserModalBody: React.FC<AddUserModalBodyProps> = ({
  usersToList,
  showInviteNewUser,
  selectedRow,
  setSelectedRow,
  deferredSearchValue,
  canInviteExternalUsers,
}) => {
  const { getValues, setValue } = useFormContext<AddUserFormValues>();
  const organizationInfo = useCurrentOrganizationInfo();

  // handle when the selected option is no longer visible
  useEffect(() => {
    // user had selected to invite a new user, then changed the search value so that option is no longer valid, clear form value
    if (selectedRow === "inviteNewUser" && !showInviteNewUser) {
      setSelectedRow(null);
      setValue("email", "", { shouldValidate: true });
    }

    // user had selected to invite a new user, then changed the search value to another valid option, clear form value and deselect
    if (selectedRow === "inviteNewUser" && deferredSearchValue !== getValues("email")) {
      setSelectedRow(null);
      setValue("email", "", { shouldValidate: true });
    }

    // user had selected a user and that user is no longer visible, clear it
    if (selectedRow && selectedRow !== "inviteNewUser" && !usersToList.find((user) => user.userId === selectedRow)) {
      setSelectedRow(null);
      setValue("email", "", { shouldValidate: true });
    }
  }, [usersToList, showInviteNewUser, selectedRow, setSelectedRow, setValue, deferredSearchValue, getValues]);

  return (
    <ModalBody className={styles.addUserModalBody}>
      {usersToList.length === 0 && !showInviteNewUser && (
        <Box p="md">
          <Text color="grey" italicized align="center">
            <FormattedMessage
              id={
                canInviteExternalUsers
                  ? !organizationInfo?.organizationId && !deferredSearchValue.length
                    ? "userInvitations.create.modal.emptyList.noOrganization" // can invite external users no org and no search value
                    : "userInvitations.create.modal.emptyList.canInvite" // can invite external users + has an org
                  : "userInvitations.create.modal.emptyList" // cannot invite external users
              }
            />
          </Text>
        </Box>
      )}
      {(usersToList.length > 0 || showInviteNewUser) && (
        <ul className={styles.addUserModalBody__list}>
          {usersToList.map((user) => {
            return (
              <li className={styles.addUserModalBody__listItem} key={user.userId}>
                <InviteUserRow
                  id={user.userId}
                  name={user.userName}
                  email={user.userEmail}
                  selectedRow={selectedRow}
                  setSelectedRow={setSelectedRow}
                  permissions={{
                    organizationPermission: user.organizationPermission,
                    workspacePermission: user.workspacePermission,
                  }}
                />
              </li>
            );
          })}
          {showInviteNewUser && (
            <li>
              <InviteUserRow
                id="inviteNewUser"
                email={deferredSearchValue}
                selectedRow={selectedRow}
                setSelectedRow={setSelectedRow}
              />
            </li>
          )}
        </ul>
      )}
    </ModalBody>
  );
};
