import { useEffect } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ModalBody } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { PermissionType, ScopeType } from "core/api/types/AirbyteClient";

import { AddUserFormValues } from "./AddUserModal";
import styles from "./AddUserModalBody.module.scss";
import { InviteUserRow } from "./InviteUserRow";
import { UnifiedUserModel } from "./util";
interface AddUserModalBodyProps {
  usersToList: UnifiedUserModel[];
  showInviteNewUser: boolean;
  selectedRow: string | null;
  setSelectedRow: (value: string | null) => void;
  deferredSearchValue: string;
  canInviteExternalUsers: boolean;
  scope: ScopeType;
}

export const AddUserModalBody: React.FC<AddUserModalBodyProps> = ({
  usersToList,
  showInviteNewUser,
  selectedRow,
  setSelectedRow,
  deferredSearchValue,
  canInviteExternalUsers,
  scope,
}) => {
  const { getValues, setValue } = useFormContext<AddUserFormValues>();

  // handle when the selected option is no longer visible
  useEffect(() => {
    const resetPredicates = [
      // user had selected to invite a new user, then changed the search value so that option is no longer valid
      selectedRow === "inviteNewUser" && !showInviteNewUser,

      // user had selected to invite a new user, then changed the search value to another valid email
      selectedRow === "inviteNewUser" && deferredSearchValue !== getValues("email"),

      // user had selected a user and that user is no longer visible
      selectedRow && selectedRow !== "inviteNewUser" && !usersToList.find((user) => user.id === selectedRow),
    ];

    if (resetPredicates.some(Boolean)) {
      setSelectedRow(null);
      setValue("email", "", { shouldValidate: true });
      setValue("permission", PermissionType.workspace_admin, { shouldValidate: true });
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
                  ? !deferredSearchValue.length
                    ? "userInvitations.create.modal.emptyList.canInvite" // can invite external users no org members and no search value
                    : "userInvitations.create.modal.emptyList.canInviteNoResults" // can invite external users + has an org
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
              <li className={styles.addUserModalBody__listItem} key={user.id}>
                <InviteUserRow
                  id={user.id}
                  name={user.userName}
                  email={user.userEmail}
                  selectedRow={selectedRow}
                  setSelectedRow={setSelectedRow}
                  user={user}
                  scope={scope}
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
                scope={scope}
              />
            </li>
          )}
        </ul>
      )}
    </ModalBody>
  );
};
