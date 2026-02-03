import { useCallback, useEffect } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ModalBody } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { PermissionType, ScopeType } from "core/api/types/AirbyteClient";
import { useModalService } from "core/services/Modal";
import { useProFeaturesModal } from "core/utils/useProFeaturesModal";

import { AddUserFormValues, AddUserModal } from "./AddUserModal";
import styles from "./AddUserModalBody.module.scss";
import { InviteUserRow } from "./InviteUserRow";
import { getInitialPermissionType, isTeamsFeaturePermissionType, UnifiedUserModel } from "./util";

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
  const { openModal, getCurrentModalTitle } = useModalService();
  const { getValues, setValue } = useFormContext<AddUserFormValues>();
  const { showProFeatureModalIfNeeded } = useProFeaturesModal("rbac");

  const openProFeaturesWarnModal = useCallback(
    async (permission: PermissionType) => {
      // Show warning only when user selects any pro feature permission
      if (!isTeamsFeaturePermissionType(permission)) {
        return;
      }

      // Capture the AddUserModal title BEFORE opening the Pro features warning modal
      const addUserModalTitle = getCurrentModalTitle();

      // Show Pro features warning modal if needed
      const proFeatureModalWasShown = await showProFeatureModalIfNeeded();

      // Only reopen the AddUserModal if the warning modal was actually shown
      if (proFeatureModalWasShown) {
        // reopen the AddUserModal with preserved state
        openModal({
          title: addUserModalTitle,
          content: ({ onComplete }) => (
            <AddUserModal
              scope={scope}
              onSubmit={() => onComplete("success")}
              initialValues={{
                searchValue: deferredSearchValue,
                email: getValues("email"),
                permission,
                selectedRow,
              }}
            />
          ),
          size: "md",
        });
      }
    },
    [deferredSearchValue, getCurrentModalTitle, getValues, openModal, scope, selectedRow, showProFeatureModalIfNeeded]
  );

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
      setValue("permission", getInitialPermissionType(scope), { shouldValidate: true });
    }
  }, [usersToList, showInviteNewUser, selectedRow, setSelectedRow, setValue, deferredSearchValue, getValues, scope]);

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
                  onPermissionChangeCb={openProFeaturesWarnModal}
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
                onPermissionChangeCb={openProFeaturesWarnModal}
              />
            </li>
          )}
        </ul>
      )}
    </ModalBody>
  );
};
