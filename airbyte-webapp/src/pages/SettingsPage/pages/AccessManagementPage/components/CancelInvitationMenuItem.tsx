import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { useCancelUserInvitation, useCurrentWorkspace } from "core/api";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";

import styles from "./RemoveRoleMenuItem.module.scss";
import { UnifiedUserModel } from "./util";

interface CancelInvitationMenuItemProps {
  user: UnifiedUserModel;
}

export const CancelInvitationMenuItem: React.FC<CancelInvitationMenuItemProps> = ({ user }) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const { name: workspaceName } = useCurrentWorkspace();

  const { mutateAsync: cancelInvitation } = useCancelUserInvitation();

  const onClick = () =>
    openConfirmationModal({
      text: (
        <FormattedMessage
          id="userInvitations.cancel.confirm.text"
          values={{
            user: (
              <Text as="span" italicized>
                {user.userEmail}
              </Text>
            ),
            resource: (
              <Text as="span" italicized>
                {workspaceName}
              </Text>
            ),
          }}
        />
      ),
      title: <FormattedMessage id="userInvitations.cancel.confirm.title" />,
      submitButtonText: "userInvitations.cancel.confirm.title",
      onSubmit: async () => {
        await cancelInvitation({ inviteCode: user.id });
        closeConfirmationModal();
      },
      submitButtonDataId: "cancel-invite",
    });

  return (
    <button onClick={onClick} className={styles.removeRoleMenuItem__button}>
      <Box py="lg" px="md">
        <Text color="red">
          <FormattedMessage id="userInvitations.cancel.confirm.title" />
        </Text>
      </Box>
    </button>
  );
};
