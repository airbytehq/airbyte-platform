import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";

import { useCurrentWorkspace } from "core/api";
import { useIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";
import { InviteUsersModal } from "packages/cloud/views/users/InviteUsersModal";

/**
 *
 * @deprecated This component is deprecated and should not be used in new code.  It is a part of our legacy invitation system.
 */
export const FirebaseInviteUserButton: React.FC = () => {
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();
  const { workspaceId } = useCurrentWorkspace();
  const canUpdateWorkspacePermissions = useIntent("UpdateWorkspacePermissions", { workspaceId });

  const onOpenInviteUsersModal = () =>
    openModal<void>({
      title: formatMessage({ id: "modals.addUser.title" }),
      content: ({ onComplete, onCancel }) => (
        <InviteUsersModal invitedFrom="user.settings" onSubmit={onComplete} onCancel={onCancel} />
      ),
      size: "md",
    });

  return (
    <Button
      onClick={onOpenInviteUsersModal}
      icon="plus"
      data-testid="userSettings.button.addNewUser"
      disabled={!canUpdateWorkspacePermissions}
    >
      <FormattedMessage id="userSettings.button.addNewUser" />
    </Button>
  );
};
