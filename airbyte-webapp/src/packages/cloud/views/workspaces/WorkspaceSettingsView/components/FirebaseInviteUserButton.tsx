import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";

import { useCurrentWorkspace } from "core/api";
import { useIntent } from "core/utils/rbac";
import { useModalService } from "hooks/services/Modal";
import { InviteUsersModal } from "packages/cloud/views/users/InviteUsersModal";

export const FirebaseInviteUserButton: React.FC = () => {
  const { openModal } = useModalService();
  const { formatMessage } = useIntl();
  const { workspaceId } = useCurrentWorkspace();
  const canUpdateWorkspacePermissions = useIntent("UpdateWorkspacePermissions", { workspaceId });

  const onOpenInviteUsersModal = () =>
    openModal({
      title: formatMessage({ id: "modals.addUser.title" }),
      content: () => <InviteUsersModal invitedFrom="user.settings" />,
      size: "md",
    });

  return (
    <Button
      onClick={onOpenInviteUsersModal}
      icon={<Icon type="plus" />}
      data-testid="userSettings.button.addNewUser"
      disabled={!canUpdateWorkspacePermissions}
    >
      <FormattedMessage id="userSettings.button.addNewUser" />
    </Button>
  );
};
