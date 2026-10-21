import { useIntl } from "react-intl";

import { useCurrentWorkspace } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useModalService } from "core/services/Modal";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { AddUserModal } from "pages/SettingsPage/pages/AccessManagementPage/components/AddUserModal";

export const useInviteUsers = () => {
  const workspace = useCurrentWorkspace();
  const { formatMessage } = useIntl();
  const inviteUsersVisible = useFeature(FeatureItem.ShowInviteUsersHint);
  const canUpdateWorkspacePermissions = useGeneratedIntent(Intent.UpdateWorkspacePermissions);
  const { openModal } = useModalService();

  const openInviteUsersModal = () =>
    openModal<void>({
      title: formatMessage({ id: "userInvitations.create.modal.title" }, { scopeName: workspace.name }),
      content: ({ onComplete }) => <AddUserModal onSubmit={onComplete} scope="workspace" />,
      size: "md",
    });

  return { showInviteUsers: inviteUsersVisible && canUpdateWorkspacePermissions, openInviteUsersModal };
};
