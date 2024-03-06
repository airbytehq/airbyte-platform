import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";

import { useConfirmWorkspaceDeletionModal } from "area/workspace/utils/useConfirmWorkspaceDeletionModal";
import { useCurrentWorkspace, useDeleteWorkspace } from "core/api";

export const DeleteWorkspace: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const { mutateAsync: deleteWorkspace, isLoading: isDeletingWorkspace } = useDeleteWorkspace();
  const confirmWorkspaceDeletion = useConfirmWorkspaceDeletionModal(workspace, deleteWorkspace);

  return (
    <Button isLoading={isDeletingWorkspace} variant="danger" onClick={confirmWorkspaceDeletion}>
      <FormattedMessage id="settings.workspaceSettings.deleteLabel" />
    </Button>
  );
};
