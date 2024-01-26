import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";

import { useConfirmWorkspaceDeletionModal } from "area/workspace/utils/useConfirmWorkspaceDeletionModal";
import { useCurrentWorkspace } from "core/api";
import { useRemoveCloudWorkspace } from "core/api/cloud";

export const DeleteCloudWorkspace: React.FC = () => {
  const workspace = useCurrentWorkspace();

  const { mutateAsync: removeCloudWorkspace, isLoading: isRemovingCloudWorkspace } = useRemoveCloudWorkspace();

  const confirmWorkspaceDeletion = useConfirmWorkspaceDeletionModal(workspace, removeCloudWorkspace);

  return (
    <Button isLoading={isRemovingCloudWorkspace} variant="danger" onClick={confirmWorkspaceDeletion}>
      <FormattedMessage id="settings.workspaceSettings.deleteLabel" />
    </Button>
  );
};
