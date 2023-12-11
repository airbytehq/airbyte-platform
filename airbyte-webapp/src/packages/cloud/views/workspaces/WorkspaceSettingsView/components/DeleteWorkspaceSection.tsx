import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { useConfirmWorkspaceDeletionModal } from "area/workspace/utils/useConfirmWorkspaceDeletionModal";
import { useCurrentWorkspace } from "core/api";
import { useRemoveCloudWorkspace } from "core/api/cloud";

export const DeleteWorkspaceSection: React.FC = () => {
  const workspace = useCurrentWorkspace();

  const { mutateAsync: removeCloudWorkspace, isLoading: isRemovingCloudWorkspace } = useRemoveCloudWorkspace();

  const confirmWorkspaceDeletion = useConfirmWorkspaceDeletionModal(workspace, removeCloudWorkspace);

  return (
    <FlexContainer justifyContent="center" alignItems="center">
      <Box p="2xl">
        <Button isLoading={isRemovingCloudWorkspace} variant="danger" onClick={confirmWorkspaceDeletion}>
          <FormattedMessage id="settings.workspaceSettings.deleteLabel" />
        </Button>
      </Box>
    </FlexContainer>
  );
};
