import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { useConfirmWorkspaceDeletionModal } from "area/workspace/utils/useConfirmWorkspaceDeletionModal";
import { useCurrentWorkspace, useDeleteWorkspace } from "core/api";

export const DeleteWorkspace: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const { mutateAsync: deleteWorkspace, isLoading: isDeletingWorkspace } = useDeleteWorkspace();
  const confirmWorkspaceDeletion = useConfirmWorkspaceDeletionModal(workspace, deleteWorkspace);

  return (
    <FlexContainer justifyContent="center" alignItems="center">
      <Box p="xl">
        <Button isLoading={isDeletingWorkspace} variant="danger" onClick={confirmWorkspaceDeletion}>
          <FormattedMessage id="settings.workspaceSettings.deleteLabel" />
        </Button>
      </Box>
    </FlexContainer>
  );
};
