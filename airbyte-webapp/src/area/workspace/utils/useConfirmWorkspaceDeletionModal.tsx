import { UseMutateAsyncFunction } from "@tanstack/react-query";
import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { WorkspaceRead } from "core/api/types/AirbyteClient";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";
import { RoutePaths } from "pages/routePaths";

import styles from "./ConfirmWorkspaceDeletionModal.module.scss";

const WorkspaceDeletionModalContent = ({ onSubmit, workspace }: { onSubmit: () => void; workspace: WorkspaceRead }) => {
  const [confirmationInput, setConfirmationInput] = useState("");
  const isConfirmationValid = confirmationInput === workspace.name;
  return (
    <div>
      <Box p="xl">
        <Text>
          <FormattedMessage id="settings.workspaceSettings.deleteWorkspace.confirmation.text" />
        </Text>
        <form onSubmit={onSubmit} className={styles.form}>
          <Input
            value={confirmationInput}
            placeholder={workspace.name}
            onChange={(event) => setConfirmationInput(event.target.value)}
          />
          <Button variant="danger" disabled={!isConfirmationValid} type="submit">
            <FormattedMessage id="settings.workspaceSettings.delete.confirmation.submitButtonText" />
          </Button>
        </form>
      </Box>
    </div>
  );
};

/**
 * Returns a function that can be used to open a confirmation modal for deleting a
 * workspace. The user must type the workspace name in a confirmation input in order to
 * proceed with the deletion.
 *
 * @param workspace - the workspace to delete
 * @param deleteWorkspace - the API function which will actually delete the workspace upon successful confirmation
 */
export const useConfirmWorkspaceDeletionModal = (
  workspace: WorkspaceRead,
  deleteWorkspace: UseMutateAsyncFunction<void, unknown, string>
) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const navigate = useNavigate();
  const { openModal } = useModalService();

  return async () => {
    const result = await openModal<"confirm">({
      title: formatMessage(
        {
          id: "settings.workspaceSettings.deleteWorkspace.confirmation.title",
        },
        { name: workspace.name }
      ),
      content: ({ onClose }) => (
        <WorkspaceDeletionModalContent workspace={workspace} onSubmit={() => onClose("confirm")} />
      ),
      size: "md",
    });

    // "closed" indicates a successful confirmation; "canceled" [sic] is its counterpart
    // when the user backs out
    if (result.type === "closed") {
      try {
        await deleteWorkspace(workspace.workspaceId);
        registerNotification({
          id: "settings.workspace.delete.success",
          text: formatMessage({ id: "settings.workspaceSettings.delete.success" }),
          type: "success",
        });
        navigate(`/${RoutePaths.Workspaces}`);
      } catch {
        registerNotification({
          id: "settings.workspace.delete.error",
          text: formatMessage({ id: "settings.workspaceSettings.delete.error" }),
          type: "error",
        });
      }
    }
  };
};
