import React, { useCallback } from "react";
import { FormattedMessage } from "react-intl";

import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectionStatus } from "core/api/types/AirbyteClient";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useDeleteModal } from "hooks/useDeleteModal";

interface DeleteBlockProps {
  onDelete: () => Promise<unknown>;
  onReset: () => Promise<unknown>;
  modalAdditionalContent?: React.ReactNode;
}

export const ConnectionDangerBlock: React.FC<DeleteBlockProps> = ({ onDelete, onReset }) => {
  const { mode, connection } = useConnectionFormService();
  const onDeleteButtonClick = useDeleteModal("connection", onDelete, undefined, connection?.name);
  const connectionStatus = useConnectionStatus(connection.connectionId ?? "");

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const resetWithModal = useCallback(() => {
    openConfirmationModal({
      title: "connection.actions.clearData.confirm.title",
      text: "connection.actions.clearData.confirm.text",
      additionalContent: (
        <Box pt="xl">
          <Text color="grey400">
            <FormattedMessage id="connection.stream.actions.clearData.confirm.additionalText" />
          </Text>
        </Box>
      ),
      submitButtonText: "connection.stream.actions.clearData.confirm.submit",
      cancelButtonText: "connection.stream.actions.clearData.confirm.cancel",
      onSubmit: async () => {
        await onReset();
        closeConfirmationModal();
      },
    });
  }, [closeConfirmationModal, onReset, openConfirmationModal]);

  const onResetButtonClick = () => {
    resetWithModal();
  };

  return (
    <Card>
      <FlexContainer direction="column" gap="xl">
        <FormFieldLayout alignItems="center" nextSizing>
          <FlexContainer direction="column">
            <Text size="lg">
              <FormattedMessage id="connection.stream.actions.clearData" />
            </Text>
            <Text size="xs" color="grey">
              <FormattedMessage id="connection.actions.clearData" />
            </Text>
          </FlexContainer>
          <Button
            variant="primaryDark"
            onClick={onResetButtonClick}
            data-id="open-reset-modal"
            disabled={
              mode === "readonly" || connectionStatus.isRunning || connection.status !== ConnectionStatus.active
            }
          >
            <FormattedMessage id="connection.stream.actions.clearData" />
          </Button>
        </FormFieldLayout>

        <FormFieldLayout alignItems="center" nextSizing>
          <FlexContainer direction="column">
            <Text size="lg">
              <FormattedMessage id="tables.connectionDelete.title" />
            </Text>
            <Text size="xs" color="grey">
              <FormattedMessage id="tables.connectionDataDelete" />
            </Text>
          </FlexContainer>
          <Button
            variant="danger"
            onClick={onDeleteButtonClick}
            data-id="open-delete-modal"
            disabled={mode === "readonly"}
          >
            <FormattedMessage id="tables.connectionDelete" />
          </Button>
        </FormFieldLayout>
      </FlexContainer>
    </Card>
  );
};
