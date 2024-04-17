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
import { useExperiment } from "hooks/services/Experiment";
import { useDeleteModal } from "hooks/useDeleteModal";

interface DeleteBlockProps {
  onDelete: () => Promise<unknown>;
  onReset: () => Promise<unknown>;
  modalAdditionalContent?: React.ReactNode;
}

export const ConnectionDangerBlock: React.FC<DeleteBlockProps> = ({ onDelete, onReset }) => {
  const { mode, connection } = useConnectionFormService();
  const onDeleteButtonClick = useDeleteModal("connection", onDelete, undefined, connection?.name);
  const sayClearInsteadOfReset = useExperiment("connection.clearNotReset", false);
  const connectionStatus = useConnectionStatus(connection.connectionId ?? "");

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const resetWithModal = useCallback(() => {
    sayClearInsteadOfReset
      ? openConfirmationModal({
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
        })
      : openConfirmationModal({
          text: `form.resetDataText`,
          title: `form.resetData`,
          submitButtonText: "form.reset",
          cancelButtonText: "form.noNeed",
          onSubmit: async () => {
            await onReset();
            closeConfirmationModal();
          },
          submitButtonDataId: "reset",
        });
  }, [closeConfirmationModal, onReset, openConfirmationModal, sayClearInsteadOfReset]);

  const onResetButtonClick = () => {
    resetWithModal();
  };

  return (
    <Card>
      <FlexContainer direction="column" gap="xl">
        <FormFieldLayout alignItems="center" nextSizing>
          <FlexContainer direction="column">
            <Text size="lg">
              <FormattedMessage
                id={sayClearInsteadOfReset ? "connection.stream.actions.clearData" : "form.resetData"}
              />
            </Text>
            <Text size="xs" color="grey">
              <FormattedMessage
                id={sayClearInsteadOfReset ? "connection.actions.clearData" : "form.resetData.description"}
              />
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
            <FormattedMessage id={sayClearInsteadOfReset ? "connection.stream.actions.clearData" : "form.resetData"} />
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
