import React, { useCallback } from "react";
import { FormattedMessage } from "react-intl";

import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

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

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const resetWithModal = useCallback(() => {
    openConfirmationModal({
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
  }, [closeConfirmationModal, openConfirmationModal, onReset]);
  const onResetButtonClick = () => {
    resetWithModal();
  };

  return (
    <Card>
      <FlexContainer direction="column" gap="xl">
        <FormFieldLayout alignItems="center" nextSizing>
          <FlexContainer direction="column">
            <Text size="lg">
              <FormattedMessage id="form.resetData" />
            </Text>
            <Text size="xs" color="grey">
              <FormattedMessage id="form.resetData.description" />
            </Text>
          </FlexContainer>
          <Button
            variant="primaryDark"
            onClick={onResetButtonClick}
            data-id="open-reset-modal"
            disabled={mode === "readonly"}
          >
            <FormattedMessage id="form.resetData" />
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
