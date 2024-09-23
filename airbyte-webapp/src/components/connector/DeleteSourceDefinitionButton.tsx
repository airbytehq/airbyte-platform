import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useDeleteSourceDefinition, useSourceDefinitionList } from "core/api";
import { trackError } from "core/utils/datadog";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";

interface EditConnectorDefinitionProps {
  sourceDefinitionId: string;
}

export const DeleteSourceDefinitionButton: React.FC<EditConnectorDefinitionProps> = ({ sourceDefinitionId }) => {
  const { sourceDefinitionMap } = useSourceDefinitionList();
  const sourceDefinition = sourceDefinitionMap.get(sourceDefinitionId);
  const { deleteSourceDefinition } = useDeleteSourceDefinition();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  if (!sourceDefinition) {
    trackError(new Error("Source definition not found"), { sourceDefinitionId });
    return null;
  }

  const onDelete = () => {
    openConfirmationModal({
      title: formatMessage(
        { id: "settings.connector.deleteDefinition.title" },
        { connectorName: sourceDefinition.name }
      ),
      text: (
        <Text>
          <FormattedMessage id="settings.connector.deleteDefinition.warning" />
        </Text>
      ),
      onSubmit: async () => {
        try {
          await deleteSourceDefinition({ sourceDefinitionId });
          registerNotification({
            type: "success",
            id: `delete-source-${sourceDefinitionId}`,
            text: formatMessage(
              { id: "settings.connector.deleteDefinitionSuccess" },
              { connectorName: sourceDefinition.name }
            ),
          });
          closeConfirmationModal();
        } catch (e) {
          registerNotification({
            type: "error",
            id: `delete-source-${sourceDefinitionId}`,
            text: formatMessage(
              { id: "settings.connector.deleteDefinitionError" },
              { connectorName: sourceDefinition.name }
            ),
          });
        }
      },
      submitButtonText: formatMessage({ id: "settings.connector.deleteDefinition.confirm" }),
      confirmationText: sourceDefinition?.name ?? "delete",
    });
  };

  return (
    <Button variant="clear" onClick={onDelete}>
      <Icon type="trash" />
    </Button>
  );
};
