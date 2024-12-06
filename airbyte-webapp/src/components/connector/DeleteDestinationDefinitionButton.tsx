import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useDeleteDestinationDefinition, useDestinationDefinitionList } from "core/api";
import { trackError } from "core/utils/datadog";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";

interface EditConnectorDefinitionProps {
  destinationDefinitionId: string;
}

export const DeleteDestinationDefinitionButton: React.FC<EditConnectorDefinitionProps> = ({
  destinationDefinitionId,
}) => {
  const { destinationDefinitionMap } = useDestinationDefinitionList();
  const destinationDefinition = destinationDefinitionMap.get(destinationDefinitionId);
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const { deleteDestinationDefinition } = useDeleteDestinationDefinition();

  if (!destinationDefinition) {
    trackError(new Error("Destination definition not found"), { destinationDefinitionId });
    return null;
  }

  const onDelete = () => {
    openConfirmationModal({
      title: formatMessage(
        { id: "settings.connector.deleteDefinition.title" },
        { connectorName: destinationDefinition.name }
      ),
      text: (
        <Text>
          <FormattedMessage id="settings.connector.deleteDefinition.warning" />
        </Text>
      ),
      onSubmit: async () => {
        try {
          await deleteDestinationDefinition({ destinationDefinitionId });
          registerNotification({
            type: "success",
            id: `delete-destination-${destinationDefinitionId}`,
            text: formatMessage(
              { id: "settings.connector.deleteDefinitionSuccess" },
              { connectorName: destinationDefinition.name }
            ),
          });
          closeConfirmationModal();
        } catch (e) {
          registerNotification({
            type: "error",
            id: `delete-destination-${destinationDefinitionId}`,
            text: formatMessage(
              { id: "settings.connector.deleteDefinitionError" },
              { connectorName: destinationDefinition.name }
            ),
          });
        }
      },
      submitButtonText: formatMessage({ id: "settings.connector.deleteDefinition.confirm" }),
      confirmationText: destinationDefinition?.name ?? "delete",
    });
  };

  return (
    <Button variant="clear" onClick={onDelete}>
      <Icon type="trash" />
    </Button>
  );
};
