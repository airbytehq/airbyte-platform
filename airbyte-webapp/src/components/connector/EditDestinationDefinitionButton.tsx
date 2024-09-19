import { FormattedMessage, useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import {
  useLatestDestinationDefinitionList,
  useUpdateDestinationDefinition,
  useDestinationDefinitionList,
} from "core/api";
import { trackError } from "core/utils/datadog";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

import { EditConnectorDefinitionModalContent, EditDefinitionFormValues } from "./EditConnectorDefinitionModal";

interface EditDestinationDefinitionButtonProps {
  definitionId: string;
  isConnectorNameEditable?: boolean;
}

export const EditDestinationDefinitionButton: React.FC<EditDestinationDefinitionButtonProps> = ({
  isConnectorNameEditable = true,
  definitionId,
}) => {
  const { destinationDefinitionMap } = useDestinationDefinitionList();
  const destinationDefinition = destinationDefinitionMap.get(definitionId);
  const { destinationDefinitions: latestDestinationDefinitions } = useLatestDestinationDefinitionList();
  const latestVersion = latestDestinationDefinitions.find(
    (destinationDefinitions) => destinationDefinitions.destinationDefinitionId === definitionId
  )?.dockerImageTag;
  const { formatMessage } = useIntl();
  const { openModal } = useModalService();
  const { mutateAsync } = useUpdateDestinationDefinition();
  const { registerNotification } = useNotificationService();

  if (!destinationDefinition) {
    trackError(new Error("Destination definition not found"), { definitionId });
    return null;
  }

  const onUpdateDestinationDefinition = async (
    values: EditDefinitionFormValues,
    onModalComplete: (result: unknown) => void
  ) => {
    try {
      const updatedDefinition = await mutateAsync({ ...values, destinationDefinitionId: definitionId });
      registerNotification({
        id: "settings.connector.editDefinition",
        type: "success",
        text: formatMessage(
          { id: "settings.connector.editDefinition.success" },
          { connectorName: updatedDefinition.name }
        ),
      });
      onModalComplete(updatedDefinition);
    } catch (e) {
      registerNotification({
        id: "settings.connector.editDefinition",
        type: "error",
        text: formatMessage(
          { id: "settings.connector.editDefinition.error" },
          { connectorName: destinationDefinition.name }
        ),
      });
    }
  };

  const onEdit = () => {
    openModal({
      title: formatMessage({ id: "settings.connector.editDefinition.title" }),
      content: ({ onComplete }) => (
        <EditConnectorDefinitionModalContent
          definition={destinationDefinition}
          isConnectorNameEditable={isConnectorNameEditable}
          latestVersion={latestVersion}
          onUpdateDefinition={async (values) => onUpdateDestinationDefinition(values, onComplete)}
        />
      ),
    });
  };

  const updateAvailable = !destinationDefinition.custom && latestVersion !== destinationDefinition?.dockerImageTag;

  return (
    <FlexContainer alignItems="center">
      {updateAvailable && (
        <Badge variant="blue">
          <FormattedMessage id="settings.connector.updateAvailable" values={{ latestVersion }} />
        </Badge>
      )}
      <Button variant="clear" onClick={onEdit}>
        <Icon type="pencil" />
      </Button>
    </FlexContainer>
  );
};
