import { FormattedMessage, useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { useLatestSourceDefinitionList, useUpdateSourceDefinition, useSourceDefinitionList } from "core/api";
import { trackError } from "core/utils/datadog";
import { useModalService } from "hooks/services/Modal";
import { useNotificationService } from "hooks/services/Notification";

import { EditConnectorDefinitionModalContent, EditDefinitionFormValues } from "./EditConnectorDefinitionModal";

interface EditSourceDefinitionButtonProps {
  definitionId: string;
  isConnectorNameEditable?: boolean;
}

export const EditSourceDefinitionButton: React.FC<EditSourceDefinitionButtonProps> = ({
  isConnectorNameEditable = true,
  definitionId,
}) => {
  const { sourceDefinitionMap } = useSourceDefinitionList();
  const sourceDefinition = sourceDefinitionMap.get(definitionId);
  const { sourceDefinitions: latestSourceDefinitions } = useLatestSourceDefinitionList();
  const latestVersion = latestSourceDefinitions.find(
    (sourceDefinitions) => sourceDefinitions.sourceDefinitionId === definitionId
  )?.dockerImageTag;
  const { formatMessage } = useIntl();
  const { openModal } = useModalService();
  const { mutateAsync } = useUpdateSourceDefinition();
  const { registerNotification } = useNotificationService();

  if (!sourceDefinition) {
    trackError(new Error("Source definition not found"), { definitionId });
    return null;
  }

  const onUpdateSourceDefinition = async (
    values: EditDefinitionFormValues,
    onModalComplete: (result: unknown) => void
  ) => {
    try {
      const updatedDefinition = await mutateAsync({ ...values, sourceDefinitionId: definitionId });
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
          { connectorName: sourceDefinition.name }
        ),
      });
    }
  };

  const onEdit = () => {
    openModal({
      title: formatMessage({ id: "settings.connector.editDefinition.title" }),
      content: ({ onComplete }) => (
        <EditConnectorDefinitionModalContent
          definition={sourceDefinition}
          isConnectorNameEditable={isConnectorNameEditable}
          latestVersion={latestVersion}
          onUpdateDefinition={async (values) => onUpdateSourceDefinition(values, onComplete)}
        />
      ),
    });
  };

  const updateAvailable = !sourceDefinition.custom && latestVersion !== sourceDefinition?.dockerImageTag;

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
