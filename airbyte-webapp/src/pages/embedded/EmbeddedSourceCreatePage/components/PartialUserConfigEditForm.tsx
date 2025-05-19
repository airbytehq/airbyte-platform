import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useDeletePartialUserConfig, useGetPartialUserConfig, useUpdatePartialUserConfig } from "core/api";
import { SourceDefinitionSpecification } from "core/api/types/AirbyteClient";
import { IsAirbyteEmbeddedContext } from "core/services/embedded";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";

export const PartialUserConfigEditForm: React.FC<{ selectedPartialConfigId: string }> = ({
  selectedPartialConfigId,
}) => {
  const workspaceId = useCurrentWorkspaceId();
  const { mutate: updatePartialUserConfig, isSuccess } = useUpdatePartialUserConfig();
  const { mutateAsync: deletePartialUserConfig, isSuccess: isDeleteSuccess } = useDeletePartialUserConfig();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const partialUserConfig = useGetPartialUserConfig(selectedPartialConfigId ?? "");

  const sourceDefinitionSpecification: SourceDefinitionSpecification = {
    ...partialUserConfig.configTemplate.configTemplateSpec,
    advancedAuth: partialUserConfig.configTemplate.advancedAuth,
    sourceDefinitionId: partialUserConfig.configTemplate.sourceDefinitionId,
  };

  if (selectedPartialConfigId === null) {
    return null;
  }

  const onSubmit = (values: ConnectorFormValues) => {
    return new Promise<void>((resolve, reject) => {
      updatePartialUserConfig(
        {
          partialUserConfigId: selectedPartialConfigId,
          connectionConfiguration: values.connectionConfiguration,
          workspaceId,
        },
        {
          onSuccess: () => resolve(),
          onError: (error) => reject(error),
        }
      );
    });
  };

  const onDeleteClick = () => {
    openConfirmationModal({
      title: <FormattedMessage id="partialUserConfig.delete.buttonText" />,
      text: (
        <Text>
          <FormattedMessage id="partialUserConfig.delete.warning" />
        </Text>
      ),
      onSubmit: async () => {
        try {
          await deletePartialUserConfig(selectedPartialConfigId);
          closeConfirmationModal();
        } catch (e) {
          registerNotification({
            type: "error",
            id: `delete-integration-${selectedPartialConfigId}`,
            text: <FormattedMessage id="partialUserConfig.delete.error" />,
          });
        }
      },
      submitButtonText: formatMessage({ id: "partialUserConfig.delete.buttonText" }),
    });
  };

  const initialValues: Partial<ConnectorFormValues> = {
    name: partialUserConfig.configTemplate.name,
    connectionConfiguration: partialUserConfig.connectionConfiguration,
  };

  return (
    <IsAirbyteEmbeddedContext.Provider value>
      <PartialUserConfigForm
        isEditMode
        connectorName={partialUserConfig.configTemplate.name}
        icon={partialUserConfig.configTemplate.icon}
        onSubmit={onSubmit}
        initialValues={initialValues}
        sourceDefinitionSpecification={sourceDefinitionSpecification}
        showSuccessView={isSuccess}
        onDelete={onDeleteClick}
        isDeleteSuccess={isDeleteSuccess}
      />
    </IsAirbyteEmbeddedContext.Provider>
  );
};
