import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useDeletePartialUserConfig, useGetPartialUserConfig, useUpdatePartialUserConfig } from "core/api";
import { SourceDefinitionSpecification } from "core/api/types/AirbyteClient";
import { IsAirbyteEmbeddedContext } from "core/services/embedded";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import styles from "./PartialUserConfigEditForm.module.scss";
import { PartialUserConfigForm } from "./PartialUserConfigForm";
import { PartialUserConfigHeader } from "./PartialUserConfigHeader";
import { PartialUserConfigSuccessView } from "./PartialUserConfigSuccessView";
export const PartialUserConfigEditForm: React.FC<{ selectedPartialConfigId: string }> = ({
  selectedPartialConfigId,
}) => {
  const workspaceId = useCurrentWorkspaceId();
  const { mutate: updatePartialUserConfig, isSuccess: isUpdateSuccess } = useUpdatePartialUserConfig();
  const {
    mutateAsync: deletePartialUserConfig,
    isSuccess: isDeleteSuccess,
    isLoading: isDeleting,
  } = useDeletePartialUserConfig();
  const partialUserConfig = useGetPartialUserConfig(selectedPartialConfigId ?? "");
  const [confirmDelete, setConfirmDelete] = React.useState(false);

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
    setConfirmDelete(true);
  };

  const handleDelete = async () => {
    await deletePartialUserConfig(selectedPartialConfigId);
    setConfirmDelete(false);
  };

  const handleCancelDelete = () => {
    setConfirmDelete(false);
  };

  if (confirmDelete === true) {
    return (
      <FlexContainer direction="column" justifyContent="space-between" alignItems="center" className={styles.content}>
        <FlexContainer direction="column" gap="xl" alignItems="center">
          <PartialUserConfigHeader
            icon={partialUserConfig.configTemplate.icon}
            connectorName={partialUserConfig.configTemplate.name}
          />

          <FormattedMessage id="partialUserConfig.delete.warning" />
        </FlexContainer>
        <FlexContainer className={styles.buttonContainer} direction="column">
          <Button full variant="danger" onClick={handleDelete} isLoading={isDeleting}>
            <FormattedMessage id="partialUserConfig.delete.warning.confirm" />
          </Button>
          <Button full variant="clear" onClick={handleCancelDelete} disabled={isDeleting}>
            <FormattedMessage id="partialUserConfig.delete.warning.cancel" />
          </Button>
        </FlexContainer>
      </FlexContainer>
    );
  }

  if (isDeleteSuccess) {
    return (
      <PartialUserConfigSuccessView
        successType="delete"
        connectorName={partialUserConfig.configTemplate.name}
        icon={partialUserConfig.configTemplate.icon}
      />
    );
  }

  if (isUpdateSuccess) {
    return (
      <PartialUserConfigSuccessView
        successType="update"
        connectorName={partialUserConfig.configTemplate.name}
        icon={partialUserConfig.configTemplate.icon}
      />
    );
  }

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
        onDelete={onDeleteClick}
      />
    </IsAirbyteEmbeddedContext.Provider>
  );
};
