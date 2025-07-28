import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { useDeletePartialUserConfig, useGetPartialUserConfig, useUpdatePartialUserConfig } from "core/api";
import { SourceDefinitionSpecification } from "core/api/types/AirbyteClient";
import { IsAirbyteEmbeddedContext } from "core/services/embedded";
import { convertUserConfigSpec } from "pages/embedded/EmbeddedSourceCreatePage/components/advancedAuthConversion";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import styles from "./PartialUserConfigEditForm.module.scss";
import { PartialUserConfigForm } from "./PartialUserConfigForm";
import { PartialUserConfigHeader } from "./PartialUserConfigHeader";
import { PartialUserConfigSuccessView } from "./PartialUserConfigSuccessView";
export const PartialUserConfigEditForm: React.FC<{ selectedPartialConfigId: string }> = ({
  selectedPartialConfigId,
}) => {
  const { mutate: updatePartialUserConfig, isSuccess: isUpdateSuccess } = useUpdatePartialUserConfig();
  const {
    mutateAsync: deletePartialUserConfig,
    isSuccess: isDeleteSuccess,
    isLoading: isDeleting,
  } = useDeletePartialUserConfig();
  const partialUserConfig = useGetPartialUserConfig(selectedPartialConfigId ?? "");
  const [confirmDelete, setConfirmDelete] = React.useState(false);

  // todo: this is not needed once we update the client to require this property
  if (!partialUserConfig.source_template || !partialUserConfig.source_config) {
    throw new Error("Source template not valid to edit");
  }

  const sourceDefinitionSpecification: SourceDefinitionSpecification = convertUserConfigSpec(
    partialUserConfig.source_template.user_config_spec,
    partialUserConfig.source_template.source_definition_id
  );

  if (selectedPartialConfigId === null) {
    return null;
  }

  const onSubmit = (values: ConnectorFormValues) => {
    return new Promise<void>((resolve, reject) => {
      updatePartialUserConfig(
        {
          id: selectedPartialConfigId,
          partialUserConfigUpdate: {
            source_config: values.connectionConfiguration,
            connection_configuration: values.connectionConfiguration,
          },
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
            icon={partialUserConfig.source_template.icon ?? ""}
            connectorName={partialUserConfig.source_template.name}
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
        connectorName={partialUserConfig.source_template.name}
        icon={partialUserConfig.source_template.icon ?? ""}
      />
    );
  }

  if (isUpdateSuccess) {
    return (
      <PartialUserConfigSuccessView
        successType="update"
        connectorName={partialUserConfig.source_template.name}
        icon={partialUserConfig.source_template.icon ?? ""}
      />
    );
  }

  const initialValues: Partial<ConnectorFormValues> = {
    name: partialUserConfig.source_template.name,
    connectionConfiguration: partialUserConfig.source_config,
  };

  return (
    <IsAirbyteEmbeddedContext.Provider value>
      <PartialUserConfigForm
        isEditMode
        connectorName={partialUserConfig.source_template.name}
        icon={partialUserConfig.source_template.icon ?? ""}
        onSubmit={onSubmit}
        initialValues={initialValues}
        sourceDefinitionSpecification={sourceDefinitionSpecification}
        onDelete={onDeleteClick}
      />
    </IsAirbyteEmbeddedContext.Provider>
  );
};
