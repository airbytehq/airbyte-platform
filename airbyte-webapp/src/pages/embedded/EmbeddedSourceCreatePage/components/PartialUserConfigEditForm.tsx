import React from "react";

import { useGetPartialUserConfig, useUpdatePartialUserConfig } from "core/api";
import { SourceDefinitionSpecification } from "core/api/types/AirbyteClient";
import { IsAirbyteEmbeddedContext } from "core/services/embedded";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

export const PartialUserConfigEditForm: React.FC = () => {
  const { selectedPartialConfigId, workspaceId } = useEmbeddedSourceParams();
  const { mutate: updatePartialUserConfig, isSuccess } = useUpdatePartialUserConfig();

  const partialUserConfig = useGetPartialUserConfig(selectedPartialConfigId ?? "");

  const sourceDefinitionSpecification: SourceDefinitionSpecification = {
    ...partialUserConfig.configTemplate.configTemplateSpec,
    sourceDefinitionId: partialUserConfig.configTemplate.sourceDefinitionId,
  };

  const onSubmit = (values: ConnectorFormValues) => {
    return new Promise<void>((resolve, reject) => {
      updatePartialUserConfig(
        {
          partialUserConfigId: selectedPartialConfigId ?? "",
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
      />
    </IsAirbyteEmbeddedContext.Provider>
  );
};
