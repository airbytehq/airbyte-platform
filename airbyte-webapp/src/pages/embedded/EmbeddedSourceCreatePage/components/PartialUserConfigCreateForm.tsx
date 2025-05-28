import { useCreatePartialUserConfig, useGetConfigTemplate } from "core/api";
import { SourceDefinitionSpecification } from "core/api/types/AirbyteClient";
import { IsAirbyteEmbeddedContext } from "core/services/embedded";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";
import { PartialUserConfigSuccessView } from "./PartialUserConfigSuccessView";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

export const PartialUserConfigCreateForm: React.FC = () => {
  const { workspaceId, selectedTemplateId } = useEmbeddedSourceParams();
  const { mutate: createPartialUserConfig, isSuccess } = useCreatePartialUserConfig();
  const configTemplate = useGetConfigTemplate(selectedTemplateId ?? "", workspaceId);

  const sourceDefinitionSpecification: SourceDefinitionSpecification = {
    ...configTemplate.configTemplateSpec,
    advancedAuth: configTemplate.advancedAuth,
    sourceDefinitionId: configTemplate.sourceDefinitionId,
  };

  const onSubmit = (values: ConnectorFormValues) => {
    return new Promise<void>((resolve, reject) => {
      createPartialUserConfig(
        {
          workspaceId,
          configTemplateId: selectedTemplateId ?? "",
          connectionConfiguration: values.connectionConfiguration,
        },
        {
          onSuccess: () => resolve(),
          onError: (error) => reject(error),
        }
      );
    });
  };

  if (isSuccess) {
    return (
      <PartialUserConfigSuccessView
        successType="create"
        connectorName={configTemplate.name}
        icon={configTemplate.icon}
      />
    );
  }

  return (
    <IsAirbyteEmbeddedContext.Provider value>
      <PartialUserConfigForm
        isEditMode={false}
        connectorName={configTemplate.name}
        icon={configTemplate.icon}
        onSubmit={onSubmit}
        sourceDefinitionSpecification={sourceDefinitionSpecification}
      />
    </IsAirbyteEmbeddedContext.Provider>
  );
};
