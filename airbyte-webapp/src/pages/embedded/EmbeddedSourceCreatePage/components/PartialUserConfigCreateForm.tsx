import { useCreatePartialUserConfig, useGetConfigTemplate } from "core/api";
import { SourceDefinitionSpecification } from "core/api/types/AirbyteClient";
import { IsAirbyteEmbeddedContext } from "core/services/embedded";
import { convertUserConfigSpec } from "pages/embedded/EmbeddedSourceCreatePage/components/advancedAuthConversion";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";
import { PartialUserConfigSuccessView } from "./PartialUserConfigSuccessView";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

export const PartialUserConfigCreateForm: React.FC = () => {
  const { workspaceId, selectedTemplateId } = useEmbeddedSourceParams();
  const { mutate: createPartialUserConfig, isSuccess } = useCreatePartialUserConfig();
  const configTemplate = useGetConfigTemplate(selectedTemplateId ?? "");

  const sourceDefinitionSpecification: SourceDefinitionSpecification = convertUserConfigSpec(
    configTemplate.user_config_spec,
    configTemplate.source_definition_id
  );

  const onSubmit = (values: ConnectorFormValues) => {
    return new Promise<void>((resolve, reject) => {
      createPartialUserConfig(
        {
          workspace_id: workspaceId,
          source_template_id: selectedTemplateId ?? "",
          source_config_template_id: selectedTemplateId ?? "",
          source_config: values.connectionConfiguration,
          connection_configuration: values.connectionConfiguration,
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
        icon={configTemplate.icon ?? ""}
      />
    );
  }

  return (
    <IsAirbyteEmbeddedContext.Provider value>
      <PartialUserConfigForm
        isEditMode={false}
        connectorName={configTemplate.name}
        icon={configTemplate.icon ?? ""}
        onSubmit={onSubmit}
        sourceDefinitionSpecification={sourceDefinitionSpecification}
      />
    </IsAirbyteEmbeddedContext.Provider>
  );
};
