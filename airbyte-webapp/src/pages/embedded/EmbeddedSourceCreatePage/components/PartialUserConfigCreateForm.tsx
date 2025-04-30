import { useCreatePartialUserConfig, useGetConfigTemplate } from "core/api";
import { SourceDefinitionSpecification } from "core/api/types/AirbyteClient";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

export const PartialUserConfigCreateForm: React.FC = () => {
  const { workspaceId, selectedTemplateId } = useEmbeddedSourceParams();
  const { mutate: createPartialUserConfig, isSuccess } = useCreatePartialUserConfig();
  const configTemplate = useGetConfigTemplate(selectedTemplateId ?? "", workspaceId);

  const sourceDefinitionSpecification: SourceDefinitionSpecification = {
    ...configTemplate.configTemplateSpec,
    sourceDefinitionId: configTemplate.sourceDefinitionId,
  };

  const onSubmit = (values: ConnectorFormValues) => {
    createPartialUserConfig({
      workspaceId,
      configTemplateId: selectedTemplateId ?? "",
      connectionConfiguration: values.connectionConfiguration,
    });
  };

  return (
    <PartialUserConfigForm
      isEditMode={false}
      connectorName={configTemplate.name}
      icon={configTemplate.icon}
      onSubmit={onSubmit}
      sourceDefinitionSpecification={sourceDefinitionSpecification}
      showSuccessView={isSuccess}
    />
  );
};
