import { useCreatePartialUserConfig, useGetConfigTemplate } from "core/api";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

export const PartialUserConfigCreateForm: React.FC = () => {
  const { workspaceId, selectedTemplateId } = useEmbeddedSourceParams();
  const { mutate: createPartialUserConfig, isSuccess } = useCreatePartialUserConfig();
  const configTemplate = useGetConfigTemplate(selectedTemplateId ?? "", workspaceId);

  const sourceDefinitionSpecification: SourceDefinitionSpecificationDraft = configTemplate.configTemplateSpec;
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
