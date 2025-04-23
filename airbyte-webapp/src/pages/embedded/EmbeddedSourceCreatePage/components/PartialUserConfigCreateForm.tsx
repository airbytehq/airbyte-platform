import { useSearchParams } from "react-router-dom";

import { useCreatePartialUserConfig, useGetConfigTemplate } from "core/api";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";

export const PartialUserConfigCreateForm: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedTemplateId = searchParams.get("selectedTemplateId") ?? "";
  const workspaceId = searchParams.get("workspaceId") ?? "";
  const { mutate: createPartialUserConfig } = useCreatePartialUserConfig();
  const configTemplate = useGetConfigTemplate(selectedTemplateId, workspaceId);

  const sourceDefinitionSpecification: SourceDefinitionSpecificationDraft = configTemplate.configTemplateSpec;
  const onSubmit = (values: ConnectorFormValues) => {
    createPartialUserConfig({
      workspaceId,
      configTemplateId: selectedTemplateId,
      connectionConfiguration: values.connectionConfiguration,
    });
  };

  return (
    <PartialUserConfigForm
      isEditMode={false}
      title={configTemplate.name}
      onBack={() => {
        setSearchParams((params) => {
          params.delete("selectedTemplateId");
          return params;
        });
      }}
      onSubmit={onSubmit}
      sourceDefinitionSpecification={sourceDefinitionSpecification}
    />
  );
};
