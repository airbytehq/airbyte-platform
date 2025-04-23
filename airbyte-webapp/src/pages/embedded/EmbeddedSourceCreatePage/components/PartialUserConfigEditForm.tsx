import { useSearchParams } from "react-router-dom";

import { useGetPartialUserConfig, useUpdatePartialUserConfig } from "core/api";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";
import { SELECTED_PARTIAL_CONFIG_ID_PARAM, WORKSPACE_ID_PARAM } from "../hooks/useEmbeddedSourceParams";

export const PartialUserConfigEditForm: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedPartialConfigId = searchParams.get(SELECTED_PARTIAL_CONFIG_ID_PARAM);
  const { mutate: updatePartialUserConfig } = useUpdatePartialUserConfig();
  const partialUserConfig = useGetPartialUserConfig(selectedPartialConfigId ?? "");
  const workspaceId = searchParams.get(WORKSPACE_ID_PARAM) ?? "";
  const sourceDefinitionSpecification: SourceDefinitionSpecificationDraft =
    partialUserConfig.configTemplate.configTemplateSpec;

  const onSubmit = (values: ConnectorFormValues) => {
    updatePartialUserConfig({
      partialUserConfigId: selectedPartialConfigId ?? "",
      connectionConfiguration: values.connectionConfiguration,
      workspaceId,
    });
  };

  const initialValues: Partial<ConnectorFormValues> = {
    name: partialUserConfig.configTemplate.name,
    connectionConfiguration: partialUserConfig.connectionConfiguration,
  };

  return (
    <PartialUserConfigForm
      isEditMode
      title={partialUserConfig.configTemplate.name}
      onBack={() => {
        setSearchParams((params) => {
          params.delete("selectedPartialConfigId");
          return params;
        });
      }}
      onSubmit={onSubmit}
      initialValues={initialValues}
      sourceDefinitionSpecification={sourceDefinitionSpecification}
    />
  );
};
