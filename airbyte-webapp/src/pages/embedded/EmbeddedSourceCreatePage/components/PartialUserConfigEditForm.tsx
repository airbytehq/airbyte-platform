import { useGetPartialUserConfig, useUpdatePartialUserConfig } from "core/api";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

export const PartialUserConfigEditForm: React.FC = () => {
  const { selectedPartialConfigId, workspaceId } = useEmbeddedSourceParams();
  const { mutate: updatePartialUserConfig, isSuccess } = useUpdatePartialUserConfig();

  const partialUserConfig = useGetPartialUserConfig(selectedPartialConfigId ?? "");

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
      connectorName={partialUserConfig.configTemplate.name}
      icon={partialUserConfig.configTemplate.icon}
      onSubmit={onSubmit}
      initialValues={initialValues}
      sourceDefinitionSpecification={sourceDefinitionSpecification}
      showSuccessView={isSuccess}
    />
  );
};
