import { useSearchParams } from "react-router-dom";

import { useGetPartialUserConfig, useUpdatePartialUserConfig } from "core/api";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { PartialUserConfigForm } from "./PartialUserConfigForm";

export const PartialUserConfigEditForm: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedPartialConfigId = searchParams.get("selectedPartialConfigId");
  const { mutate: updatePartialUserConfig } = useUpdatePartialUserConfig();
  const partialUserConfig = useGetPartialUserConfig(selectedPartialConfigId ?? "");

  const sourceDefinitionSpecification: SourceDefinitionSpecificationDraft =
    partialUserConfig.configTemplate.configTemplateSpec;

  const onSubmit = (values: ConnectorFormValues) => {
    updatePartialUserConfig({
      partialUserConfigId: selectedPartialConfigId ?? "",
      partialUserConfigProperties: values,
    });
  };

  const initialValues: Partial<ConnectorFormValues> = {
    name: partialUserConfig.configTemplate.name,
    ...partialUserConfig.partialUserConfigProperties,
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
