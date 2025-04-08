import { useSearchParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Heading } from "components/ui/Heading";

import { useCreatePartialUserConfig, useGetConfigTemplate } from "core/api";
import { SourceDefinitionSpecificationDraft } from "core/domain/connector";
import { Controls } from "views/Connector/ConnectorCard/components/Controls";
import { ConnectorForm, ConnectorFormValues } from "views/Connector/ConnectorForm";

export const MaskCreateForm: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedTemplateId = searchParams.get("selectedTemplateId");
  const workspaceId = searchParams.get("workspaceId") ?? "";
  const { mutate: createPartialUserConfig } = useCreatePartialUserConfig();
  const configTemplate = useGetConfigTemplate(selectedTemplateId ?? "");
  const maskDefinitionSpecification: SourceDefinitionSpecificationDraft = {
    // @ts-expect-error todo: fix this type https://github.com/airbytehq/airbyte-internal-issues/issues/12333s
    connectionSpecification: configTemplate.configTemplateSpec.connectionSpecification,
  };

  const onSubmit = (values: ConnectorFormValues) => {
    createPartialUserConfig({
      workspaceId: workspaceId ?? "",
      configTemplateId: selectedTemplateId ?? "",
      partialUserConfigProperties: values,
    });
  };

  return (
    <>
      <Heading as="h1" size="sm">
        {configTemplate.name}
      </Heading>
      <Box py="sm">
        <Button
          variant="light"
          onClick={() => {
            setSearchParams((params) => {
              params.delete("selectedTemplateId");
              return params;
            });
          }}
        >
          Back
        </Button>
      </Box>
      <div>
        <ConnectorForm
          trackDirtyChanges
          formType="source"
          selectedConnectorDefinitionSpecification={maskDefinitionSpecification}
          onSubmit={async (values: ConnectorFormValues) => {
            onSubmit(values);
          }}
          canEdit
          renderFooter={({ dirty, isSubmitting, isValid, resetConnectorForm }) =>
            configTemplate && (
              <>
                {/* //todo: ip allowlist banner? */}
                <Controls
                  isEditMode={false}
                  isTestConnectionInProgress={false}
                  onCancelTesting={() => {
                    return null;
                  }}
                  isSubmitting={isSubmitting}
                  formType="source"
                  hasDefinition={false}
                  onRetestClick={() => {
                    return null;
                  }}
                  onDeleteClick={() => {
                    return null;
                  }}
                  isValid={isValid}
                  dirty={dirty}
                  job={undefined}
                  onCancelClick={() => {
                    resetConnectorForm();
                  }}
                  connectionTestSuccess={false}
                />
              </>
            )
          }
        />
      </div>
    </>
  );
};
