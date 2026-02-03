import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { PageContainer } from "components/ui/PageContainer";

import { SelectConnector } from "area/connector/components/source/SelectConnector";
import { useSuggestedSources } from "area/connector/utils";
import { useCreateSource, useSourceDefinitionList } from "core/api";
import { useFormChangeTrackerService } from "core/services/FormChangeTracker";
import { AppActionCodes, trackAction } from "core/utils/datadog";
import { SourceForm, SourceFormValues } from "pages/source/CreateSourcePage/SourceForm";

import { SOURCE_ID_PARAM, SOURCE_TYPE_PARAM } from "./DefineSource";

export const SOURCE_DEFINITION_PARAM = "sourceDefinitionId";

export const CreateNewSource: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedSourceDefinitionId = searchParams.get(SOURCE_DEFINITION_PARAM);
  const suggestedSourceDefinitionIds = useSuggestedSources();
  const { sourceDefinitions } = useSourceDefinitionList();
  const { mutateAsync: createSource } = useCreateSource();

  const { clearAllFormChanges } = useFormChangeTrackerService();

  const onSelectSourceDefinitionId = (sourceDefinitionId: string) => {
    searchParams.set(SOURCE_DEFINITION_PARAM, sourceDefinitionId);
    setSearchParams(searchParams);
  };

  const onCreateSource = async (values: SourceFormValues) => {
    const sourceDefinition = sourceDefinitions.find((item) => item.sourceDefinitionId === values.serviceType);
    if (!sourceDefinition) {
      trackAction(AppActionCodes.CONNECTOR_DEFINITION_NOT_FOUND, { sourceDefinitionId: values.serviceType });
      throw new Error(AppActionCodes.CONNECTOR_DEFINITION_NOT_FOUND);
    }
    const result = await createSource({ values, sourceConnector: sourceDefinition });
    clearAllFormChanges();
    await new Promise((resolve) => setTimeout(resolve, 2000));

    const newParams = new URLSearchParams(searchParams);
    newParams.set(SOURCE_ID_PARAM, result.sourceId);
    newParams.delete(SOURCE_TYPE_PARAM);
    newParams.delete(SOURCE_DEFINITION_PARAM);
    setSearchParams(newParams);
  };

  const onGoBack = () => {
    const newParams = new URLSearchParams(searchParams);
    newParams.delete(SOURCE_DEFINITION_PARAM);
    setSearchParams(newParams);
  };

  if (selectedSourceDefinitionId) {
    return (
      <PageContainer centered>
        <Box mb="md">
          <Button variant="clear" onClick={onGoBack} icon="chevronLeft" iconSize="lg">
            <FormattedMessage id="connectorBuilder.backButtonLabel" />
          </Button>
        </Box>
        <SourceForm
          selectedSourceDefinitionId={selectedSourceDefinitionId}
          sourceDefinitions={sourceDefinitions}
          onSubmit={onCreateSource}
        />
      </PageContainer>
    );
  }

  return (
    <SelectConnector
      connectorDefinitions={sourceDefinitions}
      connectorType="source"
      onSelectConnectorDefinition={(sourceDefinitionId) => onSelectSourceDefinitionId(sourceDefinitionId)}
      suggestedConnectorDefinitionIds={suggestedSourceDefinitionIds}
    />
  );
};
