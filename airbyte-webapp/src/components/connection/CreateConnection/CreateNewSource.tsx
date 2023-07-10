import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { PageContainer } from "components/PageContainer";
import { SelectConnector } from "components/source/SelectConnector";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";

import { useAvailableSourceDefinitions } from "hooks/domain/connector/useAvailableSourceDefinitions";
import { AppActionCodes, useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { useCreateSource } from "hooks/services/useSourceHook";
import { SourceForm, SourceFormValues } from "pages/source/CreateSourcePage/SourceForm";

import { SOURCE_ID_PARAM, SOURCE_TYPE_PARAM } from "./SelectSource";

export const SOURCE_DEFINITION_PARAM = "sourceDefinitionId";

export const CreateNewSource: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedSourceDefinitionId = searchParams.get(SOURCE_DEFINITION_PARAM);

  const sourceDefinitions = useAvailableSourceDefinitions();
  const { trackAction } = useAppMonitoringService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { mutateAsync: createSource } = useCreateSource();

  const { hasFormChanges, clearAllFormChanges } = useFormChangeTrackerService();

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

    searchParams.set(SOURCE_ID_PARAM, result.sourceId);
    searchParams.delete(SOURCE_TYPE_PARAM);
    searchParams.delete(SOURCE_DEFINITION_PARAM);
    setSearchParams(searchParams);
  };

  const onGoBack = () => {
    if (hasFormChanges) {
      openConfirmationModal({
        title: "form.discardChanges",
        text: "form.discardChangesConfirmation",
        submitButtonText: "form.discardChanges",
        onSubmit: () => {
          closeConfirmationModal();
          searchParams.delete(SOURCE_DEFINITION_PARAM);
          setSearchParams(searchParams);
        },
        onClose: () => {
          closeConfirmationModal();
        },
      });
    } else {
      searchParams.delete(SOURCE_DEFINITION_PARAM);
      setSearchParams(searchParams);
    }
  };

  if (selectedSourceDefinitionId) {
    return (
      <Box px="md">
        <PageContainer centered>
          <Box mb="md">
            <Button variant="clear" onClick={onGoBack} icon={<Icon type="chevronLeft" size="lg" />}>
              <FormattedMessage id="connectorBuilder.backButtonLabel" />
            </Button>
          </Box>
          <SourceForm
            selectedSourceDefinitionId={selectedSourceDefinitionId}
            sourceDefinitions={sourceDefinitions}
            onSubmit={onCreateSource}
          />
        </PageContainer>
      </Box>
    );
  }

  return (
    <SelectConnector
      connectorDefinitions={sourceDefinitions}
      connectorType="source"
      onSelectConnectorDefinition={(sourceDefinitionId) => onSelectSourceDefinitionId(sourceDefinitionId)}
    />
  );
};
