import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { DestinationForm } from "components/destination/DestinationForm";
import { DestinationFormValues } from "components/destination/DestinationForm/DestinationForm";
import { PageContainer } from "components/PageContainer";
import { SelectConnector } from "components/source/SelectConnector";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";

import { useSuggestedDestinations } from "area/connector/utils";
import { useDestinationDefinitionList, useCreateDestination } from "core/api";
import { AppActionCodes, useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";

import { DESTINATION_ID_PARAM, DESTINATION_TYPE_PARAM } from "./SelectDestination";

export const DESTINATION_DEFINITION_PARAM = "destinationDefinitionId";

export const CreateNewDestination: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedDestinationDefinitionId = searchParams.get(DESTINATION_DEFINITION_PARAM);

  const suggestedDestinationDefinitionIds = useSuggestedDestinations();

  const { destinationDefinitions } = useDestinationDefinitionList();
  const { trackAction } = useAppMonitoringService();
  const { mutateAsync: createDestination } = useCreateDestination();

  const { clearAllFormChanges } = useFormChangeTrackerService();

  const onSelectDestinationDefinitionId = (destinationDefinitionId: string) => {
    searchParams.set(DESTINATION_DEFINITION_PARAM, destinationDefinitionId);
    setSearchParams(searchParams);
  };

  const onCreateDestination = async (values: DestinationFormValues) => {
    const destinationDefinition = destinationDefinitions.find(
      (item) => item.destinationDefinitionId === values.serviceType
    );
    if (!destinationDefinition) {
      trackAction(AppActionCodes.CONNECTOR_DEFINITION_NOT_FOUND, { destinationDefinitionId: values.serviceType });
      throw new Error(AppActionCodes.CONNECTOR_DEFINITION_NOT_FOUND);
    }
    const result = await createDestination({ values, destinationConnector: destinationDefinition });
    clearAllFormChanges();
    await new Promise((resolve) => setTimeout(resolve, 2000));

    const newParams = new URLSearchParams(searchParams);
    newParams.set(DESTINATION_ID_PARAM, result.destinationId);
    newParams.delete(DESTINATION_TYPE_PARAM);
    newParams.delete(DESTINATION_DEFINITION_PARAM);
    setSearchParams(newParams);
  };

  const onGoBack = () => {
    const newParams = new URLSearchParams(searchParams);
    newParams.delete(DESTINATION_DEFINITION_PARAM);
    setSearchParams(newParams);
  };

  if (selectedDestinationDefinitionId) {
    return (
      <Box px="md">
        <PageContainer centered>
          <Box mb="md">
            <Button variant="clear" onClick={onGoBack} icon={<Icon type="chevronLeft" size="lg" />}>
              <FormattedMessage id="connectorBuilder.backButtonLabel" />
            </Button>
          </Box>
          <DestinationForm
            selectedDestinationDefinitionId={selectedDestinationDefinitionId}
            destinationDefinitions={destinationDefinitions}
            onSubmit={onCreateDestination}
          />
        </PageContainer>
      </Box>
    );
  }

  return (
    <SelectConnector
      connectorDefinitions={destinationDefinitions}
      connectorType="destination"
      onSelectConnectorDefinition={(destinationDefinitionId) =>
        onSelectDestinationDefinitionId(destinationDefinitionId)
      }
      suggestedConnectorDefinitionIds={suggestedDestinationDefinitionIds}
    />
  );
};
