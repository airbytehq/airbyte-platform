import { FormattedMessage } from "react-intl";
import { useParams, useSearchParams } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { DESTINATION_DEFINITION_PARAM } from "components/connection/CreateConnection/CreateNewDestination";
import { FormPageContent } from "components/ConnectorBlocks";
import { DestinationFormValues } from "components/destination/DestinationForm/DestinationForm";
import { HeadTitle } from "components/HeadTitle";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { ConnectorDefinitionBranding } from "components/ui/ConnectorDefinitionBranding";
import { FlexContainer } from "components/ui/Flex";

import {
  useCreateConnectionTemplate,
  useGetDestinationDefinitionSpecificationAsync,
  useGlobalDestinationDefinitionList,
} from "core/api";
import { Action, Namespace, PageTrackingCodes, useAnalyticsService, useTrackPage } from "core/services/analytics";
import { useFormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep } from "../EmbeddedOnboardingPageLayout";

/**
 * Prepares the configuration for creating a destination config template
 */

export const SetupEmbeddedDestination: React.FC = () => {
  useTrackPage(PageTrackingCodes.EMBEDDED_ONBOARDING_SET_UP_DESTINATION);
  const { getDirtyFormIds } = useFormChangeTrackerService();
  const analyticsService = useAnalyticsService();

  const [searchParams, setSearchParams] = useSearchParams();
  const destinationDefinitionId = searchParams.get(DESTINATION_DEFINITION_PARAM) || "";
  const organizationId = useParams<{ organizationId: string }>().organizationId;
  const { mutateAsync: createConnectionTemplate } = useCreateConnectionTemplate();
  const { destinationDefinitionMap } = useGlobalDestinationDefinitionList();
  const selectedDestinationDefinition = destinationDefinitionMap.get(destinationDefinitionId);

  const {
    data: destinationDefinitionSpecification,
    error: destinationDefinitionError,
    isLoading,
  } = useGetDestinationDefinitionSpecificationAsync(destinationDefinitionId);

  useEffectOnce(() => {
    if (!selectedDestinationDefinition || !organizationId) {
      // redirect back to select destination... this should not happen ever
      searchParams.set(EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep.SelectDestination);
      setSearchParams(searchParams);
    }
  });

  if (!selectedDestinationDefinition || !organizationId) {
    // redirect happens in useEffect, so just render nothing
    return null;
  }

  const onBackClick = () => {
    const newParams = new URLSearchParams(searchParams);
    newParams.delete(DESTINATION_DEFINITION_PARAM);
    newParams.set(EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep.SelectDestination);

    const hadChanges = getDirtyFormIds().includes("embedded-destination-setup");

    analyticsService.track(Namespace.EMBEDDED, Action.BACK_TO_SELECT_DESTINATION, {
      actionDescription: "User clicked back to select destination",
      destinationId: destinationDefinitionId,
      hadChanges,
    });

    setSearchParams(newParams);
  };

  const onSubmitDestinationForm = async (values: DestinationFormValues) => {
    const connectorFormValues: ConnectorFormValues = {
      name: values.name,
      connectionConfiguration: values.connectionConfiguration,
      resourceAllocation: {},
    };

    await createConnectionTemplate({ values: connectorFormValues, destinationDefinitionId, organizationId });
    // Defer navigation until JS callstack clears.
    // This prevents the navigation from firing prior to the form registering the successful submission.
    setTimeout(() => {
      const newParams = new URLSearchParams(searchParams);
      newParams.delete(DESTINATION_DEFINITION_PARAM);
      newParams.set(EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep.EmbedCode);
      setSearchParams(newParams);
    }, 0);
  };

  return (
    <>
      <HeadTitle titles={[{ id: "settings.embedded" }]} />
      <FormPageContent>
        <FlexContainer justifyContent="flex-start">
          <Box mb="md">
            <Button variant="clear" onClick={onBackClick} icon="chevronLeft" iconSize="lg">
              <FormattedMessage id="embedded.onboarding.backToSelectDestination" />
            </Button>
          </Box>
        </FlexContainer>
        <ConnectorCard
          formId="embedded-destination-setup"
          formType="destination"
          headerBlock={<ConnectorDefinitionBranding destinationDefinitionId={destinationDefinitionId} />}
          description={<FormattedMessage id="destinations.description" />}
          isLoading={isLoading}
          fetchingConnectorError={destinationDefinitionError instanceof Error ? destinationDefinitionError : null}
          availableConnectorDefinitions={[selectedDestinationDefinition]}
          selectedConnectorDefinitionSpecification={destinationDefinitionSpecification}
          selectedConnectorDefinitionId={destinationDefinitionId}
          onSubmit={onSubmitDestinationForm}
          hideCopyConfig
          skipCheckConnection
        />
      </FormPageContent>
    </>
  );
};
