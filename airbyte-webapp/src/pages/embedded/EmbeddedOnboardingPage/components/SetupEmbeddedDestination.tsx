import { FormattedMessage } from "react-intl";
import { useParams, useSearchParams } from "react-router-dom";

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
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { ConnectorCard } from "views/Connector/ConnectorCard";
import { ConnectorDocumentationWrapper } from "views/Connector/ConnectorDocumentationLayout";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep } from "../EmbeddedOnboardingPageLayout";

/**
 * Prepares the configuration for creating a destination config template
 */

export const SetupEmbeddedDestination: React.FC = () => {
  useTrackPage(PageTrackingCodes.EMBEDDED_ONBOARDING_SET_UP_DESTINATION);

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

  if (!selectedDestinationDefinition || !organizationId) {
    // redirect back to select destination... this should not happen ever
    searchParams.set(EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep.SelectDestination);
    setSearchParams(searchParams);
    // redirect happens before return, but TS needs to know that it won't hit the blocks below :)
    return null;
  }

  const onBackClick = () => {
    searchParams.delete(DESTINATION_DEFINITION_PARAM);
    searchParams.set(EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep.SelectDestination);
    setSearchParams(searchParams);
  };

  const onSubmitDestinationForm = async (values: DestinationFormValues) => {
    const definitionId = destinationDefinitionId;

    const connectorFormValues: ConnectorFormValues = {
      name: values.name,
      connectionConfiguration: values.connectionConfiguration,
      resourceAllocation: {},
    };

    await createConnectionTemplate({ values: connectorFormValues, definitionId, organizationId });

    searchParams.set(EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep.EmbedCode);
    setSearchParams(searchParams);
  };

  return (
    <ConnectorDocumentationWrapper>
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
    </ConnectorDocumentationWrapper>
  );
};
