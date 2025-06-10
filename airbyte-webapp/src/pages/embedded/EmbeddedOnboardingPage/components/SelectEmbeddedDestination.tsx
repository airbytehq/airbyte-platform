import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { DESTINATION_DEFINITION_PARAM } from "components/connection/CreateConnection/CreateNewDestination";
import { EmptyState } from "components/EmptyState";
import { HeadTitle } from "components/HeadTitle";
import { PageContainer } from "components/PageContainer";
import { ConnectorList } from "components/source/SelectConnector/ConnectorList";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useGlobalDestinationDefinitionList } from "core/api";
import { ConnectorDefinitionOrEnterpriseStub } from "core/domain/connector";
import { isSourceDefinition } from "core/domain/connector/source";
import { Action, Namespace, PageTrackingCodes, useAnalyticsService, useTrackPage } from "core/services/analytics";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment/ExperimentService";

import { EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep } from "../EmbeddedOnboardingPageLayout";

export const SelectEmbeddedDestination: React.FC = () => {
  useTrackPage(PageTrackingCodes.EMBEDDED_ONBOARDING_SELECT_DESTINATION);
  const analyticsService = useAnalyticsService();

  const [searchParams, setSearchParams] = useSearchParams();

  const { destinationDefinitionMap } = useGlobalDestinationDefinitionList();
  const embeddedDestinations = useExperiment("embedded.operatorOnboarding.destinations");
  const embeddedDestinationIds = !embeddedDestinations ? [] : embeddedDestinations.split(",").map((id) => id.trim());
  const connectorDefinitions = embeddedDestinationIds
    .map((id) => destinationDefinitionMap.get(id))
    .filter((def): def is NonNullable<typeof def> => def !== undefined);

  const handleConnectorButtonClick = (definition: ConnectorDefinitionOrEnterpriseStub) => {
    if ("isEnterprise" in definition || isSourceDefinition(definition)) {
      throw new Error("This flow is only configured for Airbyte destination connectors.");
    }

    analyticsService.track(Namespace.EMBEDDED, Action.DESTINATION_SELECTED, {
      destinationId: definition.destinationDefinitionId,
    });

    searchParams.set(DESTINATION_DEFINITION_PARAM, definition.destinationDefinitionId);
    searchParams.set(EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep.SetupDestination);
    setSearchParams(searchParams);
  };

  return (
    <PageContainer centered>
      <Box px="xl" pt="2xl" mt="2xl">
        <HeadTitle titles={[{ id: "settings.embedded" }]} />
        <Box pt="2xl" pb="xl">
          <Heading as="h2" size="lg">
            <FormattedMessage id="embedded.onboarding.selectDestination.title" />
          </Heading>
        </Box>
        <Box pb="xl">
          <FlexContainer direction="column" gap="xl">
            <ConnectorList
              sorting={{
                column: "name",
                isAscending: true,
              }}
              displayType="grid"
              connectorDefinitions={connectorDefinitions}
              onConnectorButtonClick={handleConnectorButtonClick}
              noSearchResultsContent={
                <Box p="2xl">
                  <EmptyState text={<FormattedMessage id="embedded.destinations.emptyStateTitle" />} />
                </Box>
              }
            />
            <Box py="md">
              <Text color="grey600" size="lg">
                <FormattedMessage
                  id="embedded.onboarding.selectDestination.talkToUs"
                  values={{
                    lnk: (children) => (
                      <ExternalLink data-testid="embedded-destinations-talk-to-sales" href={links.featureTalkToSales}>
                        {children}
                      </ExternalLink>
                    ),
                  }}
                />
              </Text>
            </Box>
          </FlexContainer>
        </Box>
      </Box>
    </PageContainer>
  );
};
