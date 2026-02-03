import { FormattedMessage } from "react-intl";
import { useLocation, useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";
import { HeadTitle } from "components/ui/HeadTitle";
import { PageContainer } from "components/ui/PageContainer";

import { SelectConnector } from "area/connector/components/source/SelectConnector";
import { useSuggestedDestinations } from "area/connector/utils";
import { useDestinationDefinitionList } from "core/api";

export const SelectDestinationPage: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const { destinationDefinitions } = useDestinationDefinitionList();
  const suggestedDestinationDefinitionIds = useSuggestedDestinations();

  return (
    <Box px="xl" pt="xl">
      <HeadTitle titles={[{ id: "destinations.newDestinationTitle" }]} />
      <Box pb="md">
        <PageContainer centered>
          <Heading as="h2" size="lg">
            <FormattedMessage id="destinations.selectDestinationTitle" />
          </Heading>
        </PageContainer>
      </Box>
      <Box pb="xl">
        <SelectConnector
          connectorType="destination"
          connectorDefinitions={destinationDefinitions}
          onSelectConnectorDefinition={(id) =>
            navigate(`./${id}`, { state: { prevPath: location.pathname + location.search } })
          }
          suggestedConnectorDefinitionIds={suggestedDestinationDefinitionIds}
        />
      </Box>
    </Box>
  );
};
