import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { PageContainer } from "components/PageContainer";
import { SelectConnector } from "components/source/SelectConnector";
import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";

import { useSuggestedDestinations } from "area/connector/utils";
import { useDestinationDefinitionList } from "core/api";

export const SelectDestinationPage: React.FC = () => {
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
          onSelectConnectorDefinition={(id) => navigate(`./${id}`)}
          suggestedConnectorDefinitionIds={suggestedDestinationDefinitionIds}
        />
      </Box>
    </Box>
  );
};
