import { FormattedMessage } from "react-intl";
import { useLocation, useNavigate } from "react-router-dom";

import { HeadTitle } from "components/HeadTitle";
import { PageContainer } from "components/PageContainer";
import { SelectConnector } from "components/source/SelectConnector";
import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";

import { useSuggestedSources } from "area/connector/utils";
import { useSourceDefinitionList } from "core/api";

export const SelectSourcePage: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const { sourceDefinitions } = useSourceDefinitionList();
  const suggestedSourceDefinitionIds = useSuggestedSources();

  return (
    <Box px="xl" pt="xl">
      <HeadTitle titles={[{ id: "sources.newSourceTitle" }]} />
      <Box pb="md">
        <PageContainer centered>
          <Heading as="h2" size="lg">
            <FormattedMessage id="sources.selectSourceTitle" />
          </Heading>
        </PageContainer>
      </Box>
      <Box pb="xl">
        <SelectConnector
          connectorType="source"
          connectorDefinitions={sourceDefinitions}
          onSelectConnectorDefinition={(id) =>
            navigate(`./${id}`, { state: { prevPath: location.pathname + location.search } })
          }
          suggestedConnectorDefinitionIds={suggestedSourceDefinitionIds}
        />
      </Box>
    </Box>
  );
};
