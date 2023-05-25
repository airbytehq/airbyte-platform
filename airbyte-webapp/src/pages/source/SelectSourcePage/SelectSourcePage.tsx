import { useNavigate } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { SelectConnector } from "components/source/SelectConnector";
import { Box } from "components/ui/Box";

import { useAvailableSourceDefinitions } from "hooks/domain/connector/useAvailableSourceDefinitions";

export const SelectSourcePage: React.FC = () => {
  const navigate = useNavigate();
  const sourceDefinitions = useAvailableSourceDefinitions();

  return (
    <>
      <HeadTitle titles={[{ id: "sources.newSourceTitle" }]} />

      <Box pb="2xl">
        <SelectConnector
          connectorType="source"
          connectorDefinitions={sourceDefinitions}
          headingKey="sources.selectSourceTitle"
          onSelectConnectorDefinition={(id) => navigate(`./${id}`)}
        />
      </Box>
    </>
  );
};
