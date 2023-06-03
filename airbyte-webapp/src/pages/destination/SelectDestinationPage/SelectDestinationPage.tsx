import { useNavigate } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { SelectConnector } from "components/source/SelectConnector";
import { Box } from "components/ui/Box";

import { useAvailableDestinationDefinitions } from "hooks/domain/connector/useAvailableDestinationDefinitions";

export const SelectDestinationPage: React.FC = () => {
  const navigate = useNavigate();
  const destinationDefinitions = useAvailableDestinationDefinitions();

  return (
    <>
      <HeadTitle titles={[{ id: "destinations.newDestinationTitle" }]} />

      <Box pb="2xl">
        <SelectConnector
          connectorType="destination"
          connectorDefinitions={destinationDefinitions}
          headingKey="destinations.selectDestinationTitle"
          onSelectConnectorDefinition={(id) => navigate(`./${id}`)}
        />
      </Box>
    </>
  );
};
