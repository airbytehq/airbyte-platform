import React from "react";

import { useDestinationDefinitionList } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import ConnectorsView from "./components/ConnectorsView";

const DestinationsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_DESTINATION);
  const { destinationDefinitions } = useDestinationDefinitionList();
  const { destinationDefinitions: usedDestinationDefinitions } = useDestinationDefinitionList({ filterByUsed: true });

  return (
    <ConnectorsView
      type="destinations"
      usedConnectorsDefinitions={usedDestinationDefinitions}
      connectorsDefinitions={destinationDefinitions}
    />
  );
};

export default DestinationsPage;
