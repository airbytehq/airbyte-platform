import React, { useMemo, useRef } from "react";

import { useDestinationDefinitionList, useDestinationList } from "core/api";
import { DestinationDefinitionRead } from "core/api/types/AirbyteClient";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import ConnectorsView from "./components/ConnectorsView";

const DestinationsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_DESTINATION);

  const { destinationDefinitions } = useDestinationDefinitionList();
  const { destinations } = useDestinationList();

  const idToDestinationDefinition = useMemo(
    () =>
      destinationDefinitions.reduce((map, destinationDefinition) => {
        map.set(destinationDefinition.destinationDefinitionId, destinationDefinition);
        return map;
      }, new Map<string, DestinationDefinitionRead>()),
    [destinationDefinitions]
  );
  const definitionMap = useRef(idToDestinationDefinition);
  definitionMap.current = idToDestinationDefinition;

  const usedDestinationDefinitions: DestinationDefinitionRead[] = useMemo(() => {
    const usedDestinationDefinitionIds = new Set<string>(
      destinations.map((destination) => destination.destinationDefinitionId)
    );
    return destinationDefinitions
      .filter((destinationDefinition) =>
        usedDestinationDefinitionIds.has(destinationDefinition.destinationDefinitionId)
      )
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [destinationDefinitions, destinations]);

  return (
    <ConnectorsView
      type="destinations"
      usedConnectorsDefinitions={usedDestinationDefinitions}
      connectorsDefinitions={destinationDefinitions}
    />
  );
};

export default DestinationsPage;
