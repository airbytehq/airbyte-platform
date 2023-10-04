import { ConsumptionRead } from "core/api/types/CloudApi";

import { AvailableDestination, AvailableSource } from "./CreditsUsageContext";

export const calculateAvailableSourcesAndDestinations = (rawConsumptionData: ConsumptionRead[]) => {
  const sourceAndDestinationMaps = rawConsumptionData.reduce(
    (allSourcesAndDestinations, currentConsumptionItem) => {
      // create set of sources, including merging a set of the destinations each is connected to
      if (!allSourcesAndDestinations.sources[currentConsumptionItem.connection.sourceId]) {
        allSourcesAndDestinations.sources[currentConsumptionItem.connection.sourceId] = {
          name: currentConsumptionItem.connection.sourceConnectionName,
          id: currentConsumptionItem.connection.sourceId,
          icon: currentConsumptionItem.connection.sourceIcon,
          supportLevel: currentConsumptionItem.connection.sourceSupportLevel,
          custom: currentConsumptionItem.connection.sourceCustom,
          connectedDestinations: [currentConsumptionItem.connection.destinationId],
        };
      } else {
        allSourcesAndDestinations.sources[currentConsumptionItem.connection.sourceId] = {
          ...allSourcesAndDestinations.sources[currentConsumptionItem.connection.sourceId],
          connectedDestinations: Array.from(
            new Set([
              ...allSourcesAndDestinations.sources[currentConsumptionItem.connection.sourceId].connectedDestinations,
              currentConsumptionItem.connection.destinationId,
            ])
          ),
        };
      }
      // create set of destinations, including merging a set of the sources each is connected to
      if (!allSourcesAndDestinations.destinations[currentConsumptionItem.connection.destinationId]) {
        allSourcesAndDestinations.destinations[currentConsumptionItem.connection.destinationId] = {
          name: currentConsumptionItem.connection.destinationConnectionName,
          id: currentConsumptionItem.connection.destinationId,
          icon: currentConsumptionItem.connection.destinationIcon,
          supportLevel: currentConsumptionItem.connection.destinationSupportLevel,
          custom: currentConsumptionItem.connection.destinationCustom,
          connectedSources: [currentConsumptionItem.connection.sourceId],
        };
      } else {
        allSourcesAndDestinations.destinations[currentConsumptionItem.connection.destinationId] = {
          ...allSourcesAndDestinations.destinations[currentConsumptionItem.connection.destinationId],
          connectedSources: Array.from(
            new Set([
              ...allSourcesAndDestinations.destinations[currentConsumptionItem.connection.destinationId]
                .connectedSources,
              currentConsumptionItem.connection.sourceId,
            ])
          ),
        };
      }

      return allSourcesAndDestinations;
    },
    {
      sources: {} as Record<string, AvailableSource>,
      destinations: {} as Record<string, AvailableDestination>,
    }
  );

  return {
    sources: Object.values(sourceAndDestinationMaps.sources),
    destinations: Object.values(sourceAndDestinationMaps.destinations),
  };
};
