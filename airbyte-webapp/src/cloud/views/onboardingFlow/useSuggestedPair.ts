import { useDestinationDefinitionList } from "core/api";
import { DestinationDefinitionRead, SourceDefinitionRead } from "core/api/types/AirbyteClient";

import { resolveDestinationDefinitionId } from "./onboardingConnectors";
import { useSuggestedSource } from "./useSuggestedSource";

interface SuggestedConnectorPair {
  source: SourceDefinitionRead;
  destination: DestinationDefinitionRead;
}

/** The pair to set up first: the source pick that `useSuggestedSource` returns and the chosen destination. */
export const useSuggestedPair = (
  sourcePicks: string[],
  destinationChoice?: string
): SuggestedConnectorPair | undefined => {
  const source = useSuggestedSource(sourcePicks);
  const { destinationDefinitionMap } = useDestinationDefinitionList();
  const destination = destinationChoice
    ? destinationDefinitionMap.get(resolveDestinationDefinitionId(destinationChoice))
    : undefined;

  if (!source || !destination) {
    return undefined;
  }
  return { source, destination };
};
