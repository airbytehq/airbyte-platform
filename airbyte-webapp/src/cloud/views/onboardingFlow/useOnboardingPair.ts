import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";

import { getPickedReadySource } from "./draftConnectors";
import { useSourceLists, useStepDestination } from "./useConnectorLists";

export interface OnboardingPair {
  source: SourceRead;
  destination: DestinationRead;
}

/**
 * The source and destination of the flow's connection: the ready source of a pick and the ready destination of the
 * choice, which the source and destination steps open. A ready connector whose definition is no longer picked is not
 * used.
 */
export const useOnboardingPair = (sourcePicks: string[], destinationChoice?: string): OnboardingPair | undefined => {
  const source = getPickedReadySource(useSourceLists().readyConnectors, sourcePicks);
  const destination = useStepDestination(destinationChoice);

  if (!source || !destination || destination.isDraft) {
    return undefined;
  }
  return { source, destination };
};
