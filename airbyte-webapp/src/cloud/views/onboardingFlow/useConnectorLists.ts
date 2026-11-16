import { useFirstDestinationPage, useFirstSourcePage } from "core/api";
import { DestinationRead } from "core/api/types/AirbyteClient";

import { getStepDestination, splitByDraftState } from "./draftConnectors";
import { resolveDestinationDefinitionId } from "./onboardingConnectors";

/** The draft and ready sources of the workspace, from the first page of its sources. */
export const useSourceLists = () => splitByDraftState(useFirstSourcePage());

/** The draft and ready destinations of the workspace, from the first page of its destinations. */
export const useDestinationLists = () => splitByDraftState(useFirstDestinationPage());

/** The ready destination of the choice first, else the one draft destination that the destination screen keeps. */
export const useStepDestination = (destinationChoice?: string) => {
  const { drafts, readyConnectors: readyDestinations } = useDestinationLists();
  const chosenDefinitionId = destinationChoice ? resolveDestinationDefinitionId(destinationChoice) : undefined;
  const isChosen = ({ destinationDefinitionId }: DestinationRead) => destinationDefinitionId === chosenDefinitionId;

  return getStepDestination(readyDestinations, drafts.at(0), isChosen);
};
