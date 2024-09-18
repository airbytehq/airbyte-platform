import { useMemo } from "react";

import { useExperiment } from "hooks/services/Experiment";

export const useSuggestedDestinations = () => {
  const suggestedDestinationConnectors = useExperiment("connector.suggestedDestinationConnectors");
  return useMemo(
    () => (!suggestedDestinationConnectors ? [] : suggestedDestinationConnectors.split(",").map((id) => id.trim())),
    [suggestedDestinationConnectors]
  );
};
