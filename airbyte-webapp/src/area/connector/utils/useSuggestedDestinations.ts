import { useMemo } from "react";

import { useExperiment } from "core/services/Experiment";

export const useSuggestedDestinations = () => {
  const suggestedDestinationConnectors = useExperiment("connector.suggestedDestinationConnectors");
  return useMemo(
    () => (!suggestedDestinationConnectors ? [] : suggestedDestinationConnectors.split(",").map((id) => id.trim())),
    [suggestedDestinationConnectors]
  );
};
