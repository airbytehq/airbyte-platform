import { useMemo } from "react";

import { useExperiment } from "hooks/services/Experiment/ExperimentService";

export const useSuggestedDestinations = () => {
  const suggestedDestinationConnectors = useExperiment("connector.suggestedDestinationConnectors", "");
  return useMemo(
    () => suggestedDestinationConnectors.split(",").map((id) => id.trim()),
    [suggestedDestinationConnectors]
  );
};
