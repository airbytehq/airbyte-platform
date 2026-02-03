import { useMemo } from "react";

import { useExperiment } from "core/services/Experiment/ExperimentService";

export const useSuggestedSources = () => {
  const suggestedSourceConnectors = useExperiment("connector.suggestedSourceConnectors");
  return useMemo(
    () =>
      suggestedSourceConnectors
        .split(",")
        .filter(Boolean)
        .map((id) => id.trim()),
    [suggestedSourceConnectors]
  );
};
