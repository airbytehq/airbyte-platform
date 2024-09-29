import { useMemo } from "react";

import { useExperiment } from "hooks/services/Experiment/ExperimentService";

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
