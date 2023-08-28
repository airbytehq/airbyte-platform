import { useMemo } from "react";

import { useExperiment } from "hooks/services/Experiment/ExperimentService";

export const useSuggestedSources = () => {
  const suggestedSourceConnectors = useExperiment("connector.suggestedSourceConnectors", "");
  return useMemo(() => suggestedSourceConnectors.split(",").map((id) => id.trim()), [suggestedSourceConnectors]);
};
