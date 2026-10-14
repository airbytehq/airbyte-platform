import { useExperiment } from "core/services/Experiment";
import { useIsCloudApp } from "core/utils/app";

import { useContextLayerEligibility } from "./useContextLayerEligibility";

export type ConnectionSemanticSearchAvailability = "unavailable" | "loading" | "available";

/**
 * Decides if connection-level semantic search is available: Cloud, the semantic search UI flag is on, and the Context
 * Layer is available. Outside Cloud or with the flag off, the result is "unavailable" at once, never "loading".
 *
 * The organization read and the Context Layer provisioning read are the same reads that the sidebar and the
 * create-source page already make.
 */
export const useConnectionSemanticSearchAvailability = (): ConnectionSemanticSearchAvailability => {
  const isCloudApp = useIsCloudApp();
  const showSemanticSearch = useExperiment("platform.fusion-semantic-search-ui");
  const { statusQuery, isEligible: isContextLayerAvailable } = useContextLayerEligibility();

  if (!isCloudApp || !showSemanticSearch) {
    return "unavailable";
  }
  if (statusQuery.isInitialLoading) {
    return "loading";
  }
  return isContextLayerAvailable ? "available" : "unavailable";
};
