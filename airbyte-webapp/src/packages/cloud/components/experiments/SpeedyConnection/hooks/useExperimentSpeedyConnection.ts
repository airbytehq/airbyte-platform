import { useCurrentWorkspaceState } from "core/api";
import { useExperiment } from "hooks/services/Experiment";

export const useExperimentSpeedyConnection = () => {
  const { hasConnections } = useCurrentWorkspaceState();
  const isVariantEnabled = useExperiment("onboarding.speedyConnection", false);

  const timestamp = localStorage.getItem("exp-speedy-connection-timestamp");
  const expiredOfferDate = timestamp ? String(timestamp) : String(0);

  const now = new Date();
  const isExperimentVariant =
    !hasConnections && expiredOfferDate && new Date(expiredOfferDate) >= now && isVariantEnabled;
  return { isExperimentVariant, expiredOfferDate };
};
