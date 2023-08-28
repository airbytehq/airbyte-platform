import { useCurrentWorkspaceState } from "core/api";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useExperiment } from "hooks/services/Experiment";

export const useExperimentSpeedyConnection = () => {
  const { hasConnections } = useCurrentWorkspaceState();
  const isVariantEnabled = useExperiment("onboarding.speedyConnection", false);
  const [expiredOfferDate] = useLocalStorage("exp-speedy-connection-timestamp", "");

  const now = new Date();
  const isExperimentVariant =
    !hasConnections && expiredOfferDate && new Date(expiredOfferDate) >= now && isVariantEnabled;
  return { isExperimentVariant, expiredOfferDate };
};
