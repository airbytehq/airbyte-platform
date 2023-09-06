import { useCurrentWorkspace } from "core/api";
import { useFreeConnectorProgram, useGetCloudWorkspace } from "core/api/cloud";
import { CloudWorkspaceReadWorkspaceTrialStatus as WorkspaceTrialStatus } from "core/api/types/CloudApi";
import { useExperiment } from "hooks/services/Experiment";

import { LOW_BALANCE_CREDIT_THRESHOLD } from "./components/LowCreditBalanceHint/LowCreditBalanceHint";

/**
 * @deprecated this hook can and should be removed after the sunsetting of the free connector program
 *
 * it will cause the app to throw if called after the `platform.sunset-fcp` flag is toggled on sept 18
 *
 * it is currently in use ONLY in RemainingCredits.tsx conditionally (if that ^ flag is false)
 */
export const useDeprecatedBillingPageBanners = () => {
  const currentWorkspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(currentWorkspace.workspaceId);

  const { programStatusQuery } = useFreeConnectorProgram();
  const { hasEligibleConnections, hasNonEligibleConnections, isEnrolled } = programStatusQuery.data || {};
  const isNewTrialPolicyEnabled = useExperiment("billing.newTrialPolicy", false);

  const isPreTrial = isNewTrialPolicyEnabled
    ? cloudWorkspace.workspaceTrialStatus === WorkspaceTrialStatus.pre_trial
    : false;

  const creditStatus =
    (cloudWorkspace.remainingCredits ?? 0) < LOW_BALANCE_CREDIT_THRESHOLD
      ? (cloudWorkspace.remainingCredits ?? 0) <= 0
        ? "zero"
        : "low"
      : "positive";

  const calculateVariant = (): "warning" | "error" | "info" => {
    if (creditStatus === "low" && (hasNonEligibleConnections || !hasEligibleConnections)) {
      return "warning";
    }

    if (
      creditStatus === "zero" &&
      !isPreTrial &&
      (hasNonEligibleConnections || !hasEligibleConnections || (hasEligibleConnections && !isEnrolled))
    ) {
      return "error";
    }

    return "info";
  };

  return {
    bannerVariant: calculateVariant(),
  };
};
