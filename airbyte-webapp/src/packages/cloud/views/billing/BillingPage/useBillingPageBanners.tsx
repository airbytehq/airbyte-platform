import { useCurrentWorkspace } from "core/api";
import { useGetCloudWorkspace } from "core/api/cloud";
import { CloudWorkspaceReadWorkspaceTrialStatus as WorkspaceTrialStatus } from "core/api/types/CloudApi";
import { useExperiment } from "hooks/services/Experiment";

import { LOW_BALANCE_CREDIT_THRESHOLD } from "./components/LowCreditBalanceHint/LowCreditBalanceHint";

export const useBillingPageBanners = () => {
  const currentWorkspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(currentWorkspace.workspaceId);

  const isNewTrialPolicyEnabled = useExperiment("billing.newTrialPolicy", false);
  const isAutoRechargeEnabled = useExperiment("billing.autoRecharge", false);

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
    if (isAutoRechargeEnabled) {
      return "info";
    }

    if (creditStatus === "low") {
      return "warning";
    }

    if (creditStatus === "zero" && !isPreTrial) {
      return "error";
    }

    return "info";
  };

  return {
    bannerVariant: calculateVariant(),
  };
};
