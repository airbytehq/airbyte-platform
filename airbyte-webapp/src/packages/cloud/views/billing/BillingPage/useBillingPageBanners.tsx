import { useGetCloudWorkspace } from "core/api/cloud";
import { CloudWorkspaceReadWorkspaceTrialStatus as WorkspaceTrialStatus } from "core/api/types/CloudApi";
import { useExperiment } from "hooks/services/Experiment";
import { useFreeConnectorProgram } from "packages/cloud/components/experiments/FreeConnectorProgram";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

import { LOW_BALANCE_CREDIT_THRESHOLD } from "./components/LowCreditBalanceHint/LowCreditBalanceHint";

export const useBillingPageBanners = () => {
  const currentWorkspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(currentWorkspace.workspaceId);
  const { programStatusQuery, userDidEnroll } = useFreeConnectorProgram();
  const { hasEligibleConnections, hasNonEligibleConnections, isEnrolled, showEnrollmentUi } =
    programStatusQuery.data || {};
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

  const calculateShowFcpBanner = () => {
    if (!isEnrolled && !userDidEnroll && showEnrollmentUi && (hasEligibleConnections || isPreTrial)) {
      return true;
    }
    return false;
  };

  return {
    bannerVariant: calculateVariant(),
    showFCPBanner: calculateShowFcpBanner(),
  };
};
