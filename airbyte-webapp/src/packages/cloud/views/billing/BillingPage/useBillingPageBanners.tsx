import { useExperiment } from "hooks/services/Experiment";
import { useFreeConnectorProgram } from "packages/cloud/components/experiments/FreeConnectorProgram";
import { WorkspaceTrialStatus } from "packages/cloud/lib/domain/cloudWorkspaces/types";
import { useGetCloudWorkspace } from "packages/cloud/services/workspaces/CloudWorkspacesService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

import { LOW_BALANCE_CREDIT_THRESHOLD } from "./components/LowCreditBalanceHint/LowCreditBalanceHint";

export const useBillingPageBanners = () => {
  const currentWorkspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(currentWorkspace.workspaceId);
  const { connectionStatusQuery, userDidEnroll, enrollmentStatusQuery } = useFreeConnectorProgram();
  const { hasEligibleConnections, hasNonEligibleConnections } = connectionStatusQuery.data || {};
  const { isEnrolled, showEnrollmentUi } = enrollmentStatusQuery.data || {};
  const isNewTrialPolicyEnabled = useExperiment("billing.newTrialPolicy", false);

  const isPreTrial = isNewTrialPolicyEnabled
    ? cloudWorkspace.workspaceTrialStatus === WorkspaceTrialStatus.PRE_TRIAL
    : false;

  const creditStatus =
    cloudWorkspace.remainingCredits < LOW_BALANCE_CREDIT_THRESHOLD
      ? cloudWorkspace.remainingCredits <= 0
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
