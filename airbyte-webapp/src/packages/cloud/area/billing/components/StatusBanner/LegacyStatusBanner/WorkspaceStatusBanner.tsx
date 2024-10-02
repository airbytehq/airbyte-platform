import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { AlertBanner } from "components/ui/Banner/AlertBanner";
import { Link } from "components/ui/Link";

import {
  CloudWorkspaceRead,
  CloudWorkspaceReadCreditStatus as CreditStatus,
  CloudWorkspaceReadWorkspaceTrialStatus as WorkspaceTrialStatus,
} from "core/api/types/CloudApi";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";

interface WorkspaceStatusBannerProps {
  cloudWorkspace: CloudWorkspaceRead;
}
export const WorkspaceStatusBanner: React.FC<WorkspaceStatusBannerProps> = ({ cloudWorkspace }) => {
  const negativeCreditStatus = useMemo(() => {
    // these remain the same regardless of the new trial policy
    return (
      cloudWorkspace.creditStatus &&
      (cloudWorkspace.creditStatus === CreditStatus.negative_beyond_grace_period ||
        cloudWorkspace.creditStatus === CreditStatus.negative_max_threshold ||
        cloudWorkspace.creditStatus === CreditStatus.negative_within_grace_period)
    );
  }, [cloudWorkspace.creditStatus]);

  const workspaceCreditsBannerContent = useMemo(() => {
    if (negativeCreditStatus) {
      return (
        <FormattedMessage
          id="credits.creditsProblem"
          values={{
            lnk: (content: React.ReactNode) => <Link to={CloudRoutes.Billing}>{content}</Link>,
          }}
        />
      );
    }

    if (cloudWorkspace.workspaceTrialStatus === WorkspaceTrialStatus.pre_trial) {
      return <FormattedMessage id="trial.preTrialAlertMessage" />;
    }

    if (cloudWorkspace.workspaceTrialStatus === WorkspaceTrialStatus.in_trial) {
      const { trialExpiryTimestamp } = cloudWorkspace;

      // calculate difference between timestamp (in epoch milliseconds) and now (in epoch milliseconds)
      const trialRemainingMilliseconds = trialExpiryTimestamp ? trialExpiryTimestamp - Date.now() : 0;
      if (trialRemainingMilliseconds < 0) {
        return null;
      }
      // calculate days (rounding up if decimal)
      const trialRemainingDays = Math.ceil(trialRemainingMilliseconds / (1000 * 60 * 60 * 24));

      return (
        <FormattedMessage
          id="trial.alertMessage"
          values={{
            remainingDays: trialRemainingDays,
            lnk: (content: React.ReactNode) => <Link to={CloudRoutes.Billing}>{content}</Link>,
          }}
        />
      );
    }

    return null;
  }, [cloudWorkspace, negativeCreditStatus]);

  return (
    <>
      {!!workspaceCreditsBannerContent && (
        <AlertBanner
          message={workspaceCreditsBannerContent}
          data-testid="workspace-status-banner"
          color={negativeCreditStatus ? "warning" : "info"}
        />
      )}
    </>
  );
};
