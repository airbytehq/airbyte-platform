import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { AlertBanner } from "components/ui/Banner/AlertBanner";
import { Link } from "components/ui/Link";

import { useExperiment } from "hooks/services/Experiment";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { CreditStatus, WorkspaceTrialStatus } from "packages/cloud/lib/domain/cloudWorkspaces/types";
import { useGetCloudWorkspace } from "packages/cloud/services/workspaces/CloudWorkspacesService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

import styles from "./WorkspaceStatusBanner.module.scss";

export const WorkspaceStatusBanner: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(workspace.workspaceId);
  const isNewTrialPolicyEnabled = useExperiment("billing.newTrialPolicy", false);

  const isWorkspacePreTrial = isNewTrialPolicyEnabled
    ? cloudWorkspace.workspaceTrialStatus === WorkspaceTrialStatus.PRE_TRIAL
    : false;

  const isWorkspaceInTrial = isNewTrialPolicyEnabled
    ? cloudWorkspace.workspaceTrialStatus === WorkspaceTrialStatus.IN_TRIAL
    : !!cloudWorkspace.trialExpiryTimestamp;

  const negativeCreditStatus = useMemo(() => {
    // these remain the same regardless of the new trial policy
    return (
      cloudWorkspace.creditStatus &&
      [
        CreditStatus.NEGATIVE_BEYOND_GRACE_PERIOD,
        CreditStatus.NEGATIVE_MAX_THRESHOLD,
        CreditStatus.NEGATIVE_WITHIN_GRACE_PERIOD,
      ].includes(cloudWorkspace.creditStatus)
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

    if (isWorkspacePreTrial) {
      return <FormattedMessage id="trial.preTrialAlertMessage" />;
    }

    if (isWorkspaceInTrial) {
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
  }, [cloudWorkspace, isWorkspaceInTrial, isWorkspacePreTrial, negativeCreditStatus]);

  return (
    <>
      {!!workspaceCreditsBannerContent && (
        <div className={styles.banner}>
          <AlertBanner message={workspaceCreditsBannerContent} />
        </div>
      )}
    </>
  );
};
