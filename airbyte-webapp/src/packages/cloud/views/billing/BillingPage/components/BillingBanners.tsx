import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { ExternalLink, Link } from "components/ui/Link";
import { Message } from "components/ui/Message";

import { useCurrentWorkspace } from "core/api";
import { useGetCloudWorkspace, useResendEmailVerification } from "core/api/cloud";
import { CloudWorkspaceReadCreditStatus, CloudWorkspaceReadWorkspaceTrialStatus } from "core/api/types/CloudApi";
import { useAuthService } from "core/services/auth";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment";

const LOW_BALANCE_CREDIT_THRESHOLD = 20;

export const EmailVerificationHint: React.FC = () => {
  const { mutateAsync: resendEmailVerification, isLoading } = useResendEmailVerification();

  return (
    <Message
      type="info"
      text={<FormattedMessage id="credits.emailVerificationRequired" />}
      actionBtnText={<FormattedMessage id="credits.emailVerification.resend" />}
      actionBtnProps={{ isLoading }}
      onAction={resendEmailVerification}
    />
  );
};

const AutoRechargeEnabledBanner: React.FC = () => (
  <Message
    data-testid="autoRechargeEnabledBanner"
    text={
      <FormattedMessage
        id="credits.autoRechargeEnabled"
        values={{
          contact: (node: React.ReactNode) => (
            <Link opensInNewTab to="mailto:billing@airbyte.io" variant="primary">
              {node}
            </Link>
          ),
        }}
      />
    }
  />
);

const LowCreditBalanceHint: React.FC = () => {
  const { formatMessage } = useIntl();
  const currentWorkspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(currentWorkspace.workspaceId);
  const credits = cloudWorkspace.remainingCredits ?? 0;

  if (credits < 0 && cloudWorkspace.creditStatus === CloudWorkspaceReadCreditStatus.positive) {
    // Having a positive credit status while credits amount are negative (and no longer in pre_trial)
    // means this workspace is an internal workspace that isn't billed
    return <Message data-testid="noBillingAccount" text={formatMessage({ id: "credits.noBillingAccount" })} />;
  }

  if (cloudWorkspace.workspaceTrialStatus === CloudWorkspaceReadWorkspaceTrialStatus.pre_trial) {
    // If we're pre trial we don't have any credits yet
    return null;
  }

  const messageParams = {
    lnk: (node: React.ReactNode) => (
      <ExternalLink href={links.autoRechargeEnrollment} variant="primary">
        {node}
      </ExternalLink>
    ),
  };

  if (credits <= 0) {
    return (
      <Message
        data-testid="noCreditsBanner"
        text={formatMessage({ id: "credits.zeroBalance" }, messageParams)}
        type="warning"
      />
    );
  }

  if (credits < LOW_BALANCE_CREDIT_THRESHOLD) {
    return (
      <Message
        data-testid="lowCreditsBanner"
        text={formatMessage({ id: "credits.lowBalance" }, messageParams)}
        type="warning"
      />
    );
  }

  return null;
};

export const BillingBanners: React.FC = () => {
  const { emailVerified } = useAuthService();

  const isAutoRechargeEnabled = useExperiment("billing.autoRecharge");

  return (
    <FlexContainer direction="column">
      {!emailVerified && <EmailVerificationHint />}
      {isAutoRechargeEnabled ? <AutoRechargeEnabledBanner /> : <LowCreditBalanceHint />}
    </FlexContainer>
  );
};
