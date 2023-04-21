import { faPlus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { FormattedMessage } from "react-intl";

import { Message } from "components/ui/Message";

import { CreditStatus } from "packages/cloud/lib/domain/cloudWorkspaces/types";
import { useGetCloudWorkspace } from "packages/cloud/services/workspaces/CloudWorkspacesService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

export const LOW_BALANCE_CREDIT_THRESHOLD = 20;

export const LowCreditBalanceHint: React.FC<{ disableCheckout: boolean; onBuy: () => void; isLoading: boolean }> = ({
  disableCheckout,
  onBuy,
  isLoading,
}) => {
  const workspace = useCurrentWorkspace();
  const cloudWorkspace = useGetCloudWorkspace(workspace.workspaceId);

  const isNoBillingAccount =
    cloudWorkspace.remainingCredits <= 0 && cloudWorkspace.creditStatus === CreditStatus.POSITIVE;
  if (isNoBillingAccount || cloudWorkspace.remainingCredits > LOW_BALANCE_CREDIT_THRESHOLD) {
    return null;
  }

  const status = cloudWorkspace.remainingCredits <= 0 ? "zeroBalance" : "lowBalance";
  const variant = status === "zeroBalance" ? "error" : "info";

  return (
    <Message
      type={variant}
      text={<FormattedMessage id={`credits.${status}`} />}
      onAction={onBuy}
      actionBtnText={<FormattedMessage id="credits.buyCredits" />}
      actionBtnProps={{
        isLoading,
        disabled: disableCheckout,
        icon: <FontAwesomeIcon icon={faPlus} />,
      }}
    />
  );
};
