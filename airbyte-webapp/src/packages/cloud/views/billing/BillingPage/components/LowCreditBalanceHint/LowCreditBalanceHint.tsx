import { FormattedMessage } from "react-intl";

import { Message } from "components/ui/Message";

import { useIsFCPEnabled } from "core/api/cloud";

interface LowCreditBalanceHintProps {
  variant: "info" | "warning" | "error";
}

export const LOW_BALANCE_CREDIT_THRESHOLD = 20;

export const LowCreditBalanceHint: React.FC<React.PropsWithChildren<LowCreditBalanceHintProps>> = ({ variant }) => {
  const isFCPEnabled = useIsFCPEnabled();
  if (variant === "info") {
    return null;
  }

  const zeroBalanceId = isFCPEnabled ? "credits.zeroBalance.fcp" : "credits.zeroBalance";
  const lowBalanceId = isFCPEnabled ? "credits.lowBalance.fcp" : "credits.lowBalance";

  return <Message text={<FormattedMessage id={variant === "error" ? zeroBalanceId : lowBalanceId} />} type={variant} />;
};
