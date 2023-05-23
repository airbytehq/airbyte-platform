import { FormattedMessage } from "react-intl";

import { Message } from "components/ui/Message";

interface LowCreditBalanceHintProps {
  variant: "info" | "warning" | "error";
}

export const LOW_BALANCE_CREDIT_THRESHOLD = 20;

export const LowCreditBalanceHint: React.FC<React.PropsWithChildren<LowCreditBalanceHintProps>> = ({ variant }) => {
  if (variant === "info") {
    return null;
  }

  return (
    <Message
      text={<FormattedMessage id={variant === "error" ? "credits.zeroBalance" : "credits.lowBalance"} />}
      type={variant}
    />
  );
};
