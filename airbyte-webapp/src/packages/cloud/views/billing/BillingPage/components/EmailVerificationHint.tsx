import { FormattedMessage } from "react-intl";

import { Message } from "components/ui/Message";

import { AuthSendEmailVerification } from "core/services/auth";

interface EmailVerificationHintProps {
  variant: "info" | "warning" | "error";
  sendEmailVerification: AuthSendEmailVerification;
}
export const EmailVerificationHint: React.FC<EmailVerificationHintProps> = ({ sendEmailVerification, variant }) => {
  const onResendVerificationMail = async () => {
    return sendEmailVerification();
  };

  return (
    <Message
      type={variant}
      text={<FormattedMessage id="credits.emailVerificationRequired" />}
      actionBtnText={<FormattedMessage id="credits.emailVerification.resend" />}
      onAction={onResendVerificationMail}
    />
  );
};
