import { FormattedMessage, useIntl } from "react-intl";

import { Message } from "components/ui/Message";

import { AuthSendEmailVerification } from "core/services/auth";
import { useNotificationService } from "hooks/services/Notification";

interface EmailVerificationHintProps {
  variant: "info" | "warning" | "error";
  sendEmailVerification: AuthSendEmailVerification;
}
export const EmailVerificationHint: React.FC<EmailVerificationHintProps> = ({ sendEmailVerification, variant }) => {
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  const onResendVerificationMail = async () => {
    // the shared error handling inside `sendEmailVerification` suffices
    try {
      await sendEmailVerification();
      registerNotification({
        id: "workspace.emailVerificationResendSuccess",
        text: formatMessage({ id: "credits.emailVerification.resendConfirmation" }),
        type: "success",
      });
    } catch (e) {}
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
