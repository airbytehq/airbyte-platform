import { useMutation } from "@tanstack/react-query";
import { useMemo } from "react";
import { useIntl } from "react-intl";

import { useCurrentUser } from "core/services/auth";
import { useNotificationService } from "hooks/services/Notification";

import { sendVerificationEmail } from "../../generated/CloudApi";
import { useRequestOptions } from "../../useRequestOptions";

const RESEND_EMAIL_TOAST_ID = "resendEmail";
export const useResendEmailVerification = () => {
  const { userId } = useCurrentUser();
  const requestOptions = useRequestOptions();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  const sendEmail = useMemo(() => {
    // The new way to send a verification email with Keycloak via our API
    return () => sendVerificationEmail({ userId }, requestOptions);
  }, [userId, requestOptions]);

  return useMutation(sendEmail, {
    onSuccess: () => {
      registerNotification({
        id: RESEND_EMAIL_TOAST_ID,
        type: "success",
        text: formatMessage({ id: "credits.emailVerification.resendConfirmation" }),
      });
    },
    onError: () => {
      registerNotification({
        id: RESEND_EMAIL_TOAST_ID,
        type: "error",
        text: formatMessage({ id: "credits.emailVerification.resendConfirmationError" }),
      });
    },
  });
};
