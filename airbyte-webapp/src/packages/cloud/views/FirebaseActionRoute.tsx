import React from "react";
import { useIntl } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";
import { useAsync } from "react-use";
import { match, P } from "ts-pattern";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useNotificationService } from "hooks/services/Notification";
import { useQuery } from "hooks/useQuery";
import { useAuthService } from "packages/cloud/services/auth/AuthService";

import AuthLayout from "./auth";
import { LoginSignupNavigation } from "./auth/components/LoginSignupNavigation";

const AcceptEmailInvite = React.lazy(() => import("./AcceptEmailInvite"));
const ResetPasswordConfirmPage = React.lazy(() => import("./auth/ConfirmPasswordResetPage"));
const LoadingPage = React.lazy(() => import("components/LoadingPage"));

enum FirebaseActionMode {
  VERIFY_EMAIL = "verifyEmail",
  RESET_PASSWORD = "resetPassword",
  SIGN_IN = "signIn",
}

const VerifyEmailAction: React.FC = () => {
  const query = useQuery<{ oobCode?: string }>();
  const { verifyEmail } = useAuthService();
  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  useTrackPage(PageTrackingCodes.VERIFY_EMAIL);
  useAsync(async () => {
    if (!query.oobCode) {
      navigate("/");
      return;
    }
    // Send verification code to authentication service
    await verifyEmail(query.oobCode);
    // Show a notification that the mail got verified successfully
    registerNotification({
      id: "auth/email-verified",
      text: formatMessage({ id: "verifyEmail.notification" }),
      type: "success",
    });
    // Navigate the user to the homepage after the email is verified
    navigate("/");
  }, []);

  return <LoadingPage />;
};

export const FirebaseActionRoute: React.FC = () => {
  const { mode } = useQuery<{ mode: FirebaseActionMode }>();
  const { user } = useAuthService();

  return (
    <AuthLayout>
      {match([mode, user])
        // The verify email route is the only that should work whether the user is logged in or not
        .with([FirebaseActionMode.VERIFY_EMAIL, P.any], () => <VerifyEmailAction />)
        // All other routes will require the user to be logged out
        .with([FirebaseActionMode.SIGN_IN, P.nullish], () => <AcceptEmailInvite />)
        .with([FirebaseActionMode.RESET_PASSWORD, P.nullish], () => <ResetPasswordConfirmPage />)
        .otherwise(() => (
          <Navigate to="/" replace />
        ))}
      <LoginSignupNavigation to="login" />
    </AuthLayout>
  );
};
