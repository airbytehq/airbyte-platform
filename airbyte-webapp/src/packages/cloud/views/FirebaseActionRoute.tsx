import React, { useCallback, useEffect, useRef } from "react";
import { Navigate, useNavigate } from "react-router-dom";
import { match, P } from "ts-pattern";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { AuthVerifyEmail, useAuthService } from "core/services/auth";
import { useQuery } from "hooks/useQuery";

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

interface VerifyEmailActionProps {
  verifyEmail: AuthVerifyEmail;
}

const VerifyEmailAction: React.FC<VerifyEmailActionProps> = ({ verifyEmail }) => {
  const verifying = useRef(false);
  const query = useQuery<{ oobCode?: string }>();
  const navigate = useNavigate();
  useTrackPage(PageTrackingCodes.VERIFY_EMAIL);
  const verify = useCallback(async () => {
    if (verifying.current) {
      return;
    }
    verifying.current = true;

    if (!query.oobCode) {
      navigate("/");
    } else {
      // Send verification code to authentication service
      await verifyEmail(query.oobCode);
      // Navigate the user to the homepage after the email is verified
      navigate("/");
    }
  }, [query.oobCode, navigate, verifyEmail]);

  useEffect(() => {
    verify();
  }, [verify]);

  return <LoadingPage />;
};

export const FirebaseActionRoute: React.FC = () => {
  const { mode } = useQuery<{ mode: FirebaseActionMode }>();
  const { user, verifyEmail, signUpWithEmailLink, confirmPasswordReset } = useAuthService();

  return (
    <AuthLayout>
      {match([mode, user, verifyEmail, signUpWithEmailLink, confirmPasswordReset])
        // The verify email route is the only that should work whether the user is logged in or not
        .with([FirebaseActionMode.VERIFY_EMAIL, P.any, P.not(P.nullish), P.any, P.any], ([, , verifyEmail]) => (
          <VerifyEmailAction verifyEmail={verifyEmail} />
        ))
        // All other routes will require the user to be logged out
        .with(
          [FirebaseActionMode.SIGN_IN, P.nullish, P.any, P.not(P.nullish), P.any],
          ([, , , signUpWithEmailLink]) => <AcceptEmailInvite signUpWithEmailLink={signUpWithEmailLink} />
        )
        .with(
          [FirebaseActionMode.RESET_PASSWORD, P.nullish, P.any, P.any, P.not(P.nullish)],
          ([, , , , confirmPasswordReset]) => <ResetPasswordConfirmPage confirmPasswordReset={confirmPasswordReset} />
        )
        .otherwise(() => (
          <Navigate to="/" replace />
        ))}
      <LoginSignupNavigation type="login" />
    </AuthLayout>
  );
};
