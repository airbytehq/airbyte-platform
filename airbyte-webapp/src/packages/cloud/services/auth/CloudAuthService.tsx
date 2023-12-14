import { useQueryClient } from "@tanstack/react-query";
import {
  AuthErrorCodes,
  EmailAuthProvider,
  User as FirebaseUser,
  GithubAuthProvider,
  GoogleAuthProvider,
  applyActionCode,
  confirmPasswordReset,
  createUserWithEmailAndPassword,
  getIdToken,
  reauthenticateWithCredential,
  reload,
  sendEmailVerification,
  signInWithEmailLink,
  signInWithPopup,
  updatePassword,
  updateProfile,
  signInWithEmailAndPassword,
  sendPasswordResetEmail,
} from "firebase/auth";
import React, { PropsWithChildren, useCallback, useEffect, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";
import { Observable, Subject } from "rxjs";

import { LoadingPage } from "components";

import { useGetOrCreateUser } from "core/api";
import { useResendSigninLink, useRevokeUserSession, useUpdateUser } from "core/api/cloud";
import { AuthProvider, UserRead } from "core/api/types/AirbyteClient";
import { AuthContext, AuthContextApi, OAuthLoginState } from "core/services/auth";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useNotificationService } from "hooks/services/Notification";
import { SignupFormValues } from "packages/cloud/views/auth/SignupPage/components/SignupForm";
import { useAuth } from "packages/firebaseReact";

import { useKeycloakService } from "./KeycloakService";
import {
  EmailLinkErrorCodes,
  LoginFormErrorCodes,
  ResetPasswordConfirmErrorCodes,
  SignUpFormErrorCodes,
} from "./types";

export enum FirebaseAuthMessageId {
  NetworkFailure = "firebase.auth.error.networkRequestFailed",
  TooManyRequests = "firebase.auth.error.tooManyRequests",
  InvalidPassword = "firebase.auth.error.invalidPassword",
  DefaultError = "firebase.auth.error.default",
}

// Checks for a valid auth session with either keycloak or firebase, and returns the user if found.
export const CloudAuthService: React.FC<PropsWithChildren> = ({ children }) => {
  const [, setSpeedyConnectionTimestamp] = useLocalStorage("exp-speedy-connection-timestamp", "");
  const [logoutInProgress, setLogoutInProgress] = useState(false);
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();
  const { mutateAsync: updateAirbyteUser } = useUpdateUser();
  const { mutateAsync: getAirbyteUser } = useGetOrCreateUser();
  const { mutateAsync: resendWithSignInLink } = useResendSigninLink();
  const { mutateAsync: revokeUserSession } = useRevokeUserSession();
  const keycloakAuth = useKeycloakService();
  const firebaseAuth = useAuth();
  const navigate = useNavigate();

  // These values are set during the firebase initialization process
  const [firebaseInitialized, setFirebaseInitialized] = useState(false);
  const [airbyteUser, setAirbyteUser] = useState<UserRead | null>(null);
  const [firebaseUser, setFirebaseUser] = useState<FirebaseUser | null>(null);

  // This is only necessary to force a re-render after firebase verifies an email, because the underlying firebaseUser does not change
  const [emailDidVerify, setEmailDidVerify] = useState(false);

  const verifyFirebaseEmail = useCallback(
    async (code: string) => {
      await applyActionCode(firebaseAuth, code)
        .then(async () => {
          // Reload the user to get a fresh token with email_verified: true
          if (firebaseUser && !emailDidVerify) {
            await reload(firebaseUser);
            await getIdToken(firebaseUser, true);
            setEmailDidVerify(true);
          }
          registerNotification({
            id: "workspace.emailVerificationSuccess",
            text: <FormattedMessage id="verifyEmail.success" />,
            type: "success",
          });
        })
        .catch(() => {
          registerNotification({
            id: "workspace.emailVerificationError",
            text: <FormattedMessage id="verifyEmail.error" />,
            type: "error",
          });
        });
    },
    [firebaseAuth, registerNotification, firebaseUser, emailDidVerify]
  );

  const logout = useCallback(async () => {
    setLogoutInProgress(true);
    if (firebaseUser) {
      await revokeUserSession({ getAccessToken: () => firebaseUser.getIdToken() });
      await firebaseAuth.signOut();
      setFirebaseUser(null);
      setAirbyteUser(null);
    }
    if (keycloakAuth.isAuthenticated) {
      await keycloakAuth.signout();
    }
    setLogoutInProgress(false);
    navigate("/");
    queryClient.removeQueries();
  }, [firebaseAuth, firebaseUser, keycloakAuth, navigate, queryClient, revokeUserSession]);

  // useFirebaseAuth() does not give us the user synchronously, we instead have to subscribe to onAuthStateChanged and store the state separately
  useEffect(() => {
    return firebaseAuth.onAuthStateChanged(async (user) => {
      if (user) {
        const airbyteUser = await getAirbyteUser({
          authUserId: user.uid,
          getAccessToken: () => user.getIdToken(),
        });
        setFirebaseUser(user);
        setAirbyteUser(airbyteUser);
      } else {
        setFirebaseUser(null);
        setAirbyteUser(null);
      }
      setFirebaseInitialized(true);
    });
  }, [firebaseAuth, getAirbyteUser]);

  const authContextValue = useMemo<AuthContextApi>(() => {
    // The context value for an authenticated firebase user
    if (firebaseUser) {
      return {
        isAuthenticated: true,
        inited: true,
        user: airbyteUser,
        authProvider: AuthProvider.google_identity_platform,
        displayName: firebaseUser.displayName,
        emailVerified: firebaseUser.emailVerified,
        email: firebaseUser.email,
        getAccessToken: () => firebaseUser.getIdToken(),
        logout,
        loggedOut: false,
        updateName: async (name: string) => {
          if (!airbyteUser) {
            throw new Error("Cannot change name, airbyteUser is null");
          }
          await updateProfile(firebaseUser, { displayName: name });
          await updateAirbyteUser({
            userUpdate: { userId: airbyteUser.userId, name },
            getAccessToken: () => firebaseUser.getIdToken(),
          });
        },
        updatePassword: async (email: string, currentPassword: string, newPassword: string) => {
          // re-authentication may be needed before updating password
          // https://firebase.google.com/docs/auth/web/manage-users#re-authenticate_a_user
          if (firebaseUser === null) {
            throw new Error("You must log in first to reauthenticate!");
          }
          const credential = EmailAuthProvider.credential(email, currentPassword);
          await reauthenticateWithCredential(firebaseUser, credential);
          return updatePassword(firebaseUser, newPassword);
        },
        hasPasswordLogin: () => !!firebaseUser.providerData.filter(({ providerId }) => providerId === "password"),
        providers: firebaseUser.providerData.map(({ providerId }) => providerId),
        sendEmailVerification: async () => {
          if (!firebaseUser) {
            console.error("sendEmailVerifiedLink should be used within auth flow");
            throw new Error("Cannot send verification email if firebaseUser is null.");
          }
          return sendEmailVerification(firebaseUser)
            .then(() => {
              registerNotification({
                id: "workspace.emailVerificationResendSuccess",
                text: <FormattedMessage id="credits.emailVerification.resendConfirmation" />,
                type: "success",
              });
            })
            .catch((error) => {
              switch (error.code) {
                case AuthErrorCodes.NETWORK_REQUEST_FAILED:
                  registerNotification({
                    id: error.code,
                    text: <FormattedMessage id={FirebaseAuthMessageId.NetworkFailure} />,
                    type: "error",
                  });
                  break;
                case AuthErrorCodes.TOO_MANY_ATTEMPTS_TRY_LATER:
                  registerNotification({
                    id: error.code,
                    text: <FormattedMessage id={FirebaseAuthMessageId.TooManyRequests} />,
                    type: "warning",
                  });
                  break;
                default:
                  registerNotification({
                    id: error.code,
                    text: <FormattedMessage id={FirebaseAuthMessageId.DefaultError} />,
                    type: "error",
                  });
              }
            });
        },
        verifyEmail: verifyFirebaseEmail,
      };
    }
    // The context value for an authenticated Keycloak user. Firebase takes precedence, so we have to wait until firebase is initialized.
    if (firebaseInitialized && keycloakAuth.isAuthenticated) {
      return {
        isAuthenticated: true,
        inited: true,
        user: keycloakAuth.airbyteUser,
        authProvider: AuthProvider.keycloak,
        displayName: keycloakAuth.keycloakUser?.profile.name ?? null,
        emailVerified: true,
        email: keycloakAuth.keycloakUser?.profile.email ?? null,
        getAccessToken: () => Promise.resolve(keycloakAuth.accessTokenRef?.current),
        logout,
        loggedOut: false,
        providers: null,
      };
    }
    // The context value for an unauthenticated user
    return {
      isAuthenticated: false,
      user: null,
      inited: keycloakAuth.didInitialize && firebaseInitialized,
      userId: null,
      emailVerified: false,
      loggedOut: true,
      providers: null,
      login: async ({ email, password }: { email: string; password: string }) => {
        await signInWithEmailAndPassword(firebaseAuth, email, password).catch((err) => {
          switch (err.code) {
            case AuthErrorCodes.INVALID_EMAIL:
              throw new Error(LoginFormErrorCodes.EMAIL_INVALID);
            case AuthErrorCodes.USER_CANCELLED:
            case AuthErrorCodes.USER_DISABLED:
              throw new Error(LoginFormErrorCodes.EMAIL_DISABLED);
            case AuthErrorCodes.USER_DELETED:
              throw new Error(LoginFormErrorCodes.EMAIL_NOT_FOUND);
            case AuthErrorCodes.INVALID_PASSWORD:
              throw new Error(LoginFormErrorCodes.PASSWORD_INVALID);
          }

          throw err;
        });
      },
      loginWithOAuth(provider): Observable<OAuthLoginState> {
        const state = new Subject<OAuthLoginState>();
        try {
          state.next("waiting");
          // Instantiate the appropriate auth provider. For Google we're specifying the `hd` parameter, to only show
          // Google accounts in the selector that are linked to a business (GSuite) account.
          const authProvider =
            provider === "github"
              ? new GithubAuthProvider()
              : new GoogleAuthProvider().setCustomParameters({ hd: "*" });
          signInWithPopup(firebaseAuth, authProvider)
            .then(async () => {
              state.next("loading");
              if (firebaseAuth.currentUser) {
                state.next("done");
                state.complete();
              }
            })
            .catch((e) => state.error(e));
        } catch (e) {
          state.error(e);
        }
        return state.asObservable();
      },
      requirePasswordReset: (email: string) => {
        return sendPasswordResetEmail(firebaseAuth, email);
      },
      confirmPasswordReset: async (code: string, newPassword: string) => {
        try {
          return await confirmPasswordReset(firebaseAuth, code, newPassword);
        } catch (e) {
          switch (e?.code) {
            case AuthErrorCodes.EXPIRED_OOB_CODE:
              throw new Error(ResetPasswordConfirmErrorCodes.LINK_EXPIRED);
            case AuthErrorCodes.INVALID_OOB_CODE:
              throw new Error(ResetPasswordConfirmErrorCodes.LINK_INVALID);
            case AuthErrorCodes.WEAK_PASSWORD:
              throw new Error(ResetPasswordConfirmErrorCodes.PASSWORD_WEAK);
          }

          throw e;
        }
      },
      signUp: async ({ email, password }: SignupFormValues) => {
        try {
          const { user } = await createUserWithEmailAndPassword(firebaseAuth, email, password);

          // Send verification mail via firebase
          await sendEmailVerification(user);

          // exp-speedy-connection
          if (firebaseAuth.currentUser) {
            setSpeedyConnectionTimestamp(String(new Date(new Date().getTime() + 24 * 60 * 60 * 1000)));
          }
        } catch (err) {
          switch (err.code) {
            case AuthErrorCodes.EMAIL_EXISTS:
              throw new Error(SignUpFormErrorCodes.EMAIL_DUPLICATE);
            case AuthErrorCodes.INVALID_EMAIL:
              throw new Error(SignUpFormErrorCodes.EMAIL_INVALID);
            case AuthErrorCodes.WEAK_PASSWORD:
              throw new Error(SignUpFormErrorCodes.PASSWORD_WEAK);
          }

          throw err;
        }
      },
      signUpWithEmailLink: async ({ name, email, password, news }) => {
        let firebaseUser: FirebaseUser;

        try {
          ({ user: firebaseUser } = await signInWithEmailLink(firebaseAuth, email));
          await updatePassword(firebaseUser, password);
        } catch (e) {
          await firebaseAuth.signOut();
          if (e.message === EmailLinkErrorCodes.LINK_EXPIRED) {
            await resendWithSignInLink(email);
          }
          throw e;
        }

        if (firebaseUser) {
          const airbyteUser = await getAirbyteUser({
            authUserId: firebaseUser.uid,
            getAccessToken: () => firebaseUser.getIdToken(),
          });
          await updateAirbyteUser({
            userUpdate: { userId: airbyteUser.userId, authUserId: firebaseUser.uid, name, news },
            getAccessToken: () => firebaseUser.getIdToken(),
          });
        }
      },
      verifyEmail: verifyFirebaseEmail,
    };
  }, [
    airbyteUser,
    firebaseAuth,
    firebaseInitialized,
    firebaseUser,
    getAirbyteUser,
    keycloakAuth,
    logout,
    registerNotification,
    resendWithSignInLink,
    setSpeedyConnectionTimestamp,
    updateAirbyteUser,
    verifyFirebaseEmail,
  ]);

  if (logoutInProgress) {
    return <LoadingPage />;
  }

  return <AuthContext.Provider value={authContextValue}>{children}</AuthContext.Provider>;
};
