import { useQueryClient } from "@tanstack/react-query";
import {
  AuthErrorCodes,
  User as FirebaseUser,
  applyActionCode,
  confirmPasswordReset,
  getIdToken,
  reload,
  sendEmailVerification,
  updateProfile,
  signInWithEmailAndPassword,
} from "firebase/auth";
import React, { PropsWithChildren, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { LoadingPage } from "components";

import { useGetOrCreateUser } from "core/api";
import { useCreateKeycloakUser, useRevokeUserSession, useUpdateUser } from "core/api/cloud";
import { AuthProvider, UserRead } from "core/api/types/AirbyteClient";
import { AuthContext, AuthContextApi } from "core/services/auth";
import { useNotificationService } from "hooks/services/Notification";
import { useAuth } from "packages/firebaseReact";

import { useKeycloakService } from "./KeycloakService";
import { LoginFormErrorCodes, ResetPasswordConfirmErrorCodes } from "./types";

// Checks for a valid auth session with either keycloak or firebase, and returns the user if found.
export const CloudAuthService: React.FC<PropsWithChildren> = ({ children }) => {
  const passwordRef = useRef<undefined | string>(undefined);
  const [logoutInProgress, setLogoutInProgress] = useState(false);
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();
  const { mutateAsync: updateAirbyteUser } = useUpdateUser();
  const { mutateAsync: getAirbyteUser } = useGetOrCreateUser();
  const { mutateAsync: revokeUserSession } = useRevokeUserSession();
  const { mutateAsync: createKeycloakUser } = useCreateKeycloakUser();
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
        // [Firebase deprecation] We want to ensure the user is dual-written to Keycloak when they sign in with Firebase
        createKeycloakUser({
          authUserId: user.uid,
          getAccessToken: () => user.getIdToken(),
          password: passwordRef.current,
        });
        setFirebaseUser(user);
        setAirbyteUser(airbyteUser);
      } else {
        setFirebaseUser(null);
        setAirbyteUser(null);
      }
      setFirebaseInitialized(true);
    });
  }, [firebaseAuth, getAirbyteUser, createKeycloakUser]);

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
          }).then(() => setAirbyteUser({ ...airbyteUser, name }));
        },
        providers: firebaseUser.providerData.map(({ providerId }) => providerId),
        provider: null,
        sendEmailVerification: async () => {
          if (!firebaseUser) {
            console.error("sendEmailVerifiedLink should be used within auth flow");
            throw new Error("Cannot send verification email if firebaseUser is null.");
          }
          return sendEmailVerification(firebaseUser);
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
        emailVerified: keycloakAuth.keycloakUser?.profile.email_verified ?? false,
        email: keycloakAuth.keycloakUser?.profile.email ?? null,
        getAccessToken: () => Promise.resolve(keycloakAuth.accessTokenRef?.current),
        updateName: async (name: string) => {
          const user = keycloakAuth.airbyteUser;
          if (!user) {
            throw new Error("Cannot change name, airbyteUser is null");
          }
          await updateAirbyteUser({
            userUpdate: { userId: user.userId, name },
            getAccessToken: async () => keycloakAuth.accessTokenRef?.current ?? "",
          }).then(() => {
            keycloakAuth.updateAirbyteUser({ ...user, name });
          });
        },
        logout,
        loggedOut: false,
        providers: null,
        provider: keycloakAuth.isSso
          ? "sso"
          : (keycloakAuth.keycloakUser?.profile.identity_provider as string | undefined) ?? "none",
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
      provider: null,
      login: async ({ email, password }: { email: string; password: string }) => {
        await signInWithEmailAndPassword(firebaseAuth, email, password)
          .then(() => {
            if (firebaseAuth.currentUser) {
              // Update the keycloak user with the new password
              passwordRef.current = password;
            }
          })
          .catch((err) => {
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
      confirmPasswordReset: async (code: string, newPassword: string) => {
        try {
          await confirmPasswordReset(firebaseAuth, code, newPassword);
          if (firebaseAuth.currentUser) {
            // Update the keycloak user with the new password
            createKeycloakUser({
              authUserId: firebaseAuth.currentUser.uid,
              getAccessToken: firebaseAuth.currentUser.getIdToken,
              password: newPassword,
            });
          }
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
      verifyEmail: verifyFirebaseEmail,
    };
  }, [
    airbyteUser,
    createKeycloakUser,
    firebaseAuth,
    firebaseInitialized,
    firebaseUser,
    keycloakAuth,
    logout,
    updateAirbyteUser,
    verifyFirebaseEmail,
  ]);

  if (logoutInProgress) {
    return <LoadingPage />;
  }

  return <AuthContext.Provider value={authContextValue}>{children}</AuthContext.Provider>;
};
