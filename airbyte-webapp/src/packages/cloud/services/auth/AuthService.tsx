import { useQueryClient } from "@tanstack/react-query";
import { User as FirebaseUser, AuthErrorCodes } from "firebase/auth";
import React, { useCallback, useMemo, useRef } from "react";
import { useIntl } from "react-intl";
import { useEffectOnce } from "react-use";
import { Observable, Subject } from "rxjs";

import { useGetUserService, useListUsers } from "core/api/cloud";
import { UserRead } from "core/api/types/CloudApi";
import { isCommonRequestError } from "core/request/CommonRequestError";
import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { AuthProviders, AuthContextApi, OAuthLoginState, AuthContext, useAuthService } from "core/services/auth";
import { trackSignup } from "core/utils/fathom";
import { isCorporateEmail } from "core/utils/freeEmailProviders";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useNotificationService } from "hooks/services/Notification";
import useTypesafeReducer from "hooks/useTypesafeReducer";
import { GoogleAuthService } from "packages/cloud/lib/auth/GoogleAuthService";
import { SignupFormValues } from "packages/cloud/views/auth/SignupPage/components/SignupForm";
import { useAuth } from "packages/firebaseReact";
import { useInitService } from "services/useInitService";

import { actions, AuthServiceState, authStateReducer, initialState } from "./reducer";
import { EmailLinkErrorCodes } from "./types";

export enum FirebaseAuthMessageId {
  NetworkFailure = "firebase.auth.error.networkRequestFailed",
  TooManyRequests = "firebase.auth.error.tooManyRequests",
  InvalidPassword = "firebase.auth.error.invalidPassword",
  DefaultError = "firebase.auth.error.default",
}

export const AuthenticationProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [state, { loggedIn, emailVerified, authInited, loggedOut, updateUserName }] = useTypesafeReducer<
    AuthServiceState,
    typeof actions
  >(authStateReducer, initialState, actions);
  const [, setSpeedyConnectionTimestamp] = useLocalStorage("exp-speedy-connection-timestamp", "");
  const auth = useAuth();
  const userService = useGetUserService();
  const analytics = useAnalyticsService();
  const authService = useInitService(() => new GoogleAuthService(() => auth), [auth]);
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  /**
   * Create a user object in the Airbyte database from an existing Firebase user.
   * This will make sure that the user account is tracked in our database as well
   * as create a workspace for that user. This method also takes care of sending
   * the relevant user creation analytics events.
   */
  const createAirbyteUser = async (
    firebaseUser: FirebaseUser,
    userData: { name?: string; companyName?: string; news?: boolean } = {}
  ): Promise<UserRead> => {
    // Create the Airbyte user on our server
    const user = await userService.create({
      authProvider: AuthProviders.GoogleIdentityPlatform,
      authUserId: firebaseUser.uid,
      email: firebaseUser.email ?? "",
      name: userData.name ?? firebaseUser.displayName ?? "",
      companyName: userData.companyName ?? "",
      news: userData.news ?? false,
    });
    const isCorporate = isCorporateEmail(user.email);

    analytics.track(Namespace.USER, Action.CREATE, {
      actionDescription: "New user registered",
      user_id: firebaseUser.uid,
      name: user.name,
      email: user.email,
      isCorporate,
      // Which login provider was used, e.g. "password", "google.com", "github.com"
      provider: firebaseUser.providerData[0]?.providerId,
    });

    trackSignup(isCorporate);

    return user;
  };

  const onAfterAuth = useCallback(
    async (currentUser: FirebaseUser, user?: UserRead) => {
      try {
        user ??= await userService.getByAuthId(currentUser.uid, AuthProviders.GoogleIdentityPlatform);
        loggedIn({
          user,
          emailVerified: currentUser.emailVerified,
          providers: currentUser.providerData.map(({ providerId }) => providerId),
        });
      } catch (e) {
        if (isCommonRequestError(e) && e.status === 404) {
          // If there is a firebase user but not database user we'll create a db user in this step
          // and retry the onAfterAuth step. This will always happen when a user logins via OAuth
          // the first time and we don't have a database user yet for them. In rare cases this can
          // also happen for email/password users if they closed their browser or got some network
          // errors in between creating the firebase user and the database user originally.
          const user = await createAirbyteUser(currentUser);
          // exp-speedy-connection
          setSpeedyConnectionTimestamp(String(new Date(new Date().getTime() + 24 * 60 * 60 * 1000)));
          await onAfterAuth(currentUser, user);
        } else {
          throw e;
        }
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [userService]
  );

  const stateRef = useRef(state);
  stateRef.current = state;

  useEffectOnce(() => {
    return auth.onAuthStateChanged(async (currentUser) => {
      // We want to run this effect only once on initial page opening
      if (!stateRef.current.inited) {
        if (stateRef.current.currentUser === null && currentUser) {
          await onAfterAuth(currentUser);
        } else {
          authInited();
        }
      }
      if (!currentUser) {
        loggedOut();
      }
    });
  });

  const queryClient = useQueryClient();

  const ctx: AuthContextApi = useMemo(
    () => ({
      inited: state.inited,
      isLoading: state.loading,
      emailVerified: state.emailVerified,
      loggedOut: state.loggedOut,
      providers: state.providers,
      hasPasswordLogin(): boolean {
        return !!state.providers?.includes("password");
      },
      async login(values: { email: string; password: string }): Promise<void> {
        await authService.login(values.email, values.password);

        if (auth.currentUser) {
          await onAfterAuth(auth.currentUser);
        }
      },
      loginWithOAuth(provider): Observable<OAuthLoginState> {
        const state = new Subject<OAuthLoginState>();
        try {
          state.next("waiting");
          authService
            .loginWithOAuth(provider)
            .then(async () => {
              state.next("loading");
              if (auth.currentUser) {
                await onAfterAuth(auth.currentUser);
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
      async logout(): Promise<void> {
        await userService.revokeUserSession();
        await authService.signOut();
        queryClient.removeQueries();
        loggedOut();
      },
      async updateName(name: string): Promise<void> {
        if (!state.currentUser) {
          return;
        }
        await userService.changeName(state.currentUser.authUserId, state.currentUser.userId, name);
        await authService.updateProfile(name);
        updateUserName({ value: name });
      },
      async updatePassword(email: string, currentPassword: string, newPassword: string): Promise<void> {
        // re-authentication may be needed before updating password
        // https://firebase.google.com/docs/auth/web/manage-users#re-authenticate_a_user
        await authService.reauthenticate(email, currentPassword);
        return authService.updatePassword(newPassword);
      },
      async requirePasswordReset(email: string): Promise<void> {
        await authService.resetPassword(email);
      },
      async sendEmailVerification(): Promise<void> {
        return authService.sendEmailVerifiedLink().catch((error) => {
          switch (error.code) {
            case AuthErrorCodes.NETWORK_REQUEST_FAILED:
              registerNotification({
                id: error.code,
                text: formatMessage({
                  id: FirebaseAuthMessageId.NetworkFailure,
                }),
                type: "error",
              });
              break;
            case AuthErrorCodes.TOO_MANY_ATTEMPTS_TRY_LATER:
              registerNotification({
                id: error.code,
                text: formatMessage({
                  id: FirebaseAuthMessageId.TooManyRequests,
                }),
                type: "warning",
              });
              break;
            default:
              registerNotification({
                id: error.code,
                text: formatMessage({
                  id: FirebaseAuthMessageId.DefaultError,
                }),
                type: "error",
              });
          }
        });
      },
      async verifyEmail(code: string): Promise<void> {
        await authService.confirmEmailVerify(code);
        emailVerified(true);
      },
      async confirmPasswordReset(code: string, newPassword: string): Promise<void> {
        await authService.finishResetPassword(code, newPassword);
      },
      async signUpWithEmailLink({ name, email, password, news }): Promise<void> {
        let firebaseUser: FirebaseUser;

        try {
          ({ user: firebaseUser } = await authService.signInWithEmailLink(email));
          await authService.updatePassword(password);
        } catch (e) {
          await authService.signOut();
          if (e.message === EmailLinkErrorCodes.LINK_EXPIRED) {
            await userService.resendWithSignInLink({ email });
          }
          throw e;
        }

        if (firebaseUser) {
          const user = await userService.getByAuthId(firebaseUser.uid, AuthProviders.GoogleIdentityPlatform);
          await userService.update({ userId: user.userId, authUserId: firebaseUser.uid, name, news });
          await onAfterAuth(firebaseUser, { ...user, name });
        }
      },
      async signUp(form: SignupFormValues): Promise<void> {
        // Create a user account in firebase
        const { user: firebaseUser } = await authService.signUp(form.email, form.password);

        // Create a user in our database for that firebase user
        await createAirbyteUser(firebaseUser, { name: form.name, companyName: form.companyName, news: form.news });

        // Send verification mail via firebase
        await authService.sendEmailVerifiedLink();

        if (auth.currentUser) {
          // exp-speedy-connection
          setSpeedyConnectionTimestamp(String(new Date(new Date().getTime() + 24 * 60 * 60 * 1000)));
          await onAfterAuth(auth.currentUser);
        }
      },
      user: state.currentUser,
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [state, userService]
  );

  return <AuthContext.Provider value={ctx}>{children}</AuthContext.Provider>;
};

export const useIsForeignWorkspace = () => {
  const { user } = useAuthService();
  const workspaceUsers = useListUsers();

  return !user || !workspaceUsers.users.some((member) => member.userId === user.userId);
};
