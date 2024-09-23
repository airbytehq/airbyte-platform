// Disabling @airbyte/no-local-storage because this is a rare case where we do not want to load the local storage value into react state, so the hook is not helpful.
/* eslint-disable @airbyte/no-local-storage */
import React, { PropsWithChildren, useCallback, useEffect, useMemo, useReducer, useRef } from "react";
import { useIntl } from "react-intl";

import { SimpleAuthLoginFormValues } from "components/login/SimpleAuthLoginForm";

import { useGetDefaultUserAsync, useSimpleAuthLogin, useSimpleAuthLogout } from "core/api";
import { UserRead } from "core/api/types/AirbyteClient";
import { useNotificationService } from "hooks/services/Notification";

import { AuthContext, AuthContextApi } from "./AuthContext";
import { SimpleAuthTokenRefresher } from "./SimpleAuthTokenRefresher";

type AuthState = Pick<AuthContextApi, "user" | "inited" | "loggedOut">;

interface InitializingState extends AuthState {
  user: null;
  inited: false;
  loggedOut: true;
}

interface LoggedInState extends AuthState {
  user: UserRead;
  inited: true;
  loggedOut: false;
}

interface LoggedOutState extends AuthState {
  user: null;
  inited: true;
  loggedOut: true;
}

type SimpleAuthServiceAuthState = InitializingState | LoggedInState | LoggedOutState;

type AuthAction = { type: "login"; user: UserRead } | { type: "logout" };

const simpleAuthStateReducer = (state: SimpleAuthServiceAuthState, action: AuthAction): SimpleAuthServiceAuthState => {
  switch (action.type) {
    case "login":
      return {
        ...state,
        inited: true,
        user: action.user,
        loggedOut: false,
      };
    case "logout":
      return {
        ...state,
        inited: true,
        user: null,
        loggedOut: true,
      };
    default:
      return state;
  }
};

const initialAuthState: InitializingState = {
  user: null,
  inited: false,
  loggedOut: true,
};

export class InvalidCredentialsError extends Error {
  constructor() {
    super("Invalid credentials");
    this.name = "InvalidCredentialsError";
  }
}

export class MissingCookieError extends Error {
  constructor() {
    super("Missing cookie");
    this.name = "MissingCookieError";
  }
}

// This is a static auth service in case the auth mode of the Airbyte instance is set to "none"
export const SimpleAuthService: React.FC<PropsWithChildren> = ({ children }) => {
  const [authState, dispatch] = useReducer(simpleAuthStateReducer, initialAuthState);
  const { mutateAsync: login } = useSimpleAuthLogin();
  const { mutateAsync: logout } = useSimpleAuthLogout();
  const { mutateAsync: getDefaultUser } = useGetDefaultUserAsync();
  const notificationService = useNotificationService();
  const initializingRef = useRef(false);
  const { formatMessage } = useIntl();

  // This effect is explicitly run once to initialize the auth state
  useEffect(() => {
    if (initializingRef.current) {
      return;
    }
    async function initializeSimpleAuthService() {
      initializingRef.current = true;
      try {
        const user = await getDefaultUser();
        dispatch({ type: "login", user });
      } catch {
        dispatch({ type: "logout" });
      }
    }
    initializeSimpleAuthService();
  }, [getDefaultUser]);

  const loginCallback = useCallback(
    async (values: SimpleAuthLoginFormValues) => {
      try {
        await login({ username: values.username, password: values.password });
      } catch (e) {
        if (e.status === 401) {
          throw new InvalidCredentialsError();
        }
        throw e;
      }
      try {
        const user = await getDefaultUser();
        dispatch({ type: "login", user });
      } catch (e) {
        // This indicates that the cookie probably could not be set by the server because the user did not deploy with insecure cookies enabled
        if (e.status === 401 && window.location.protocol === "http:") {
          throw new MissingCookieError();
        }
        // Otherwise some unexpected error occurred
        throw new Error(formatMessage({ id: "login.getUserFailed" }));
      }
    },
    [getDefaultUser, login, formatMessage]
  );

  const contextValue = useMemo(() => {
    return {
      authType: "simple",
      applicationSupport: "single",
      provider: null,
      emailVerified: false,
      ...authState,
      getAccessToken: undefined, // With simple auth, the JWT is stored in a cookie that is set server-side
      login: authState.loggedOut ? loginCallback : undefined,
      logout: authState.loggedOut
        ? undefined
        : async () => {
            try {
              await logout();
              dispatch({ type: "logout" });
            } catch (e) {
              notificationService.registerNotification({
                type: "error",
                id: "",
                text: formatMessage({ id: "sidebar.logout.failed" }),
              });
              console.error("Error logging out", e);
            }
          },
    } as const;
  }, [loginCallback, authState, logout, notificationService, formatMessage]);

  return (
    <AuthContext.Provider value={contextValue}>
      {authState.inited && !authState.loggedOut && <SimpleAuthTokenRefresher />}
      {children}
    </AuthContext.Provider>
  );
};
