// Disabling @airbyte/no-local-storage because this is a rare case where we do not want to load the local storage value into react state, so the hook is not helpful.
/* eslint-disable @airbyte/no-local-storage */
import { jwtDecode } from "jwt-decode";
import React, { PropsWithChildren, useCallback, useEffect, useMemo, useReducer, useRef } from "react";
import { useNavigate } from "react-router-dom";

import { SimpleAuthLoginFormValues } from "components/login/SimpleAuthLoginForm";

import { useGetInstanceConfiguration, useGetOrCreateUser, useSimpleAuthLogin } from "core/api";
import { UserRead } from "core/api/types/AirbyteClient";

import { AuthContext, AuthContextApi } from "./AuthContext";

const SIMPLE_AUTH_LOCAL_STORAGE_KEY = "airbyte_simple-auth-token";

const isJwtExpired = (jwt: string) => {
  if (jwt.length === 0) {
    return false;
  }
  const decoded = jwtDecode(jwt);
  return !!decoded.exp && decoded.exp < Date.now() / 1000;
};

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

type AuthAction = { type: "login"; user: UserRead; accessToken: string } | { type: "logout" };

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

// This is a static auth service in case the auth mode of the Airbyte instance is set to "none"
export const SimpleAuthService: React.FC<PropsWithChildren> = ({ children }) => {
  const [authState, dispatch] = useReducer(simpleAuthStateReducer, initialAuthState);
  // Stored in a ref so we can update the access token without re-rendering the whole context
  const accessTokenRef = useRef<string | null>(null);
  const { mutateAsync: login } = useSimpleAuthLogin();
  const { mutateAsync: getAirbyteUser } = useGetOrCreateUser();
  const { defaultUserId } = useGetInstanceConfiguration();
  const initializingRef = useRef(false);
  const navigate = useNavigate();

  // This effect is explicitly run once to initialize the auth state
  useEffect(() => {
    if (initializingRef.current) {
      return;
    }
    async function initializeSimpleAuthService() {
      initializingRef.current = true;
      const token = localStorage.getItem(SIMPLE_AUTH_LOCAL_STORAGE_KEY);
      if (!token) {
        dispatch({ type: "logout" });
        return;
      }
      if (isJwtExpired(token)) {
        localStorage.removeItem(SIMPLE_AUTH_LOCAL_STORAGE_KEY);
        dispatch({ type: "logout" });
        return;
      }
      try {
        accessTokenRef.current = token;
        const user = await getAirbyteUser({
          authUserId: defaultUserId,
          getAccessToken: () => Promise.resolve(token),
        });
        dispatch({ type: "login", user, accessToken: token });
      } catch {
        dispatch({ type: "logout" });
      }
    }
    initializeSimpleAuthService();
  }, [defaultUserId, getAirbyteUser]);

  const loginCallback = useCallback(
    async (values: SimpleAuthLoginFormValues) => {
      const loginResponse = await login({ username: values.username, password: values.password });
      accessTokenRef.current = loginResponse.access_token;
      localStorage.setItem(SIMPLE_AUTH_LOCAL_STORAGE_KEY, loginResponse.access_token);
      const user = await getAirbyteUser({
        authUserId: defaultUserId,
        getAccessToken: () => Promise.resolve(loginResponse.access_token),
      });
      dispatch({ type: "login", user, accessToken: loginResponse.access_token });
    },
    [defaultUserId, getAirbyteUser, login]
  );

  const contextValue = useMemo(() => {
    return {
      authType: "simple",
      provider: null,
      emailVerified: false,
      ...authState,
      getAccessToken: () => Promise.resolve(accessTokenRef.current),
      login: authState.loggedOut ? loginCallback : undefined,
      logout: authState.loggedOut
        ? undefined
        : async () => {
            localStorage.removeItem(SIMPLE_AUTH_LOCAL_STORAGE_KEY);
            accessTokenRef.current = null;
            navigate("/");
            dispatch({ type: "logout" });
          },
    } as const;
  }, [loginCallback, authState, navigate]);

  return <AuthContext.Provider value={contextValue}>{children}</AuthContext.Provider>;
};
