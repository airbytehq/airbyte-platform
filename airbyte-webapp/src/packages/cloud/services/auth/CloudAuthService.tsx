import { useQueryClient } from "@tanstack/react-query";
import { BroadcastChannel } from "broadcast-channel";
import Keycloak from "keycloak-js";
import isEqual from "lodash/isEqual";
import { User, UserManager, WebStorageStateStore } from "oidc-client-ts";
import React, { PropsWithChildren, useCallback, useEffect, useMemo, useReducer, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { LoadingPage } from "components";

import { HttpProblem, useGetOrCreateUser, useUpdateUser } from "core/api";
import { UserRead } from "core/api/types/AirbyteClient";
import { buildConfig } from "core/config";
import { useFormatError } from "core/errors";
import { AuthContext, AuthContextApi } from "core/services/auth";
import { EmbeddedAuthService } from "core/services/auth/EmbeddedAuthService";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { RoutePaths } from "pages/routePaths";

/**
 * The ID of the client in Keycloak that should be used by the webapp.
 */
const KEYCLOAK_CLIENT_ID = "airbyte-webapp";
/**
 * The IDP hint to provide keycloak for SSO users (i.e. the name of the identity provider to directly forward to).
 */
const KEYCLOAK_IDP_HINT = "default";
/**
 * The realm name that is used for all default (non-SSO) cloud users.
 */
const AIRBYTE_CLOUD_REALM = "_airbyte-cloud-users";

export const SSO_LOGIN_REQUIRED_STATE = "ssoLoginRequired";

interface KeycloakAuthState {
  airbyteUser: UserRead | null;
  keycloakUser: User | null;
  error: Error | null;
  didInitialize: boolean;
  isAuthenticated: boolean;
}

const keycloakAuthStateInitialState: KeycloakAuthState = {
  airbyteUser: null,
  keycloakUser: null,
  error: null,
  didInitialize: false,
  isAuthenticated: false,
};

type KeycloakAuthStateAction =
  | {
      type: "userLoaded";
      keycloakUser: User;
      airbyteUser: UserRead;
    }
  | {
      type: "userUnloaded";
    }
  | {
      type: "error";
      error: Error;
    }
  | {
      type: "userUpdated";
      airbyteUser: UserRead;
    };

type BroadcastEvent = Extract<KeycloakAuthStateAction, { type: "userLoaded" | "userUnloaded" }>;

function createRedirectUri(realm: string) {
  const searchParams = new URLSearchParams(window.location.search);
  searchParams.set("realm", realm);
  return `${window.location.origin}${window.location.pathname}?${searchParams.toString()}`;
}

function createUserManager(realm: string) {
  return new UserManager({
    userStore: new WebStorageStateStore({ store: window.localStorage }),
    authority: `${buildConfig.keycloakBaseUrl}/auth/realms/${realm}`,
    client_id: KEYCLOAK_CLIENT_ID,
    redirect_uri: createRedirectUri(realm),
  });
}

export function initializeUserManager() {
  // First, check if there's an active redirect in progress. If so, we can pull the realm & clientId from the query params
  const searchParams = new URLSearchParams(window.location.search);
  const realm = searchParams.get("realm");
  if (realm) {
    return createUserManager(realm);
  }

  // If there's no active redirect, so we can check for an existing session based on an entry in local storage
  // The local storage key looks like this: oidc.user:https://example.com/auth/realms/<realm>:<client-id>
  const localStorageKeys = Object.keys(window.localStorage);

  // Look for a localStorage entry that matches the current backend we're connecting to
  const existingLocalStorageEntry = localStorageKeys.find((key) =>
    key.startsWith(`oidc.user:${buildConfig.keycloakBaseUrl}`)
  );

  if (existingLocalStorageEntry) {
    const realmAndClientId = existingLocalStorageEntry.match(/^oidc.user:.*\/(?<realm>[^:]+):(?<clientId>.+)$/);
    if (realmAndClientId?.groups) {
      return createUserManager(realmAndClientId.groups.realm);
    }
  }

  // If no session is found, we can fall back to the default realm and client id
  return createUserManager(AIRBYTE_CLOUD_REALM);
}

// During local development there may be multiple oidc sessions present in local storage when switching between environments. Clearing them avoids initializing the userManager with the wrong realm.
function clearLocalStorageOidcSessions() {
  const localStorageKeys = Object.keys(window.localStorage);
  localStorageKeys.forEach((key) => {
    if (key.startsWith("oidc.user:")) {
      window.localStorage.removeItem(key);
    }
  });
}

// Removes OIDC params from URL, but doesn't remove other params that might be present
export function createUriWithoutSsoParams() {
  // state, code and session_state are from keycloak. realm is added by us to indicate which realm the user is signing in to.
  const SSO_SEARCH_PARAMS = ["state", "code", "session_state", "realm"];

  const searchParams = new URLSearchParams(window.location.search);

  SSO_SEARCH_PARAMS.forEach((param) => searchParams.delete(param));

  return searchParams.toString().length > 0
    ? `${window.location.origin}?${searchParams.toString()}`
    : window.location.origin;
}

function clearSsoSearchParams() {
  const newUrl = createUriWithoutSsoParams();
  window.history.replaceState({}, document.title, newUrl);
}

const hasAuthParams = (location = window.location): boolean => {
  const searchParams = new URLSearchParams(location.search);
  if ((searchParams.get("code") || searchParams.get("error")) && searchParams.get("state")) {
    return true;
  }

  return false;
};

// Checks whether users are the same, ignoring properties like the access_token or refresh_token
function usersAreSame(
  newState: { keycloakUser: User | null; airbyteUser: UserRead | null },
  authState: KeycloakAuthState
): boolean {
  if (!!authState.airbyteUser !== !!newState.airbyteUser) {
    return false;
  }

  if (!!authState.keycloakUser !== !!newState.keycloakUser) {
    return false;
  }

  // We only really care if the keycloakUser id changes. If only the access token or refresh token has updated,
  // we don't need to cause a re-render of the context value.
  if (authState.keycloakUser?.profile.sub !== newState.keycloakUser?.profile.sub) {
    return false;
  }

  if (!isEqual(authState.airbyteUser, newState.airbyteUser)) {
    return false;
  }

  return true;
}

const keycloakAuthStateReducer = (state: KeycloakAuthState, action: KeycloakAuthStateAction): KeycloakAuthState => {
  switch (action.type) {
    case "userLoaded":
      return {
        ...state,
        keycloakUser: action.keycloakUser,
        airbyteUser: action.airbyteUser,
        isAuthenticated: true,
        didInitialize: true,
        error: null,
      };
    case "userUpdated":
      return {
        ...state,
        airbyteUser: action.airbyteUser,
      };
    case "userUnloaded":
      return {
        ...state,
        keycloakUser: null,
        airbyteUser: null,
        isAuthenticated: false,
        didInitialize: true,
        error: null,
      };
    case "error":
      clearLocalStorageOidcSessions();
      return {
        ...state,
        didInitialize: true,
        error: action.error,
      };
  }
};

const broadcastChannel = new BroadcastChannel<BroadcastEvent>("keycloak-state-sync");

// Checks for a valid auth session with keycloak and returns the user if found.
const CloudKeycloakAuthService: React.FC<PropsWithChildren> = ({ children }) => {
  const userSigninInitialized = useRef(false);
  const queryClient = useQueryClient();
  const [userManager] = useState<UserManager>(initializeUserManager);
  const [authState, dispatch] = useReducer(keycloakAuthStateReducer, keycloakAuthStateInitialState);
  const [logoutInProgress, setLogoutInProgress] = useState(false);
  const { mutateAsync: getAirbyteUser } = useGetOrCreateUser();
  const { mutateAsync: updateAirbyteUser } = useUpdateUser();
  const formatError = useFormatError();
  const navigate = useNavigate();

  // Allows us to get the access token as a callback, instead of re-rendering every time a new access token arrives
  const keycloakAccessTokenRef = useRef<string | null>(null);

  const handleAirbyteUserError = useCallback(
    async (error: unknown) => {
      if (HttpProblem.isType(error, "error:auth/sso-required")) {
        // Navigate to the SSO signin page and set the state to show an info message there
        navigate(CloudRoutes.Sso, { state: { [SSO_LOGIN_REQUIRED_STATE]: true } });
        return await userManager.signoutSilent();
      }
      if (HttpProblem.isType(error, "error:auth/user-already-exists")) {
        navigate(CloudRoutes.Login, { state: { errorMessage: formatError(error) } });
        return await userManager.signoutSilent();
      }
      throw error;
    },
    [navigate, userManager, formatError]
  );

  // Handle login/logoff that happened in another tab
  useEffect(() => {
    broadcastChannel.onmessage = (event) => {
      if (event.type === "userUnloaded") {
        console.debug("🔑 Received userUnloaded event from other tab.");
        dispatch({ type: "userUnloaded" });
        // Need to clear all queries from cache. In the tab that triggered the logout this is
        // handled inside CloudAuthService.logout
        queryClient.removeQueries();
      } else if (event.type === "userLoaded") {
        console.debug("🔑 Received userLoaded event from other tab.");
        keycloakAccessTokenRef.current = event.keycloakUser.access_token;
        dispatch({ type: "userLoaded", keycloakUser: event.keycloakUser, airbyteUser: event.airbyteUser });
      }
    };
  }, [queryClient]);

  // Initialization of the current user
  useEffect(() => {
    if (userSigninInitialized.current) {
      return;
    }
    // We strictly need to initialize once, because authorization codes are only valid for a single use
    userSigninInitialized.current = true;

    (async (): Promise<void> => {
      let keycloakUser: User | void | null = null;
      try {
        // Check if user is returning back from IdP login
        if (hasAuthParams()) {
          keycloakUser = await userManager.signinCallback();
          clearSsoSearchParams();
          // Otherwise, check if there is a session currently
        } else if ((keycloakUser ??= await userManager.signinSilent())) {
          try {
            const airbyteUser = await getAirbyteUser({
              authUserId: keycloakUser.profile.sub,
              getAccessToken: () => Promise.resolve(keycloakUser?.access_token ?? ""),
            });
            // Initialize the access token ref with a value
            keycloakAccessTokenRef.current = keycloakUser.access_token;
            dispatch({ type: "userLoaded", airbyteUser, keycloakUser });
          } catch (error) {
            handleAirbyteUserError(error);
          }
          // Finally, we can assume there is no active session
        } else {
          dispatch({ type: "userUnloaded" });
        }
      } catch (error) {
        dispatch({ type: "error", error });
      }
    })();
  }, [userManager, getAirbyteUser, handleAirbyteUserError]);

  // Hook in to userManager events
  useEffect(() => {
    const handleUserLoaded = async (keycloakUser: User) => {
      try {
        const airbyteUser = await getAirbyteUser({
          authUserId: keycloakUser.profile.sub,
          getAccessToken: () => Promise.resolve(keycloakUser?.access_token ?? ""),
        });

        // Update the access token ref with the new access token. This happens each time we get a fresh token.
        keycloakAccessTokenRef.current = keycloakUser.access_token;

        // Only if actual user values (not just access_token) have changed, do we need to update the state and cause a re-render
        if (!usersAreSame({ keycloakUser, airbyteUser }, authState)) {
          dispatch({ type: "userLoaded", keycloakUser, airbyteUser });
          // Notify other tabs that this tab got a new user loaded (usually meaning this tab signed in)
          broadcastChannel.postMessage({ type: "userLoaded", keycloakUser, airbyteUser });
        }
      } catch (error: unknown) {
        handleAirbyteUserError(error);
      }
    };
    userManager.events.addUserLoaded(handleUserLoaded);

    const handleUserUnloaded = () => {
      dispatch({ type: "userUnloaded" });
      // Notify other open tabs that the user got unloaded (i.e. this tab signed out)
      broadcastChannel.postMessage({ type: "userUnloaded" });
    };
    userManager.events.addUserUnloaded(handleUserUnloaded);

    const handleSilentRenewError = (error: Error) => {
      dispatch({ type: "error", error });
    };
    userManager.events.addSilentRenewError(handleSilentRenewError);

    const handleExpiredToken = async () => {
      await userManager.signinSilent().catch(async () => {
        // We need to manually sign out, otherwise the expired token and user will stick around
        await userManager.signoutSilent();
        dispatch({ type: "userUnloaded" });
      });
    };
    userManager.events.addAccessTokenExpired(handleExpiredToken);

    return () => {
      userManager.events.removeUserLoaded(handleUserLoaded);
      userManager.events.removeUserUnloaded(handleUserUnloaded);
      userManager.events.removeSilentRenewError(handleSilentRenewError);
      userManager.events.removeAccessTokenExpired(handleExpiredToken);
    };
  }, [userManager, getAirbyteUser, authState, handleAirbyteUserError]);

  const changeRealmAndRedirectToSignin = useCallback(async (realm: string) => {
    // This is not a security measure. The realm is publicly accessible, but we don't want users to access it via the SSO flow, because that could cause confusion.
    if (realm === AIRBYTE_CLOUD_REALM) {
      throw new Error("Realm inaccessible via SSO flow. Use the default login flow instead.");
    }
    const newUserManager = createUserManager(realm);
    await newUserManager.signinRedirect({ extraQueryParams: { kc_idp_hint: KEYCLOAK_IDP_HINT } });
  }, []);

  const redirectToSignInWithGoogle = useCallback(async () => {
    const newUserManager = createUserManager(AIRBYTE_CLOUD_REALM);
    await newUserManager.signinRedirect({ extraQueryParams: { kc_idp_hint: "google" } });
  }, []);

  const redirectToSignInWithGithub = useCallback(async () => {
    const newUserManager = createUserManager(AIRBYTE_CLOUD_REALM);
    await newUserManager.signinRedirect({ extraQueryParams: { kc_idp_hint: "github" } });
  }, []);

  const redirectToSignInWithPassword = useCallback(async () => {
    const newUserManager = createUserManager(AIRBYTE_CLOUD_REALM);
    await newUserManager.signinRedirect();
  }, []);

  /**
   * Using the keycloak-js library here instead of oidc-ts, because keycloak-js knows how to route us directly to Keycloak's registration page.
   * oidc-ts does not (because that's not part of the OIDC spec) and recreating the logic to set the correct state, code_challenge, etc. would be complicated to maintain.
   */
  const redirectToRegistrationWithPassword = useCallback(async () => {
    const keycloak = new Keycloak({
      url: `${buildConfig.keycloakBaseUrl}/auth`,
      realm: AIRBYTE_CLOUD_REALM,
      clientId: KEYCLOAK_CLIENT_ID,
    });
    await keycloak.init({});
    keycloak.register({ redirectUri: createRedirectUri(AIRBYTE_CLOUD_REALM) });
  }, []);

  const logout = useCallback(async () => {
    setLogoutInProgress(true);
    if (authState.isAuthenticated) {
      await userManager.signoutRedirect({ post_logout_redirect_uri: window.location.origin });
    }
    setLogoutInProgress(false);
    navigate("/");
    queryClient.removeQueries();
  }, [authState.isAuthenticated, navigate, queryClient, userManager]);

  const authContextValue = useMemo<AuthContextApi>(() => {
    // The context value for an authenticated Keycloak user.
    if (authState.isAuthenticated) {
      return {
        authType: "cloud",
        applicationSupport: "multiple",
        inited: true,
        user: authState.airbyteUser,
        emailVerified: authState.keycloakUser?.profile.email_verified ?? false,
        getAccessToken: () => Promise.resolve(keycloakAccessTokenRef?.current),
        updateName: async (name: string) => {
          const user = authState.airbyteUser;
          if (!user) {
            throw new Error("Cannot change name, airbyteUser is null");
          }
          await updateAirbyteUser({
            userUpdate: { userId: user.userId, name },
            getAccessToken: async () => keycloakAccessTokenRef?.current ?? "",
          }).then(() => {
            dispatch({ type: "userUpdated", airbyteUser: { ...user, name } });
          });
        },
        logout,
        loggedOut: false,
        provider: !authState.keycloakUser?.profile.iss.endsWith(AIRBYTE_CLOUD_REALM)
          ? "sso"
          : (authState.keycloakUser?.profile.identity_provider as string | undefined) ?? "none",
      };
    }
    // The context value for an unauthenticated user
    return {
      authType: "cloud",
      applicationSupport: "none",
      user: null,
      inited: authState.didInitialize,
      emailVerified: false,
      loggedOut: true,
      provider: null,
      changeRealmAndRedirectToSignin,
      redirectToSignInWithGoogle,
      redirectToSignInWithGithub,
      redirectToSignInWithPassword,
      redirectToRegistrationWithPassword,
    };
  }, [
    authState.airbyteUser,
    authState.didInitialize,
    authState.isAuthenticated,
    authState.keycloakUser?.profile.email_verified,
    authState.keycloakUser?.profile.identity_provider,
    authState.keycloakUser?.profile.iss,
    changeRealmAndRedirectToSignin,
    logout,
    redirectToRegistrationWithPassword,
    redirectToSignInWithGithub,
    redirectToSignInWithGoogle,
    redirectToSignInWithPassword,
    updateAirbyteUser,
  ]);

  if (logoutInProgress) {
    return <LoadingPage />;
  }

  return <AuthContext.Provider value={authContextValue}>{children}</AuthContext.Provider>;
};

export const CloudAuthService: React.FC<PropsWithChildren> = ({ children }) => {
  const location = useLocation();
  /* This is the route for the embedded widget.  It uses scoped auth tokens and will not have an associated user.
      Thus, it leverages the EmbeddedAuthService to provide an empty user object to the AuthContext. */
  if (location.pathname === `/${RoutePaths.EmbeddedWidget}`) {
    return <EmbeddedAuthService>{children}</EmbeddedAuthService>;
  }

  return <CloudKeycloakAuthService>{children}</CloudKeycloakAuthService>;
};
