import { useQueryClient } from "@tanstack/react-query";
import { BroadcastChannel } from "broadcast-channel";
import Keycloak from "keycloak-js";
import isEqual from "lodash/isEqual";
import { User, WebStorageStateStore, UserManager } from "oidc-client-ts";
import {
  MutableRefObject,
  PropsWithChildren,
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
} from "react";

import { useGetOrCreateUser } from "core/api";
import { UserRead } from "core/api/types/AirbyteClient";
import { config } from "core/config";

const DEFAULT_KEYCLOAK_REALM = "airbyte";
const DEFAULT_KEYCLOAK_CLIENT_ID = "airbyte-webapp";
const KEYCLOAK_IDP_HINT = "default";
const AIRBYTE_CLOUD_REALM = "_airbyte-cloud-users";

export type KeycloakServiceContext = {
  userManager: UserManager;
  signin: () => Promise<void>;
  signout: () => Promise<void>;
  changeRealmAndRedirectToSignin: (realm: string) => Promise<void>;
  // The access token is stored in a ref so we don't cause a re-render each time it changes. Instead, we can use the current ref value when we call the API.
  accessTokenRef: MutableRefObject<string | null>;
  updateAirbyteUser: (airbyteUser: UserRead) => void;
  redirectToSignInWithGoogle: () => Promise<void>;
  redirectToSignInWithGithub: () => Promise<void>;
  redirectToSignInWithPassword: () => Promise<void>;
  redirectToRegistrationWithPassword: () => Promise<void>;
} & KeycloakAuthState;

const keycloakServiceContext = createContext<KeycloakServiceContext | undefined>(undefined);

export const useKeycloakService = () => {
  const context = useContext(keycloakServiceContext);

  if (context === undefined) {
    throw new Error(`${useKeycloakService.name} must be used within a KeycloakRealmContext`);
  }

  return context;
};

interface KeycloakAuthState {
  airbyteUser: UserRead | null;
  keycloakUser: User | null;
  error: Error | null;
  didInitialize: boolean;
  isAuthenticated: boolean;
  isSso: boolean | null;
}

const keycloakAuthStateInitialState: KeycloakAuthState = {
  airbyteUser: null,
  keycloakUser: null,
  error: null,
  didInitialize: false,
  isAuthenticated: false,
  isSso: null,
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

const keycloakAuthStateReducer = (state: KeycloakAuthState, action: KeycloakAuthStateAction): KeycloakAuthState => {
  switch (action.type) {
    case "userLoaded":
      return {
        ...state,
        keycloakUser: action.keycloakUser,
        airbyteUser: action.airbyteUser,
        isAuthenticated: true,
        didInitialize: true,
        // We are using an SSO login if we're not in the AIRBYTE_CLOUD_REALM, which would be the end of the issuer
        isSso: !action.keycloakUser.profile.iss.endsWith(AIRBYTE_CLOUD_REALM),
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
        isSso: null,
        error: null,
      };
    case "error":
      return {
        ...state,
        didInitialize: true,
        error: action.error,
      };
  }
};

const broadcastChannel = new BroadcastChannel<BroadcastEvent>("keycloak-state-sync");

export const KeycloakService: React.FC<PropsWithChildren> = ({ children }) => {
  const userSigninInitialized = useRef(false);
  const queryClient = useQueryClient();
  const [userManager] = useState<UserManager>(initializeUserManager);
  const [authState, dispatch] = useReducer(keycloakAuthStateReducer, keycloakAuthStateInitialState);
  const { mutateAsync: getAirbyteUser } = useGetOrCreateUser();

  // Allows us to get the access token as a callback, instead of re-rendering every time a new access token arrives
  const keycloakAccessTokenRef = useRef<string | null>(null);

  useEffect(() => {
    broadcastChannel.onmessage = (event) => {
      console.log("broadcastChannel.onmessage", event);
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
    if (!userManager || userSigninInitialized.current) {
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
          // Initialize the access token ref with a value
          keycloakAccessTokenRef.current = keycloakUser.access_token;
          const airbyteUser = await getAirbyteUser({
            authUserId: keycloakUser.profile.sub,
            getAccessToken: () => Promise.resolve(keycloakUser?.access_token ?? ""),
          });
          dispatch({ type: "userLoaded", airbyteUser, keycloakUser });
          // Finally, we can assume there is no active session
        } else {
          dispatch({ type: "userUnloaded" });
        }
      } catch (error) {
        dispatch({ type: "error", error });
      }
    })();
  }, [userManager, getAirbyteUser]);

  // Hook in to userManager events
  useEffect(() => {
    if (!userManager) {
      return undefined;
    }

    const handleUserLoaded = async (keycloakUser: User) => {
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
  }, [userManager, getAirbyteUser, authState]);

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
      url: `${config.keycloakBaseUrl}/auth`,
      realm: AIRBYTE_CLOUD_REALM,
      clientId: DEFAULT_KEYCLOAK_CLIENT_ID,
    });
    await keycloak.init({});
    keycloak.register({ redirectUri: createRedirectUri(AIRBYTE_CLOUD_REALM) });
  }, []);

  const updateAirbyteUser = useCallback((airbyteUser: UserRead) => {
    dispatch({ type: "userUpdated", airbyteUser });
  }, []);

  const contextValue = useMemo(() => {
    const value = {
      ...authState,
      userManager,
      signin: () => userManager.signinRedirect(),
      signout: () => userManager.signoutRedirect({ post_logout_redirect_uri: window.location.origin }),
      updateAirbyteUser,
      isAuthenticated: authState.isAuthenticated,
      changeRealmAndRedirectToSignin,
      accessTokenRef: keycloakAccessTokenRef,
      redirectToSignInWithGoogle,
      redirectToSignInWithGithub,
      redirectToSignInWithPassword,
      redirectToRegistrationWithPassword,
    };
    return value;
  }, [
    authState,
    userManager,
    updateAirbyteUser,
    changeRealmAndRedirectToSignin,
    redirectToSignInWithGoogle,
    redirectToSignInWithGithub,
    redirectToSignInWithPassword,
    redirectToRegistrationWithPassword,
  ]);

  return <keycloakServiceContext.Provider value={contextValue}>{children}</keycloakServiceContext.Provider>;
};

function createRedirectUri(realm: string) {
  const searchParams = new URLSearchParams(window.location.search);
  searchParams.set("realm", realm);
  return `${window.location.origin}${window.location.pathname}?${searchParams.toString()}`;
}

function createUserManager(realm: string) {
  return new UserManager({
    userStore: new WebStorageStateStore({ store: window.localStorage }),
    authority: `${config.keycloakBaseUrl}/auth/realms/${realm}`,
    client_id: DEFAULT_KEYCLOAK_CLIENT_ID,
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
  const realmAndClientId = localStorageKeys.find((key) => key.startsWith("oidc.user:"));
  if (realmAndClientId) {
    const match = realmAndClientId.match(/^oidc.user:.*\/(?<realm>[^:]+):(?<clientId>.+)$/);
    if (match?.groups) {
      return createUserManager(match.groups.realm);
    }
  }

  // If no session is found, we can fall back to the default realm and client id
  return createUserManager(DEFAULT_KEYCLOAK_REALM);
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
