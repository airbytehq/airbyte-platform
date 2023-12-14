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

export type KeycloakServiceContext = {
  userManager: UserManager;
  signin: () => Promise<void>;
  signout: () => Promise<void>;
  changeRealmAndRedirectToSignin: (realm: string) => Promise<void>;
  // The access token is stored in a ref so we don't cause a re-render each time it changes. Instead, we can use the current ref value when we call the API.
  accessTokenRef: MutableRefObject<string | null>;
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
    };

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
      return {
        ...state,
        didInitialize: true,
        error: action.error,
      };
  }
};

export const KeycloakService: React.FC<PropsWithChildren> = ({ children }) => {
  const userSigninInitialized = useRef(false);
  const [userManager] = useState<UserManager>(initializeUserManager);
  const [authState, dispatch] = useReducer(keycloakAuthStateReducer, keycloakAuthStateInitialState);
  const { mutateAsync: getAirbyteUser } = useGetOrCreateUser();

  // Allows us to get the access token as a callback, instead of re-rendering every time a new access token arrives
  const keycloakAccessTokenRef = useRef<string | null>(null);

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
      }
    };
    userManager.events.addUserLoaded(handleUserLoaded);

    const handleUserUnloaded = () => {
      dispatch({ type: "userUnloaded" });
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
    const newUserManager = createUserManager(realm);
    await newUserManager.signinRedirect({ extraQueryParams: { kc_idp_hint: KEYCLOAK_IDP_HINT } });
  }, []);

  const contextValue = useMemo(() => {
    const value = {
      ...authState,
      userManager,
      signin: () => userManager.signinRedirect(),
      signout: () => userManager.signoutRedirect({ post_logout_redirect_uri: window.location.origin }),
      isAuthenticated: authState.isAuthenticated,
      changeRealmAndRedirectToSignin,
      accessTokenRef: keycloakAccessTokenRef,
    };
    return value;
  }, [userManager, changeRealmAndRedirectToSignin, authState]);

  return <keycloakServiceContext.Provider value={contextValue}>{children}</keycloakServiceContext.Provider>;
};

function createUserManager(realm: string) {
  const searchParams = new URLSearchParams(window.location.search);
  searchParams.set("realm", realm);
  const redirect_uri = `${window.location.origin}${window.location.pathname}?${searchParams.toString()}`;
  const userManager = new UserManager({
    userStore: new WebStorageStateStore({ store: window.localStorage }),
    authority: `${config.keycloakBaseUrl}/auth/realms/${realm}`,
    client_id: DEFAULT_KEYCLOAK_CLIENT_ID,
    redirect_uri,
  });
  return userManager;
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
