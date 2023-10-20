import { User, WebStorageStateStore } from "oidc-client-ts";
import { UserManager } from "oidc-client-ts";
import {
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

import { config } from "config";

export const DEFAULT_KEYCLOAK_REALM = "airbyte";
export const DEFAULT_KEYCLOAK_CLIENT_ID = "airbyte-webapp";

interface KeycloakRealmContextType {
  userManager: UserManager;
  signinRedirect: () => Promise<void>;
  signoutRedirect: () => Promise<void>;
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: Error | null;
  changeRealmAndRedirectToSignin: (realm: string) => Promise<void>;
}

const keycloakServiceContext = createContext<KeycloakRealmContextType | undefined>(undefined);

export const useKeycloakService = () => {
  const context = useContext(keycloakServiceContext);

  if (context === undefined) {
    throw new Error(`${useKeycloakService.name} must be used within a KeycloakRealmContext`);
  }

  return context;
};

interface KeycloakAuthState {
  user: User | null;
  error: Error | null;
  isInitializing: boolean;
  isAuthenticated: boolean;
}

const keycloakAuthStateInitialState: KeycloakAuthState = {
  user: null,
  error: null,
  isInitializing: true,
  isAuthenticated: false,
};

type KeycloakAuthStateAction =
  | {
      type: "userLoaded";
      user: User;
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
        user: action.user,
        isAuthenticated: true,
        isInitializing: false,
        error: null,
      };
    case "userUnloaded":
      return {
        ...state,
        user: null,
        isAuthenticated: false,
        isInitializing: false,
        error: null,
      };
    case "error":
      return {
        ...state,
        isInitializing: false,
        error: action.error,
      };
  }
};

export const KeycloakService: React.FC<PropsWithChildren> = ({ children }) => {
  const userSigninInitialized = useRef(false);
  const [userManager] = useState<UserManager>(initializeUserManager);
  const [authState, dispatch] = useReducer(keycloakAuthStateReducer, keycloakAuthStateInitialState);

  // Initialization of the current user
  useEffect(() => {
    if (!userManager || userSigninInitialized.current) {
      return;
    }
    // We strictly need to initialize once, because authorization codes are only valid for a single use
    userSigninInitialized.current = true;

    (async (): Promise<void> => {
      let user: User | void | null = null;
      try {
        // check if returning back from authority server
        if (hasAuthParams()) {
          user = await userManager.signinCallback();
          clearSsoSearchParams();
        }
        // If not returning from authority server, check if we can get a user
        if ((user ??= await userManager.getUser())) {
          dispatch({ type: "userLoaded", user });
        } else {
          dispatch({ type: "userUnloaded" });
        }
      } catch (error) {
        dispatch({ type: "error", error });
      }
    })();
  }, [userManager]);

  // Hook in to userManager events
  useEffect(() => {
    if (!userManager) {
      return undefined;
    }

    const handleUserLoaded = (user: User) => {
      dispatch({ type: "userLoaded", user });
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

    const handleExpiredToken = () => {
      userManager.signinSilent().catch(async () => {
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
  }, [userManager]);

  const changeRealmAndRedirectToSignin = useCallback(async (realm: string) => {
    const newUserManager = createUserManager(realm);
    await newUserManager.signinRedirect();
  }, []);

  const contextValue = useMemo(() => {
    return {
      ...authState,
      userManager,
      signinRedirect: () => userManager.signinRedirect(),
      signoutRedirect: () => userManager.signoutRedirect(),
      isAuthenticated: userManager.getUser() !== null,
      isLoading: false,
      error: null,
      changeRealmAndRedirectToSignin,
    };
  }, [userManager, changeRealmAndRedirectToSignin, authState]);

  return <keycloakServiceContext.Provider value={contextValue}>{children}</keycloakServiceContext.Provider>;
};

function createUserManager(realm: string) {
  const searchParams = new URLSearchParams(window.location.search);
  searchParams.set("realm", realm);
  const redirect_uri = `${window.location.origin}?${searchParams.toString()}`;
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

function clearSsoSearchParams() {
  const searchParams = new URLSearchParams(window.location.search);

  // Remove OIDC params from URL, but don't remove other params that might be present
  searchParams.delete("state");
  searchParams.delete("code");
  searchParams.delete("session_state");

  // Remove our own params we set in the redirect_uri
  searchParams.delete("realm");

  const newUrl = searchParams.toString().length
    ? `${window.location.pathname}?${searchParams.toString()}`
    : window.location.pathname;
  window.history.replaceState({}, document.title, newUrl);
}

export const hasAuthParams = (location = window.location): boolean => {
  // response_mode: query
  const searchParams = new URLSearchParams(location.search);
  if ((searchParams.get("code") || searchParams.get("error")) && searchParams.get("state")) {
    return true;
  }

  return false;
};
