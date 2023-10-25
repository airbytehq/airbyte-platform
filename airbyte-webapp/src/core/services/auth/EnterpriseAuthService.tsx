import { PropsWithChildren, useCallback, useMemo, useRef } from "react";
import { FormattedMessage } from "react-intl";
import { AuthProvider, useAuth } from "react-oidc-context";

import LoadingPage from "components/LoadingPage";

import { useGetDefaultUser, useGetInstanceConfiguration } from "core/api";

import { AuthContext } from "./AuthContext";

// This wrapper is conditionally present if the KeycloakAuthentication feature is enabled
export const EnterpriseAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const { auth, webappUrl } = useGetInstanceConfiguration();

  if (!auth) {
    throw new Error("Authentication is enabled, but the server returned an invalid auth configuration: ", auth);
  }

  const oidcConfig = {
    authority: `${webappUrl}/auth/realms/${auth.defaultRealm}`,
    client_id: auth.clientId,
    redirect_uri: window.location.href,
    onSigninCallback: () => {
      // Remove OIDC params from URL, but don't remove other params that might be present
      const searchParams = new URLSearchParams(window.location.search);
      searchParams.delete("state");
      searchParams.delete("code");
      searchParams.delete("session_state");
      const newUrl = searchParams.toString().length
        ? `${window.location.pathname}?${searchParams.toString()}`
        : window.location.pathname;
      window.history.replaceState({}, document.title, newUrl);
    },
  };

  return (
    <AuthProvider {...oidcConfig}>
      <LoginRedirectCheck>
        <AuthServiceProvider>{children}</AuthServiceProvider>
      </LoginRedirectCheck>
    </AuthProvider>
  );
};

// While auth status is loading we want to suspend rendering of the app
const LoginRedirectCheck: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const auth = useAuth();

  if (auth.isLoading) {
    return <LoadingPage />;
  }

  if (auth.error) {
    return (
      <>
        <button onClick={() => auth.signinRedirect()}>Return to login</button>
        <FormattedMessage id="auth.authError" values={{ errorMessage: auth.error.message }} />;
      </>
    );
  }

  if (!auth.isAuthenticated) {
    auth.signinRedirect();
    return <LoadingPage />;
  }

  return <>{children}</>;
};

const AuthServiceProvider: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const keycloakAuth = useAuth();

  // Allows us to get the access token as a callback, instead of re-rendering every time a new access token arrives
  const keycloakAccessTokenRef = useRef<string | null>(keycloakAuth.user?.access_token ?? null);
  keycloakAccessTokenRef.current = keycloakAuth.user?.access_token ?? null;
  const getAccessToken = useCallback(() => Promise.resolve(keycloakAccessTokenRef.current), []);

  const defaultUser = useGetDefaultUser({ getAccessToken });

  const contextValue = useMemo(() => {
    return {
      user: defaultUser,
      inited: true,
      emailVerified: false,
      providers: [],
      loggedOut: false,
      getAccessToken,
    };
  }, [defaultUser, getAccessToken]);

  return <AuthContext.Provider value={contextValue}>{children}</AuthContext.Provider>;
};
