import { PropsWithChildren, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { AuthProvider, useAuth } from "react-oidc-context";

import LoadingPage from "components/LoadingPage";

import { useGetInstanceConfiguration } from "core/api";
import { useGetService, useInjectServices } from "core/servicesProvider";

import { RequestAuthMiddleware } from "./RequestAuthMiddleware";

// This wrapper is conditionally present if the KeycloakAuthentication feature is enabled
export const KeycloakAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
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
        <KeycloakAuthMiddleware>{children}</KeycloakAuthMiddleware>
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

// This middleware will add the keycloak access_token to all requests
const KeycloakAuthMiddleware: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const auth = useAuth();

  const middlewares = useMemo(
    () => [
      RequestAuthMiddleware({
        getValue() {
          return auth.user?.access_token ?? "";
        },
      }),
    ],
    [auth.user?.access_token]
  );

  const inject = useMemo(
    () => ({
      DefaultRequestMiddlewares: middlewares,
    }),
    [middlewares]
  );

  useInjectServices(inject);

  const registeredMiddlewares = useGetService("DefaultRequestMiddlewares");

  return registeredMiddlewares ? <>{children}</> : <LoadingPage />;
};
