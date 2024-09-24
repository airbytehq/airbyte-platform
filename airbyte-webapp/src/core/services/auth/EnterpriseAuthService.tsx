import { PropsWithChildren, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { AuthProvider, useAuth } from "react-oidc-context";

import LoadingPage from "components/LoadingPage";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useGetInstanceConfiguration, useGetOrCreateUser } from "core/api";
import { AuthConfigurationMode, UserRead } from "core/api/types/AirbyteClient";
import { useFormatError } from "core/errors";
import { useNotificationService } from "hooks/services/Notification";
import { createUriWithoutSsoParams } from "packages/cloud/services/auth/CloudAuthService";

import { AuthContext, AuthContextApi } from "./AuthContext";

export const EnterpriseAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const { auth, airbyteUrl } = useGetInstanceConfiguration();

  if (auth.mode !== AuthConfigurationMode.oidc || !auth.defaultRealm || !auth.clientId) {
    throw new Error(`Authentication is enabled, but the server returned an invalid auth configuration: ${auth}`);
  }

  const oidcConfig = {
    authority: `${airbyteUrl}/auth/realms/${auth.defaultRealm}`,
    client_id: auth.clientId,
    redirect_uri: createUriWithoutSsoParams(true), // creates redirect uri and adds `checkLicense=true` query param to trigger Enterprise license check.
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
  const formatError = useFormatError();

  if (auth.isLoading) {
    return <LoadingPage />;
  }

  if (auth.error) {
    return (
      <FlexContainer justifyContent="center">
        <FlexContainer direction="column" justifyContent="center" style={{ height: "100vh" }}>
          <Text>
            <FormattedMessage id="auth.authError" values={{ errorMessage: formatError(auth.error) }} />
          </Text>
          <div>
            <Button onClick={() => auth.signinRedirect()}>
              <FormattedMessage id="login.returnToLogin" />
            </Button>
          </div>
        </FlexContainer>
      </FlexContainer>
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
  const { mutateAsync: getAirbyteUser } = useGetOrCreateUser();
  const [airbyteUser, setAirbyteUser] = useState<UserRead | null>(null);
  const [inited, setInited] = useState(false);
  const fetchingAirbyteUser = useRef(false);
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  // Allows us to get the access token as a callback, instead of re-rendering every time a new access token arrives
  const keycloakAccessTokenRef = useRef<string | undefined>(keycloakAuth.user?.access_token);
  keycloakAccessTokenRef.current = keycloakAuth.user?.access_token;

  const getAccessToken = useCallback(() => {
    return keycloakAccessTokenRef.current ? Promise.resolve(keycloakAccessTokenRef.current) : Promise.reject();
  }, []);

  useEffect(() => {
    if (fetchingAirbyteUser.current) {
      return;
    }
    fetchingAirbyteUser.current = true;
    (async () => {
      try {
        if (keycloakAuth.user) {
          const user = await getAirbyteUser({
            authUserId: keycloakAuth.user.profile.sub,
            getAccessToken: () => Promise.resolve(keycloakAuth.user?.access_token ?? ""),
          });
          setAirbyteUser(user);
        }
      } catch {
        registerNotification({
          id: "login.sso.unknownError",
          text: formatMessage({ id: "login.sso.unknownError" }),
          type: "error",
        });
      } finally {
        setInited(true);
      }
    })();
  }, [formatMessage, getAirbyteUser, keycloakAuth.user, registerNotification]);
  const contextValue = useMemo((): AuthContextApi => {
    return {
      authType: "oidc",
      applicationSupport: "multiple",
      user: airbyteUser,
      inited,
      emailVerified: false,
      provider: null,
      loggedOut: false,
      logout: keycloakAuth.signoutRedirect,
      getAccessToken,
    };
  }, [airbyteUser, getAccessToken, inited, keycloakAuth.signoutRedirect]);

  return <AuthContext.Provider value={contextValue}>{children}</AuthContext.Provider>;
};
