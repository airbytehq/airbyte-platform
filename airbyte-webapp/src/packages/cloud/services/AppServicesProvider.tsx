import React, { useMemo } from "react";

import { LoadingPage } from "components";

import { MissingConfigError, useConfig } from "config";
import { RequestMiddleware } from "core/request/RequestMiddleware";
import { RequestAuthMiddleware } from "core/services/auth";
import { ServicesProvider, useGetService, useInjectServices } from "core/servicesProvider";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useAuth } from "packages/firebaseReact";

import { KeycloakService } from "./auth/KeycloakService";
import { FirebaseSdkProvider } from "./FirebaseSdkProvider";

/**
 * This Provider is main services entrypoint
 * It initializes all required services for app to work
 * and also adds all overrides of hooks/services
 */
const AppServicesProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  // The keycloak service is behind a flag until it's ready to test on cloud
  const [showSsoLogin] = useLocalStorage("airbyte_show-sso-login", false);

  return (
    <ServicesProvider>
      <FirebaseSdkProvider>
        {showSsoLogin ? (
          <KeycloakService>
            <ServiceOverrides>{children}</ServiceOverrides>
          </KeycloakService>
        ) : (
          <ServiceOverrides>{children}</ServiceOverrides>
        )}
      </FirebaseSdkProvider>
    </ServicesProvider>
  );
};

const ServiceOverrides: React.FC<React.PropsWithChildren<unknown>> = React.memo(({ children }) => {
  const auth = useAuth();

  const middlewares: RequestMiddleware[] = useMemo(
    () => [
      RequestAuthMiddleware({
        getValue() {
          return auth.currentUser?.getIdToken() ?? "";
        },
      }),
    ],
    [auth]
  );

  const { cloudApiUrl } = useConfig();

  if (!cloudApiUrl) {
    throw new MissingConfigError("Missing required configuration cloudApiUrl");
  }

  const inject = useMemo(
    () => ({
      DefaultRequestMiddlewares: middlewares,
    }),
    [middlewares]
  );

  useInjectServices(inject);

  const registeredMiddlewares = useGetService("DefaultRequestMiddlewares");

  return registeredMiddlewares ? <>{children}</> : <LoadingPage />;
});
ServiceOverrides.displayName = "ServiceOverrides";

export { AppServicesProvider };
