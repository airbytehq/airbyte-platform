import React, { Suspense } from "react";
import { HelmetProvider } from "react-helmet-async";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import { ThemeProvider } from "styled-components";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import LoadingPage from "components/LoadingPage";

import { ConfigServiceProvider, config } from "config";
import { QueryProvider } from "core/api";
import { AnalyticsProvider } from "core/services/analytics";
import { defaultCloudFeatures, FeatureService } from "core/services/features";
import { I18nProvider } from "core/services/i18n";
import { BlockerService } from "core/services/navigation";
import { AppMonitoringServiceProvider } from "hooks/services/AppMonitoringService";
import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { FormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { ModalServiceProvider } from "hooks/services/Modal";
import { NotificationService } from "hooks/services/Notification";
import { AirbyteThemeProvider } from "hooks/theme/useAirbyteTheme";
import en from "locales/en.json";
import { Routing } from "packages/cloud/cloudRoutes";
import { AuthenticationProvider } from "packages/cloud/services/auth/AuthService";
import { theme } from "packages/cloud/theme";
import { ConnectorBuilderTestInputProvider } from "services/connectorBuilder/ConnectorBuilderTestInputService";

import { AppServicesProvider } from "./services/AppServicesProvider";
import { ZendeskProvider } from "./services/thirdParty/zendesk";

const StyleProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <ThemeProvider theme={theme}>{children}</ThemeProvider>
);

const Services: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <NotificationService>
    <ConfirmationModalService>
      <FormChangeTrackerService>
        <FeatureService features={defaultCloudFeatures}>
          <AppServicesProvider>
            <ModalServiceProvider>
              <AuthenticationProvider>
                <ConnectorBuilderTestInputProvider>
                  <HelmetProvider>
                    <ZendeskProvider>{children}</ZendeskProvider>
                  </HelmetProvider>
                </ConnectorBuilderTestInputProvider>
              </AuthenticationProvider>
            </ModalServiceProvider>
          </AppServicesProvider>
        </FeatureService>
      </FormChangeTrackerService>
    </ConfirmationModalService>
  </NotificationService>
);

const App: React.FC = () => {
  return (
    <React.StrictMode>
      <AirbyteThemeProvider>
        <StyleProvider>
          <I18nProvider locale="en" messages={en}>
            <QueryProvider>
              <BlockerService>
                <Suspense fallback={<LoadingPage />}>
                  <ConfigServiceProvider config={config}>
                    <AnalyticsProvider>
                      <AppMonitoringServiceProvider>
                        <ApiErrorBoundary>
                          <Services>
                            <Routing />
                          </Services>
                        </ApiErrorBoundary>
                      </AppMonitoringServiceProvider>
                    </AnalyticsProvider>
                  </ConfigServiceProvider>
                </Suspense>
              </BlockerService>
            </QueryProvider>
          </I18nProvider>
        </StyleProvider>
      </AirbyteThemeProvider>
    </React.StrictMode>
  );
};

const router = createBrowserRouter([{ path: "*", element: <App /> }]);

const CloudApp: React.FC = () => <RouterProvider router={router} />;
export default React.memo(CloudApp);
