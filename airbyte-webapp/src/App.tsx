import React, { Suspense } from "react";
import { HelmetProvider } from "react-helmet-async";
import { BrowserRouter as Router } from "react-router-dom";
import { ThemeProvider } from "styled-components";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";

import { config } from "config";
import { QueryProvider, useGetInstanceConfiguration } from "core/api";
import { AnalyticsProvider } from "core/services/analytics";
import { defaultOssFeatures, FeatureService } from "core/services/features";
import { I18nProvider } from "core/services/i18n";
import { ServicesProvider } from "core/servicesProvider";
import { AppMonitoringServiceProvider } from "hooks/services/AppMonitoringService";
import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { FormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { ModalServiceProvider } from "hooks/services/Modal";
import { NotificationService } from "hooks/services/Notification";
import { AirbyteThemeProvider } from "hooks/theme/useAirbyteTheme";
import { ConnectorBuilderTestInputProvider } from "services/connectorBuilder/ConnectorBuilderTestInputService";
import { KeycloakAuthService } from "services/KeycloakAuthService";

import LoadingPage from "./components/LoadingPage";
import { ConfigServiceProvider } from "./config";
import en from "./locales/en.json";
import { Routing } from "./pages/routes";
import { theme } from "./theme";

const StyleProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <ThemeProvider theme={theme}>{children}</ThemeProvider>
);

const Services: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <FeatureService features={defaultOssFeatures} instanceConfig={useGetInstanceConfiguration()}>
    <KeycloakAuthService>
      <NotificationService>
        <ConfirmationModalService>
          <ModalServiceProvider>
            <FormChangeTrackerService>
              <ConnectorBuilderTestInputProvider>
                <HelmetProvider>{children}</HelmetProvider>
              </ConnectorBuilderTestInputProvider>
            </FormChangeTrackerService>
          </ModalServiceProvider>
        </ConfirmationModalService>
      </NotificationService>
    </KeycloakAuthService>
  </FeatureService>
);

const App: React.FC = () => {
  return (
    <React.StrictMode>
      <AirbyteThemeProvider>
        <StyleProvider>
          <I18nProvider locale="en" messages={en}>
            <QueryProvider>
              <ServicesProvider>
                <Suspense fallback={<LoadingPage />}>
                  <ConfigServiceProvider config={config}>
                    <Router>
                      <AnalyticsProvider>
                        <AppMonitoringServiceProvider>
                          <ApiErrorBoundary>
                            <Services>
                              <Routing />
                            </Services>
                          </ApiErrorBoundary>
                        </AppMonitoringServiceProvider>
                      </AnalyticsProvider>
                    </Router>
                  </ConfigServiceProvider>
                </Suspense>
              </ServicesProvider>
            </QueryProvider>
          </I18nProvider>
        </StyleProvider>
      </AirbyteThemeProvider>
    </React.StrictMode>
  );
};

export default App;
