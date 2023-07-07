import React, { Suspense } from "react";
import { HelmetProvider } from "react-helmet-async";
import { BrowserRouter as Router } from "react-router-dom";
import { ThemeProvider } from "styled-components";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";

import { config } from "config";
import { QueryProvider } from "core/api";
import { I18nProvider } from "core/i18n";
import { AnalyticsProvider } from "core/services/analytics";
import { defaultOssFeatures, FeatureService } from "core/services/features";
import { ServicesProvider } from "core/servicesProvider";
import { AppMonitoringServiceProvider } from "hooks/services/AppMonitoringService";
import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { FormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { ModalServiceProvider } from "hooks/services/Modal";
import { NotificationService } from "hooks/services/Notification";
import { ConnectorBuilderTestInputProvider } from "services/connectorBuilder/ConnectorBuilderTestInputService";

import LoadingPage from "./components/LoadingPage";
import { ConfigServiceProvider } from "./config";
import en from "./locales/en.json";
import { Routing } from "./pages/routes";
import { theme } from "./theme";

const StyleProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <ThemeProvider theme={theme}>{children}</ThemeProvider>
);

const Services: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <AnalyticsProvider>
    <AppMonitoringServiceProvider>
      <ApiErrorBoundary>
        <FeatureService features={defaultOssFeatures}>
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
        </FeatureService>
      </ApiErrorBoundary>
    </AppMonitoringServiceProvider>
  </AnalyticsProvider>
);

const App: React.FC = () => {
  return (
    <React.StrictMode>
      <StyleProvider>
        <I18nProvider locale="en" messages={en}>
          <QueryProvider>
            <ServicesProvider>
              <Suspense fallback={<LoadingPage />}>
                <ConfigServiceProvider config={config}>
                  <Router>
                    <Services>
                      <Routing />
                    </Services>
                  </Router>
                </ConfigServiceProvider>
              </Suspense>
            </ServicesProvider>
          </QueryProvider>
        </I18nProvider>
      </StyleProvider>
    </React.StrictMode>
  );
};

export default App;
