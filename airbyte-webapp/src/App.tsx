import React, { Suspense } from "react";
import { HelmetProvider } from "react-helmet-async";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import { ThemeProvider } from "styled-components";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import { DevToolsToggle } from "components/DevToolsToggle";

import { QueryProvider, useGetInstanceConfiguration } from "core/api";
import { config, ConfigServiceProvider } from "core/config";
import { AnalyticsProvider } from "core/services/analytics";
import { OSSAuthService } from "core/services/auth";
import { defaultOssFeatures, FeatureService } from "core/services/features";
import { I18nProvider } from "core/services/i18n";
import { BlockerService } from "core/services/navigation";
import { isDevelopment } from "core/utils/isDevelopment";
import { AppMonitoringServiceProvider } from "hooks/services/AppMonitoringService";
import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { FormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { ModalServiceProvider } from "hooks/services/Modal";
import { NotificationService } from "hooks/services/Notification";
import { AirbyteThemeProvider } from "hooks/theme/useAirbyteTheme";
import { ConnectorBuilderTestInputProvider } from "services/connectorBuilder/ConnectorBuilderTestInputService";

import LoadingPage from "./components/LoadingPage";
import en from "./locales/en.json";
import { Routing } from "./pages/routes";
import { theme } from "./theme";

const StyleProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <ThemeProvider theme={theme}>{children}</ThemeProvider>
);

const Services: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <FeatureService features={defaultOssFeatures} instanceConfig={useGetInstanceConfiguration()}>
    <NotificationService>
      <OSSAuthService>
        <ConfirmationModalService>
          <ModalServiceProvider>
            <FormChangeTrackerService>
              <ConnectorBuilderTestInputProvider>
                <HelmetProvider>{children}</HelmetProvider>
              </ConnectorBuilderTestInputProvider>
            </FormChangeTrackerService>
          </ModalServiceProvider>
        </ConfirmationModalService>
      </OSSAuthService>
    </NotificationService>
  </FeatureService>
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
      {isDevelopment() && <DevToolsToggle />}
    </React.StrictMode>
  );
};

const router = createBrowserRouter([{ path: "*", element: <App /> }]);

const OssApp: React.FC = () => <RouterProvider router={router} />;
export default React.memo(OssApp);
