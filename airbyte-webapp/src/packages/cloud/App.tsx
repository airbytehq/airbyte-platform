import React, { Suspense } from "react";
import { HelmetProvider } from "react-helmet-async";
import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import { DeployPreviewMessage } from "components/DeployPreviewMessage";
import { DevToolsToggle } from "components/DevToolsToggle";
import LoadingPage from "components/LoadingPage";

import { QueryProvider } from "core/api";
import { ConfigServiceProvider, config } from "core/config";
import { AnalyticsProvider } from "core/services/analytics";
import { defaultCloudFeatures, FeatureService } from "core/services/features";
import { I18nProvider } from "core/services/i18n";
import { BlockerService } from "core/services/navigation";
import { isDevelopment } from "core/utils/isDevelopment";
import { AppMonitoringServiceProvider } from "hooks/services/AppMonitoringService";
import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { FormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { ModalServiceProvider } from "hooks/services/Modal";
import { NotificationService } from "hooks/services/Notification";
import { AirbyteThemeProvider } from "hooks/theme/useAirbyteTheme";
import en from "locales/en.json";
import { Routing } from "packages/cloud/cloudRoutes";

import { AppServicesProvider } from "./services/AppServicesProvider";
import { ZendeskProvider } from "./services/thirdParty/zendesk";

const Services: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <NotificationService>
    <ConfirmationModalService>
      <FormChangeTrackerService>
        <FeatureService features={defaultCloudFeatures}>
          <AppServicesProvider>
            <ModalServiceProvider>
              <HelmetProvider>
                <ZendeskProvider>{children}</ZendeskProvider>
              </HelmetProvider>
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
        <I18nProvider locale="en" messages={en}>
          <QueryProvider>
            <BlockerService>
              <Suspense fallback={<LoadingPage />}>
                <ConfigServiceProvider config={config}>
                  <ApiErrorBoundary>
                    <AnalyticsProvider>
                      <AppMonitoringServiceProvider>
                        <Services>
                          <DeployPreviewMessage />
                          <Routing />
                        </Services>
                      </AppMonitoringServiceProvider>
                    </AnalyticsProvider>
                  </ApiErrorBoundary>
                </ConfigServiceProvider>
              </Suspense>
            </BlockerService>
          </QueryProvider>
        </I18nProvider>
      </AirbyteThemeProvider>
      {isDevelopment() && <DevToolsToggle />}
    </React.StrictMode>
  );
};

const router = createBrowserRouter([{ path: "*", element: <App /> }]);

const CloudApp: React.FC = () => <RouterProvider router={router} />;
export default React.memo(CloudApp);
