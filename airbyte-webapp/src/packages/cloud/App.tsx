import React, { Suspense } from "react";
import { HelmetProvider } from "react-helmet-async";
import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { DeployPreviewMessage } from "components/DeployPreviewMessage";
import { DevToolsToggle } from "components/DevToolsToggle";
import LoadingPage from "components/LoadingPage";

import { QueryProvider } from "core/api";
import { DefaultErrorBoundary } from "core/errors";
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
                <DefaultErrorBoundary>
                  <AnalyticsProvider>
                    <AppMonitoringServiceProvider>
                      <Services>
                        <DeployPreviewMessage />
                        <Routing />
                      </Services>
                    </AppMonitoringServiceProvider>
                  </AnalyticsProvider>
                </DefaultErrorBoundary>
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
