import React, { Suspense } from "react";
import { HelmetProvider } from "react-helmet-async";
import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { DeployPreviewMessage } from "components/DeployPreviewMessage";
import { DevToolsToggle } from "components/DevToolsToggle";
import LoadingPage from "components/LoadingPage";

import { QueryProvider } from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { AnalyticsProvider } from "core/services/analytics";
import { HockeyStackAnalytics } from "core/services/analytics/HockeyStackAnalytics";
import { PostHogAnalytics } from "core/services/analytics/PostHogAnalytics";
import { defaultCloudFeatures, FeatureService } from "core/services/features";
import { I18nProvider } from "core/services/i18n";
import { BlockerService } from "core/services/navigation";
import { DrawerContextProvider } from "core/services/ui/DrawerService";
import { isDevelopment } from "core/utils/isDevelopment";
import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { FormChangeTrackerService } from "hooks/services/FormChangeTracker";
import { ModalServiceProvider } from "hooks/services/Modal";
import { NotificationService } from "hooks/services/Notification";
import { AirbyteThemeProvider } from "hooks/theme/useAirbyteTheme";
import { Routing } from "packages/cloud/cloudRoutes";

import { CloudAuthService } from "./services/auth/CloudAuthService";
import { ZendeskProvider } from "./services/thirdParty/zendesk";

const Services: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <NotificationService>
    <ConfirmationModalService>
      <FormChangeTrackerService>
        <FeatureService features={defaultCloudFeatures}>
          <CloudAuthService>
            <ModalServiceProvider>
              <DrawerContextProvider>
                <HelmetProvider>
                  <ZendeskProvider>{children}</ZendeskProvider>
                </HelmetProvider>
              </DrawerContextProvider>
            </ModalServiceProvider>
          </CloudAuthService>
        </FeatureService>
      </FormChangeTrackerService>
    </ConfirmationModalService>
  </NotificationService>
);

const App: React.FC = () => {
  return (
    <>
      <AirbyteThemeProvider>
        <I18nProvider>
          <QueryProvider>
            <BlockerService>
              <Suspense fallback={<LoadingPage />}>
                <DefaultErrorBoundary>
                  <AnalyticsProvider>
                    <HockeyStackAnalytics>
                      <PostHogAnalytics>
                        <Services>
                          <DeployPreviewMessage />
                          <Routing />
                        </Services>
                      </PostHogAnalytics>
                    </HockeyStackAnalytics>
                  </AnalyticsProvider>
                </DefaultErrorBoundary>
              </Suspense>
            </BlockerService>
          </QueryProvider>
        </I18nProvider>
      </AirbyteThemeProvider>
      {isDevelopment() && <DevToolsToggle />}
    </>
  );
};

const router = createBrowserRouter([{ path: "*", element: <App /> }]);

const CloudApp: React.FC = () => <RouterProvider router={router} />;
export default React.memo(CloudApp);
