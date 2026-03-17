import React, { Suspense } from "react";
import { HelmetProvider } from "react-helmet-async";
import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { DeployPreviewMessage } from "components/ui/DeployPreviewMessage";
import { DevToolsToggle } from "components/ui/DevToolsToggle";
import LoadingPage from "components/ui/LoadingPage";
import { SupportAgentFloatingButton } from "components/ui/support";

import { Routing } from "cloud/cloudRoutes";
import { SupportAgentServiceProvider } from "cloud/services/supportAgent";
import { QueryProvider } from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { AnalyticsProvider } from "core/services/analytics";
import { HockeyStackAnalytics } from "core/services/analytics/HockeyStackAnalytics";
import { ConfirmationModalService } from "core/services/ConfirmationModal";
import { defaultCloudFeatures, FeatureService } from "core/services/features";
import { FormChangeTrackerService } from "core/services/FormChangeTracker";
import { I18nProvider } from "core/services/i18n";
import { ModalServiceProvider } from "core/services/Modal";
import { BlockerService } from "core/services/navigation";
import { NotificationService } from "core/services/Notification";
import { DrawerContextProvider } from "core/services/ui/DrawerService";
import { isDevelopment } from "core/utils/isDevelopment";
import { AirbyteThemeProvider } from "core/utils/useAirbyteTheme";

import { CloudAuthService } from "./services/auth/CloudAuthService";
import { FullStoryGuidesProvider } from "./services/thirdParty/fullstoryGuides/FullStoryGuidesProvider";

const Services: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <NotificationService>
    <ConfirmationModalService>
      <FormChangeTrackerService>
        <FeatureService features={defaultCloudFeatures}>
          <CloudAuthService>
            <ModalServiceProvider>
              <DrawerContextProvider>
                <HelmetProvider>
                  <FullStoryGuidesProvider>
                    <SupportAgentServiceProvider>
                      {children}
                      <SupportAgentFloatingButton />
                    </SupportAgentServiceProvider>
                  </FullStoryGuidesProvider>
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
                      <Services>
                        <DeployPreviewMessage />
                        <Routing />
                      </Services>
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
