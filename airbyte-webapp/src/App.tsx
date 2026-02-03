import React, { Suspense } from "react";
import { HelmetProvider } from "react-helmet-async";
import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { DevToolsToggle } from "components/ui/DevToolsToggle";
import LoadingPage from "components/ui/LoadingPage";

import { QueryProvider, useGetInstanceConfiguration } from "core/api";
import {
  InstanceConfigurationResponseEdition,
  InstanceConfigurationResponseTrackingStrategy,
} from "core/api/types/AirbyteClient";
import { DefaultErrorBoundary } from "core/errors";
import { AnalyticsProvider } from "core/services/analytics";
import { OSSAuthService } from "core/services/auth";
import { ConfirmationModalService } from "core/services/ConfirmationModal";
import { defaultOssFeatures, defaultEnterpriseFeatures, FeatureService } from "core/services/features";
import { FormChangeTrackerService } from "core/services/FormChangeTracker";
import { I18nProvider } from "core/services/i18n";
import { ModalServiceProvider } from "core/services/Modal";
import { BlockerService } from "core/services/navigation";
import { NotificationService } from "core/services/Notification";
import { DrawerContextProvider } from "core/services/ui/DrawerService";
import { isDevelopment } from "core/utils/isDevelopment";
import { AirbyteThemeProvider } from "core/utils/useAirbyteTheme";

import { Routing } from "./pages/routes";

const Services: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const instanceConfig = useGetInstanceConfiguration();
  const instanceFeatures =
    instanceConfig.edition === InstanceConfigurationResponseEdition.community
      ? defaultOssFeatures
      : defaultEnterpriseFeatures;

  return (
    <AnalyticsProvider
      disableSegment={instanceConfig.trackingStrategy !== InstanceConfigurationResponseTrackingStrategy.segment}
    >
      <FeatureService features={instanceFeatures} instanceConfig={instanceConfig}>
        <NotificationService>
          <OSSAuthService>
            <ConfirmationModalService>
              <ModalServiceProvider>
                <DrawerContextProvider>
                  <FormChangeTrackerService>
                    <HelmetProvider>{children}</HelmetProvider>
                  </FormChangeTrackerService>
                </DrawerContextProvider>
              </ModalServiceProvider>
            </ConfirmationModalService>
          </OSSAuthService>
        </NotificationService>
      </FeatureService>
    </AnalyticsProvider>
  );
};

const App: React.FC = () => {
  return (
    <>
      <AirbyteThemeProvider>
        <I18nProvider>
          <QueryProvider>
            <BlockerService>
              <Suspense fallback={<LoadingPage />}>
                <DefaultErrorBoundary>
                  <Services>
                    <Routing />
                  </Services>
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

const OssApp: React.FC = () => <RouterProvider router={router} />;
export default React.memo(OssApp);
