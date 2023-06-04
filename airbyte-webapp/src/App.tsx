import React, {createContext, Suspense, useState} from "react";
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
import { WorkspaceServiceProvider } from "./services/workspaces/WorkspacesService";
import { theme } from "./theme";
import {GlobalStylesDark, GlobalStylesLight} from "./scss/_all-themes";

const StyleProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <ThemeProvider theme={theme}>{children}</ThemeProvider>
);

export const darkModeContext = createContext(null);

const Services: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <AnalyticsProvider>
    <AppMonitoringServiceProvider>
      <ApiErrorBoundary>
        <WorkspaceServiceProvider>
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
        </WorkspaceServiceProvider>
      </ApiErrorBoundary>
    </AppMonitoringServiceProvider>
  </AnalyticsProvider>
);

const App: React.FC = () => {
    const [inDarkMode, setInDarkMode] = useState(false);

    return (
    <React.StrictMode>
        {inDarkMode ? <GlobalStylesDark/> : <GlobalStylesLight/>}

      <StyleProvider>
        <I18nProvider locale="en" messages={en}>
          <QueryProvider>
            <ServicesProvider>

                <darkModeContext.Provider value={{ inDarkMode, setInDarkMode }}>

                    <Suspense fallback={<LoadingPage />}>
                        <ConfigServiceProvider config={config}>
                            <Router>
                                <Services>
                                    <Routing />
                                </Services>
                            </Router>
                        </ConfigServiceProvider>
                    </Suspense>

                </darkModeContext.Provider>

            </ServicesProvider>
          </QueryProvider>
        </I18nProvider>
      </StyleProvider>


    </React.StrictMode>
  );
};

export default App;
