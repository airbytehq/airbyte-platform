import React from "react";
import { Decorator } from "@storybook/react";

import { MemoryRouter } from "react-router-dom";
import { IntlProvider } from "react-intl";
import { ThemeProvider } from "styled-components";
import { QueryClientProvider, QueryClient } from "@tanstack/react-query";

// TODO: theme was not working correctly so imported directly
import { theme } from "../src/theme";
import messages from "../src/locales/en.json";
import { FeatureService } from "../src/core/services/features";
import { ConfigServiceProvider, config } from "../src/config";
import { DocumentationPanelProvider } from "../src/views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";
import { ServicesProvider } from "../src/core/servicesProvider";
import { analyticsServiceContext } from "../src/core/services/analytics";
import { AppMonitoringServiceProvider } from "../src/hooks/services/AppMonitoringService";
import type { AnalyticsService } from "../src/core/services/analytics";
import { AirbyteThemeProvider } from "../src/hooks/theme/useAirbyteTheme";

const analyticsContextMock: AnalyticsService = {
  track: () => {},
  setContext: () => {},
  removeFromContext: () => {},
} as unknown as AnalyticsService;

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
      suspense: true,
    },
  },
});

export const withProviders = (getStory: Parameters<Decorator>[0]) => (
  <React.Suspense fallback={null}>
    <AirbyteThemeProvider>
      <analyticsServiceContext.Provider value={analyticsContextMock}>
        <QueryClientProvider client={queryClient}>
          <ServicesProvider>
            <MemoryRouter>
              <IntlProvider
                messages={messages}
                locale={"en"}
                defaultRichTextElements={{
                  b: (chunk) => <strong>{chunk}</strong>,
                }}
              >
                <ThemeProvider theme={theme}>
                  <ConfigServiceProvider config={config}>
                    <DocumentationPanelProvider>
                      <AppMonitoringServiceProvider>
                        <FeatureService features={[]}>{getStory()}</FeatureService>
                      </AppMonitoringServiceProvider>
                    </DocumentationPanelProvider>
                  </ConfigServiceProvider>
                </ThemeProvider>
              </IntlProvider>
            </MemoryRouter>
          </ServicesProvider>
        </QueryClientProvider>
      </analyticsServiceContext.Provider>
    </AirbyteThemeProvider>
  </React.Suspense>
);
