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
import { ConfigServiceProvider, config } from "../src/core/config";
import { DocumentationPanelProvider } from "../src/views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";
import { AppMonitoringServiceProvider } from "../src/hooks/services/AppMonitoringService";
import { AirbyteThemeProvider } from "../src/hooks/theme/useAirbyteTheme";

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
      <QueryClientProvider client={queryClient}>
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
      </QueryClientProvider>
    </AirbyteThemeProvider>
  </React.Suspense>
);
