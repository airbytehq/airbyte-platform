import React from "react";
import { Decorator } from "@storybook/react";

import { MemoryRouter } from "react-router-dom";
import { IntlProvider } from "react-intl";
import { QueryClientProvider, QueryClient } from "@tanstack/react-query";

import messages from "../src/locales/en.json";
import { FeatureService } from "../src/core/services/features";
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
            <DocumentationPanelProvider>
              <AppMonitoringServiceProvider>
                <FeatureService features={[]}>{getStory()}</FeatureService>
              </AppMonitoringServiceProvider>
            </DocumentationPanelProvider>
          </IntlProvider>
        </MemoryRouter>
      </QueryClientProvider>
    </AirbyteThemeProvider>
  </React.Suspense>
);
