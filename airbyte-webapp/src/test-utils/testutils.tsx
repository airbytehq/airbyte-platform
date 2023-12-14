import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, Queries, queries, render as rtlRender, RenderOptions, RenderResult } from "@testing-library/react";
import React, { Suspense } from "react";
import { IntlProvider } from "react-intl";
import { MemoryRouter } from "react-router-dom";
import { ThemeProvider } from "styled-components";

import {
  ConnectionStatus,
  DestinationRead,
  NamespaceDefinitionType,
  SourceRead,
  WebBackendConnectionRead,
} from "core/api/types/AirbyteClient";
import { ConfigContext, config } from "core/config";
import { AnalyticsProvider } from "core/services/analytics";
import { defaultOssFeatures, FeatureItem, FeatureService } from "core/services/features";
import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { ModalServiceProvider } from "hooks/services/Modal";
import { NotificationService } from "hooks/services/Notification";
import en from "locales/en.json";

interface WrapperProps {
  children?: React.ReactElement;
}

export async function render<
  Q extends Queries = typeof queries,
  Container extends Element | DocumentFragment = HTMLElement,
>(
  ui: React.ReactNode,
  renderOptions?: RenderOptions<Q, Container>,
  features?: FeatureItem[]
): Promise<RenderResult<Q, Container>> {
  const Wrapper = ({ children }: WrapperProps) => {
    return (
      <TestWrapper features={features}>
        <Suspense fallback={<div>testutils render fallback content</div>}>{children}</Suspense>
      </TestWrapper>
    );
  };

  let renderResult: RenderResult<Q, Container>;
  await act(async () => {
    renderResult = rtlRender<Q, Container>(<div>{ui}</div>, { wrapper: Wrapper, ...renderOptions });
  });

  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  return renderResult!;
}

interface TestWrapperOptions {
  features?: FeatureItem[];
}
export const TestWrapper: React.FC<React.PropsWithChildren<TestWrapperOptions>> = ({
  children,
  features = defaultOssFeatures,
}) => (
  <ThemeProvider theme={{}}>
    <IntlProvider locale="en" messages={en} onError={() => null}>
      <ConfigContext.Provider value={{ config }}>
        <AnalyticsProvider>
          <NotificationService>
            <FeatureService features={features}>
              <ModalServiceProvider>
                <ConfirmationModalService>
                  <QueryClientProvider client={new QueryClient()}>
                    <MemoryRouter>{children}</MemoryRouter>
                  </QueryClientProvider>
                </ConfirmationModalService>
              </ModalServiceProvider>
            </FeatureService>
          </NotificationService>
        </AnalyticsProvider>
      </ConfigContext.Provider>
    </IntlProvider>
  </ThemeProvider>
);

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyFunction = (...args: any[]) => any;

/**
 * Casts a function to be jest mocked. This does not actually mock the function.
 * It's just a helper function in case you need to tell TypeScript that a reference
 * is already mocked.
 */
export const mocked = <T extends AnyFunction>(input: T): jest.MockedFunction<T> => input as jest.MockedFunction<T>;

export const useMockIntersectionObserver = () => {
  // IntersectionObserver isn't available in test environment but is used by the dialog component
  const mockIntersectionObserver = jest.fn();
  mockIntersectionObserver.mockReturnValue({
    observe: jest.fn().mockReturnValue(null),
    unobserve: jest.fn().mockReturnValue(null),
    disconnect: jest.fn().mockReturnValue(null),
  });
  window.IntersectionObserver = mockIntersectionObserver;
};

export const mockSource: SourceRead = {
  sourceId: "test-source",
  name: "test source",
  sourceName: "test-source-name",
  workspaceId: "test-workspace-id",
  sourceDefinitionId: "test-source-definition-id",
  connectionConfiguration: undefined,
};

export const mockDestination: DestinationRead = {
  destinationId: "test-destination",
  name: "test destination",
  destinationName: "test destination name",
  workspaceId: "test-workspace-id",
  destinationDefinitionId: "test-destination-definition-id",
  connectionConfiguration: undefined,
};

export const mockConnection: WebBackendConnectionRead = {
  connectionId: "test-connection",
  name: "test connection",
  prefix: "test",
  sourceId: "test-source",
  destinationId: "test-destination",
  status: ConnectionStatus.active,
  schedule: undefined,
  syncCatalog: {
    streams: [],
  },
  namespaceDefinition: NamespaceDefinitionType.source,
  namespaceFormat: "",
  operationIds: [],
  source: mockSource,
  destination: mockDestination,
  operations: [],
  catalogId: "",
  isSyncing: false,
  schemaChange: "no_change",
  notifySchemaChanges: true,
  notifySchemaChangesByEmail: false,
  nonBreakingChangesPreference: "ignore",
};
