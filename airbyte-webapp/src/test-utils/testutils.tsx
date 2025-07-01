import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, Queries, queries, render as rtlRender, RenderOptions, RenderResult } from "@testing-library/react";
import React, { Suspense } from "react";
import { MemoryRouter } from "react-router-dom";

import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";
import { WebappConfigContextProvider } from "core/config";
import { defaultOssFeatures, FeatureItem, FeatureService } from "core/services/features";
import { I18nProvider } from "core/services/i18n";
import { ConfirmationModalService } from "hooks/services/ConfirmationModal";
import { ModalServiceProvider } from "hooks/services/Modal";
import { NotificationService } from "hooks/services/Notification";

import { mockWebappConfig } from "./mock-data/mockWebappConfig";

export async function render<
  Q extends Queries = typeof queries,
  Container extends Element | DocumentFragment = HTMLElement,
>(
  ui: React.ReactNode,
  renderOptions?: RenderOptions<Q, Container>,
  features?: FeatureItem[]
): Promise<RenderResult<Q, Container>> {
  const Wrapper: React.FC<React.PropsWithChildren> = ({ children }) => {
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
  route?: string;
}
export const TestWrapper: React.FC<React.PropsWithChildren<TestWrapperOptions>> = ({
  children,
  features = defaultOssFeatures,
  route,
}) => (
  <MemoryRouter initialEntries={route ? [route] : undefined}>
    <I18nProvider locale="en">
      <NotificationService>
        <FeatureService features={features}>
          <ModalServiceProvider>
            <ConfirmationModalService>
              <QueryClientProvider client={new QueryClient()}>
                <WebappConfigContextProvider config={mockWebappConfig}>{children}</WebappConfigContextProvider>
              </QueryClientProvider>
            </ConfirmationModalService>
          </ModalServiceProvider>
        </FeatureService>
      </NotificationService>
    </I18nProvider>
  </MemoryRouter>
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
  connectionConfiguration: {},
  createdAt: 966690000,
};

export const mockDestination: DestinationRead = {
  destinationId: "test-destination",
  name: "test destination",
  destinationName: "test destination name",
  workspaceId: "test-workspace-id",
  destinationDefinitionId: "test-destination-definition-id",
  connectionConfiguration: {},
  createdAt: 966690000,
};
