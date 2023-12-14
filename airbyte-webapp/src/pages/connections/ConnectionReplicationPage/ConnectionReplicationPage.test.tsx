import { render as tlr, act } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React, { Suspense } from "react";
import { VirtuosoMockContext } from "react-virtuoso";

import { mockConnection } from "test-utils/mock-data/mockConnection";
import {
  mockDestinationDefinition,
  mockDestinationDefinitionSpecification,
  mockDestinationDefinitionVersion,
} from "test-utils/mock-data/mockDestination";
import {
  mockSourceDefinition,
  mockSourceDefinitionSpecification,
  mockSourceDefinitionVersion,
} from "test-utils/mock-data/mockSource";
import { mockTheme } from "test-utils/mock-data/mockTheme";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";
import { mockWorkspaceId } from "test-utils/mock-data/mockWorkspaceId";
import { TestWrapper, useMockIntersectionObserver } from "test-utils/testutils";

import { useGetConnectionQuery } from "core/api";
import { WebBackendConnectionUpdate } from "core/api/types/AirbyteClient";
import { defaultOssFeatures, FeatureItem } from "core/services/features";
import { ConnectionEditServiceProvider } from "hooks/services/ConnectionEdit/ConnectionEditService";

import { ConnectionReplicationPage } from "./ConnectionReplicationPage";

jest.setTimeout(40000);

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: () => mockWorkspaceId,
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
  useGetConnectionQuery: jest.fn(() => async () => mockConnection),
  useGetConnection: () => mockConnection,
  useGetStateTypeQuery: () => async () => "global",
  useUpdateConnection: () => ({
    mutateAsync: async (connection: WebBackendConnectionUpdate) => connection,
    isLoading: false,
  }),
  useSourceDefinitionVersion: () => mockSourceDefinitionVersion,
  useDestinationDefinitionVersion: () => mockDestinationDefinitionVersion,
  useGetSourceDefinitionSpecification: () => mockSourceDefinitionSpecification,
  useGetDestinationDefinitionSpecification: () => mockDestinationDefinitionSpecification,
  useSourceDefinition: () => mockSourceDefinition,
  useDestinationDefinition: () => mockDestinationDefinition,
  LogsRequestError: jest.requireActual("core/api/errors").LogsRequestError,
}));

jest.mock("hooks/theme/useAirbyteTheme", () => ({
  useAirbyteTheme: () => mockTheme,
}));

describe("ConnectionReplicationPage", () => {
  const Wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
    <Suspense fallback={<div>I should not show up in a snapshot</div>}>
      <TestWrapper>
        <ConnectionEditServiceProvider connectionId={mockConnection.connectionId}>
          <VirtuosoMockContext.Provider value={{ viewportHeight: 1000, itemHeight: 50 }}>
            {children}
          </VirtuosoMockContext.Provider>
        </ConnectionEditServiceProvider>
      </TestWrapper>
    </Suspense>
  );

  const render = async () => {
    let renderResult: ReturnType<typeof tlr>;

    await act(async () => {
      renderResult = tlr(
        <Wrapper>
          <ConnectionReplicationPage />
        </Wrapper>
      );
    });
    return renderResult!;
  };

  const setupSpies = (getConnection?: () => Promise<void>) => {
    (useGetConnectionQuery as jest.Mock).mockImplementation(() => getConnection);
  };

  beforeEach(() => {
    useMockIntersectionObserver();
  });

  it("should render", async () => {
    setupSpies();

    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
  });

  it("should show an error if there is a schemaError", async () => {
    setupSpies(() => Promise.reject("Test Error"));

    const renderResult = await render();

    await act(async () => {
      renderResult.queryByText("Refresh source schema")?.click();
    });
    expect(renderResult).toMatchSnapshot();
  });

  it("should show loading if the schema is refreshing", async () => {
    // Return pending promise
    setupSpies(() => new Promise(() => null));

    const renderResult = await render();
    await act(async () => {
      renderResult.queryByText("Refresh source schema")?.click();
    });

    await act(async () => {
      expect(renderResult.findByText("We are fetching the schema of your data source.", { exact: false })).toBeTruthy();
    });
  });

  describe("cron expression validation", () => {
    const INVALID_CRON_EXPRESSION = "invalid cron expression";
    const CRON_EXPRESSION_EVERY_MINUTE = "* * * * * * ?";

    it("should display an error for an invalid cron expression", async () => {
      setupSpies();
      const renderResult = await render();

      await userEvent.click(renderResult.getByTestId("configuration-card-expand-arrow"));

      await userEvent.click(renderResult.getByTestId("schedule-type-listbox-button"));
      await userEvent.click(renderResult.getByTestId("cron-option"));

      const cronExpressionInput = renderResult.getByTestId("cronExpression");

      await userEvent.clear(cronExpressionInput);
      await userEvent.type(cronExpressionInput, INVALID_CRON_EXPRESSION, { delay: 1 });

      const errorMessage = renderResult.getByText(/must contain at least 6 fields/);

      expect(errorMessage).toBeInTheDocument();
    });

    it("should allow cron expressions under one hour when feature enabled", async () => {
      setupSpies();

      const renderResult = await render();

      await userEvent.click(renderResult.getByTestId("configuration-card-expand-arrow"));

      await userEvent.click(renderResult.getByTestId("schedule-type-listbox-button"));
      await userEvent.click(renderResult.getByTestId("cron-option"));

      const cronExpressionField = renderResult.getByTestId("cronExpression");

      await userEvent.clear(cronExpressionField);
      await userEvent.type(cronExpressionField, CRON_EXPRESSION_EVERY_MINUTE, { delay: 1 });

      const errorMessage = renderResult.queryByTestId("cronExpressionError");

      expect(errorMessage).not.toBeInTheDocument();
    });

    it("should not allow cron expressions under one hour when feature not enabled", async () => {
      setupSpies();

      const featuresToInject = defaultOssFeatures.filter((f) => f !== FeatureItem.AllowSyncSubOneHourCronExpressions);

      const container = tlr(
        <TestWrapper features={featuresToInject}>
          <ConnectionEditServiceProvider connectionId={mockConnection.connectionId}>
            <ConnectionReplicationPage />
          </ConnectionEditServiceProvider>
        </TestWrapper>
      );

      await userEvent.click(container.getByTestId("configuration-card-expand-arrow"));

      await userEvent.click(container.getByTestId("schedule-type-listbox-button"));
      await userEvent.click(container.getByTestId("cron-option"));

      const cronExpressionField = container.getByTestId("cronExpression");

      await userEvent.clear(cronExpressionField);
      await userEvent.type(cronExpressionField, CRON_EXPRESSION_EVERY_MINUTE, { delay: 1 });

      const errorMessage = container.getByTestId("cronExpressionError");

      expect(errorMessage).toBeInTheDocument();
    });
  });
});
