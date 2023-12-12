import { act, render as tlr } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
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
import { mocked, TestWrapper, useMockIntersectionObserver } from "test-utils/testutils";

import type { SchemaError } from "core/api";
import { useDiscoverSchema } from "core/api";
import { defaultOssFeatures, FeatureItem } from "core/services/features";

import { CreateConnectionForm } from "./CreateConnectionForm";

const mockBaseUseDiscoverSchema = {
  schemaErrorStatus: null,
  isLoading: false,
  schema: mockConnection.syncCatalog,
  catalogId: "",
  onDiscoverSchema: () => Promise.resolve(),
};

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: () => "workspace-id",
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => ({}),
  useInvalidateWorkspaceStateQuery: () => () => null,
  useCreateConnection: () => async () => null,
  useSourceDefinitionVersion: () => mockSourceDefinitionVersion,
  useDestinationDefinitionVersion: () => mockDestinationDefinitionVersion,
  useGetSourceDefinitionSpecification: () => mockSourceDefinitionSpecification,
  useGetDestinationDefinitionSpecification: () => mockDestinationDefinitionSpecification,
  useSourceDefinition: () => mockSourceDefinition,
  useDestinationDefinition: () => mockDestinationDefinition,
  useDiscoverSchema: jest.fn(() => mockBaseUseDiscoverSchema),
  LogsRequestError: jest.requireActual("core/api/errors").LogsRequestError,
}));

jest.mock("area/connector/utils", () => ({
  useGetSourceFromSearchParams: () => mockConnection.source,
  useGetDestinationFromSearchParams: () => mockConnection.destination,
  ConnectorIds: jest.requireActual("area/connector/utils").ConnectorIds,
}));

jest.mock("hooks/theme/useAirbyteTheme", () => ({
  useAirbyteTheme: () => mockTheme,
}));

jest.setTimeout(40000);

describe("CreateConnectionForm", () => {
  const Wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
    <TestWrapper>
      <VirtuosoMockContext.Provider value={{ viewportHeight: 1000, itemHeight: 50 }}>
        {children}
      </VirtuosoMockContext.Provider>
    </TestWrapper>
  );
  const render = async () => {
    let renderResult: ReturnType<typeof tlr>;

    await act(async () => {
      renderResult = tlr(
        <Wrapper>
          <CreateConnectionForm />
        </Wrapper>
      );
    });
    return renderResult!;
  };

  beforeEach(() => {
    useMockIntersectionObserver();
  });

  it("should render", async () => {
    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
    expect(renderResult.queryByText("Please wait a little bit more…")).toBeFalsy();
  });

  it("should render when loading", async () => {
    mocked(useDiscoverSchema).mockImplementationOnce(() => ({ ...mockBaseUseDiscoverSchema, isLoading: true }));
    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
  });

  it("should render with an error", async () => {
    mocked(useDiscoverSchema).mockImplementationOnce(() => ({
      ...mockBaseUseDiscoverSchema,
      schemaErrorStatus: new Error("Test Error") as SchemaError,
    }));

    const renderResult = await render();
    expect(renderResult).toMatchSnapshot();
  });

  describe("cron expression validation", () => {
    const INVALID_CRON_EXPRESSION = "invalid cron expression";
    const CRON_EXPRESSION_EVERY_MINUTE = "* * * * * * ?";

    it("should display an error for an invalid cron expression", async () => {
      const container = tlr(
        <TestWrapper>
          <CreateConnectionForm />
        </TestWrapper>
      );

      await userEvent.click(container.getByTestId("schedule-type-listbox-button"));
      await userEvent.click(container.getByTestId("cron-option"));

      const cronExpressionInput = container.getByTestId("cronExpression");

      await userEvent.clear(cronExpressionInput);
      await userEvent.type(cronExpressionInput, INVALID_CRON_EXPRESSION, { delay: 1 });

      const errorMessage = container.getByText(/must contain at least 6 fields/);

      expect(errorMessage).toBeInTheDocument();
    });

    it("should allow cron expressions under one hour when feature enabled", async () => {
      const container = tlr(
        <TestWrapper>
          <CreateConnectionForm />
        </TestWrapper>
      );

      await userEvent.click(container.getByTestId("schedule-type-listbox-button"));
      await userEvent.click(container.getByTestId("cron-option"));

      const cronExpressionField = container.getByTestId("cronExpression");

      await userEvent.clear(cronExpressionField);
      await userEvent.type(cronExpressionField, CRON_EXPRESSION_EVERY_MINUTE, { delay: 1 });

      const errorMessage = container.queryByTestId("cronExpressionError");

      expect(errorMessage).not.toBeInTheDocument();
    });

    it("should not allow cron expressions under one hour when feature not enabled", async () => {
      const featuresToInject = defaultOssFeatures.filter((f) => f !== FeatureItem.AllowSyncSubOneHourCronExpressions);

      const container = tlr(
        <TestWrapper features={featuresToInject}>
          <CreateConnectionForm />
        </TestWrapper>
      );

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
