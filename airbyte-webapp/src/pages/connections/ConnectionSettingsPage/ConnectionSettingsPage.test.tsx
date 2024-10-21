import { render as tlr, act } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Suspense } from "react";

import { mockConnection } from "test-utils/mock-data/mockConnection";
import {
  mockDestinationDefinition,
  mockDestinationDefinitionSpecification,
  mockDestinationDefinitionVersion,
} from "test-utils/mock-data/mockDestination";
import { mockJobList } from "test-utils/mock-data/mockJobsList";
import {
  mockSourceDefinition,
  mockSourceDefinitionSpecification,
  mockSourceDefinitionVersion,
} from "test-utils/mock-data/mockSource";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";
import { TestWrapper } from "test-utils/testutils";

import { useGetConnectionQuery } from "core/api";
import { WebBackendConnectionUpdate } from "core/api/types/AirbyteClient";
import { defaultOssFeatures, FeatureItem } from "core/services/features";
import { ConnectionEditServiceProvider } from "hooks/services/ConnectionEdit/ConnectionEditService";

import { ConnectionSettingsPage } from "./ConnectionSettingsPage";

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
  useGetConnectionQuery: jest.fn(() => async () => mockConnection),
  useGetConnection: () => mockConnection,
  useCurrentConnection: () => mockConnection,
  useUpdateConnection: () => ({
    mutateAsync: async (connection: WebBackendConnectionUpdate) => connection,
    isLoading: false,
  }),
  useGetConnectionSyncProgress: () => {
    return [];
  },
  useSourceDefinitionVersion: () => mockSourceDefinitionVersion,
  useDestinationDefinitionVersion: () => mockDestinationDefinitionVersion,
  useGetSourceDefinitionSpecification: () => mockSourceDefinitionSpecification,
  useGetDestinationDefinitionSpecification: () => mockDestinationDefinitionSpecification,
  useSourceDefinition: () => mockSourceDefinition,
  useDestinationDefinition: () => mockDestinationDefinition,
  useDeleteConnection: () => ({
    mutateAsync: jest.fn(),
  }),
  useClearConnection: () => ({
    mutateAsync: jest.fn(),
  }),
  useListJobsForConnectionStatus: () => mockJobList,
  useSyncConnection: () => ({
    mutateAsync: jest.fn(),
    isLoading: false,
  }),
  useCancelJob: () => ({
    mutateAsync: jest.fn(),
    isLoading: false,
  }),
  useRefreshConnectionStreams: () => ({
    mutateAsync: jest.fn(),
    isLoading: false,
  }),
  useClearConnectionStream: () => ({
    mutateAsync: jest.fn(),
    isLoading: false,
  }),
  useListStreamsStatuses: () => [],
}));

jest.mock("core/utils/rbac", () => ({
  useIntent: () => true,
  useGeneratedIntent: () => true,
  Intent: {
    RunAndCancelConnectionSyncAndRefresh: "RunAndCancelConnectionSyncAndRefresh",
  },
}));

jest.mock("components/connection/ConnectionStatus/useConnectionStatus", () => ({
  useConnectionStatus: () => ({
    status: "pending",
    lastSyncJobStatus: undefined,
    lastSuccessfulSync: undefined,
    nextSync: undefined,
    isRunning: false,
  }),
}));

const setupSpies = (getConnection?: () => Promise<void>) => {
  (useGetConnectionQuery as jest.Mock).mockImplementation(() => getConnection);
};

describe("ConnectionSettingsPage", () => {
  const Wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
    <Suspense fallback={<div>I should not show up in a snapshot</div>}>
      <TestWrapper>
        <ConnectionEditServiceProvider connectionId={mockConnection.connectionId}>
          {children}
        </ConnectionEditServiceProvider>
      </TestWrapper>
    </Suspense>
  );

  const render = async () => {
    let renderResult: ReturnType<typeof tlr>;

    await act(async () => {
      renderResult = tlr(
        <Wrapper>
          <ConnectionSettingsPage />
        </Wrapper>
      );
    });
    return renderResult!;
  };

  describe("cron expression validation", () => {
    const INVALID_CRON_EXPRESSION = "invalid cron expression";
    const CRON_EXPRESSION_EVERY_MINUTE = "* * * * * * ?";

    it("should display an error for an invalid cron expression", async () => {
      setupSpies();
      const renderResult = await render();

      await userEvent.click(renderResult.getByTestId("advanced-settings-button"));

      await userEvent.click(renderResult.getByTestId("schedule-type-listbox-button"));
      await userEvent.click(renderResult.getByTestId("cron-option"));

      const cronExpressionInput = renderResult.getByTestId("cronExpression");

      await userEvent.clear(cronExpressionInput);
      await userEvent.type(cronExpressionInput, INVALID_CRON_EXPRESSION, { delay: 1 });

      const errorMessage = await renderResult.findByText(/invalid cron expression/i);

      expect(errorMessage).toBeInTheDocument();
    });

    it("should allow cron expressions under one hour when feature enabled", async () => {
      setupSpies();

      const renderResult = await render();

      await userEvent.click(renderResult.getByTestId("advanced-settings-button"));

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
            <ConnectionSettingsPage />
          </ConnectionEditServiceProvider>
        </TestWrapper>
      );

      await userEvent.click(container.getByTestId("advanced-settings-button"));

      await userEvent.click(container.getByTestId("schedule-type-listbox-button"));
      await userEvent.click(container.getByTestId("cron-option"));

      const cronExpressionField = container.getByTestId("cronExpression");

      await userEvent.clear(cronExpressionField);
      await userEvent.type(cronExpressionField, CRON_EXPRESSION_EVERY_MINUTE, { delay: 1 });

      const errorMessage = await container.findByTestId("cronExpressionError");

      expect(errorMessage).toBeInTheDocument();
    });
  });
});
