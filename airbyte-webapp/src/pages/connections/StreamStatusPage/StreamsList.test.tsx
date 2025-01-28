import { render, screen } from "@testing-library/react";
import dayjs from "dayjs";
import { VirtuosoMockContext } from "react-virtuoso";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { StreamStatusType } from "components/connection/StreamStatusIndicator";
import { TestWrapper, mocked } from "test-utils";
import { mockConnection } from "test-utils/mock-data/mockConnection";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { useUiStreamStates } from "area/connection/utils/useUiStreamsStates";
import { ConnectionSyncStatus } from "core/api/types/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

import { StreamsList } from "./StreamsList";
import { StreamsListContextProvider } from "./StreamsListContext";

jest.mock("hooks/services/ConnectionEdit/ConnectionEditService");
jest.mock("hooks/services/ConnectionForm/ConnectionFormService", () => ({
  useConnectionFormService: () => ({
    connection: mockConnection,
  }),
}));
jest.mock("components/connection/ConnectionSync/ConnectionSyncContext", () => ({
  useConnectionSyncContext: () => ({
    syncConnection: jest.fn(),
    isSyncConnectionAvailable: true,
    syncStarting: false,
  }),
}));
jest.mock("components/connection/ConnectionStatus/useConnectionStatus");
jest.mock("core/api", () => ({
  useDestinationDefinitionVersion: () => ({ supportsRefreshes: true }),
  useListStreamsStatuses: () => [],
  useGetConnectionSyncProgress: () => ({ data: { streams: [] } }),
  useGetConnection: () => mockConnection,
  useCurrentConnection: () => mockConnection,
  useCurrentWorkspace: () => mockWorkspace,
}));
jest.mock("core/utils/rbac", () => ({
  useGeneratedIntent: () => true,
  Intent: {
    RunAndCancelConnectionSyncAndRefresh: "RunAndCancelConnectionSyncAndRefresh",
  },
}));
jest.mock("area/connection/utils/useUiStreamsStates");
jest.mock("area/connection/utils/useStreamsTableAnalytics");

describe("StreamsList", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const renderStreamsList = () =>
    render(
      <TestWrapper>
        <VirtuosoMockContext.Provider value={{ viewportHeight: 1000, itemHeight: 50 }}>
          <StreamsListContextProvider>
            <StreamsList />
          </StreamsListContextProvider>
        </VirtuosoMockContext.Provider>
      </TestWrapper>
    );

  it.each([
    {
      description: "one stream has synced before, one stream has not",
      mockStates: [
        {
          streamName: "test-stream-1",
          streamNameWithPrefix: "test-stream-1",
          streamNamespace: "test-namespace",
          status: StreamStatusType.Synced,
          isLoadingHistoricalData: false,
          recordsExtracted: 1000,
          recordsLoaded: 1000,
          dataFreshAsOf: dayjs().subtract(1, "minute").unix() * 1000,
        },
        {
          streamName: "test-stream-2",
          streamNameWithPrefix: "test-stream-2",
          streamNamespace: "test-namespace",
          status: StreamStatusType.QueuedForNextSync,
          isLoadingHistoricalData: false,
        },
      ],
      expectedStatus: ["Synced", "Queued for next sync"],
      expectedNames: ["test-stream-1", "test-stream-2"],
      expectedLatestSyncStats: ["1,000 loaded", "-"],
      expectedFreshness: ["a minute ago", "-"],
      expectedLoadingAttributes: ["false", "false"],
    },
    {
      description: "active sync - one queued, one syncing",
      mockStates: [
        {
          streamName: "test-stream-1",
          streamNameWithPrefix: "test-stream-1",
          streamNamespace: "test-namespace",
          status: StreamStatusType.Queued,
          isLoadingHistoricalData: false,
          recordsExtracted: 500,
          recordsLoaded: 0,
        },
        {
          streamName: "test-stream-2",
          streamNameWithPrefix: "test-stream-2",
          streamNamespace: "test-namespace",
          status: StreamStatusType.Syncing,
          isLoadingHistoricalData: false,
          recordsExtracted: 0,
          recordsLoaded: 0,
        },
      ],
      expectedStatus: ["Queued", "Syncing"],
      expectedNames: ["test-stream-1", "test-stream-2"],
      expectedLatestSyncStats: ["500 extracted", "Starting…"],
      expectedFreshness: ["-", "-"],
      expectedLoadingAttributes: ["false", "false"],
    },
    {
      description: "active sync - loading historical data but should show sync progress",
      mockStates: [
        {
          streamName: "test-stream-1",
          streamNameWithPrefix: "test-stream-1",
          streamNamespace: "test-namespace",
          status: StreamStatusType.Queued,
          isLoadingHistoricalData: true,
          recordsExtracted: 500,
          recordsLoaded: 0,
        },
        {
          streamName: "test-stream-2",
          streamNameWithPrefix: "test-stream-2",
          streamNamespace: "test-namespace",
          status: StreamStatusType.Syncing,
          isLoadingHistoricalData: true,
          recordsExtracted: 0,
          recordsLoaded: 0,
        },
      ],
      expectedStatus: ["Queued", "Syncing"],
      expectedNames: ["test-stream-1", "test-stream-2"],
      expectedLatestSyncStats: ["500 extracted", "Starting…"],
      expectedFreshness: ["-", "-"],
      expectedLoadingAttributes: ["false", "false"],
    },
    {
      description: "no active sync - loading historical data so should show spinner for stats",
      mockStates: [
        {
          streamName: "test-stream-1",
          streamNameWithPrefix: "test-stream-1",
          streamNamespace: "test-namespace",
          status: StreamStatusType.Paused,
          isLoadingHistoricalData: true,
          recordsExtracted: 500,
          recordsLoaded: 0,
          dataFreshAsOf: dayjs().subtract(1, "day").unix() * 1000,
        },
        {
          streamName: "test-stream-2",
          streamNameWithPrefix: "test-stream-2",
          streamNamespace: "test-namespace",
          status: StreamStatusType.Paused,
          isLoadingHistoricalData: true,
          recordsExtracted: 0,
          recordsLoaded: 0,
          dataFreshAsOf: dayjs().subtract(1, "day").unix() * 1000,
        },
      ],
      expectedStatus: ["Paused", "Paused"],
      expectedNames: ["test-stream-1", "test-stream-2"],
      expectedLatestSyncStats: ["", ""],
      expectedFreshness: ["a day ago", "a day ago"],
      expectedLoadingAttributes: ["true", "true"],
    },
  ])(
    "$description",
    ({
      mockStates,
      expectedStatus,
      expectedNames,
      expectedLatestSyncStats,
      expectedFreshness,
      expectedLoadingAttributes,
    }) => {
      mocked(useConnectionEditService).mockReturnValue({
        connection: mockConnection,
        setConnection: jest.fn(),
        schemaHasBeenRefreshed: false,
        connectionUpdating: false,
        schemaRefreshing: false,
        updateConnection: jest.fn(),
        updateConnectionStatus: jest.fn(),
        discardRefreshedSchema: jest.fn(),
        streamsByRefreshType: {
          streamsSupportingMergeRefresh: [],
          streamsSupportingTruncateRefresh: [],
        },
      });

      mocked(useConnectionStatus).mockReturnValue({
        status: ConnectionSyncStatus.running,
        nextSync: Math.floor(Date.now() / 1000),
        recordsExtracted: 1000,
        recordsLoaded: 900,
        lastSuccessfulSync: Math.floor(Date.now() / 1000) - 360_000,
      });

      mocked(useUiStreamStates).mockReturnValue(mockStates);

      renderStreamsList();

      expectedStatus.forEach((value, index) => {
        expect(screen.getAllByTestId("streams-list-status-cell-content")[index]).toHaveTextContent(value);
      });
      expectedNames.forEach((value, index) => {
        expect(screen.getAllByTestId("streams-list-name-cell-content")[index]).toHaveTextContent(value);
      });
      expectedLatestSyncStats.forEach((value, index) => {
        expect(screen.getAllByTestId("streams-list-latest-sync-cell-content")[index]).toHaveTextContent(value);
      });
      expectedFreshness.forEach((value, index) => {
        expect(screen.getAllByTestId("streams-list-data-freshness-cell-content")[index]).toHaveTextContent(value);
      });

      expectedLoadingAttributes.forEach((expectedValue, index) => {
        const cell = screen.getAllByTestId("streams-list-latest-sync-cell-content")[index];
        expect(cell).toHaveAttribute("data-loading", expectedValue);
      });
    }
  );
});
