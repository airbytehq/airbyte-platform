import { useQueryClient } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { StreamStatusType } from "components/connection/StreamStatusIndicator";
import { TestWrapper } from "test-utils";

import { connectionsKeys, useGetConnectionSyncProgress, useCurrentConnection } from "core/api";
import { ConnectionSyncStatus, StreamStatusJobType, StreamStatusRunState } from "core/api/types/AirbyteClient";
import { useStreamsListContext } from "pages/connections/StreamStatusPage/StreamsListContext";

import { useHistoricalStreamData } from "./useStreamsHistoricalData";
import { useStreamsStatuses } from "./useStreamsStatuses";
import { useStreamsSyncProgress } from "./useStreamsSyncProgress";
import { RateLimitedUIStreamState, useUiStreamStates } from "./useUiStreamsStates";

jest.mock("components/connection/ConnectionStatus/useConnectionStatus");
jest.mock("core/api");
jest.mock("pages/connections/StreamStatusPage/StreamsListContext");
jest.mock("./useStreamsHistoricalData");
jest.mock("./useStreamsStatuses");
jest.mock("./useStreamsSyncProgress");
jest.mock("@tanstack/react-query", () => ({
  ...jest.requireActual("@tanstack/react-query"),
  useQueryClient: jest.fn(),
}));

const mockConnectionId = "test-connection-id";
const mockStreamSyncProgress = new Map([
  [
    "stream1-namespace1",
    {
      recordsEmitted: 1000,
      recordsCommitted: 950,
      bytesEmitted: 10200,
      bytesCommitted: 9540,
      configType: "sync",
    },
  ],
]);

const mockHistoricalData = new Map([
  [
    "stream1-namespace1",
    {
      recordsEmitted: 1200,
      recordsCommitted: 1200,
      bytesEmitted: 10200,
      bytesCommitted: 10200,
      configType: "sync",
    },
  ],
]);

const mockFilteredStreams = [
  {
    streamName: "stream1",
    streamNamespace: "namespace1",
  },
];

describe("useUiStreamStates", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    (useStreamsListContext as jest.Mock).mockReturnValue({ enabledStreamsByName: mockFilteredStreams });
  });

  it.each`
    description                             | connectionStatus                                              | historicalStreamsData | syncProgress                         | streamSyncProgress        | streamStatuses                                                                                                               | expectedRecordsExtracted | expectedRecordsLoaded | expectedBytesExtracted | expectedBytesLoaded | expectedStatus              | expectedIsLoadingHistoricalData | expectedDataFreshAsOf
    ${"not running, no historical data"}    | ${{ status: ConnectionSyncStatus.pending, isRunning: false }} | ${new Map()}          | ${new Map()}                         | ${new Map()}              | ${new Map([["stream1-namespace1", { status: StreamStatusType.Pending }]])}                                                   | ${undefined}             | ${undefined}          | ${undefined}           | ${undefined}        | ${StreamStatusType.Pending} | ${false}                        | ${undefined}
    ${"not running, with historical data"}  | ${{ status: ConnectionSyncStatus.synced, isRunning: false }}  | ${mockHistoricalData} | ${new Map()}                         | ${new Map()}              | ${new Map([["stream1-namespace1", { status: StreamStatusType.Synced, relevantHistory: [], lastSuccessfulSyncAt: 12345 }]])}  | ${undefined}             | ${1200}               | ${undefined}           | ${10200}            | ${StreamStatusType.Synced}  | ${false}                        | ${12345}
    ${"sync running, no historical data"}   | ${{ status: ConnectionSyncStatus.running, isRunning: true }}  | ${new Map()}          | ${{ activeSyncJobId: "active-job" }} | ${mockStreamSyncProgress} | ${new Map([["stream1-namespace1", { status: StreamStatusType.Syncing, relevantHistory: [] }]])}                              | ${1000}                  | ${950}                | ${10200}               | ${9540}             | ${StreamStatusType.Syncing} | ${false}                        | ${undefined}
    ${"sync running, with historical data"} | ${{ status: ConnectionSyncStatus.running, isRunning: true }}  | ${mockHistoricalData} | ${{ activeSyncJobId: "active-job" }} | ${mockStreamSyncProgress} | ${new Map([["stream1-namespace1", { status: StreamStatusType.Syncing, relevantHistory: [], lastSuccessfulSyncAt: 12345 }]])} | ${1000}                  | ${950}                | ${10200}               | ${9540}             | ${StreamStatusType.Syncing} | ${false}                        | ${undefined}
  `(
    "$description",
    async ({
      connectionStatus,
      historicalStreamsData,
      syncProgress,
      streamSyncProgress,
      streamStatuses,
      expectedRecordsExtracted,
      expectedRecordsLoaded,
      expectedBytesExtracted,
      expectedBytesLoaded,
      expectedStatus,
      expectedIsLoadingHistoricalData,
      expectedDataFreshAsOf,
    }) => {
      (useCurrentConnection as jest.Mock).mockReturnValue({ prefix: "", syncCatalog: { streams: [] } });
      (useConnectionStatus as jest.Mock).mockReturnValue(connectionStatus);
      (useGetConnectionSyncProgress as jest.Mock).mockReturnValue(syncProgress);
      (useStreamsSyncProgress as jest.Mock).mockReturnValue(streamSyncProgress);
      (useStreamsStatuses as jest.Mock).mockReturnValue({ streamStatuses });
      (useHistoricalStreamData as jest.Mock).mockReturnValue({
        historicalStreamsData,
        isFetching: expectedIsLoadingHistoricalData,
      });

      const { result } = renderHook(() => useUiStreamStates(mockConnectionId), { wrapper: TestWrapper });
      const uiStreamStates = result.current;

      expect(uiStreamStates[0].recordsExtracted).toBe(expectedRecordsExtracted);
      expect(uiStreamStates[0].recordsLoaded).toBe(expectedRecordsLoaded);
      expect(uiStreamStates[0].bytesExtracted).toBe(expectedBytesExtracted);
      expect(uiStreamStates[0].bytesLoaded).toBe(expectedBytesLoaded);
      expect(uiStreamStates[0].status).toBe(expectedStatus);
      expect(uiStreamStates[0].isLoadingHistoricalData).toBe(expectedIsLoadingHistoricalData);
      expect(uiStreamStates[0].dataFreshAsOf).toBe(expectedDataFreshAsOf);
    }
  );
});

it("should handle RateLimited status", () => {
  (useCurrentConnection as jest.Mock).mockReturnValue({ prefix: "", syncCatalog: { streams: [] } });
  (useStreamsStatuses as jest.Mock).mockReturnValue({
    streamStatuses: new Map([
      [
        "stream1-namespace1",
        {
          status: StreamStatusType.RateLimited,
          relevantHistory: [
            {
              jobType: StreamStatusJobType.SYNC,
              runState: StreamStatusRunState.COMPLETE,
              metadata: { quotaReset: 1234567890 },
            },
          ],
        },
      ],
    ]),
  });

  const { result } = renderHook(() => useUiStreamStates(mockConnectionId), {
    wrapper: TestWrapper,
  });

  const uiStreamStates = result.current as RateLimitedUIStreamState[];

  expect(uiStreamStates).toHaveLength(1);
  expect(uiStreamStates[0].status).toBe(StreamStatusType.RateLimited);
  expect(uiStreamStates[0].quotaReset).toBe(1234567890);
});

it("should handle post-job fetching correctly", async () => {
  const mockInvalidateQueries = jest.fn();
  const mockQueryClient = {
    invalidateQueries: mockInvalidateQueries,
  };
  (useCurrentConnection as jest.Mock).mockReturnValue({ prefix: "", syncCatalog: { streams: [] } });
  (useConnectionStatus as jest.Mock).mockReturnValueOnce({
    status: ConnectionSyncStatus.running,
    isRunning: true,
  });
  (useQueryClient as jest.Mock).mockReturnValue(mockQueryClient);
  (useStreamsListContext as jest.Mock).mockReturnValue({ enabledStreamsByName: mockFilteredStreams });

  (useGetConnectionSyncProgress as jest.Mock).mockReturnValue(new Map());
  (useStreamsSyncProgress as jest.Mock).mockReturnValue(new Map());
  (useHistoricalStreamData as jest.Mock).mockReturnValue({
    historicalStreamsData: new Map(),
    isFetching: false,
  });

  (useStreamsStatuses as jest.Mock).mockReturnValue({
    streamStatuses: new Map([
      [
        "stream1-namespace1",
        {
          status: StreamStatusType.RateLimited,
          relevantHistory: [
            {
              jobType: StreamStatusJobType.SYNC,
              runState: StreamStatusRunState.COMPLETE,
              metadata: { quotaReset: 1234567890 },
            },
          ],
        },
      ],
    ]),
  });

  const { rerender } = renderHook(() => useUiStreamStates(mockConnectionId));

  // Simulate job completion by updating the connection status
  (useConnectionStatus as jest.Mock).mockReturnValue({
    status: ConnectionSyncStatus.synced,
    isRunning: false,
  });

  await act(async () => {
    rerender();
    await new Promise((resolve) => setTimeout(resolve, 0)); // Wait for next tick
  });

  expect(mockInvalidateQueries).toHaveBeenCalledWith(connectionsKeys.lastJobPerStream(mockConnectionId));
  expect(mockInvalidateQueries).toHaveBeenCalledWith(connectionsKeys.uptimeHistory(mockConnectionId));
  expect(mockInvalidateQueries).toHaveBeenCalledWith(connectionsKeys.dataHistory(mockConnectionId));
});
