import { QueryClient, useQueryClient } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusType } from "components/connection/ConnectionStatusIndicator";
import { StreamStatusType } from "components/connection/StreamStatusIndicator";
import { TestWrapper } from "test-utils";

import { connectionsKeys, useGetConnectionSyncProgress } from "core/api";
import { StreamStatusJobType, StreamStatusRunState } from "core/api/types/AirbyteClient";
import { useStreamsListContext } from "pages/connections/StreamStatusPage/StreamsListContext";

import { useHistoricalStreamData } from "./useStreamsHistoricalData";
import { useStreamsStatuses } from "./useStreamsStatuses";
import { useStreamsSyncProgress } from "./useStreamsSyncProgress";
import { useUiStreamStates } from "./useUiStreamsStates";

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

describe("useUiStreamStates", () => {
  const mockConnectionId = "test-connection-id";

  const mockQueryClient = new QueryClient();
  const mockInvalidateQueries = jest.fn();
  mockQueryClient.invalidateQueries = mockInvalidateQueries;

  (useQueryClient as jest.Mock).mockReturnValue(mockQueryClient);

  const mockConnectionStatus = {
    status: ConnectionStatusType.Pending,
    isRunning: false,
  };

  const mockStreamStatus = new Map([
    [
      "stream1-namespace1",
      {
        status: StreamStatusType.Synced,
        relevantHistory: [],
        lastSuccessfulSyncAt: 12345,
      },
    ],
  ]);

  const mockSyncProgress = new Map([
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
        recordsEmitted: 1000,
        recordsCommitted: 950,
        bytesEmitted: 10200,
        bytesCommitted: 9540,
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

  beforeEach(() => {
    jest.clearAllMocks();

    (useConnectionStatus as jest.Mock).mockReturnValue(mockConnectionStatus);
    (useGetConnectionSyncProgress as jest.Mock).mockReturnValue({ data: { jobId: 1 } });
    (useStreamsListContext as jest.Mock).mockReturnValue({ filteredStreamsByName: mockFilteredStreams });
    (useHistoricalStreamData as jest.Mock).mockReturnValue({
      historicalStreamsData: mockHistoricalData,
      isFetching: false,
    });
    (useStreamsStatuses as jest.Mock).mockReturnValue({ streamStatuses: mockStreamStatus });
    (useStreamsSyncProgress as jest.Mock).mockReturnValue(mockSyncProgress);
  });

  describe("No running sync", () => {
    it("should return correct UIStreamState when no syncs have been run", () => {
      (useStreamsSyncProgress as jest.Mock).mockReturnValueOnce(new Map());
      (useHistoricalStreamData as jest.Mock).mockReturnValue({
        historicalStreamsData: new Map(),
        isFetching: false,
      });
      (useStreamsStatuses as jest.Mock).mockReturnValue({ streamStatuses: new Map() });

      const { result } = renderHook(() => useUiStreamStates(mockConnectionId), {
        wrapper: TestWrapper,
      });

      const uiStreamStates = result.current;

      expect(uiStreamStates).toHaveLength(1);
      const uiStreamState = uiStreamStates[0];

      expect(uiStreamState.streamName).toBe("stream1");
      expect(uiStreamState.streamNamespace).toBe("namespace1");
      expect(uiStreamState.recordsExtracted).toBeUndefined();
      expect(uiStreamState.recordsLoaded).toBeUndefined();
      expect(uiStreamState.bytesLoaded).toBeUndefined();
      expect(uiStreamState.status).toBe(StreamStatusType.Pending);
      expect(uiStreamState.dataFreshAsOf).toBeUndefined();
    });

    it("should return correct UIStreamState when historical data is present", () => {
      const { result } = renderHook(() => useUiStreamStates(mockConnectionId), {
        wrapper: TestWrapper,
      });

      const uiStreamStates = result.current;

      expect(uiStreamStates).toHaveLength(1);
      const uiStreamState = uiStreamStates[0];

      expect(uiStreamState.streamName).toBe("stream1");
      expect(uiStreamState.streamNamespace).toBe("namespace1");
      expect(uiStreamState.recordsExtracted).toBe(1000);
      expect(uiStreamState.recordsLoaded).toBe(950);
      expect(uiStreamState.bytesLoaded).toBeUndefined();
      expect(uiStreamState.status).toBe(StreamStatusType.Synced);
      expect(uiStreamState.dataFreshAsOf).toBeUndefined();
    });
  });
  describe("During running sync", () => {
    it("should return correct UIStreamState for initial sync", () => {
      (useHistoricalStreamData as jest.Mock).mockReturnValue({
        historicalStreamsData: new Map(),
        isFetching: false,
      });
      (useStreamsStatuses as jest.Mock).mockReturnValue({
        streamStatuses: new Map([
          [
            "stream1-namespace1",
            {
              status: StreamStatusType.Syncing,
              relevantHistory: [],
              lastSuccessfulSyncAt: 12345,
            },
          ],
        ]),
      });

      const { result } = renderHook(() => useUiStreamStates(mockConnectionId), {
        wrapper: TestWrapper,
      });

      const uiStreamStates = result.current;

      expect(uiStreamStates).toHaveLength(1);
      const uiStreamState = uiStreamStates[0];

      expect(uiStreamState.streamName).toBe("stream1");
      expect(uiStreamState.streamNamespace).toBe("namespace1");
      expect(uiStreamState.recordsExtracted).toBe(1000);
      expect(uiStreamState.recordsLoaded).toBe(950);
      expect(uiStreamState.bytesLoaded).toBeUndefined();
      expect(uiStreamState.status).toBe(StreamStatusType.Syncing);
      expect(uiStreamState.dataFreshAsOf).toBeUndefined();
    });
  });

  it("should correctly set isLoadingHistoricalData flag", () => {
    (useHistoricalStreamData as jest.Mock).mockReturnValueOnce({
      historicalStreamsData: mockHistoricalData,
      isFetching: true,
    });

    const { result } = renderHook(() => useUiStreamStates(mockConnectionId), {
      wrapper: TestWrapper,
    });

    const uiStreamStates = result.current;

    expect(uiStreamStates).toHaveLength(1);
    expect(uiStreamStates[0].isLoadingHistoricalData).toBe(true);
  });

  it("should handle RateLimited status", () => {
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

    const uiStreamStates = result.current;

    expect(uiStreamStates).toHaveLength(1);
    expect(uiStreamStates[0].status).toBe(StreamStatusType.RateLimited);
  });

  it("should handle post-job fetching correctly", async () => {
    (useConnectionStatus as jest.Mock).mockReturnValueOnce({
      ...mockConnectionStatus,
      isRunning: true,
    });

    const { rerender } = renderHook(() => useUiStreamStates(mockConnectionId));

    // Simulate job completion by updating the connection status
    (useConnectionStatus as jest.Mock).mockReturnValueOnce({
      ...mockConnectionStatus,
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
});
