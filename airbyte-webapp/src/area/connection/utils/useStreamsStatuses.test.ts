import { renderHook } from "@testing-library/react";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { StreamStatusType } from "components/connection/StreamStatusIndicator";

import { useListStreamsStatuses, useGetConnection } from "core/api";
import {
  ConnectionSyncStatus,
  StreamStatusIncompleteRunCause,
  StreamStatusJobType,
  StreamStatusRunState,
} from "core/api/types/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";

import { useStreamsStatuses } from "./useStreamsStatuses";
import { useStreamsSyncProgress } from "./useStreamsSyncProgress";

jest.mock("core/api");
jest.mock("hooks/connection/useSchemaChanges");
jest.mock("components/connection/ConnectionStatus/useConnectionStatus");
jest.mock("./useStreamsSyncProgress");

describe("useStreamsStatuses", () => {
  const mockConnectionId = "test-connection-id";

  const mockConnection = {
    status: "active",
    syncCatalog: {
      streams: [
        {
          stream: { name: "stream1", namespace: "namespace1" },
          config: { selected: true },
        },
        {
          stream: { name: "stream2", namespace: "namespace2" },
          config: { selected: false },
        },
      ],
    },
    schemaChange: "none",
    scheduleType: "manual",
    scheduleData: {},
  };

  const mockCompletedStreamStatus = {
    transitionedAt: 12345,
    jobType: StreamStatusJobType.SYNC,
    streamName: "stream1",
    streamNamespace: "namespace1",
    runState: StreamStatusRunState.COMPLETE,
  };

  const mockIncompleteStreamStatus = {
    transitionedAt: 23456,
    jobType: StreamStatusJobType.SYNC,
    runState: StreamStatusRunState.INCOMPLETE,
    streamName: "stream1",
    streamNamespace: "namespace1",
    incompleteRunCause: StreamStatusIncompleteRunCause.FAILED,
  };

  const mockStreamStatuses = [mockCompletedStreamStatus];
  const mockIncompleteStreamStatuses = [mockCompletedStreamStatus, mockIncompleteStreamStatus];

  beforeEach(() => {
    (useGetConnection as jest.Mock).mockReturnValue(mockConnection);
    (useListStreamsStatuses as jest.Mock).mockReturnValue({ streamStatuses: mockStreamStatuses });
    (useSchemaChanges as jest.Mock).mockReturnValue({ hasBreakingSchemaChange: false });
    (useConnectionStatus as jest.Mock).mockReturnValue({
      status: ConnectionSyncStatus.pending,
      isRunning: true,
      lastSuccessfulSync: 1609459200,
    });
    (useStreamsSyncProgress as jest.Mock).mockReturnValue(new Map());
  });

  it("should return stream statuses and enabled streams", () => {
    const { result } = renderHook(() => useStreamsStatuses(mockConnectionId));

    expect(result.current.enabledStreams).toHaveLength(1);
    expect(result.current.enabledStreams[0].stream.name).toBe("stream1");

    const streamStatuses = result.current.streamStatuses;
    const stream1Status = streamStatuses.get("stream1-namespace1");

    expect(stream1Status?.status).toBe(StreamStatusType.Synced);
    expect(stream1Status?.isRunning).toBe(false);
    expect(stream1Status?.relevantHistory).toHaveLength(1);
    expect(stream1Status?.relevantHistory[0]).toEqual(mockCompletedStreamStatus);
  });

  it("no history of stream statuses, stream should return pending", () => {
    (useListStreamsStatuses as jest.Mock).mockReturnValueOnce({ streamStatuses: [] });

    const { result } = renderHook(() => useStreamsStatuses(mockConnectionId));

    const streamStatuses = result.current.streamStatuses;
    const stream1Status = streamStatuses.get("stream1-namespace1");

    expect(stream1Status?.status).toBe(StreamStatusType.Pending);
    expect(stream1Status?.isRunning).toBe(false);
    expect(stream1Status?.relevantHistory).toHaveLength(0);
  });

  it("no per-stream statuses, stream should return connection status", () => {
    (useConnectionStatus as jest.Mock).mockReturnValue({
      status: ConnectionSyncStatus.failed,
      isRunning: true,
      lastSuccessfulSync: 1609459200,
    });

    (useListStreamsStatuses as jest.Mock).mockReturnValueOnce({ streamStatuses: [] });

    const { result } = renderHook(() => useStreamsStatuses(mockConnectionId));

    const streamStatuses = result.current.streamStatuses;
    const stream1Status = streamStatuses.get("stream1-namespace1");

    expect(stream1Status?.status).toBe(StreamStatusType.Failed);
    expect(stream1Status?.isRunning).toBe(false);
    expect(stream1Status?.relevantHistory).toHaveLength(0); // No history should be recorded
  });

  it("should force Incomplete stream status to Failed if connection status is Failed", () => {
    (useListStreamsStatuses as jest.Mock).mockReturnValueOnce({ streamStatuses: mockIncompleteStreamStatuses });

    (useConnectionStatus as jest.Mock).mockReturnValue({
      status: ConnectionSyncStatus.failed,
      isRunning: false,
      lastSuccessfulSync: 1609459200,
    });

    const { result } = renderHook(() => useStreamsStatuses(mockConnectionId));

    const streamStatuses = result.current.streamStatuses;
    const stream1Status = streamStatuses.get("stream1-namespace1");

    expect(stream1Status?.status).toBe(StreamStatusType.Failed);
  });

  it("should retain the Incomplete status if the connection status is not Failed", () => {
    (useListStreamsStatuses as jest.Mock).mockReturnValueOnce({ streamStatuses: mockIncompleteStreamStatuses });

    (useConnectionStatus as jest.Mock).mockReturnValue({
      status: ConnectionSyncStatus.incomplete,
      isRunning: false,
      lastSuccessfulSync: 1609459200,
    });

    const { result } = renderHook(() => useStreamsStatuses(mockConnectionId));

    const streamStatuses = result.current.streamStatuses;
    const stream1Status = streamStatuses.get("stream1-namespace1");

    expect(stream1Status?.status).toBe(StreamStatusType.Incomplete);
  });

  it("should set streams to paused if connection disabled", () => {
    (useGetConnection as jest.Mock).mockReturnValueOnce({ ...mockConnection, status: "inactive" });

    const { result } = renderHook(() => useStreamsStatuses(mockConnectionId));

    const streamStatuses = result.current.streamStatuses;
    const stream1Status = streamStatuses.get("stream1-namespace1");

    expect(stream1Status?.status).toBe(StreamStatusType.Paused);
    expect(stream1Status?.isRunning).toBe(false);
  });
});
