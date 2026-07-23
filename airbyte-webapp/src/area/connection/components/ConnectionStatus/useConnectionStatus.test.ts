import { UseQueryResult } from "@tanstack/react-query";
import { renderHook } from "@testing-library/react";

import { mockConnection } from "test-utils/mock-data/mockConnection";

import { useListConnectionsStatuses, useGetConnection, useGetConnectionSyncProgress } from "core/api";
import {
  ConnectionStatusRead,
  ConnectionSyncProgressRead,
  ConnectionSyncStatus,
  JobConfigType,
  WebBackendConnectionRead,
} from "core/api/types/AirbyteClient";

import { useConnectionStatus } from "./useConnectionStatus";

jest.mock("core/api");
const mockUseGetConnection = useGetConnection as unknown as jest.Mock<WebBackendConnectionRead>;

jest.mock("core/api");
const mockUseListConnectionsStatuses = useListConnectionsStatuses as unknown as jest.Mock<
  Array<Partial<ConnectionStatusRead>>
>;

jest.mock("core/api");
const mockUseGetConnectionSyncProgress = useGetConnectionSyncProgress as unknown as jest.Mock<
  Partial<UseQueryResult<ConnectionSyncProgressRead, unknown>>
>;

const resetAndSetupMocks = (connectionStatusRead: ConnectionStatusRead) => {
  mockUseGetConnection.mockClear();
  mockUseGetConnection.mockImplementation(() => mockConnection);

  mockUseListConnectionsStatuses.mockClear();
  mockUseListConnectionsStatuses.mockReturnValue([connectionStatusRead]);

  mockUseGetConnectionSyncProgress.mockClear();
  mockUseGetConnectionSyncProgress.mockReturnValue({
    data: { connectionId: mockConnection.connectionId, streams: [], configType: JobConfigType.sync },
  });
};

describe("useConnectionStatus", () => {
  it("disables useGetConnectionSyncProgress when a sync is running", () => {
    resetAndSetupMocks({
      connectionId: mockConnection.connectionId,
      connectionSyncStatus: ConnectionSyncStatus.synced,
    });
    renderHook(() => useConnectionStatus(mockConnection.connectionId));
    expect(mockUseGetConnectionSyncProgress).toHaveBeenCalledTimes(1);
    expect(mockUseGetConnectionSyncProgress.mock.calls).toEqual([[mockConnection.connectionId, false]]);
  });

  it("enables useGetConnectionSyncProgress when a sync is running", () => {
    resetAndSetupMocks({
      connectionId: mockConnection.connectionId,
      connectionSyncStatus: ConnectionSyncStatus.running,
    });
    renderHook(() => useConnectionStatus(mockConnection.connectionId));
    expect(mockUseGetConnectionSyncProgress).toHaveBeenCalledTimes(1);
    expect(mockUseGetConnectionSyncProgress.mock.calls).toEqual([[mockConnection.connectionId, true]]);
  });
});
