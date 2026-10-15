import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitionIds } from "./agentsPlatform";
import {
  connectionSemanticSearchKeys,
  useConnectionSemanticSearchEnablement,
  useCreateConnectionSemanticSearchEnablement,
  useRequestConnectionBackfill,
  useSemanticSearchConnections,
  useSourceSearchStatuses,
  useUpdateConnectionSemanticSearchEnablement,
} from "./connectionSemanticSearch";
import {
  createFusionConnectionEnablement,
  getFusionConnectionEnablement,
  getFusionSourceSearchStatus,
  requestFusionConnectionBackfill,
  updateFusionConnectionEnablement,
  webBackendListConnectionsForWorkspace,
} from "../generated/AirbyteClient";
import { WebBackendConnectionListItem } from "../types/AirbyteClient";

jest.mock("./agentsPlatform", () => ({
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
}));

jest.mock("../generated/AirbyteClient", () => ({
  createFusionConnectionEnablement: jest.fn(),
  getFusionConnectionEnablement: jest.fn(),
  getFusionSourceSearchStatus: jest.fn(),
  requestFusionConnectionBackfill: jest.fn(),
  updateFusionConnectionEnablement: jest.fn(),
  webBackendListConnectionsForWorkspace: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({ signal: "request-options" })),
}));

const mockWebBackendListConnectionsForWorkspace = webBackendListConnectionsForWorkspace as jest.MockedFunction<
  typeof webBackendListConnectionsForWorkspace
>;
const mockCreateFusionConnectionEnablement = createFusionConnectionEnablement as jest.MockedFunction<
  typeof createFusionConnectionEnablement
>;
const mockGetFusionConnectionEnablement = getFusionConnectionEnablement as jest.MockedFunction<
  typeof getFusionConnectionEnablement
>;
const mockUpdateFusionConnectionEnablement = updateFusionConnectionEnablement as jest.MockedFunction<
  typeof updateFusionConnectionEnablement
>;
const mockGetFusionSourceSearchStatus = getFusionSourceSearchStatus as jest.MockedFunction<
  typeof getFusionSourceSearchStatus
>;
const mockRequestFusionConnectionBackfill = requestFusionConnectionBackfill as jest.MockedFunction<
  typeof requestFusionConnectionBackfill
>;
const mockUseAgentsSupportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds as jest.MockedFunction<
  typeof useAgentsSupportedSourceDefinitionIds
>;
const mockUseAgentsSupportedDestinationDefinitionIds =
  useAgentsSupportedDestinationDefinitionIds as jest.MockedFunction<typeof useAgentsSupportedDestinationDefinitionIds>;

const connection = (connectionId: string) => ({ connectionId }) as WebBackendConnectionListItem;

describe("useSemanticSearchConnections", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    mockUseAgentsSupportedSourceDefinitionIds.mockReturnValue(new Set(["source-definition-1", "source-definition-2"]));
    mockUseAgentsSupportedDestinationDefinitionIds.mockReturnValue(new Set(["destination-definition-1"]));
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  });

  afterEach(() => {
    queryClient.clear();
  });

  it("requests one workspace with the supported definition filters", async () => {
    mockWebBackendListConnectionsForWorkspace.mockResolvedValue({ connections: [connection("connection-1")] });

    const { result } = renderHook(() => useSemanticSearchConnections("workspace-1"), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mockWebBackendListConnectionsForWorkspace).toHaveBeenCalledWith(
      {
        workspaceId: "workspace-1",
        cursor: undefined,
        pageSize: 25,
        filters: {
          sourceDefinitionIds: ["source-definition-1", "source-definition-2"],
          destinationDefinitionIds: ["destination-definition-1"],
        },
      },
      expect.anything()
    );
  });

  it("uses the last connection of a full page as the next cursor", async () => {
    mockWebBackendListConnectionsForWorkspace
      .mockResolvedValueOnce({
        connections: [connection("connection-1"), connection("connection-2")],
        page_size: 2,
      })
      .mockResolvedValueOnce({ connections: [connection("connection-3")], page_size: 2 });

    const { result } = renderHook(() => useSemanticSearchConnections("workspace-1"), { wrapper });

    await waitFor(() => expect(result.current.hasNextPage).toBe(true));
    await act(async () => {
      await result.current.fetchNextPage();
    });

    expect(mockWebBackendListConnectionsForWorkspace).toHaveBeenLastCalledWith(
      expect.objectContaining({ workspaceId: "workspace-1", cursor: "connection-2" }),
      expect.anything()
    );
    await waitFor(() => expect(result.current.hasNextPage).toBe(false));
  });

  it.each([
    ["source", new Set<string>(), new Set(["destination-definition-1"])],
    ["destination", new Set(["source-definition-1"]), new Set<string>()],
  ])(
    "returns no connections without a request when no %s definitions are supported",
    async (_kind, sourceDefinitionIds, destinationDefinitionIds) => {
      mockUseAgentsSupportedSourceDefinitionIds.mockReturnValue(sourceDefinitionIds);
      mockUseAgentsSupportedDestinationDefinitionIds.mockReturnValue(destinationDefinitionIds);

      const { result } = renderHook(() => useSemanticSearchConnections("workspace-1"), { wrapper });

      await waitFor(() => expect(result.current.isSuccess).toBe(true));
      expect(result.current.data?.pages[0].connections).toEqual([]);
      expect(result.current.hasNextPage).toBe(false);
      expect(mockWebBackendListConnectionsForWorkspace).not.toHaveBeenCalled();
    }
  );

  it("returns request failures to the caller", async () => {
    mockWebBackendListConnectionsForWorkspace.mockRejectedValue(new Error("list failed"));

    const { result } = renderHook(() => useSemanticSearchConnections("workspace-1"), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error).toEqual(new Error("list failed"));
    expect(result.current.data).toBeUndefined();
  });
});

const enablement = (connectionId: string, enabled: boolean) => ({
  organizationId: "organization-1",
  workspaceId: "workspace-1",
  connectionId,
  enable_indexing: enabled,
});

describe("connection semantic search enablement", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  });

  afterEach(() => {
    queryClient.clear();
  });

  it("reads the saved value for one connection", async () => {
    mockGetFusionConnectionEnablement.mockResolvedValue(enablement("connection-1", true));

    const { result } = renderHook(() => useConnectionSemanticSearchEnablement("connection-1"), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(mockGetFusionConnectionEnablement).toHaveBeenCalledWith("connection-1", { signal: "request-options" });
    expect(result.current.data?.enable_indexing).toBe(true);
  });

  it("returns read failures to the caller without retrying", async () => {
    mockGetFusionConnectionEnablement.mockRejectedValue(new Error("read failed"));

    const { result } = renderHook(() => useConnectionSemanticSearchEnablement("connection-1"), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error).toEqual(new Error("read failed"));
    expect(mockGetFusionConnectionEnablement).toHaveBeenCalledTimes(1);
  });

  it("sends the new value with the shown value as the expected state", async () => {
    mockUpdateFusionConnectionEnablement.mockResolvedValue(enablement("connection-1", true));

    const { result } = renderHook(() => useUpdateConnectionSemanticSearchEnablement("connection-1"), { wrapper });
    await act(() => result.current.mutateAsync({ enabled: true, expectedEnabled: false }));

    expect(mockUpdateFusionConnectionEnablement).toHaveBeenCalledWith(
      "connection-1",
      { enable_indexing: true, expectedState: { enable_indexing: false } },
      { signal: "request-options" }
    );
  });

  it("writes the saved value to the connection cache", async () => {
    queryClient.setQueryData(
      connectionSemanticSearchKeys.enablement("connection-1"),
      enablement("connection-1", false)
    );
    mockUpdateFusionConnectionEnablement.mockResolvedValue(enablement("connection-1", true));

    const { result } = renderHook(() => useUpdateConnectionSemanticSearchEnablement("connection-1"), { wrapper });
    await act(() => result.current.mutateAsync({ enabled: true, expectedEnabled: false }));

    expect(queryClient.getQueryData(connectionSemanticSearchKeys.enablement("connection-1"))).toEqual(
      enablement("connection-1", true)
    );
  });

  it("reads the source status again after a saved update", async () => {
    const completeStatus = {
      sources: [
        {
          source_id: "source-1",
          destination_id: "destination-1",
          connection_id: "connection-1",
          streams: [{ name: "stream-1", indexes: [], backfill: { status: "complete" } }],
        },
      ],
    };
    const offStatus = { sources: [] };
    mockGetFusionSourceSearchStatus.mockResolvedValue(completeStatus as never);
    mockUpdateFusionConnectionEnablement.mockResolvedValue(enablement("connection-1", false));
    const { result } = renderHook(
      () => ({
        statuses: useSourceSearchStatuses(["source-1"], { refetchInterval: () => false }),
        mutation: useUpdateConnectionSemanticSearchEnablement("connection-1"),
      }),
      { wrapper }
    );
    await waitFor(() => expect(result.current.statuses[0].data).toEqual(completeStatus));
    mockGetFusionSourceSearchStatus.mockResolvedValue(offStatus);

    await act(() => result.current.mutation.mutateAsync({ enabled: false, expectedEnabled: true }));

    await waitFor(() => expect(result.current.statuses[0].data).toEqual(offStatus));
    expect(mockGetFusionSourceSearchStatus).toHaveBeenCalledTimes(2);
  });

  it("does not read the source status again after a failed update", async () => {
    mockGetFusionSourceSearchStatus.mockResolvedValue({ sources: [] });
    mockGetFusionConnectionEnablement.mockResolvedValue(enablement("connection-1", true));
    mockUpdateFusionConnectionEnablement.mockRejectedValue(new Error("conflict"));
    const { result } = renderHook(
      () => ({
        statuses: useSourceSearchStatuses(["source-1"], { refetchInterval: () => false }),
        mutation: useUpdateConnectionSemanticSearchEnablement("connection-1"),
      }),
      { wrapper }
    );
    await waitFor(() => expect(result.current.statuses[0].isSuccess).toBe(true));

    await act(async () => {
      await expect(result.current.mutation.mutateAsync({ enabled: false, expectedEnabled: true })).rejects.toEqual(
        new Error("conflict")
      );
    });

    expect(mockGetFusionSourceSearchStatus).toHaveBeenCalledTimes(1);
  });

  it("reads the saved value again after a failed update", async () => {
    mockGetFusionConnectionEnablement.mockResolvedValue(enablement("connection-1", true));
    mockUpdateFusionConnectionEnablement.mockRejectedValue(new Error("conflict"));
    const { result } = renderHook(
      () => ({
        query: useConnectionSemanticSearchEnablement("connection-1"),
        mutation: useUpdateConnectionSemanticSearchEnablement("connection-1"),
      }),
      { wrapper }
    );
    await waitFor(() => expect(result.current.query.isSuccess).toBe(true));
    mockGetFusionConnectionEnablement.mockResolvedValue(enablement("connection-1", false));

    await act(async () => {
      await expect(result.current.mutation.mutateAsync({ enabled: false, expectedEnabled: true })).rejects.toEqual(
        new Error("conflict")
      );
    });

    await waitFor(() => expect(result.current.query.data?.enable_indexing).toBe(false));
    expect(mockGetFusionConnectionEnablement).toHaveBeenCalledTimes(2);
  });

  it("saves the first value of a new connection and writes it to the connection cache", async () => {
    mockCreateFusionConnectionEnablement.mockResolvedValue(enablement("connection-2", true));

    const { result } = renderHook(() => useCreateConnectionSemanticSearchEnablement(), { wrapper });
    await act(() => result.current.mutateAsync({ connectionId: "connection-2", enabled: true }));

    expect(mockCreateFusionConnectionEnablement).toHaveBeenCalledWith(
      "connection-2",
      { enable_indexing: true },
      { signal: "request-options" }
    );
    expect(queryClient.getQueryData(connectionSemanticSearchKeys.enablement("connection-2"))).toEqual(
      enablement("connection-2", true)
    );
  });

  it("returns a failed first save to the caller and caches nothing", async () => {
    mockCreateFusionConnectionEnablement.mockRejectedValue(new Error("create failed"));

    const { result } = renderHook(() => useCreateConnectionSemanticSearchEnablement(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync({ connectionId: "connection-2", enabled: true })).rejects.toEqual(
        new Error("create failed")
      );
    });
    expect(queryClient.getQueryData(connectionSemanticSearchKeys.enablement("connection-2"))).toBeUndefined();
  });
});

const searchStatus = (sourceId: string) => ({
  sources: [{ source_id: sourceId, destination_id: "destination-1", connection_id: "connection-1", streams: [] }],
});

describe("connection backfill", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  });

  afterEach(() => {
    queryClient.clear();
  });

  it("reads the search status once for each source", async () => {
    mockGetFusionSourceSearchStatus.mockImplementation(async (sourceId) => searchStatus(sourceId));

    const { result } = renderHook(
      () => ({
        first: useSourceSearchStatuses(["source-1", "source-2"], { refetchInterval: () => false }),
        second: useSourceSearchStatuses(["source-1"], { refetchInterval: () => false }),
      }),
      { wrapper }
    );

    await waitFor(() => expect(result.current.first.every((status) => status.isSuccess)).toBe(true));
    expect(result.current.first.map((status) => status.data)).toEqual([
      searchStatus("source-1"),
      searchStatus("source-2"),
    ]);
    expect(mockGetFusionSourceSearchStatus).toHaveBeenCalledTimes(2);
    expect(mockGetFusionSourceSearchStatus).toHaveBeenCalledWith("source-1", { signal: "request-options" });
    expect(mockGetFusionSourceSearchStatus).toHaveBeenCalledWith("source-2", { signal: "request-options" });
  });

  it("returns a failed status read for its source only", async () => {
    mockGetFusionSourceSearchStatus.mockImplementation(async (sourceId) => {
      if (sourceId === "source-2") {
        throw new Error("status failed");
      }
      return searchStatus(sourceId);
    });

    const { result } = renderHook(
      () => useSourceSearchStatuses(["source-1", "source-2"], { refetchInterval: () => false }),
      { wrapper }
    );

    await waitFor(() => expect(result.current[1].isError).toBe(true));
    expect(result.current[0].isSuccess).toBe(true);
    expect(mockGetFusionSourceSearchStatus).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["sends the start time when one is set", "2025-09-19T00:00:00Z", { start_time: "2025-09-19T00:00:00Z" }],
    ["omits the start time when it is empty", "", {}],
    ["omits the start time when it is not set", undefined, {}],
  ])("%s and never sends streams", async (_name, startTime, body) => {
    mockRequestFusionConnectionBackfill.mockResolvedValue({ streams: [] });

    const { result } = renderHook(() => useRequestConnectionBackfill("connection-1", "source-1"), { wrapper });
    await act(() => result.current.mutateAsync({ startTime }));

    expect(mockRequestFusionConnectionBackfill).toHaveBeenCalledWith("connection-1", body, {
      signal: "request-options",
    });
    expect(mockRequestFusionConnectionBackfill.mock.calls[0][1]).not.toHaveProperty("streams");
  });

  it("reads the source status again after a started backfill", async () => {
    mockGetFusionSourceSearchStatus.mockResolvedValue(searchStatus("source-1"));
    mockRequestFusionConnectionBackfill.mockResolvedValue({ streams: [] });
    const { result } = renderHook(
      () => ({
        statuses: useSourceSearchStatuses(["source-1"], { refetchInterval: () => false }),
        backfill: useRequestConnectionBackfill("connection-1", "source-1"),
      }),
      { wrapper }
    );
    await waitFor(() => expect(result.current.statuses[0].isSuccess).toBe(true));

    await act(() => result.current.backfill.mutateAsync({}));

    await waitFor(() => expect(mockGetFusionSourceSearchStatus).toHaveBeenCalledTimes(2));
  });

  it("returns a failed request to the caller", async () => {
    mockRequestFusionConnectionBackfill.mockRejectedValue(new Error("conflict"));

    const { result } = renderHook(() => useRequestConnectionBackfill("connection-1", "source-1"), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync({})).rejects.toEqual(new Error("conflict"));
    });
    expect(queryClient.getQueryState(connectionSemanticSearchKeys.sourceSearchStatus("source-1"))).toBeUndefined();
  });
});
