import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitionIds } from "./agentsPlatform";
import {
  connectionSemanticSearchKeys,
  useConnectionSemanticSearchEnablement,
  useSemanticSearchConnections,
  useUpdateConnectionSemanticSearchEnablement,
} from "./connectionSemanticSearch";
import {
  getFusionConnectionEnablement,
  updateFusionConnectionEnablement,
  webBackendListConnectionsForWorkspace,
} from "../generated/AirbyteClient";
import { WebBackendConnectionListItem } from "../types/AirbyteClient";

jest.mock("./agentsPlatform", () => ({
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
}));

jest.mock("../generated/AirbyteClient", () => ({
  getFusionConnectionEnablement: jest.fn(),
  updateFusionConnectionEnablement: jest.fn(),
  webBackendListConnectionsForWorkspace: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({ signal: "request-options" })),
}));

const mockWebBackendListConnectionsForWorkspace = webBackendListConnectionsForWorkspace as jest.MockedFunction<
  typeof webBackendListConnectionsForWorkspace
>;
const mockGetFusionConnectionEnablement = getFusionConnectionEnablement as jest.MockedFunction<
  typeof getFusionConnectionEnablement
>;
const mockUpdateFusionConnectionEnablement = updateFusionConnectionEnablement as jest.MockedFunction<
  typeof updateFusionConnectionEnablement
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
});
