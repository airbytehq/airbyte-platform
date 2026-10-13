import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useAgentsSupportedDestinationDefinitionIds, useAgentsSupportedSourceDefinitionIds } from "./agentsPlatform";
import { useSemanticSearchConnections } from "./connectionSemanticSearch";
import { webBackendListConnectionsForWorkspace } from "../generated/AirbyteClient";
import { WebBackendConnectionListItem } from "../types/AirbyteClient";

jest.mock("./agentsPlatform", () => ({
  useAgentsSupportedSourceDefinitionIds: jest.fn(),
  useAgentsSupportedDestinationDefinitionIds: jest.fn(),
}));

jest.mock("../generated/AirbyteClient", () => ({
  webBackendListConnectionsForWorkspace: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({ signal: "request-options" })),
}));

const mockWebBackendListConnectionsForWorkspace = webBackendListConnectionsForWorkspace as jest.MockedFunction<
  typeof webBackendListConnectionsForWorkspace
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
