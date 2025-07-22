import { InfiniteData, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { mockSource } from "test-utils";

import { useDeleteSource, sourcesKeys } from "./sources";
import { deleteSource } from "../generated/AirbyteClient";
import { SourceRead, SourceReadList } from "../types/AirbyteClient";

// Mock the required modules
jest.mock("../generated/AirbyteClient", () => ({
  deleteSource: jest.fn(),
}));

jest.mock("./connections", () => ({
  useRemoveConnectionsFromList: jest.fn(() => jest.fn()),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

jest.mock("../useRequestErrorHandler", () => ({
  useRequestErrorHandler: jest.fn(() => jest.fn()),
}));

jest.mock("core/services/analytics", () => ({
  useAnalyticsService: jest.fn(() => ({
    track: jest.fn(),
  })),
  Namespace: { SOURCE: "source" },
  Action: { DELETE: "delete" },
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn(() => "test-workspace-id"),
}));

const mockDeleteSource = deleteSource as jest.MockedFunction<typeof deleteSource>;

const SOURCE_ONE: SourceRead = {
  ...mockSource,
  sourceId: "source-one-id",
};
const SOURCE_TWO: SourceRead = {
  ...mockSource,
  sourceId: "source-two-id",
};
const SOURCE_THREE: SourceRead = {
  ...mockSource,
  sourceId: "source-three-id",
};
const SOURCE_FOUR: SourceRead = {
  ...mockSource,
  sourceId: "source-four-id",
};

describe("useDeleteSource", () => {
  let queryClient: QueryClient;

  const createWrapper = (client: QueryClient) => {
    return ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient();
  });

  it("should remove source from query cache and update source list on successful deletion", async () => {
    // Populate the query cache with mock data
    const mockSourceListData = {
      pages: [
        {
          sources: [SOURCE_ONE, SOURCE_TWO],
        },
      ],
      pageParams: [undefined],
    };

    // Set initial data in the cache
    queryClient.setQueryData(sourcesKeys.list(), mockSourceListData);
    queryClient.setQueryData(sourcesKeys.detail(SOURCE_ONE.sourceId), SOURCE_ONE);

    const { result } = renderHook(() => useDeleteSource(), {
      wrapper: createWrapper(queryClient),
    });

    // Execute the mutation
    await result.current.mutateAsync({ source: SOURCE_ONE });

    // Wait for mutation to complete
    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    // Assert that the source detail cache was removed
    const sourceDetailData = queryClient.getQueryData(sourcesKeys.detail(SOURCE_ONE.sourceId));
    expect(sourceDetailData).toBeUndefined();

    // Assert that the source was removed from the list cache
    const sourceListData = queryClient.getQueryData(sourcesKeys.list()) as InfiniteData<SourceReadList>;
    expect(sourceListData).toBeDefined();

    const updatedPages = sourceListData.pages;
    expect(updatedPages[0].sources).toHaveLength(1);
    expect(updatedPages[0].sources[0].sourceId).toBe(SOURCE_TWO.sourceId);
    expect(updatedPages[0].sources.find((s: SourceRead) => s.sourceId === SOURCE_ONE.sourceId)).toBeUndefined();

    // Verify deleteSource API was called
    expect(mockDeleteSource).toHaveBeenCalledWith({ sourceId: SOURCE_ONE.sourceId }, {});
  });

  it("should handle multiple pages in infinite data", async () => {
    // Populate cache with multiple pages
    const mockSourceListData = {
      pages: [
        {
          sources: [SOURCE_ONE, SOURCE_TWO],
        },
        {
          sources: [SOURCE_THREE, SOURCE_FOUR],
        },
      ],
      pageParams: [undefined, "cursor-1"],
    };

    queryClient.setQueryData(sourcesKeys.list(), mockSourceListData);
    queryClient.setQueryData(sourcesKeys.detail(SOURCE_ONE.sourceId), SOURCE_ONE);

    const { result } = renderHook(() => useDeleteSource(), {
      wrapper: createWrapper(queryClient),
    });

    await result.current.mutateAsync({ source: SOURCE_ONE });

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    // Check that source detail was removed
    const sourceDetailData = queryClient.getQueryData(sourcesKeys.detail(SOURCE_ONE.sourceId));
    expect(sourceDetailData).toBeUndefined();

    // Check that source was filtered from the first page only
    const sourceListData = queryClient.getQueryData(sourcesKeys.list()) as InfiniteData<SourceReadList>;
    expect(sourceListData.pages[0].sources).toHaveLength(1);
    expect(sourceListData.pages[0].sources[0].sourceId).toBe(SOURCE_TWO.sourceId);

    // Verify other pages remain unchanged
    expect(sourceListData.pages[1].sources).toHaveLength(2);
    expect(sourceListData.pages[1].sources.map((s: SourceRead) => s.sourceId)).toEqual([
      SOURCE_THREE.sourceId,
      SOURCE_FOUR.sourceId,
    ]);
  });

  it("should handle empty cache gracefully", async () => {
    // Don't populate cache - it should handle undefined data
    const { result } = renderHook(() => useDeleteSource(), {
      wrapper: createWrapper(queryClient),
    });

    await result.current.mutateAsync({ source: SOURCE_ONE });

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    // Should not throw and cache should remain empty
    const sourceListData = queryClient.getQueryData(sourcesKeys.list());
    expect(sourceListData).toBeUndefined();

    // Verify deleteSource API was still called
    expect(mockDeleteSource).toHaveBeenCalledWith({ sourceId: SOURCE_ONE.sourceId }, {});
  });
});
