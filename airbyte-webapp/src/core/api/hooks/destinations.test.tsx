import { InfiniteData, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { mockDestination } from "test-utils";

import { useDeleteDestination, destinationsKeys } from "./destinations";
import { deleteDestination } from "../generated/AirbyteClient";
import { DestinationRead, DestinationReadList } from "../types/AirbyteClient";

// Mock the required modules
jest.mock("../generated/AirbyteClient", () => ({
  deleteDestination: jest.fn(),
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
  Namespace: { DESTINATION: "destination" },
  Action: { DELETE: "delete" },
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn(() => "test-workspace-id"),
}));

const mockDeleteDestination = deleteDestination as jest.MockedFunction<typeof deleteDestination>;

const DESTINATION_ONE: DestinationRead = {
  ...mockDestination,
  destinationId: "destination-one-id",
};
const DESTINATION_TWO: DestinationRead = {
  ...mockDestination,
  destinationId: "destination-two-id",
};
const DESTINATION_THREE: DestinationRead = {
  ...mockDestination,
  destinationId: "destination-three-id",
};
const DESTINATION_FOUR: DestinationRead = {
  ...mockDestination,
  destinationId: "destination-four-id",
};

describe("useDeleteDestination", () => {
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

  it("should remove destination from query cache and update destination list on successful deletion", async () => {
    // Populate the query cache with mock data
    const mockDestinationListData = {
      pages: [
        {
          destinations: [DESTINATION_ONE, DESTINATION_TWO],
        },
      ],
      pageParams: [undefined],
    };

    // Set initial data in the cache
    queryClient.setQueryData(destinationsKeys.list(), mockDestinationListData);
    queryClient.setQueryData(destinationsKeys.detail(DESTINATION_ONE.destinationId), DESTINATION_ONE);

    const { result } = renderHook(() => useDeleteDestination(), {
      wrapper: createWrapper(queryClient),
    });

    // Execute the mutation
    await result.current.mutateAsync({ destination: DESTINATION_ONE });

    // Wait for mutation to complete
    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    // Assert that the destination detail cache was removed
    const destinationDetailData = queryClient.getQueryData(destinationsKeys.detail(DESTINATION_ONE.destinationId));
    expect(destinationDetailData).toBeUndefined();

    // Assert that the destination was removed from the list cache
    const destinationListData = queryClient.getQueryData(destinationsKeys.list()) as InfiniteData<DestinationReadList>;
    expect(destinationListData).toBeDefined();

    const updatedPages = destinationListData.pages;
    expect(updatedPages[0].destinations).toHaveLength(1);
    expect(updatedPages[0].destinations[0].destinationId).toBe(DESTINATION_TWO.destinationId);
    expect(
      updatedPages[0].destinations.find((d: DestinationRead) => d.destinationId === DESTINATION_ONE.destinationId)
    ).toBeUndefined();

    // Verify deleteDestination API was called
    expect(mockDeleteDestination).toHaveBeenCalledWith({ destinationId: DESTINATION_ONE.destinationId }, {});
  });

  it("should handle multiple pages in infinite data", async () => {
    // Populate cache with multiple pages
    const mockDestinationListData = {
      pages: [
        {
          destinations: [DESTINATION_ONE, DESTINATION_TWO],
        },
        {
          destinations: [DESTINATION_THREE, DESTINATION_FOUR],
        },
      ],
      pageParams: [undefined, "cursor-1"],
    };

    queryClient.setQueryData(destinationsKeys.list(), mockDestinationListData);
    queryClient.setQueryData(destinationsKeys.detail(DESTINATION_ONE.destinationId), DESTINATION_ONE);

    const { result } = renderHook(() => useDeleteDestination(), {
      wrapper: createWrapper(queryClient),
    });

    await result.current.mutateAsync({ destination: DESTINATION_ONE });

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    // Check that destination detail was removed
    const destinationDetailData = queryClient.getQueryData(destinationsKeys.detail(DESTINATION_ONE.destinationId));
    expect(destinationDetailData).toBeUndefined();

    // Check that destination was filtered from the first page only
    const destinationListData = queryClient.getQueryData(destinationsKeys.list()) as InfiniteData<DestinationReadList>;
    expect(destinationListData.pages[0].destinations).toHaveLength(1);
    expect(destinationListData.pages[0].destinations[0].destinationId).toBe(DESTINATION_TWO.destinationId);

    // Verify other pages remain unchanged
    expect(destinationListData.pages[1].destinations).toHaveLength(2);
    expect(destinationListData.pages[1].destinations.map((d: DestinationRead) => d.destinationId)).toEqual([
      DESTINATION_THREE.destinationId,
      DESTINATION_FOUR.destinationId,
    ]);
  });

  it("should handle empty cache gracefully", async () => {
    // Don't populate cache - it should handle undefined data
    const { result } = renderHook(() => useDeleteDestination(), {
      wrapper: createWrapper(queryClient),
    });

    await result.current.mutateAsync({ destination: DESTINATION_ONE });

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    // Should not throw and cache should remain empty
    const destinationListData = queryClient.getQueryData(destinationsKeys.list());
    expect(destinationListData).toBeUndefined();

    // Verify deleteDestination API was still called
    expect(mockDeleteDestination).toHaveBeenCalledWith({ destinationId: DESTINATION_ONE.destinationId }, {});
  });
});
