import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import {
  dataWorkerCapacityKeys,
  useReallocateDataWorkerCapacity,
  useRegionDataWorkerCapacity,
} from "./dataWorkerCapacity";
import { listDataWorkerAllocations, reallocateDataWorkerCapacity } from "../generated/AirbyteClient";
import { DataWorkerAllocationListResponse } from "../types/AirbyteClient";

jest.mock("../generated/AirbyteClient", () => ({
  listDataWorkerAllocations: jest.fn(),
  reallocateDataWorkerCapacity: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(() => "organization-1"),
}));

const mockRegisterNotification = jest.fn();
jest.mock("core/services/Notification", () => ({
  useNotificationService: jest.fn(() => ({ registerNotification: mockRegisterNotification })),
}));

jest.mock("react-intl", () => ({
  useIntl: jest.fn(() => ({ formatMessage: ({ id }: { id: string }) => id })),
}));

const mockReallocateDataWorkerCapacity = reallocateDataWorkerCapacity as jest.MockedFunction<
  typeof reallocateDataWorkerCapacity
>;

const mockListDataWorkerAllocations = listDataWorkerAllocations as jest.MockedFunction<
  typeof listDataWorkerAllocations
>;

describe(`${useReallocateDataWorkerCapacity.name}`, () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  });

  afterEach(() => {
    queryClient.clear();
  });

  it("seeds the allocation list cache with the response on success", async () => {
    const response: DataWorkerAllocationListResponse = {
      organization_id: "organization-1",
      total_allocated_capacity: 5,
      allocations: [{ dataplane_group_id: "us-east", allocated_capacity: 5 }],
    };
    mockReallocateDataWorkerCapacity.mockResolvedValue(response);

    const { result } = renderHook(() => useReallocateDataWorkerCapacity(), { wrapper });

    await result.current.mutateAsync({ fromDataplaneGroupId: "eu-west", toDataplaneGroupId: "us-east", amount: 1 });

    expect(queryClient.getQueryData(dataWorkerCapacityKeys.allocationList("organization-1"))).toEqual(response);
    expect(mockRegisterNotification).not.toHaveBeenCalled();
  });

  it("invalidates the allocation list and shows an error notification when the request fails", async () => {
    const consoleError = jest.spyOn(console, "error").mockImplementation(() => undefined);
    queryClient.setQueryData(dataWorkerCapacityKeys.allocationList("organization-1"), {
      organization_id: "organization-1",
      total_allocated_capacity: 5,
      allocations: [],
    });
    mockReallocateDataWorkerCapacity.mockRejectedValue(new Error("409 Conflict"));

    const { result } = renderHook(() => useReallocateDataWorkerCapacity(), { wrapper });

    await expect(
      result.current.mutateAsync({ fromDataplaneGroupId: "eu-west", toDataplaneGroupId: "us-east", amount: 1 })
    ).rejects.toThrow("409 Conflict");

    await waitFor(() =>
      expect(queryClient.getQueryState(dataWorkerCapacityKeys.allocationList("organization-1"))?.isInvalidated).toBe(
        true
      )
    );
    expect(mockRegisterNotification).toHaveBeenCalledWith({
      id: "settings.organization.usage.capacity.reallocateError",
      text: "settings.organization.usage.capacity.reallocateError",
      type: "error",
    });
    consoleError.mockRestore();
  });
});

describe(`${useRegionDataWorkerCapacity.name}`, () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  const allocationList: DataWorkerAllocationListResponse = {
    organization_id: "organization-1",
    total_allocated_capacity: 3,
    allocations: [{ dataplane_group_id: "us-east", allocated_capacity: 2.5 }],
  };

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    mockListDataWorkerAllocations.mockResolvedValue(allocationList);
  });

  afterEach(() => {
    queryClient.clear();
  });

  it("returns the capacity the organization holds in the region", async () => {
    const { result } = renderHook(() => useRegionDataWorkerCapacity("us-east", true), { wrapper });

    await waitFor(() => expect(result.current).toBe(2.5));
  });

  it("returns zero for a region the allocation list leaves out", async () => {
    const { result } = renderHook(() => useRegionDataWorkerCapacity("eu-west", true), { wrapper });

    await waitFor(() => expect(result.current).toBe(0));
  });

  it("returns undefined until a region is selected", async () => {
    const { result } = renderHook(() => useRegionDataWorkerCapacity(null, true), { wrapper });

    await waitFor(() => expect(mockListDataWorkerAllocations).toHaveBeenCalled());
    expect(result.current).toBeUndefined();
  });

  it("requests nothing while disabled", () => {
    const { result } = renderHook(() => useRegionDataWorkerCapacity("us-east", false), { wrapper });

    expect(result.current).toBeUndefined();
    expect(mockListDataWorkerAllocations).not.toHaveBeenCalled();
  });
});
