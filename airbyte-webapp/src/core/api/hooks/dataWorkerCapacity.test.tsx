import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { dataWorkerCapacityKeys, useReallocateDataWorkerCapacity } from "./dataWorkerCapacity";
import { reallocateDataWorkerCapacity } from "../generated/AirbyteClient";
import { DataWorkerAllocationListResponse } from "../types/AirbyteClient";

jest.mock("../generated/AirbyteClient", () => ({
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
