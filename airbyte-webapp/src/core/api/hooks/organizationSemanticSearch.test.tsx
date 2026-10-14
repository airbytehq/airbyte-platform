import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useCurrentOrganizationId } from "area/organization/utils";

import {
  organizationSemanticSearchKeys,
  useOrganizationSemanticSearchEnablement,
  useUpdateOrganizationSemanticSearchEnablement,
} from "./organizationSemanticSearch";
import {
  getOrganizationSemanticSearchEnablement,
  updateOrganizationSemanticSearchEnablement,
} from "../generated/AirbyteClient";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("../generated/AirbyteClient", () => ({
  getOrganizationSemanticSearchEnablement: jest.fn(),
  updateOrganizationSemanticSearchEnablement: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({ signal: "request-options" })),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockGetOrganizationSemanticSearchEnablement = getOrganizationSemanticSearchEnablement as jest.MockedFunction<
  typeof getOrganizationSemanticSearchEnablement
>;
const mockUpdateOrganizationSemanticSearchEnablement =
  updateOrganizationSemanticSearchEnablement as jest.MockedFunction<typeof updateOrganizationSemanticSearchEnablement>;

describe("organization semantic search hooks", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("organization-1");
    queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
        mutations: { retry: false },
      },
    });
  });

  afterEach(() => {
    queryClient.clear();
  });

  it.each([true, false])("reads semantic search enablement %s", async (semanticSearchEnabled) => {
    mockGetOrganizationSemanticSearchEnablement.mockResolvedValue({
      organizationId: "organization-1",
      semanticSearchEnabled,
    });

    const { result } = renderHook(() => useOrganizationSemanticSearchEnablement(), { wrapper });

    await waitFor(() => expect(result.current.data?.semanticSearchEnabled).toBe(semanticSearchEnabled));
    expect(mockGetOrganizationSemanticSearchEnablement).toHaveBeenCalledWith(
      { organizationId: "organization-1" },
      { signal: "request-options" }
    );
  });

  it("does not read the setting when the caller disables the query", () => {
    const { result } = renderHook(() => useOrganizationSemanticSearchEnablement({ enabled: false }), { wrapper });

    expect(result.current.fetchStatus).toBe("idle");
    expect(mockGetOrganizationSemanticSearchEnablement).not.toHaveBeenCalled();
  });

  it("returns read failures to the caller", async () => {
    mockGetOrganizationSemanticSearchEnablement.mockRejectedValue(new Error("read failed"));

    const { result } = renderHook(() => useOrganizationSemanticSearchEnablement(), { wrapper });

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error).toEqual(new Error("read failed"));
  });

  it("updates semantic search enablement and writes the persisted response to cache", async () => {
    mockUpdateOrganizationSemanticSearchEnablement.mockResolvedValue({
      organizationId: "organization-1",
      semanticSearchEnabled: true,
    });

    const { result } = renderHook(() => useUpdateOrganizationSemanticSearchEnablement(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync(true);
    });

    expect(mockUpdateOrganizationSemanticSearchEnablement).toHaveBeenCalledWith(
      { organizationId: "organization-1", semanticSearchEnabled: true },
      { signal: "request-options" }
    );
    expect(queryClient.getQueryData(organizationSemanticSearchKeys.all("organization-1"))).toEqual({
      organizationId: "organization-1",
      semanticSearchEnabled: true,
    });
  });

  it("returns mutation failures to the caller and leaves cached state unchanged", async () => {
    queryClient.setQueryData(organizationSemanticSearchKeys.all("organization-1"), {
      organizationId: "organization-1",
      semanticSearchEnabled: false,
    });
    mockUpdateOrganizationSemanticSearchEnablement.mockRejectedValue(new Error("write failed"));

    const { result } = renderHook(() => useUpdateOrganizationSemanticSearchEnablement(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync(true)).rejects.toThrow("write failed");
    });

    expect(queryClient.getQueryData(organizationSemanticSearchKeys.all("organization-1"))).toEqual({
      organizationId: "organization-1",
      semanticSearchEnabled: false,
    });
  });

  it("isolates cache entries by organization id", async () => {
    mockUpdateOrganizationSemanticSearchEnablement.mockResolvedValueOnce({
      organizationId: "organization-1",
      semanticSearchEnabled: true,
    });
    queryClient.setQueryData(organizationSemanticSearchKeys.all("organization-2"), {
      organizationId: "organization-2",
      semanticSearchEnabled: false,
    });

    const { result } = renderHook(() => useUpdateOrganizationSemanticSearchEnablement(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync(true);
    });

    expect(queryClient.getQueryData(organizationSemanticSearchKeys.all("organization-1"))).toEqual({
      organizationId: "organization-1",
      semanticSearchEnabled: true,
    });
    expect(queryClient.getQueryData(organizationSemanticSearchKeys.all("organization-2"))).toEqual({
      organizationId: "organization-2",
      semanticSearchEnabled: false,
    });
  });

  it("updates the original organization cache when selection changes during a pending mutation", async () => {
    let resolveMutation: (value: { organizationId: string; semanticSearchEnabled: boolean }) => void = () => undefined;
    mockUpdateOrganizationSemanticSearchEnablement.mockReturnValue(
      new Promise((resolve) => {
        resolveMutation = resolve;
      })
    );

    const { result, rerender } = renderHook(() => useUpdateOrganizationSemanticSearchEnablement(), { wrapper });
    const mutation = result.current.mutateAsync(true);

    mockUseCurrentOrganizationId.mockReturnValue("organization-2");
    rerender();

    await act(async () => {
      resolveMutation({ organizationId: "organization-1", semanticSearchEnabled: true });
      await mutation;
    });

    expect(queryClient.getQueryData(organizationSemanticSearchKeys.all("organization-1"))).toEqual({
      organizationId: "organization-1",
      semanticSearchEnabled: true,
    });
    expect(queryClient.getQueryData(organizationSemanticSearchKeys.all("organization-2"))).toBeUndefined();
  });
});
