import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, screen, waitFor } from "@testing-library/react";
import { Component, ReactNode, Suspense } from "react";

import { useCurrentOrganizationId } from "area/organization/utils";

import {
  useCurrentOrganizationInfo,
  useOrganizationHistoricalWorkerUsage,
  useOrganizationWorkerUsage,
  useOrgInfo,
} from "./organizations";
import { getOrganizationDataWorkerUsage, getOrgInfo } from "../generated/AirbyteClient";
import { OrganizationDataWorkerUsageRead, OrganizationInfoRead } from "../types/AirbyteClient";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/services/auth", () => ({
  useCurrentUser: jest.fn(),
}));

jest.mock("../generated/AirbyteClient", () => ({
  getOrganizationDataWorkerUsage: jest.fn(),
  getOrgInfo: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: { ViewOrganizationSettings: "ViewOrganizationSettings" },
  useGeneratedIntent: jest.fn(() => false),
}));

jest.mock("./workspaces", () => ({
  getWorkspaceQueryKey: jest.fn(),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockGetOrgInfo = getOrgInfo as jest.MockedFunction<typeof getOrgInfo>;
const mockGetOrganizationDataWorkerUsage = getOrganizationDataWorkerUsage as jest.MockedFunction<
  typeof getOrganizationDataWorkerUsage
>;

class QueryErrorBoundary extends Component<{ children: ReactNode }, { error?: Error }> {
  state: { error?: Error } = {};

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  override render() {
    return this.state.error ? <div data-testid="query-error">{this.state.error.message}</div> : this.props.children;
  }
}

describe("organization info hooks", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <QueryErrorBoundary>
        <Suspense fallback={null}>{children}</Suspense>
      </QueryErrorBoundary>
    </QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  });

  afterEach(() => {
    queryClient.clear();
    jest.useRealTimers();
  });

  it("does not request current organization info without an organization id", () => {
    mockUseCurrentOrganizationId.mockReturnValue(undefined as unknown as string);

    const { result } = renderHook(() => useCurrentOrganizationInfo(), { wrapper });

    expect(result.current).toBeUndefined();
    expect(mockGetOrgInfo).not.toHaveBeenCalled();
  });

  it("does not request organization info without an organization id when otherwise enabled", () => {
    const { result } = renderHook(() => useOrgInfo(undefined as unknown as string, true), { wrapper });

    expect(result.current).toBeUndefined();
    expect(mockGetOrgInfo).not.toHaveBeenCalled();
  });

  it("requests current organization info when an organization id is available", async () => {
    const organizationInfo = { organizationId: "org-123" } as OrganizationInfoRead;
    mockUseCurrentOrganizationId.mockReturnValue("org-123");
    mockGetOrgInfo.mockResolvedValue(organizationInfo);

    const { result } = renderHook(() => useCurrentOrganizationInfo(), { wrapper });

    await waitFor(() => expect(result.current).toEqual(organizationInfo));
    expect(mockGetOrgInfo).toHaveBeenCalledWith({ organizationId: "org-123" }, {});
  });

  it("polls organization worker usage every 60 seconds by default", async () => {
    jest.useFakeTimers();
    const usage: OrganizationDataWorkerUsageRead = { organizationId: "org-123", committedDataWorkers: 4, regions: [] };
    mockUseCurrentOrganizationId.mockReturnValue("org-123");
    mockGetOrganizationDataWorkerUsage.mockResolvedValue(usage);

    const { result } = renderHook(
      () => useOrganizationWorkerUsage({ startDate: "2026-08-01", endDate: "2026-08-25" }),
      { wrapper }
    );

    await waitFor(() => expect(result.current).toEqual(usage));
    expect(mockGetOrganizationDataWorkerUsage).toHaveBeenCalledTimes(1);

    await act(async () => jest.advanceTimersByTime(60_000));

    await waitFor(() => expect(mockGetOrganizationDataWorkerUsage).toHaveBeenCalledTimes(2));
  });

  it("uses an explicit five-minute worker usage polling interval", async () => {
    jest.useFakeTimers();
    const usage: OrganizationDataWorkerUsageRead = { organizationId: "org-123", committedDataWorkers: 4, regions: [] };
    mockUseCurrentOrganizationId.mockReturnValue("org-123");
    mockGetOrganizationDataWorkerUsage.mockResolvedValue(usage);

    const { result } = renderHook(
      () => useOrganizationWorkerUsage({ startDate: "2025-08-25", endDate: "2026-08-25" }, 300_000),
      { wrapper }
    );

    await waitFor(() => expect(result.current).toEqual(usage));
    expect(mockGetOrganizationDataWorkerUsage).toHaveBeenCalledTimes(1);

    await act(async () => jest.advanceTimersByTime(60_000));
    expect(mockGetOrganizationDataWorkerUsage).toHaveBeenCalledTimes(1);

    await act(async () => jest.advanceTimersByTime(240_000));

    await waitFor(() => expect(mockGetOrganizationDataWorkerUsage).toHaveBeenCalledTimes(2));
  });

  it("requests historical worker usage only when enabled", async () => {
    const usage: OrganizationDataWorkerUsageRead = { organizationId: "org-123", committedDataWorkers: 4, regions: [] };
    mockUseCurrentOrganizationId.mockReturnValue("org-123");
    mockGetOrganizationDataWorkerUsage.mockResolvedValue(usage);

    const { result, rerender } = renderHook(
      ({ enabled }) =>
        useOrganizationHistoricalWorkerUsage({ startDate: "2026-07-01", endDate: "2026-07-31" }, { enabled }),
      { initialProps: { enabled: false }, wrapper }
    );

    expect(result.current.fetchStatus).toBe("idle");
    expect(mockGetOrganizationDataWorkerUsage).not.toHaveBeenCalled();

    rerender({ enabled: true });

    await waitFor(() => expect(result.current.data).toEqual(usage));
    expect(mockGetOrganizationDataWorkerUsage).toHaveBeenCalledWith(
      { organizationId: "org-123", startDate: "2026-07-01", endDate: "2026-07-31" },
      {}
    );
  });

  it("reuses cached historical worker usage for the same period", async () => {
    const usage: OrganizationDataWorkerUsageRead = { organizationId: "org-123", committedDataWorkers: 4, regions: [] };
    mockUseCurrentOrganizationId.mockReturnValue("org-123");
    mockGetOrganizationDataWorkerUsage.mockResolvedValue(usage);

    const firstHook = renderHook(
      () => useOrganizationHistoricalWorkerUsage({ startDate: "2026-07-01", endDate: "2026-07-31" }, { enabled: true }),
      { wrapper }
    );

    await waitFor(() => expect(firstHook.result.current.data).toEqual(usage));
    firstHook.unmount();

    const secondHook = renderHook(
      () => useOrganizationHistoricalWorkerUsage({ startDate: "2026-07-01", endDate: "2026-07-31" }, { enabled: true }),
      { wrapper }
    );

    expect(secondHook.result.current.data).toEqual(usage);
    expect(mockGetOrganizationDataWorkerUsage).toHaveBeenCalledTimes(1);
  });

  it("does not poll historical worker usage", async () => {
    jest.useFakeTimers();
    const usage: OrganizationDataWorkerUsageRead = { organizationId: "org-123", committedDataWorkers: 4, regions: [] };
    mockUseCurrentOrganizationId.mockReturnValue("org-123");
    mockGetOrganizationDataWorkerUsage.mockResolvedValue(usage);

    const { result } = renderHook(
      () => useOrganizationHistoricalWorkerUsage({ startDate: "2026-07-01", endDate: "2026-07-31" }, { enabled: true }),
      { wrapper }
    );

    await waitFor(() => expect(result.current.data).toEqual(usage));
    expect(mockGetOrganizationDataWorkerUsage).toHaveBeenCalledTimes(1);

    await act(async () => jest.advanceTimersByTime(300_000));

    expect(mockGetOrganizationDataWorkerUsage).toHaveBeenCalledTimes(1);
  });

  it("returns enabled historical worker usage errors to the caller", async () => {
    const error = new Error("historical request failed");
    const consoleError = jest.spyOn(console, "error").mockImplementation(() => undefined);
    mockUseCurrentOrganizationId.mockReturnValue("org-123");
    mockGetOrganizationDataWorkerUsage.mockRejectedValue(error);

    const { result } = renderHook(
      () => useOrganizationHistoricalWorkerUsage({ startDate: "2026-07-01", endDate: "2026-07-31" }, { enabled: true }),
      { wrapper }
    );

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.error).toBe(error);
    expect(screen.queryByTestId("query-error")).not.toBeInTheDocument();
    consoleError.mockRestore();
  });
});
