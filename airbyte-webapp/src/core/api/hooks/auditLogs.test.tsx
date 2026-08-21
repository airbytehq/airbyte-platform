import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import { AUDIT_LOGS_PAGE_SIZE, useListAuditLogs } from "./auditLogs";
import { listAuditLogs } from "../generated/AirbyteClient";
import { AuditLogRead, AuditLogReadList } from "../types/AirbyteClient";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("../generated/AirbyteClient", () => ({
  listAuditLogs: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({})),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockListAuditLogs = listAuditLogs as jest.MockedFunction<typeof listAuditLogs>;

const organizationId = "org-123";

const buildEntry = (id: string, operation: string): AuditLogRead => ({
  id,
  timestamp: 1755800000000,
  operation,
  success: true,
  organizationId,
});

const buildList = (auditLogs: AuditLogRead[]): AuditLogReadList => ({ auditLogs });

describe("useListAuditLogs", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    mockUseCurrentOrganizationId.mockReturnValue(organizationId);
  });

  afterEach(() => {
    queryClient.clear();
  });

  it("requests a page of entries for the current organization", async () => {
    mockListAuditLogs.mockResolvedValue(buildList([buildEntry("entry-1", "updateConnection")]));

    const { result } = renderHook(() => useListAuditLogs({ operation: "updateConnection" }, "token-2"), { wrapper });

    await waitFor(() => expect(result.current.data).toBeDefined());
    expect(mockListAuditLogs).toHaveBeenCalledWith(
      {
        organizationId,
        operation: "updateConnection",
        pageSize: AUDIT_LOGS_PAGE_SIZE,
        pageToken: "token-2",
      },
      {}
    );
  });

  it("strips the webBackend prefix from operation names", async () => {
    mockListAuditLogs.mockResolvedValue(
      buildList([
        buildEntry("entry-1", "webBackendUpdateConnection"),
        buildEntry("entry-2", "webBackendCreateConnection"),
      ])
    );

    const { result } = renderHook(() => useListAuditLogs({}), { wrapper });

    await waitFor(() => expect(result.current.data).toBeDefined());
    expect(result.current.data?.auditLogs.map((entry) => entry.operation)).toEqual([
      "updateConnection",
      "createConnection",
    ]);
  });

  it("leaves operations that do not carry the prefix untouched", async () => {
    mockListAuditLogs.mockResolvedValue(
      buildList([
        buildEntry("entry-1", "updateConnection"),
        // The prefix is only stripped when it is a prefix, not wherever the substring appears.
        buildEntry("entry-2", "createWebBackendThing"),
        buildEntry("entry-3", "webBackend"),
      ])
    );

    const { result } = renderHook(() => useListAuditLogs({}), { wrapper });

    await waitFor(() => expect(result.current.data).toBeDefined());
    expect(result.current.data?.auditLogs.map((entry) => entry.operation)).toEqual([
      "updateConnection",
      "createWebBackendThing",
      "webBackend",
    ]);
  });

  it("preserves the rest of the response", async () => {
    mockListAuditLogs.mockResolvedValue({
      auditLogs: [{ ...buildEntry("entry-1", "webBackendUpdateConnection"), request: { connectionId: "conn-1" } }],
      nextPageToken: "token-2",
    });

    const { result } = renderHook(() => useListAuditLogs({}), { wrapper });

    await waitFor(() => expect(result.current.data).toBeDefined());
    expect(result.current.data?.nextPageToken).toBe("token-2");
    expect(result.current.data?.auditLogs[0].request).toEqual({ connectionId: "conn-1" });
  });

  it("does not call the endpoint when no organization id is available", () => {
    mockUseCurrentOrganizationId.mockReturnValue(undefined as unknown as string);

    renderHook(() => useListAuditLogs({}), { wrapper });

    expect(mockListAuditLogs).not.toHaveBeenCalled();
  });
});
