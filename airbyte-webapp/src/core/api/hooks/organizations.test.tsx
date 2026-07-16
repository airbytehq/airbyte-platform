import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode, Suspense } from "react";

import { useCurrentOrganizationId } from "area/organization/utils";

import { useCurrentOrganizationInfo, useOrgInfo } from "./organizations";
import { getOrgInfo } from "../generated/AirbyteClient";
import { OrganizationInfoRead } from "../types/AirbyteClient";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/services/auth", () => ({
  useCurrentUser: jest.fn(),
}));

jest.mock("../generated/AirbyteClient", () => ({
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

describe("organization info hooks", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <Suspense fallback={null}>{children}</Suspense>
    </QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  });

  afterEach(() => {
    queryClient.clear();
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
});
