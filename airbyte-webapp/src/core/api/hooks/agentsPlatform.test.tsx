import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useWebappConfig } from "core/config/webappConfig";

import { useAgentsProvisioningStatus } from "./agentsPlatform";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("core/config/webappConfig", () => ({
  useWebappConfig: jest.fn(),
}));

jest.mock("../useRequestOptions", () => ({
  useRequestOptions: jest.fn(() => ({
    getAccessToken: jest.fn().mockResolvedValue("token"),
  })),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;
const mockUseWebappConfig = useWebappConfig as jest.MockedFunction<typeof useWebappConfig>;
const mockFetch = jest.fn();

describe("useAgentsProvisioningStatus", () => {
  let queryClient: QueryClient;

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    mockUseCurrentOrganizationId.mockReturnValue("organization-1");
    mockUseWebappConfig.mockReturnValue({ sonarApiUrl: "https://agents.example.com" } as never);
    Object.assign(globalThis, { fetch: mockFetch });
  });

  afterEach(() => {
    queryClient.clear();
    jest.restoreAllMocks();
  });

  it("returns null when the provisioning status belongs to another organization", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        is_enrolled: true,
        is_instance_admin: false,
        provisioning_state: "provisioned",
        organization_id: "organization-2",
        organization_kind: "external_cloud",
        external_cloud_eligible: false,
        eligible_external_organization_id: null,
      }),
    } as Response);

    const { result } = renderHook(() => useAgentsProvisioningStatus(), { wrapper });

    await waitFor(() => expect(result.current).toBeNull());
  });
});
