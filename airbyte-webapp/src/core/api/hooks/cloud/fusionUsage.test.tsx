import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { ReactNode } from "react";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useIsCloudApp } from "core/utils/app";

import { useFusionUsage } from "./fusionUsage";
import { getFusionUsage } from "../../generated/AirbyteClient";
import { FusionUsageRead } from "../../types/AirbyteClient";
import { useAgentsProvisioningStatus } from "../agentsPlatform";

jest.mock("area/organization/utils", () => ({ useCurrentOrganizationId: jest.fn() }));
jest.mock("core/utils/app", () => ({ useIsCloudApp: jest.fn() }));
jest.mock("../agentsPlatform", () => ({ useAgentsProvisioningStatus: jest.fn() }));
jest.mock("../../generated/AirbyteClient", () => ({ getFusionUsage: jest.fn() }));
jest.mock("../../useRequestOptions", () => ({ useRequestOptions: () => ({}) }));

const status = {
  is_enrolled: true,
  is_instance_admin: false,
  provisioning_state: "enrolled",
  organization_id: "org-a",
  organization_kind: "external_cloud",
  external_cloud_eligible: true,
  eligible_external_organization_id: "org-a",
};

const usage: FusionUsageRead = {
  organizationId: "org-a",
  freeCapsApplicable: true,
  day: { used: 0, limit: 300, windowStart: "2026-10-02T00:00:00Z", resetsAt: "2026-10-03T00:00:00Z" },
  month: { used: null, limit: null, windowStart: "2026-10-01T00:00:00Z", resetsAt: "2026-11-01T00:00:00Z" },
};

describe("useFusionUsage", () => {
  let queryClient: QueryClient;
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    jest.mocked(useIsCloudApp).mockReturnValue(true);
    jest.mocked(useCurrentOrganizationId).mockReturnValue("org-a");
    jest.mocked(useAgentsProvisioningStatus).mockReturnValue(status);
    jest.mocked(getFusionUsage).mockResolvedValue(usage);
  });

  afterEach(() => queryClient.clear());

  it("calls the Cloud consumer and preserves nullable counters and limits", async () => {
    const { result } = renderHook(useFusionUsage, { wrapper });
    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(getFusionUsage).toHaveBeenCalledWith("org-a", {});
    expect(result.current.data?.month).toMatchObject({ used: null, limit: null });
  });

  it.each([
    ["unprovisioned", { ...status, is_enrolled: false }],
    ["legacy embedded", { ...status, organization_kind: "embedded" }],
    ["another organization", { ...status, organization_id: "org-b" }],
    ["unknown", undefined],
  ])("does not fetch for %s organizations", (_name, provisioning) => {
    jest.mocked(useAgentsProvisioningStatus).mockReturnValue(provisioning);
    const { result } = renderHook(useFusionUsage, { wrapper });
    expect(result.current.isEligible).toBe(false);
    expect(getFusionUsage).not.toHaveBeenCalled();
  });

  it("does not fetch outside Cloud", () => {
    jest.mocked(useIsCloudApp).mockReturnValue(false);
    renderHook(useFusionUsage, { wrapper });
    expect(useAgentsProvisioningStatus).toHaveBeenCalledWith({ enabled: false });
    expect(getFusionUsage).not.toHaveBeenCalled();
  });

  it("drops the previous organization's data while the new organization is being provisioned", async () => {
    const { result, rerender } = renderHook(useFusionUsage, { wrapper });
    await waitFor(() => expect(result.current.data?.organizationId).toBe("org-a"));

    jest.mocked(useCurrentOrganizationId).mockReturnValue("org-b");
    rerender();
    expect(result.current.isEligible).toBe(false);
    expect(result.current.data).toBeUndefined();

    jest.mocked(useAgentsProvisioningStatus).mockReturnValue({ ...status, organization_id: "org-b" });
    jest.mocked(getFusionUsage).mockResolvedValue({ ...usage, organizationId: "org-b" });
    rerender();
    await waitFor(() => expect(result.current.data?.organizationId).toBe("org-b"));
  });

  it("rejects data belonging to another organization", async () => {
    jest.mocked(getFusionUsage).mockResolvedValue({ ...usage, organizationId: "org-b" });
    const { result } = renderHook(useFusionUsage, { wrapper });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.data).toBeUndefined();
  });

  it("surfaces request errors inline", async () => {
    jest.mocked(getFusionUsage).mockRejectedValue(new Error("unavailable"));
    const { result } = renderHook(useFusionUsage, { wrapper });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.data).toBeUndefined();
  });
});
