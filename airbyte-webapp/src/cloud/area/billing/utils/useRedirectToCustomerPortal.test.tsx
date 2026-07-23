import { act, renderHook } from "@testing-library/react";

import { TestWrapper, mocked } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useGetCustomerPortalUrl } from "core/api";
import { trackError } from "core/utils/datadog";

import { useRedirectToCustomerPortal } from "./useRedirectToCustomerPortal";

jest.mock("area/organization/utils/useCurrentOrganizationId", () => ({
  useCurrentOrganizationId: jest.fn(),
}));

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
}));

jest.mock("core/api", () => ({
  useGetCustomerPortalUrl: jest.fn(),
}));

jest.mock("core/utils/datadog", () => ({
  trackError: jest.fn(),
}));

describe("useRedirectToCustomerPortal", () => {
  const mockGetCustomerPortalUrl = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    mockExperiments({ "billing.selfServePlusPlan": false });
    mocked(useCurrentOrganizationId).mockReturnValue("test-organization-id");
    mockGetCustomerPortalUrl.mockRejectedValue(new Error("stop before navigation"));
    mocked(useGetCustomerPortalUrl).mockReturnValue({
      mutateAsync: mockGetCustomerPortalUrl,
      isLoading: false,
    } as unknown as ReturnType<typeof useGetCustomerPortalUrl>);
  });

  it("returns setup flows to Billing when self-serve Plus is disabled", async () => {
    const { result } = renderHook(() => useRedirectToCustomerPortal("setup"), { wrapper: TestWrapper });

    await act(async () => {
      await result.current.goToCustomerPortal();
    });

    expect(mockGetCustomerPortalUrl).toHaveBeenCalledWith(
      expect.objectContaining({
        flow: "setup",
        organizationId: "test-organization-id",
        returnUrl: "http://localhost/organization/test-organization-id/settings/billing",
      })
    );
    expect(trackError).toHaveBeenCalled();
  });

  it("returns setup flows to Plan when self-serve Plus is enabled", async () => {
    mockExperiments({ "billing.selfServePlusPlan": true });
    const { result } = renderHook(() => useRedirectToCustomerPortal("setup", "plus"), { wrapper: TestWrapper });

    await act(async () => {
      await result.current.goToCustomerPortal();
    });

    expect(mockGetCustomerPortalUrl).toHaveBeenCalledWith({
      flow: "setup",
      plan: "plus",
      organizationId: "test-organization-id",
      returnUrl: "http://localhost/organization/test-organization-id/settings/plan",
    });
  });

  it("keeps non-setup flows returning to Billing when self-serve Plus is enabled", async () => {
    mockExperiments({ "billing.selfServePlusPlan": true });
    const { result } = renderHook(() => useRedirectToCustomerPortal("portal"), { wrapper: TestWrapper });

    await act(async () => {
      await result.current.goToCustomerPortal();
    });

    expect(mockGetCustomerPortalUrl).toHaveBeenCalledWith(
      expect.objectContaining({
        flow: "portal",
        organizationId: "test-organization-id",
        returnUrl: "http://localhost/organization/test-organization-id/settings/billing",
      })
    );
  });
});
