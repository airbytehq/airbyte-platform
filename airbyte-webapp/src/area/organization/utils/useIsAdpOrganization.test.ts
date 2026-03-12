import { renderHook } from "@testing-library/react";

import { useCurrentOrganizationInfo } from "core/api";

import { ADP_PLAN_IDS, ORG_PLAN_IDS } from "./organizationPlans";
import { useIsAdpOrganization } from "./useIsAdpOrganization";

jest.mock("core/api");

const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.MockedFunction<
  typeof useCurrentOrganizationInfo
>;

const mockOrganizationId = "test-org-id";

const createMockOrgInfo = (organizationPlanId?: string) => ({
  organizationId: mockOrganizationId,
  organizationName: "Test Organization",
  organizationPlanId,
  sso: false,
});

describe("useIsAdpOrganization", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("returns true for ADP plan IDs", () => {
    it.each([
      ["EMBEDDED_PAYG", ADP_PLAN_IDS.EMBEDDED_PAYG],
      ["EMBEDDED_ANNUAL_COMMITMENT", ADP_PLAN_IDS.EMBEDDED_ANNUAL_COMMITMENT],
      ["AGENT_ENGINE_PAYG", ADP_PLAN_IDS.AGENT_ENGINE_PAYG],
    ])("should return true for %s plan", (_planName, planId) => {
      mockUseCurrentOrganizationInfo.mockReturnValue(createMockOrgInfo(planId));

      const { result } = renderHook(() => useIsAdpOrganization());

      expect(result.current).toBe(true);
    });
  });

  describe("returns false for non-ADP plan IDs", () => {
    it.each([
      ["CORE", ORG_PLAN_IDS.CORE],
      ["STANDARD", ORG_PLAN_IDS.STANDARD],
      ["SME", ORG_PLAN_IDS.SME],
      ["FLEX", ORG_PLAN_IDS.FLEX],
      ["PRO", ORG_PLAN_IDS.PRO],
      ["STANDARD_TRIAL", ORG_PLAN_IDS.STANDARD_TRIAL],
      ["UNIFIED_TRIAL", ORG_PLAN_IDS.UNIFIED_TRIAL],
    ])("should return false for %s plan", (_planName, planId) => {
      mockUseCurrentOrganizationInfo.mockReturnValue(createMockOrgInfo(planId));

      const { result } = renderHook(() => useIsAdpOrganization());

      expect(result.current).toBe(false);
    });
  });

  it("should return false when organizationPlanId is undefined", () => {
    mockUseCurrentOrganizationInfo.mockReturnValue(createMockOrgInfo(undefined));

    const { result } = renderHook(() => useIsAdpOrganization());

    expect(result.current).toBe(false);
  });

  it("should return false for unknown plan IDs", () => {
    mockUseCurrentOrganizationInfo.mockReturnValue(createMockOrgInfo("plan-unknown"));

    const { result } = renderHook(() => useIsAdpOrganization());

    expect(result.current).toBe(false);
  });
});
