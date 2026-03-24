import { renderHook } from "@testing-library/react";

import { useCurrentOrganizationInfo } from "core/api";

import { useOrganizationPlan } from "./useOrganizationPlan";

jest.mock("core/api");

const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.MockedFunction<
  typeof useCurrentOrganizationInfo
>;

const mockOrgId = "test-org-id";

const mockOrgInfo = (organizationPlanId: string | undefined) => ({
  organizationId: mockOrgId,
  organizationName: "Test Organization",
  organizationPlanId,
  sso: false,
});

describe("useOrganizationPlan", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationInfo.mockReturnValue(mockOrgInfo("plan-airbyte-standard"));
  });

  describe("isStiggPlanEnabled", () => {
    it("returns true when organizationPlanId is set", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue(mockOrgInfo("plan-airbyte-standard"));
      const { result } = renderHook(() => useOrganizationPlan());
      expect(result.current.isStiggPlanEnabled).toBe(true);
    });

    it("returns false when organizationPlanId is undefined", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue(mockOrgInfo(undefined));
      const { result } = renderHook(() => useOrganizationPlan());
      expect(result.current.isStiggPlanEnabled).toBe(false);
    });
  });

  describe.each([
    ["isUnifiedTrialPlan", "plan-airbyte-unified-trial"],
    ["isStandardTrialPlan", "plan-airbyte-standard-trial"],
    ["isStandardPlan", "plan-airbyte-standard"],
    ["isSmePlan", "plan-airbyte-sme"],
    ["isFlexPlan", "plan-airbyte-flex"],
    ["isProPlan", "plan-airbyte-pro"],
  ] as const)("%s", (flag, planId) => {
    it(`returns true when organizationPlanId is ${planId}`, () => {
      mockUseCurrentOrganizationInfo.mockReturnValue(mockOrgInfo(planId));
      const { result } = renderHook(() => useOrganizationPlan());
      expect(result.current[flag]).toBe(true);
    });

    it("returns false when organizationPlanId is a different plan", () => {
      const differentPlan = planId === "plan-airbyte-standard" ? "plan-airbyte-pro" : "plan-airbyte-standard";
      mockUseCurrentOrganizationInfo.mockReturnValue(mockOrgInfo(differentPlan));
      const { result } = renderHook(() => useOrganizationPlan());
      expect(result.current[flag]).toBe(false);
    });

    it("returns false when organizationPlanId is undefined", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue(mockOrgInfo(undefined));
      const { result } = renderHook(() => useOrganizationPlan());
      expect(result.current[flag]).toBe(false);
    });
  });
});
