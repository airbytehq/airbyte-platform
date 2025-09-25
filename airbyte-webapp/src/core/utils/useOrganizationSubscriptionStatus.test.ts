import { renderHook } from "@testing-library/react";
import dayjs from "dayjs";

import { useOrganizationTrialStatus, useOrgInfo, useCurrentOrganizationInfo } from "core/api";
import {
  OrganizationTrialStatusReadTrialStatus,
  OrganizationPaymentConfigReadPaymentStatus,
  OrganizationPaymentConfigReadSubscriptionStatus,
  OrganizationInfoReadBillingAccountType,
} from "core/api/types/AirbyteClient";
import { useGeneratedIntent } from "core/utils/rbac";

import { useOrganizationSubscriptionStatus } from "./useOrganizationSubscriptionStatus";

// Mock dependencies
jest.mock("core/api");
jest.mock("core/utils/rbac");
jest.mock("dayjs");
jest.mock("components/ui/BrandingBadge/BrandingBadge", () => ({
  ORG_PLAN_IDS: {
    STANDARD: "plan-airbyte-standard",
    SME: "plan-airbyte-sme",
    FLEX: "plan-airbyte-flex",
    STANDARD_TRIAL: "plan-airbyte-standard-trial",
    UNIFIED_TRIAL: "plan-airbyte-unified-trial",
    PRO: "plan-airbyte-pro",
  },
}));

const mockUseOrgInfo = useOrgInfo as jest.MockedFunction<typeof useOrgInfo>;
const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.MockedFunction<
  typeof useCurrentOrganizationInfo
>;
const mockUseOrganizationTrialStatus = useOrganizationTrialStatus as jest.MockedFunction<
  typeof useOrganizationTrialStatus
>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;
const mockDayjs = dayjs as jest.MockedFunction<typeof dayjs>;

// Test data fixtures
const mockOrganizationId = "test-org-id";
const mockTrialEndDate = "2024-01-15T00:00:00Z";
const mockPastTrialEndDate = "2024-01-01T00:00:00Z";

const createMockOrgInfo = (
  paymentStatus: OrganizationPaymentConfigReadPaymentStatus,
  subscriptionStatus: OrganizationPaymentConfigReadSubscriptionStatus = "unsubscribed",
  accountType: OrganizationInfoReadBillingAccountType = "free",
  gracePeriodEndsAt?: number
) => ({
  organizationId: mockOrganizationId,
  organizationName: "Test Organization",
  sso: false,
  billing: {
    paymentStatus,
    subscriptionStatus,
    accountType,
    gracePeriodEndsAt,
  },
});

const createMockTrialStatus = (trialStatus: OrganizationTrialStatusReadTrialStatus, trialEndsAt?: string) => ({
  trialStatus,
  trialEndsAt,
});

describe("useOrganizationSubscriptionStatus", () => {
  beforeEach(() => {
    // Reset all mocks
    jest.clearAllMocks();

    // Setup default mocks
    mockUseOrgInfo.mockReturnValue(createMockOrgInfo("uninitialized"));
    mockUseCurrentOrganizationInfo.mockReturnValue({
      organizationId: mockOrganizationId,
      organizationName: "Test Organization",
      organizationPlanId: "plan-airbyte-standard",
      sso: false,
    });
    mockUseOrganizationTrialStatus.mockReturnValue(undefined);
    mockUseGeneratedIntent.mockReturnValue(true);

    // Setup simple dayjs mock - default returns 0 days/hours
    const mockDayjsInstance = {
      diff: jest.fn().mockReturnValue(0),
    };
    mockDayjs.mockReturnValue(mockDayjsInstance as unknown as dayjs.Dayjs);
  });

  describe("Trial state", () => {
    it.each([
      ["in_trial", mockTrialEndDate, 5, "active trial with future end date"],
      ["in_trial", mockPastTrialEndDate, 0, "expired trial"],
      ["in_trial", undefined, 0, "trial without end date"],
      ["post_trial", mockTrialEndDate, 0, "non-trial status"],
    ])("should calculate %s days for %s", (trialStatus, trialEndsAt, expectedDays, _scenario) => {
      // Setup specific dayjs mock for this test
      if (trialEndsAt === mockTrialEndDate) {
        mockDayjs.mockReturnValue({ diff: jest.fn().mockReturnValue(5) } as unknown as dayjs.Dayjs);
      } else if (trialEndsAt === mockPastTrialEndDate) {
        mockDayjs.mockReturnValue({ diff: jest.fn().mockReturnValue(-5) } as unknown as dayjs.Dayjs);
      }

      mockUseOrganizationTrialStatus.mockReturnValue(
        createMockTrialStatus(trialStatus as OrganizationTrialStatusReadTrialStatus, trialEndsAt)
      );

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.trialDaysLeft).toBe(expectedDays);
    });

    it.each([
      ["in_trial", true],
      ["pre_trial", false],
      ["post_trial", false],
    ])("should return isInTrial=%s for trial status '%s'", (trialStatus, expected) => {
      mockUseOrganizationTrialStatus.mockReturnValue(
        createMockTrialStatus(trialStatus as OrganizationTrialStatusReadTrialStatus)
      );

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isInTrial).toBe(expected);
    });

    describe("isTrialEndingWithin24Hours", () => {
      it.each([
        [25, false, "more than 24 hours left"],
        [20, true, "20 hours left"],
        [12, true, "12 hours left"],
        [1, true, "1 hour left"],
        [0, false, "trial already ended"],
        [-5, false, "trial ended 5 hours ago"],
      ])("should return %s when %s hours left (%s)", (hoursLeft, expected, _scenario) => {
        const mockDiffFn = jest.fn();
        mockDiffFn.mockReturnValueOnce(5); // days call
        mockDiffFn.mockReturnValueOnce(hoursLeft); // hours call

        const mockDayjsInstance = {
          diff: mockDiffFn,
        };
        mockDayjs.mockReturnValue(mockDayjsInstance as unknown as dayjs.Dayjs);

        mockUseOrganizationTrialStatus.mockReturnValue(createMockTrialStatus("in_trial", mockTrialEndDate));

        const { result } = renderHook(() => useOrganizationSubscriptionStatus());

        expect(result.current.isTrialEndingWithin24Hours).toBe(expected);
      });

      it("should return false when not in trial", () => {
        mockUseOrganizationTrialStatus.mockReturnValue(createMockTrialStatus("post_trial", mockTrialEndDate));

        const { result } = renderHook(() => useOrganizationSubscriptionStatus());

        expect(result.current.isTrialEndingWithin24Hours).toBe(false);
      });

      it("should return false when no trial end date", () => {
        mockUseOrganizationTrialStatus.mockReturnValue(createMockTrialStatus("in_trial", undefined));

        const { result } = renderHook(() => useOrganizationSubscriptionStatus());

        expect(result.current.isTrialEndingWithin24Hours).toBe(false);
      });
    });
  });

  describe("Conditional trial status fetching", () => {
    it("should fetch trial status when payment status is uninitialized and user has permission", () => {
      mockUseOrgInfo.mockReturnValue(createMockOrgInfo("uninitialized"));
      mockUseGeneratedIntent.mockReturnValue(true);
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-unified-trial",
        sso: false,
      });

      renderHook(() => useOrganizationSubscriptionStatus());

      expect(mockUseOrganizationTrialStatus).toHaveBeenCalledWith(mockOrganizationId, {
        enabled: true,
      });
    });

    it("should fetch trial status when payment status is okay and user has permission", () => {
      mockUseOrgInfo.mockReturnValue(createMockOrgInfo("okay"));
      mockUseGeneratedIntent.mockReturnValue(true);
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-unified-trial",
        sso: false,
      });

      renderHook(() => useOrganizationSubscriptionStatus());

      expect(mockUseOrganizationTrialStatus).toHaveBeenCalledWith(mockOrganizationId, {
        enabled: true,
      });
    });

    it("should fetch trial status when payment status is locked and user has permission", () => {
      mockUseOrgInfo.mockReturnValue(createMockOrgInfo("locked"));
      mockUseGeneratedIntent.mockReturnValue(true);
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-unified-trial",
        sso: false,
      });

      renderHook(() => useOrganizationSubscriptionStatus());

      expect(mockUseOrganizationTrialStatus).toHaveBeenCalledWith(mockOrganizationId, {
        enabled: true,
      });
    });

    it("should not fetch trial status when user lacks permission", () => {
      mockUseOrgInfo.mockReturnValue(createMockOrgInfo("uninitialized"));
      mockUseGeneratedIntent.mockReturnValue(false);

      renderHook(() => useOrganizationSubscriptionStatus());

      expect(mockUseOrganizationTrialStatus).toHaveBeenCalledWith(mockOrganizationId, {
        enabled: false,
      });
    });

    it("should not fetch trial status when payment status is locked but user lacks permission", () => {
      mockUseOrgInfo.mockReturnValue(createMockOrgInfo("locked"));
      mockUseGeneratedIntent.mockReturnValue(false);

      renderHook(() => useOrganizationSubscriptionStatus());

      expect(mockUseOrganizationTrialStatus).toHaveBeenCalledWith(mockOrganizationId, {
        enabled: false,
      });
    });
  });

  describe("Organization plan detection", () => {
    it("should return true for isUnifiedTrialPlan when organizationPlanId matches UNIFIED_TRIAL", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-unified-trial",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isUnifiedTrialPlan).toBe(true);
    });

    it("should return false for isUnifiedTrialPlan when organizationPlanId does not match UNIFIED_TRIAL", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-standard",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isUnifiedTrialPlan).toBe(false);
    });

    it("should return false for isUnifiedTrialPlan when organizationPlanId is undefined", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: undefined,
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isUnifiedTrialPlan).toBe(false);
    });

    it("should return true for isStandardPlan when organizationPlanId matches STANDARD", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-standard",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isStandardPlan).toBe(true);
    });

    it("should return false for isStandardPlan when organizationPlanId does not match STANDARD", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-unified-trial",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isStandardPlan).toBe(false);
    });

    it("should return false for isStandardPlan when organizationPlanId is undefined", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: undefined,
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isStandardPlan).toBe(false);
    });

    it("should return true for isStandardTrialPlan when organizationPlanId matches STANDARD_TRIAL", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-standard-trial",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isStandardTrialPlan).toBe(true);
    });

    it("should return false for isStandardTrialPlan when organizationPlanId does not match STANDARD_TRIAL", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-standard",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isStandardTrialPlan).toBe(false);
    });

    it("should return false for isStandardTrialPlan when organizationPlanId is undefined", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: undefined,
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isStandardTrialPlan).toBe(false);
    });

    // Tests for SME plan
    it("should return true for isSmePlan when organizationPlanId matches SME", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-sme",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isSmePlan).toBe(true);
    });

    it("should return false for isSmePlan when organizationPlanId does not match SME", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-standard",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isSmePlan).toBe(false);
    });

    it("should return false for isSmePlan when organizationPlanId is undefined", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: undefined,
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isSmePlan).toBe(false);
    });

    // Tests for Flex plan
    it("should return true for isFlexPlan when organizationPlanId matches FLEX", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-flex",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isFlexPlan).toBe(true);
    });

    it("should return false for isFlexPlan when organizationPlanId does not match FLEX", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-pro",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isFlexPlan).toBe(false);
    });

    it("should return false for isFlexPlan when organizationPlanId is undefined", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: undefined,
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isFlexPlan).toBe(false);
    });

    // Tests for Pro plan
    it("should return true for isProPlan when organizationPlanId matches PRO", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-pro",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isProPlan).toBe(true);
    });

    it("should return false for isProPlan when organizationPlanId does not match PRO", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-sme",
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isProPlan).toBe(false);
    });

    it("should return false for isProPlan when organizationPlanId is undefined", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: undefined,
        sso: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isProPlan).toBe(false);
    });
  });

  describe("Billing data access", () => {
    it("should provide access to specific billing properties", () => {
      const mockBilling = {
        paymentStatus: "okay" as const,
        subscriptionStatus: "subscribed" as const,
        accountType: "free" as const,
        gracePeriodEndsAt: 1705276800000,
      };
      mockUseOrgInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        sso: false,
        billing: mockBilling,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.paymentStatus).toBe("okay");
      expect(result.current.subscriptionStatus).toBe("subscribed");
      expect(result.current.accountType).toBe("free");
      expect(result.current.gracePeriodEndsAt).toBe(1705276800000);
      expect(result.current.canManageOrganizationBilling).toBe(true);
    });
  });
});
