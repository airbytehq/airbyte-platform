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

const mockUseOrgInfo = useOrgInfo as jest.MockedFunction<typeof useOrgInfo>;
const mockUseCurrentOrganizationInfo = useCurrentOrganizationInfo as jest.MockedFunction<
  typeof useCurrentOrganizationInfo
>;
const mockUseGeneratedIntent = useGeneratedIntent as jest.MockedFunction<typeof useGeneratedIntent>;
const mockUseOrganizationTrialStatus = useOrganizationTrialStatus as jest.MockedFunction<
  typeof useOrganizationTrialStatus
>;
const mockDayjs = dayjs as jest.MockedFunction<typeof dayjs>;

// Test data fixtures
const mockOrganizationId = "test-org-id";
const mockTrialEndDate = "2024-01-15T00:00:00Z";
const createMockTrialStatus = (trialStatus: OrganizationTrialStatusReadTrialStatus, trialEndsAt?: string) => ({
  trialStatus,
  trialEndsAt,
});
const createMockOrgInfo = (
  paymentStatus: OrganizationPaymentConfigReadPaymentStatus,
  subscriptionStatus: OrganizationPaymentConfigReadSubscriptionStatus = "unsubscribed",
  accountType: OrganizationInfoReadBillingAccountType = "free",
  gracePeriodEndsAt?: number
) => ({
  organizationId: mockOrganizationId,
  organizationName: "Test Organization",
  sso: false,
  scim: false,
  billing: {
    paymentStatus,
    subscriptionStatus,
    accountType,
    gracePeriodEndsAt,
  },
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
      scim: false,
    });
    mockUseGeneratedIntent.mockReturnValue(true);
    mockUseOrganizationTrialStatus.mockReturnValue(undefined);
    mockDayjs.mockReturnValue({ diff: jest.fn().mockReturnValue(0) } as unknown as dayjs.Dayjs);
  });

  describe("Trial state", () => {
    it("calculates remaining trial days", () => {
      mockDayjs.mockReturnValue({ diff: jest.fn().mockReturnValue(5) } as unknown as dayjs.Dayjs);
      mockUseOrganizationTrialStatus.mockReturnValue(createMockTrialStatus("in_trial", mockTrialEndDate));
      const { result } = renderHook(() => useOrganizationSubscriptionStatus());
      expect(result.current.trialDaysLeft).toBe(5);
      expect(result.current.isInTrial).toBe(true);
    });

    it("detects a trial ending within 24 hours", () => {
      const diff = jest.fn().mockReturnValueOnce(0).mockReturnValueOnce(20);
      mockDayjs.mockReturnValue({ diff } as unknown as dayjs.Dayjs);
      mockUseOrganizationTrialStatus.mockReturnValue(createMockTrialStatus("in_trial", mockTrialEndDate));
      const { result } = renderHook(() => useOrganizationSubscriptionStatus());
      expect(result.current.isTrialEndingWithin24Hours).toBe(true);
    });

    it("does not calculate trial state outside a trial", () => {
      mockUseOrganizationTrialStatus.mockReturnValue(createMockTrialStatus("post_trial", mockTrialEndDate));
      const { result } = renderHook(() => useOrganizationSubscriptionStatus());
      expect(result.current.isInTrial).toBe(false);
      expect(result.current.trialDaysLeft).toBe(0);
      expect(result.current.isTrialEndingWithin24Hours).toBe(false);
    });
  });

  describe("Conditional trial status fetching", () => {
    it("fetches trial status for Standard Trial organizations with permission", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-standard-trial",
        sso: false,
        scim: false,
      });
      renderHook(() => useOrganizationSubscriptionStatus());
      expect(mockUseOrganizationTrialStatus).toHaveBeenCalledWith(mockOrganizationId, { enabled: true });
    });

    it("does not fetch trial status without permission", () => {
      mockUseGeneratedIntent.mockReturnValue(false);
      renderHook(() => useOrganizationSubscriptionStatus());
      expect(mockUseOrganizationTrialStatus).toHaveBeenCalledWith(mockOrganizationId, { enabled: false });
    });
  });

  describe("Organization plan detection", () => {
    it("should return true for isStandardPlan when organizationPlanId matches STANDARD", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-standard",
        sso: false,
        scim: false,
      });

      const { result } = renderHook(() => useOrganizationSubscriptionStatus());

      expect(result.current.isStandardPlan).toBe(true);
    });

    it("should return false for isStandardPlan when organizationPlanId does not match STANDARD", () => {
      mockUseCurrentOrganizationInfo.mockReturnValue({
        organizationId: mockOrganizationId,
        organizationName: "Test Organization",
        organizationPlanId: "plan-airbyte-plus",
        sso: false,
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
        scim: false,
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
