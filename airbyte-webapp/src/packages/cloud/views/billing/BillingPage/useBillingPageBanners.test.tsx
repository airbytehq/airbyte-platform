import { renderHook } from "@testing-library/react";

import { TestWrapper } from "test-utils";

import { useCurrentWorkspace } from "core/api";
import { useGetCloudWorkspace } from "core/api/cloud";
import { CloudWorkspaceReadWorkspaceTrialStatus as WorkspaceTrialStatus } from "core/api/types/CloudApi";
import { useExperiment } from "hooks/services/Experiment";

import { useBillingPageBanners } from "./useBillingPageBanners";

jest.mock("hooks/services/Experiment/ExperimentService");
const mockUseExperiment = useExperiment as unknown as jest.Mock<Partial<typeof useExperiment>>;

jest.mock("core/api");
const mockGetCurrentWorkspace = useCurrentWorkspace as unknown as jest.Mock<Partial<typeof useCurrentWorkspace>>;

jest.mock("core/api/cloud");
const mockUseGetCloudWorkspace = useGetCloudWorkspace as unknown as jest.Mock<Partial<typeof useGetCloudWorkspace>>;

const mockHooks = (workspaceTrialStatus: WorkspaceTrialStatus, remainingCredits: number) => {
  mockGetCurrentWorkspace.mockImplementation(() => {
    return { workspaceId: "1234" };
  });
  mockUseGetCloudWorkspace.mockImplementation(() => {
    return {
      workspaceId: "1234",
      workspaceTrialStatus,
      remainingCredits,
    };
  });
};

describe("useBillingPageBanners", () => {
  describe("pre-trial", () => {
    // because the pre-trial state can only exist with the flag enabled, we need to mock the flag as true!
    mockUseExperiment.mockImplementation((id) => {
      if (id === "billing.newTrialPolicy") {
        return true;
      }
      if (id === "billing.autoRecharge") {
        return false;
      }
      throw new Error(`experiment ${id} is not mocked.`);
    });
    it("should show an info variant banner", () => {
      mockHooks(WorkspaceTrialStatus.pre_trial, 0);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("info");
    });
  });
  describe("out of trial with 0 connections enabled", () => {
    it("should show info banner if > 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 100);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("info");
    });
    it("should show warning banners if < 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 5);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("warning");
    });

    it("should show error banner if 0 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 0);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("error");
    });
  });

  describe("out of trial with only eligible connections enabled", () => {
    it("should show  info variant banner if > 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 100);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("info");
    });
    it("should show error variant banner if < 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 10);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("warning");
    });
    it("should show error variant banner if user is in 0 credits state", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 0);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("error");
    });
  });

  describe("out of trial with only non-eligible connections enabled", () => {
    it("should show info banner if > 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 100);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("info");
    });
    it("should show warning banner if < 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 10);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("warning");
    });
    it("should show error banner if 0 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 0);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("error");
    });
  });
  describe("out of trial with eligible and non-eligible connections enabled", () => {
    it("should show info banner if > 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 100);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("info");
    });
    it("should show warning banner if < 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 10);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("warning");
    });
    it("should show error banner if 0 credits", () => {
      mockHooks(WorkspaceTrialStatus.out_of_trial, 0);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("error");
    });
  });
  describe("credit purchased with only eligible connections enabled", () => {
    it("should show info variant banner if > 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.credit_purchased, 100);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("info");
    });
    it("should show info variant banner if < 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.credit_purchased, 10);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("warning");
    });
    it("should show info variant banner if user is in 0 credits state", () => {
      mockHooks(WorkspaceTrialStatus.credit_purchased, 0);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("error");
    });
  });
  describe("credit purchased with only non-eligible connections enabled", () => {
    it("should show info banner if > 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.credit_purchased, 100);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("info");
    });
    it("should show warning banner if < 20 credits", () => {
      mockHooks(WorkspaceTrialStatus.credit_purchased, 10);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("warning");
    });
    it("should show error banner if 0 credits", () => {
      mockHooks(WorkspaceTrialStatus.credit_purchased, 0);

      const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
      expect(result.current.bannerVariant).toEqual("error");
    });
  });

  it("should always return info variant when auto recharge is on", () => {
    mockHooks(WorkspaceTrialStatus.out_of_trial, 0);
    mockUseExperiment.mockReturnValue(true);

    const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
    expect(result.current.bannerVariant).toEqual("info");
  });
});
