import { renderHook } from "@testing-library/react-hooks";

import { TestWrapper } from "test-utils";

import { useCurrentWorkspace } from "core/api";
import { useFreeConnectorProgram, useGetCloudWorkspace } from "core/api/cloud";
import { CloudWorkspaceReadWorkspaceTrialStatus as WorkspaceTrialStatus } from "core/api/types/CloudApi";
import { useExperiment } from "hooks/services/Experiment";

import { useBillingPageBanners } from "./useBillingPageBanners";

jest.mock("hooks/services/Experiment/ExperimentService");
const mockUseExperiment = useExperiment as unknown as jest.Mock<Partial<typeof useExperiment>>;

jest.mock("packages/cloud/components/experiments/FreeConnectorProgram");
const mockUseFreeConnectorProgram = useFreeConnectorProgram as unknown as jest.Mock<
  Partial<typeof useFreeConnectorProgram>
>;

jest.mock("core/api");
const mockGetCurrentWorkspace = useCurrentWorkspace as unknown as jest.Mock<Partial<typeof useCurrentWorkspace>>;

jest.mock("core/api/cloud");
const mockUseGetCloudWorkspace = useGetCloudWorkspace as unknown as jest.Mock<Partial<typeof useGetCloudWorkspace>>;

const mockHooks = (
  workspaceTrialStatus: WorkspaceTrialStatus,
  remainingCredits: number,
  hasEligibleConnections: boolean,
  hasNonEligibleConnections: boolean,
  isEnrolled: boolean
) => {
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

  mockUseFreeConnectorProgram.mockImplementation(() => {
    return {
      userDidEnroll: false,
      programStatusQuery: {
        data: { isEnrolled, showEnrollmentUi: true, hasEligibleConnections, hasNonEligibleConnections },
      },
    };
  });
};

describe("useBillingPageBanners", () => {
  describe("pre-trial", () => {
    // because the pre-trial state can only exist with the flag enabled, we need to mock the flag as true!
    mockUseExperiment.mockImplementation(() => {
      return {
        useExperiment: (id: string) => {
          if (id === "billing.newTrialPolicy") {
            return true;
          }
          return undefined;
        },
        ExperimentProvider: ({ children }: React.PropsWithChildren<unknown>) => <>{children}</>,
      };
    });
    describe("enrolled in FCP", () => {
      it("should show an info variant banner and no FCP materials", () => {
        mockHooks(WorkspaceTrialStatus.pre_trial, 0, false, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show FCP enrollment materials + an info variant banner", () => {
        mockHooks(WorkspaceTrialStatus.pre_trial, 0, false, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(true);
      });
    });
  });
  describe("out of trial with 0 connections enabled", () => {
    describe("enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 100, false, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banners + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 5, false, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });

      it("should show error banners + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 0, false, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 100, false, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banners + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 5, false, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });

      it("should show error banners + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 0, false, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
  });

  describe("out of trial with only eligible connections enabled", () => {
    describe("enrolled in FCP", () => {
      it("should show  info variant banner + no FCP banner if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 100, true, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show error variant banner + no FCP banner if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 10, true, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show error variant banner + no FCP banner if user is in 0 credits state", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 0, true, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show FCP banner + info variant banner if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 100, true, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(true);
      });
      it("should show FCP banner + warning variant banner if user has < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 10, true, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(true);
      });
      it("should show FCP banner + error variant banner if user has 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 0, true, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(true);
      });
    });
  });

  describe("out of trial with only non-eligible connections enabled", () => {
    describe("enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 100, false, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banner + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 10, false, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show error banner + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 0, false, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 100, false, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banner + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 10, false, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show error banner + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 0, false, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
  });
  describe("out of trial with eligible and non-eligible connections enabled", () => {
    describe("enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 100, true, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banner + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 10, true, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show error banner + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 0, true, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show info banner + FCP enrollment materials if more than 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 100, true, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(true);
      });
      it("should show warning banner + FCP enrollment materials if fewer than 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 10, true, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(true);
      });
      it("should show error banner + FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.out_of_trial, 0, true, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(true);
      });
    });
  });
  describe("credit purchased with only eligible connections enabled", () => {
    describe("enrolled in FCP", () => {
      it("should show  info variant banner + no FCP banner if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 100, true, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show  info variant banner + no FCP banner if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 10, true, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show info variant banner + no FCP banner if user is in 0 credits state", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 0, true, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show FCP banner + info variant banner if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 100, true, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(true);
      });
      it("should show FCP banner + info variant banner if user has < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 10, true, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(true);
      });
      it("should show FCP banner + error variant banner if user has 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 0, true, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(true);
      });
    });
  });
  describe("credit purchased with only non-eligible connections enabled", () => {
    describe("enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 100, false, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banner + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 10, false, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show error banner + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 0, false, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 100, false, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banner + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 10, false, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show error banner + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.credit_purchased, 0, false, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
  });
});
