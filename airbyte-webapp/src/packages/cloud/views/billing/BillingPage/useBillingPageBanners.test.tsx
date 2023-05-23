import { renderHook } from "@testing-library/react-hooks";
import { TestWrapper } from "test-utils";

import { useExperiment } from "hooks/services/Experiment";
import { useFreeConnectorProgram } from "packages/cloud/components/experiments/FreeConnectorProgram";
import { WorkspaceTrialStatus } from "packages/cloud/lib/domain/cloudWorkspaces/types";
import { useGetCloudWorkspace } from "packages/cloud/services/workspaces/CloudWorkspacesService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

import { useBillingPageBanners } from "./useBillingPageBanners";

jest.mock("hooks/services/Experiment/ExperimentService");
const mockUseExperiment = useExperiment as unknown as jest.Mock<Partial<typeof useExperiment>>;

jest.mock("packages/cloud/components/experiments/FreeConnectorProgram");
const mockUseFreeConnectorProgram = useFreeConnectorProgram as unknown as jest.Mock<
  Partial<typeof useFreeConnectorProgram>
>;

jest.mock("services/workspaces/WorkspacesService");
const mockGetCurrentWorkspace = useCurrentWorkspace as unknown as jest.Mock<Partial<typeof useCurrentWorkspace>>;

jest.mock("packages/cloud/services/workspaces/CloudWorkspacesService");
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
      connectionStatusQuery: { data: { hasEligibleConnections, hasNonEligibleConnections } },
      userDidEnroll: false,
      enrollmentStatusQuery: { data: { isEnrolled, showEnrollmentUi: true } },
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
        mockHooks(WorkspaceTrialStatus.PRE_TRIAL, 0, false, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show FCP enrollment materials + an info variant banner", () => {
        mockHooks(WorkspaceTrialStatus.PRE_TRIAL, 0, false, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(true);
      });
    });
  });
  describe("out of trial with 0 connections enabled", () => {
    describe("enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 100, false, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banners + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 5, false, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });

      it("should show error banners + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 0, false, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 100, false, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banners + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 5, false, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });

      it("should show error banners + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 0, false, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
  });

  describe("out of trial with only eligible connections enabled", () => {
    describe("enrolled in FCP", () => {
      it("should show  info variant banner + no FCP banner if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 100, true, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show  info variant banner + no FCP banner if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 10, true, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show info variant banner + no FCP banner if user is in 0 credits state", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 0, true, false, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show FCP banner + info variant banner if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 100, true, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(true);
      });
      it("should show FCP banner + info variant banner if user has < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 10, true, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(true);
      });
      it("should show FCP banner + error variant banner if user has 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 0, true, false, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(true);
      });
    });
  });

  describe("out of trial with only non-eligible connections enabled", () => {
    describe("enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 100, false, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banner + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 10, false, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show error banner + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 0, false, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 100, false, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banner + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 10, false, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show error banner + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 0, false, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
  });
  describe("out of trial with eligible and non-eligible connections enabled", () => {
    describe("enrolled in FCP", () => {
      it("should show info banner + no FCP enrollment materials if > 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 100, true, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show warning banner + no FCP enrollment materials if < 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 10, true, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(false);
      });
      it("should show error banner + no FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 0, true, true, true);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(false);
      });
    });
    describe("not enrolled in FCP", () => {
      it("should show info banner + FCP enrollment materials if more than 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 100, true, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("info");
        expect(result.current.showFCPBanner).toEqual(true);
      });
      it("should show warning banner + FCP enrollment materials if fewer than 20 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 10, true, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("warning");
        expect(result.current.showFCPBanner).toEqual(true);
      });
      it("should show error banner + FCP enrollment materials if 0 credits", () => {
        mockHooks(WorkspaceTrialStatus.OUT_OF_TRIAL, 0, true, true, false);

        const { result } = renderHook(() => useBillingPageBanners(), { wrapper: TestWrapper });
        expect(result.current.bannerVariant).toEqual("error");
        expect(result.current.showFCPBanner).toEqual(true);
      });
    });
  });
});
