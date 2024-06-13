import { getByTestId, queryByTestId } from "@testing-library/react";

import { mocked, render } from "test-utils";

import { useGetCloudWorkspace } from "core/api/cloud";
import { CloudWorkspaceReadCreditStatus, CloudWorkspaceReadWorkspaceTrialStatus } from "core/api/types/CloudApi";
import { useExperiment } from "hooks/services/Experiment";

import { BillingBanners } from "./BillingBanners";

jest.mock("core/api/cloud", () => ({
  useGetCloudWorkspace: jest.fn(),
}));

jest.mock("core/api", () => ({
  useCurrentWorkspace: jest.fn().mockReturnValue({
    workspaceId: "123",
  }),
}));

jest.mock("hooks/services/Experiment", () => ({
  useExperiment: jest.fn().mockReturnValue(false),
}));

jest.mock("core/services/auth", () => ({
  useAuthService: jest.fn().mockReturnValue({
    emailVerified: true,
    sendEmailVerification: jest.fn(),
  }),
}));

function mockAutoRechargeExperiment(enabled: boolean) {
  mocked(useExperiment).mockImplementation((experiment) => {
    if (experiment === "billing.autoRecharge") {
      return enabled;
    }

    throw new Error("Unexpected experiment");
  });
}

function mockWorkspace(
  credits: number,
  creditStatus: CloudWorkspaceReadCreditStatus,
  trialStatus: CloudWorkspaceReadWorkspaceTrialStatus
) {
  mocked(useGetCloudWorkspace).mockReturnValue({
    workspaceId: "123",
    remainingCredits: credits,
    creditStatus,
    workspaceTrialStatus: trialStatus,
  });
}

describe("BillingBanners", () => {
  beforeEach(() => {
    mockAutoRechargeExperiment(false);
  });

  describe("auto recharge banner", () => {
    it("should show auto recharge enabled banner", async () => {
      mockAutoRechargeExperiment(true);
      mockWorkspace(
        500,
        CloudWorkspaceReadCreditStatus.positive,
        CloudWorkspaceReadWorkspaceTrialStatus.credit_purchased
      );
      const banners = await render(<BillingBanners />);
      expect(getByTestId(banners.baseElement, "autoRechargeEnabledBanner")).toBeVisible();
    });

    it("should only show auto recharge banner even on low credit", async () => {
      mockAutoRechargeExperiment(true);
      mockWorkspace(
        5,
        CloudWorkspaceReadCreditStatus.positive,
        CloudWorkspaceReadWorkspaceTrialStatus.credit_purchased
      );
      const banners = await render(<BillingBanners />);
      expect(queryByTestId(banners.baseElement, "autoRechargeEnabledBanner")).toBeInTheDocument();
      expect(queryByTestId(banners.baseElement, "lowCreditsBanner")).not.toBeInTheDocument();
      expect(queryByTestId(banners.baseElement, "noCreditsBanner")).not.toBeInTheDocument();
    });

    it("should only show auto recharge banner even on no credits", async () => {
      mockAutoRechargeExperiment(true);
      mockWorkspace(
        -2,
        CloudWorkspaceReadCreditStatus.negative_beyond_grace_period,
        CloudWorkspaceReadWorkspaceTrialStatus.credit_purchased
      );
      const banners = await render(<BillingBanners />);
      expect(queryByTestId(banners.baseElement, "autoRechargeEnabledBanner")).toBeInTheDocument();
      expect(queryByTestId(banners.baseElement, "lowCreditsBanner")).not.toBeInTheDocument();
      expect(queryByTestId(banners.baseElement, "noCreditsBanner")).not.toBeInTheDocument();
    });
  });

  describe("no billing account", () => {
    it("should show only no billing account banner", async () => {
      mockWorkspace(-5, CloudWorkspaceReadCreditStatus.positive, CloudWorkspaceReadWorkspaceTrialStatus.out_of_trial);
      const banners = await render(<BillingBanners />);
      expect(queryByTestId(banners.baseElement, "noBillingAccount")).toBeInTheDocument();
      expect(queryByTestId(banners.baseElement, "lowCreditsBanner")).not.toBeInTheDocument();
      expect(queryByTestId(banners.baseElement, "noCreditsBanner")).not.toBeInTheDocument();
    });
  });

  describe("low credit warnings", () => {
    it("should show low credit banner when credits are low", async () => {
      mockWorkspace(
        5,
        CloudWorkspaceReadCreditStatus.positive,
        CloudWorkspaceReadWorkspaceTrialStatus.credit_purchased
      );
      const banners = await render(<BillingBanners />);
      expect(queryByTestId(banners.baseElement, "lowCreditsBanner")).toBeInTheDocument();
      expect(queryByTestId(banners.baseElement, "noCreditsBanner")).not.toBeInTheDocument();
    });

    it("should show no credit banner when credits are negative", async () => {
      mockWorkspace(
        0,
        CloudWorkspaceReadCreditStatus.negative_within_grace_period,
        CloudWorkspaceReadWorkspaceTrialStatus.credit_purchased
      );
      const banners = await render(<BillingBanners />);
      expect(queryByTestId(banners.baseElement, "noCreditsBanner")).toBeInTheDocument();
      expect(queryByTestId(banners.baseElement, "lowCreditsBanner")).not.toBeInTheDocument();
    });
  });
});
