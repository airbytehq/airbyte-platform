import dayjs from "dayjs";

import { mocked, render } from "test-utils";

import {
  ISO8601DateTime,
  OrganizationInfoReadBillingAccountType,
  OrganizationInfoReadBillingPaymentStatus,
  OrganizationTrialStatusReadTrialStatus,
  OrganizationInfoReadBillingSubscriptionStatus,
} from "core/api/types/AirbyteClient";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";

import { StatusBanner } from "./StatusBanner";

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceLink: jest.fn().mockReturnValue((link: string) => link),
  useCurrentWorkspaceId: jest.fn().mockReturnValue("test-workspace-id"),
}));

jest.mock("core/utils/useOrganizationSubscriptionStatus", () => ({
  useOrganizationSubscriptionStatus: jest.fn(),
}));

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
}));

const mockSubscriptionStatus = (
  options: {
    paymentStatus?: OrganizationInfoReadBillingPaymentStatus;
    subscriptionStatus?: OrganizationInfoReadBillingSubscriptionStatus;
    accountType?: OrganizationInfoReadBillingAccountType;
    gracePeriodEndsAt?: number;
    trialStatus?: OrganizationTrialStatusReadTrialStatus;
    trialEndsAt?: ISO8601DateTime;
    trialDaysLeft?: number;
    canManageOrganizationBilling?: boolean;
    isTrialEndingWithin24Hours?: boolean;
    isUnifiedTrialPlan?: boolean;
    isStandardPlan?: boolean;
  } = {}
) => {
  mocked(useOrganizationSubscriptionStatus).mockReturnValue({
    trialStatus: options.trialStatus || "post_trial",
    trialEndsAt: options.trialEndsAt,
    isInTrial: options.trialStatus === "in_trial",
    isUnifiedTrialPlan: options.isUnifiedTrialPlan ?? true,
    isStandardPlan: options.isStandardPlan ?? true,
    trialDaysLeft: options.trialDaysLeft || 0,
    isTrialEndingWithin24Hours: options.isTrialEndingWithin24Hours || false,
    paymentStatus: options.paymentStatus || "okay",
    subscriptionStatus: options.subscriptionStatus || "subscribed",
    accountType: options.accountType,
    gracePeriodEndsAt: options.gracePeriodEndsAt,
    canManageOrganizationBilling: options.canManageOrganizationBilling ?? true,
  });
};

describe("StatusBanner", () => {
  it("should render nothing with paymentStatus=OKAY and not in trial", async () => {
    mockSubscriptionStatus({
      paymentStatus: "okay",
      subscriptionStatus: "subscribed",
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container).toHaveTextContent("");
  });

  it("should not render anything for manual billing", async () => {
    mockSubscriptionStatus({
      paymentStatus: "manual",
      subscriptionStatus: "subscribed",
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container).toHaveTextContent("");
  });

  it("should not render locked banner", async () => {
    mockSubscriptionStatus({
      paymentStatus: "locked",
      subscriptionStatus: "subscribed",
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your syncs are disabled.");
    expect(wrapper.container.textContent).toContain("Airbyte Support");
  });

  it("should render disabled banner w/o link", async () => {
    mockSubscriptionStatus({
      paymentStatus: "disabled",
      subscriptionStatus: "subscribed",
      trialStatus: "post_trial",
      canManageOrganizationBilling: false,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your syncs are disabled due to unpaid invoices.");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render disabled banner w/ link", async () => {
    mockSubscriptionStatus({
      paymentStatus: "disabled",
      subscriptionStatus: "subscribed",
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your syncs are disabled due to unpaid invoices.");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render grace period banner w/o link (1 day)", async () => {
    mockSubscriptionStatus({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().add(25, "hours").valueOf(),
      trialStatus: "post_trial",
      canManageOrganizationBilling: false,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled in 1 day");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render grace period banner w/o link (very soon)", async () => {
    mockSubscriptionStatus({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().add(5, "hours").valueOf(),
      trialStatus: "post_trial",
      canManageOrganizationBilling: false,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled very soon");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render grace period banner w/ link (1 day)", async () => {
    mockSubscriptionStatus({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().add(25, "hours").valueOf(),
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled in 1 day");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render grace period banner w/ link (very soon)", async () => {
    mockSubscriptionStatus({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().add(5, "hours").valueOf(),
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled very soon");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render pre-trial banner", async () => {
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "pre_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("once your first sync has succeeded");
  });

  it("should render pre-trial banner after leaving payment information", async () => {
    mockSubscriptionStatus({
      paymentStatus: "okay",
      subscriptionStatus: "subscribed",
      trialStatus: "pre_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("once your first sync has succeeded");
  });

  it("should not show a trial banner if the user cannot view trial status", async () => {
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "pre_trial",
      canManageOrganizationBilling: false,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("");
  });

  it("should render in-trial banner w/o link", async () => {
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "in_trial",
      trialEndsAt: dayjs().add(5, "days").add(1, "hours").toISOString(),
      trialDaysLeft: 5,
      canManageOrganizationBilling: false,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your trial ends in 5 days.");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render in-trial banner w/ link", async () => {
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "in_trial",
      trialEndsAt: dayjs().add(5, "days").add(1, "hours").toISOString(),
      trialDaysLeft: 5,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your trial ends in 5 days.");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render post-trial banner w/o link", async () => {
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "post_trial",
      canManageOrganizationBilling: false,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Subscribe to Airbyte");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render post-trial banner w/o link if unsubscribed", async () => {
    mockSubscriptionStatus({
      paymentStatus: "okay",
      subscriptionStatus: "unsubscribed",
      trialStatus: "post_trial",
      canManageOrganizationBilling: false,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Subscribe to Airbyte");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render post-trial banner w/ link", async () => {
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Subscribe to Airbyte");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render in-trial banner w/ payment method", async () => {
    mockSubscriptionStatus({
      paymentStatus: "okay",
      subscriptionStatus: "subscribed",
      trialStatus: "in_trial",
      trialEndsAt: dayjs().add(5, "days").add(1, "hours").toISOString(),
      trialDaysLeft: 5,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your trial ends in 5 days.");
  });

  it("should not render banner for manual payment status in top_level context", async () => {
    mockSubscriptionStatus({
      paymentStatus: "manual",
      subscriptionStatus: "subscribed",
      accountType: "free",
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container).toHaveTextContent("");
  });

  it("should not render banner for manual payment status with internal account in top_level context", async () => {
    mockSubscriptionStatus({
      paymentStatus: "manual",
      subscriptionStatus: "subscribed",
      accountType: "internal",
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container).toHaveTextContent("");
  });

  it("should not render banner when billing is undefined", async () => {
    mockSubscriptionStatus({
      paymentStatus: undefined,
      subscriptionStatus: undefined,
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container).toHaveTextContent("");
  });

  it("should handle grace period with undefined gracePeriodEndsAt", async () => {
    mockSubscriptionStatus({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: undefined,
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled very soon");
  });

  it("should handle grace period with past date (0 days)", async () => {
    mockSubscriptionStatus({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().subtract(1, "day").valueOf(),
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled very soon");
  });

  it("should render post-trial banner when subscriptionStatus is unsubscribed", async () => {
    mockSubscriptionStatus({
      paymentStatus: "okay",
      subscriptionStatus: "unsubscribed",
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Subscribe to Airbyte");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  describe("unified trial plan behavior", () => {
    it("should render 24-hour trial warning with error level and exact time (with link)", async () => {
      const trialEndTime = dayjs("2024-01-01T21:28:00Z").toISOString();
      mockSubscriptionStatus({
        trialStatus: "in_trial",
        paymentStatus: "uninitialized",
        subscriptionStatus: "subscribed",
        trialEndsAt: trialEndTime,
        trialDaysLeft: 0,
        isTrialEndingWithin24Hours: true,
        canManageOrganizationBilling: true,
      });
      const wrapper = await render(<StatusBanner />);
      expect(wrapper.container.textContent).toContain(
        "Your trial will end at 1:28 PM. Upgrade now to keep your syncs going."
      );
      expect(wrapper.queryByRole("link")).toBeInTheDocument();
    });

    it("should render 24-hour trial warning with error level and exact time (without link)", async () => {
      const trialEndTime = dayjs("2024-01-01T21:28:00Z").toISOString();
      mockSubscriptionStatus({
        trialStatus: "in_trial",
        paymentStatus: "uninitialized",
        subscriptionStatus: "subscribed",
        trialEndsAt: trialEndTime,
        trialDaysLeft: 0,
        isTrialEndingWithin24Hours: true,
        canManageOrganizationBilling: false,
      });
      const wrapper = await render(<StatusBanner />);
      expect(wrapper.container.textContent).toBe(
        "Your trial will end at 1:28 PM. Upgrade now to keep your syncs going."
      );
      expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
    });

    it("should render 'today' trial warning with warning level (with link)", async () => {
      mockSubscriptionStatus({
        trialStatus: "in_trial",
        paymentStatus: "uninitialized",
        subscriptionStatus: "subscribed",
        trialDaysLeft: 0,
        isTrialEndingWithin24Hours: false,
        canManageOrganizationBilling: true,
      });
      const wrapper = await render(<StatusBanner />);
      expect(wrapper.container.textContent).toBe("Your trial ends today. Upgrade now to keep your syncs going.");
      expect(wrapper.queryByRole("link")).toBeInTheDocument();
    });

    it("should render 'today' trial warning with warning level (without link)", async () => {
      mockSubscriptionStatus({
        trialStatus: "in_trial",
        paymentStatus: "uninitialized",
        subscriptionStatus: "subscribed",
        trialDaysLeft: 0,
        isTrialEndingWithin24Hours: false,
        canManageOrganizationBilling: false,
      });
      const wrapper = await render(<StatusBanner />);
      expect(wrapper.container.textContent).toBe("Your trial ends today. Upgrade now to keep your syncs going.");
      expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
    });

    it("should render 'tomorrow' trial warning with warning level (with link)", async () => {
      mockSubscriptionStatus({
        trialStatus: "in_trial",
        paymentStatus: "uninitialized",
        subscriptionStatus: "subscribed",
        trialDaysLeft: 1,
        isTrialEndingWithin24Hours: false,
        canManageOrganizationBilling: true,
      });
      const wrapper = await render(<StatusBanner />);
      expect(wrapper.container.textContent).toBe("Your trial ends tomorrow. Upgrade now to keep your syncs going.");
      expect(wrapper.queryByRole("link")).toBeInTheDocument();
    });

    it("should render 'tomorrow' trial warning with warning level (without link)", async () => {
      mockSubscriptionStatus({
        trialStatus: "in_trial",
        paymentStatus: "uninitialized",
        subscriptionStatus: "subscribed",
        trialDaysLeft: 1,
        isTrialEndingWithin24Hours: false,
        canManageOrganizationBilling: false,
      });
      const wrapper = await render(<StatusBanner />);
      expect(wrapper.container.textContent).toBe("Your trial ends tomorrow. Upgrade now to keep your syncs going.");
      expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
    });

    it("should render multi-day trial warning with warning level (with link)", async () => {
      mockSubscriptionStatus({
        trialStatus: "in_trial",
        paymentStatus: "uninitialized",
        subscriptionStatus: "subscribed",
        trialDaysLeft: 5,
        isTrialEndingWithin24Hours: false,
        canManageOrganizationBilling: true,
      });
      const wrapper = await render(<StatusBanner />);
      expect(wrapper.container.textContent).toBe("Your trial ends in 5 days. Upgrade now to keep your syncs going.");
      expect(wrapper.queryByRole("link")).toBeInTheDocument();
    });

    it("should render multi-day trial warning with warning level (without link)", async () => {
      mockSubscriptionStatus({
        trialStatus: "in_trial",
        paymentStatus: "uninitialized",
        subscriptionStatus: "subscribed",
        trialDaysLeft: 5,
        isTrialEndingWithin24Hours: false,
        canManageOrganizationBilling: false,
      });
      const wrapper = await render(<StatusBanner />);
      expect(wrapper.container.textContent).toBe("Your trial ends in 5 days. Upgrade now to keep your syncs going.");
      expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
    });

    it("should render subscribe text when not using unified trial plan", async () => {
      mockSubscriptionStatus({
        trialStatus: "in_trial",
        paymentStatus: "uninitialized",
        subscriptionStatus: "subscribed",
        trialDaysLeft: 5,
        isTrialEndingWithin24Hours: false,
        canManageOrganizationBilling: true,
        isUnifiedTrialPlan: false,
      });
      const wrapper = await render(<StatusBanner />);
      expect(wrapper.container.textContent).toBe(
        "Your trial ends in 5 days. Subscribe today to keep using Airbyte after your trial ends."
      );
      expect(wrapper.queryByRole("link")).toBeInTheDocument();
    });

    it("should render upgrade warning for trials with more than 7 days left with unified trial plan", async () => {
      mockSubscriptionStatus({
        trialStatus: "in_trial",
        paymentStatus: "uninitialized",
        subscriptionStatus: "subscribed",
        trialDaysLeft: 10,
        isTrialEndingWithin24Hours: false,
        canManageOrganizationBilling: true,
      });
      const wrapper = await render(<StatusBanner />);
      expect(wrapper.container.textContent).toBe("Your trial ends in 10 days. Upgrade now to keep your syncs going.");
      expect(wrapper.queryByRole("link")).toBeInTheDocument();
    });
  });
});
