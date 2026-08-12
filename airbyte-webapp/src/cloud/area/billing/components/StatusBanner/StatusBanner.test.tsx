import dayjs from "dayjs";

import { mocked, render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import {
  ISO8601DateTime,
  OrganizationInfoReadBillingAccountType,
  OrganizationInfoReadBillingPaymentStatus,
  OrganizationInfoReadBillingSubscriptionStatus,
  OrganizationTrialStatusReadTrialStatus,
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

jest.mock("core/api", () => ({}));

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
    isStiggPlanEnabled?: boolean;
    isStandardTrialPlan?: boolean;
  } = {}
) => {
  mocked(useOrganizationSubscriptionStatus).mockReturnValue({
    isStiggPlanEnabled: options.isStiggPlanEnabled ?? false,
    trialStatus: options.trialStatus ?? "post_trial",
    trialEndsAt: options.trialEndsAt,
    isInTrial: options.trialStatus === "in_trial",
    isStandardTrialPlan: options.isStandardTrialPlan ?? false,
    isStandardPlan: false,
    isSmePlan: false,
    isFlexPlan: false,
    isProPlan: false,
    trialDaysLeft: options.trialDaysLeft ?? 0,
    isTrialEndingWithin24Hours: options.isTrialEndingWithin24Hours ?? false,
    paymentStatus: options.paymentStatus ?? "okay",
    subscriptionStatus: options.subscriptionStatus ?? "subscribed",
    accountType: options.accountType,
    gracePeriodEndsAt: options.gracePeriodEndsAt,
    canManageOrganizationBilling: options.canManageOrganizationBilling ?? true,
  });
};

describe("StatusBanner", () => {
  beforeEach(() => {
    mockExperiments({ "billing.selfServePlusPlan": false });
  });

  it("should render nothing with paymentStatus=OKAY", async () => {
    mockSubscriptionStatus({ paymentStatus: "okay", subscriptionStatus: "subscribed" });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container).toHaveTextContent("");
  });

  it("should not render anything for manual billing", async () => {
    mockSubscriptionStatus({ paymentStatus: "manual", subscriptionStatus: "subscribed" });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container).toHaveTextContent("");
  });

  it("should render locked banner", async () => {
    mockSubscriptionStatus({ paymentStatus: "locked", subscriptionStatus: "subscribed" });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your syncs are disabled.");
    expect(wrapper.container.textContent).toContain("Airbyte Support");
  });

  it("should render disabled banner without link", async () => {
    mockSubscriptionStatus({
      paymentStatus: "disabled",
      subscriptionStatus: "subscribed",
      canManageOrganizationBilling: false,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your syncs are disabled due to unpaid invoices.");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render disabled banner with link", async () => {
    mockSubscriptionStatus({ paymentStatus: "disabled", subscriptionStatus: "subscribed" });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your syncs are disabled due to unpaid invoices.");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should keep payment issue CTAs linked to Billing when self-serve Plus is enabled", async () => {
    mockExperiments({ "billing.selfServePlusPlan": true });
    mockSubscriptionStatus({ paymentStatus: "disabled", subscriptionStatus: "subscribed" });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.getByRole("link")).toHaveAttribute("href", "/organization/test-organization-id/settings/billing");
  });

  it.each([
    ["without link (1 day)", 25, false],
    ["without link (very soon)", 5, false],
    ["with link (1 day)", 25, true],
    ["with link (very soon)", 5, true],
  ])("should render grace period banner %s", async (_scenario, hours, canManageOrganizationBilling) => {
    mockSubscriptionStatus({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().add(hours, "hours").valueOf(),
      canManageOrganizationBilling,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain(
      hours === 25 ? "your syncs will be disabled in 1 day" : "your syncs will be disabled very soon"
    );
    expect(wrapper.queryByRole("link") !== null).toBe(canManageOrganizationBilling);
  });

  it("should not render banner when billing is undefined", async () => {
    mockSubscriptionStatus();
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container).toHaveTextContent("");
  });

  it("should handle grace period with undefined gracePeriodEndsAt", async () => {
    mockSubscriptionStatus({ paymentStatus: "grace_period", subscriptionStatus: "subscribed" });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled very soon");
  });

  it("should handle grace period with past date", async () => {
    mockSubscriptionStatus({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().subtract(1, "day").valueOf(),
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled very soon");
  });

  it("should render pre-trial banner", async () => {
    mockSubscriptionStatus({ paymentStatus: "okay", subscriptionStatus: "subscribed", trialStatus: "pre_trial" });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("once your first sync has succeeded");
  });

  it("should render in-trial banner without a link", async () => {
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "in_trial",
      trialDaysLeft: 5,
      canManageOrganizationBilling: false,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your trial ends in 5 days.");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render in-trial banner with a link", async () => {
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "in_trial",
      trialDaysLeft: 5,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your trial ends in 5 days.");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render in-trial banner with a payment method", async () => {
    mockSubscriptionStatus({
      paymentStatus: "okay",
      subscriptionStatus: "subscribed",
      trialStatus: "in_trial",
      trialDaysLeft: 5,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your trial ends in 5 days.");
  });

  it("should render post-trial banner without a link", async () => {
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

  it("should render post-trial banner with a link", async () => {
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "post_trial",
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Subscribe to Airbyte");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should link trial CTAs to the Plan page when self-serve Plus is enabled", async () => {
    mockExperiments({ "billing.selfServePlusPlan": true });
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "post_trial",
      canManageOrganizationBilling: false,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Subscribe to Airbyte");
  });

  it("should render the Standard Trial entitlement warning", async () => {
    mockSubscriptionStatus({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
      trialStatus: "in_trial",
      trialDaysLeft: 5,
      isStiggPlanEnabled: true,
      isStandardTrialPlan: true,
      canManageOrganizationBilling: true,
    });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Upgrade now to keep your syncs going.");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });
});
