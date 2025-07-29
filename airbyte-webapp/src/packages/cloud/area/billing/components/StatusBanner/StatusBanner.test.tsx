import dayjs from "dayjs";

import { mocked, render } from "test-utils";

import { useOrganizationTrialStatus, useOrgInfo } from "core/api";
import { OrganizationInfoRead, OrganizationTrialStatusRead } from "core/api/types/AirbyteClient";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { StatusBanner } from "./StatusBanner";

jest.mock("core/api", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("org-1"),
  useOrgInfo: jest.fn(),
  useOrganizationTrialStatus: jest.fn(),
  useCurrentWorkspaceOrUndefined: jest.fn().mockReturnValue({
    workspaceId: "workspace-1",
    organizationId: "org-1",
  }),
  useFirstOrg: jest.fn().mockReturnValue({ organizationId: "org-1" }),
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn().mockReturnValue("workspace-1"),
  useCurrentWorkspaceLink: jest.fn().mockReturnValue((link: string) => link),
}));

jest.mock("core/utils/rbac", () => ({
  useGeneratedIntent: jest.fn(),
  Intent: {
    ViewOrganizationTrialStatus: "ViewOrganizationTrialStatus",
    ManageOrganizationBilling: "ManageOrganizationBilling",
  },
}));

const mockOrgInfo = (billing: Partial<OrganizationInfoRead["billing"]> = {}) => {
  mocked(useOrgInfo).mockReturnValue({
    organizationId: "org-1",
    organizationName: "Test Org",
    sso: false,
    billing: {
      paymentStatus: "okay",
      subscriptionStatus: "subscribed",
      ...billing,
    },
  });
};

const mockTrialStatus = (trialStatus: OrganizationTrialStatusRead) => {
  mocked(useOrganizationTrialStatus).mockReturnValue(trialStatus);
};

const mockGeneratedIntent = (options: { canViewTrialStatus: boolean; canManageOrganizationBilling: boolean }) => {
  mocked(useGeneratedIntent).mockImplementation((intent) => {
    switch (intent) {
      case Intent.ViewOrganizationTrialStatus:
        return options.canViewTrialStatus;
      case Intent.ManageOrganizationBilling:
        return options.canManageOrganizationBilling;
      default:
        throw new Error(`Intent ${intent} is not mocked.`);
    }
  });
};

describe("StatusBanner", () => {
  it("should render nothing with paymentStatus=OKAY and not in trial", async () => {
    mockOrgInfo({
      paymentStatus: "okay",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container).toHaveTextContent("");
  });

  it("should not render anything for manual billing", async () => {
    mockOrgInfo({
      paymentStatus: "manual",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container).toHaveTextContent("");
  });

  it("should not render locked banner", async () => {
    mockOrgInfo({
      paymentStatus: "locked",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your syncs are disabled.");
    expect(wrapper.container.textContent).toContain("Airbyte Support");
  });

  it("should render disabled banner w/o link", async () => {
    mockOrgInfo({
      paymentStatus: "disabled",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: false });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your syncs are disabled due to unpaid invoices.");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render disabled banner w/ link", async () => {
    mockOrgInfo({
      paymentStatus: "disabled",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your syncs are disabled due to unpaid invoices.");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render grace period banner w/o link (1 day)", async () => {
    mockOrgInfo({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().add(25, "hours").valueOf(),
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: false });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled in 1 day");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render grace period banner w/o link (very soon)", async () => {
    mockOrgInfo({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().add(5, "hours").valueOf(),
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: false });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled very soon");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render grace period banner w/ link (1 day)", async () => {
    mockOrgInfo({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().add(25, "hours").valueOf(),
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled in 1 day");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render grace period banner w/ link (very soon)", async () => {
    mockOrgInfo({
      paymentStatus: "grace_period",
      subscriptionStatus: "subscribed",
      gracePeriodEndsAt: dayjs().add(5, "hours").valueOf(),
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("your syncs will be disabled very soon");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render pre-trial banner", async () => {
    mockOrgInfo({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({ trialStatus: "pre_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("once your first sync has succeeded");
  });

  it("should render pre-trial banner after leaving payment information", async () => {
    mockOrgInfo({
      paymentStatus: "okay",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({ trialStatus: "pre_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("once your first sync has succeeded");
  });

  it("should not show a trial banner if the user cannot view trial status", async () => {
    mockOrgInfo({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({ trialStatus: "pre_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: false });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("");
  });

  it("should render in-trial banner w/o link", async () => {
    mockOrgInfo({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({
      trialStatus: "in_trial",
      trialEndsAt: dayjs().add(5, "days").add(1, "hours").toISOString(),
    });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: false });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your trial ends in 5 days.");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render in-trial banner w/ link", async () => {
    mockOrgInfo({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({
      trialStatus: "in_trial",
      trialEndsAt: dayjs().add(5, "days").add(1, "hours").toISOString(),
    });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your trial ends in 5 days.");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render post-trial banner w/o link", async () => {
    mockOrgInfo({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: false });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Subscribe to Airbyte");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render post-trial banner w/o link if unsubscribed", async () => {
    mockOrgInfo({
      paymentStatus: "okay",
      subscriptionStatus: "unsubscribed",
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: false });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Subscribe to Airbyte");
    expect(wrapper.queryByRole("link")).not.toBeInTheDocument();
  });

  it("should render post-trial banner w/ link", async () => {
    mockOrgInfo({
      paymentStatus: "uninitialized",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({ trialStatus: "post_trial" });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Subscribe to Airbyte");
    expect(wrapper.queryByRole("link")).toBeInTheDocument();
  });

  it("should render in-trial banner w/ payment method", async () => {
    mockOrgInfo({
      paymentStatus: "okay",
      subscriptionStatus: "subscribed",
    });
    mockTrialStatus({
      trialStatus: "in_trial",
      trialEndsAt: dayjs().add(5, "days").add(1, "hours").toISOString(),
    });
    mockGeneratedIntent({ canViewTrialStatus: true, canManageOrganizationBilling: true });
    const wrapper = await render(<StatusBanner />);
    expect(wrapper.container.textContent).toContain("Your trial ends in 5 days.");
  });
});
