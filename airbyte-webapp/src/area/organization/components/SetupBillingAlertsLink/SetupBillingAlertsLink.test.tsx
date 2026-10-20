import { mocked, render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useOrganizationPlan } from "area/organization/utils";
import { useOrganization, useOrgInfo } from "core/api";
import { OrganizationInfoReadBilling } from "core/api/types/AirbyteClient";
import { useGeneratedIntent } from "core/utils/rbac";

import { SetupBillingAlertsLink } from "./SetupBillingAlertsLink";

jest.mock("area/organization/utils", () => ({
  ...jest.requireActual("area/organization/utils/isOrganizationSubscribed"),
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
  useOrganizationPlan: jest.fn(),
}));

jest.mock("core/api", () => ({
  useOrganization: jest.fn(),
  useOrgInfo: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: { ManageOrganizationBilling: "ManageOrganizationBilling" },
  useGeneratedIntent: jest.fn(),
}));

type Plan = "standard" | "plus" | "flex" | "pro";

const setup = async ({
  plan = "standard",
  flagEnabled = true,
  subscriptionStatus = "subscribed",
  paymentStatus = "okay",
}: {
  plan?: Plan;
  flagEnabled?: boolean;
  subscriptionStatus?: OrganizationInfoReadBilling["subscriptionStatus"];
  paymentStatus?: OrganizationInfoReadBilling["paymentStatus"];
} = {}) => {
  mockExperiments({ "platform.billing-notification-settings": flagEnabled });
  mocked(useGeneratedIntent).mockReturnValue(true);
  mocked(useOrganizationPlan).mockReturnValue({
    isStiggPlanEnabled: true,
    isStandardTrialPlan: false,
    isStandardPlan: plan === "standard",
    isPlusPlan: plan === "plus",
    isFlexPlan: plan === "flex",
    isProPlan: plan === "pro",
  });
  mocked(useOrganization).mockReturnValue({ email: "billing@example.com" } as ReturnType<typeof useOrganization>);
  mocked(useOrgInfo).mockReturnValue({
    billing: { subscriptionStatus, paymentStatus },
  } as unknown as ReturnType<typeof useOrgInfo>);

  const wrapper = await render(<SetupBillingAlertsLink />);
  return wrapper.queryByRole("link", { name: "Set up billing alerts" });
};

describe("SetupBillingAlertsLink", () => {
  it.each<Plan>(["standard", "plus"])("links to the billing page on %s when the flag is on", async (plan) => {
    const link = await setup({ plan });

    expect(link).toHaveAttribute("href", "/organization/test-organization-id/settings/billing");
    expect(link).not.toHaveAttribute("target");
  });

  it.each<Plan>(["pro", "flex"])("links to the Retool form on %s", async (plan) => {
    const link = await setup({ plan });

    expect(link).toHaveAttribute("href", expect.stringContaining("retool"));
    expect(link).toHaveAttribute("target", "_blank");
  });

  it("links to the Retool form when the flag is off", async () => {
    const link = await setup({ flagEnabled: false });

    expect(link).toHaveAttribute("href", expect.stringContaining("retool"));
  });

  it("links to the Retool form when the organization has unsubscribed", async () => {
    const link = await setup({ subscriptionStatus: "unsubscribed" });

    expect(link).toHaveAttribute("href", expect.stringContaining("retool"));
  });

  it("renders nothing without billing history", async () => {
    const link = await setup({ subscriptionStatus: "pre_subscription", paymentStatus: "uninitialized" });

    expect(link).not.toBeInTheDocument();
  });
});
