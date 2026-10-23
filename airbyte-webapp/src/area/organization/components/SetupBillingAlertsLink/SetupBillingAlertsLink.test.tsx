import { mocked, render } from "test-utils";

import { useOrganizationPlan } from "area/organization/utils";
import { useOrgInfo } from "core/api";
import { OrganizationInfoReadBilling } from "core/api/types/AirbyteClient";
import { useGeneratedIntent } from "core/utils/rbac";

import { SetupBillingAlertsLink } from "./SetupBillingAlertsLink";

jest.mock("area/organization/utils", () => ({
  ...jest.requireActual("area/organization/utils/isOrganizationSubscribed"),
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
  useOrganizationPlan: jest.fn(),
}));

jest.mock("core/api", () => ({
  useOrgInfo: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: { ManageOrganizationBilling: "ManageOrganizationBilling" },
  useGeneratedIntent: jest.fn(),
}));

type Plan = "standard" | "plus" | "flex" | "pro";

const setup = async ({
  plan = "standard",
  canManageBilling = true,
  subscriptionStatus = "subscribed",
  paymentStatus = "okay",
}: {
  plan?: Plan;
  canManageBilling?: boolean;
  subscriptionStatus?: OrganizationInfoReadBilling["subscriptionStatus"];
  paymentStatus?: OrganizationInfoReadBilling["paymentStatus"];
} = {}) => {
  mocked(useGeneratedIntent).mockReturnValue(canManageBilling);
  mocked(useOrganizationPlan).mockReturnValue({
    isStiggPlanEnabled: true,
    isStandardTrialPlan: false,
    isStandardPlan: plan === "standard",
    isPlusPlan: plan === "plus",
    isFlexPlan: plan === "flex",
    isProPlan: plan === "pro",
  });
  mocked(useOrgInfo).mockReturnValue({
    billing: { subscriptionStatus, paymentStatus },
  } as unknown as ReturnType<typeof useOrgInfo>);

  const wrapper = await render(<SetupBillingAlertsLink />);
  return wrapper.queryByRole("link", { name: "Set up billing alerts" });
};

describe("SetupBillingAlertsLink", () => {
  it.each<Plan>(["standard", "plus"])("links to the billing page on %s", async (plan) => {
    const link = await setup({ plan });

    expect(link).toHaveAttribute("href", "/organization/test-organization-id/settings/billing");
    expect(link).not.toHaveAttribute("target");
  });

  it.each<Plan>(["pro", "flex"])("renders nothing on %s", async (plan) => {
    const link = await setup({ plan });

    expect(link).not.toBeInTheDocument();
  });

  it("renders nothing when the user cannot manage billing", async () => {
    const link = await setup({ canManageBilling: false });

    expect(link).not.toBeInTheDocument();
  });

  it("renders nothing when the organization has unsubscribed", async () => {
    const link = await setup({ subscriptionStatus: "unsubscribed" });

    expect(link).not.toBeInTheDocument();
  });

  it("renders nothing before the organization subscribes", async () => {
    const link = await setup({ subscriptionStatus: "pre_subscription", paymentStatus: "uninitialized" });

    expect(link).not.toBeInTheDocument();
  });
});
