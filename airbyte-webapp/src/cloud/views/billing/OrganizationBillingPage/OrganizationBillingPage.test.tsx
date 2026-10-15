import { mocked, render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useOrganizationPlan } from "area/organization/utils";
import { useGetOrganizationSubscriptionInfo, useOrgInfo } from "core/api";
import { useGeneratedIntent } from "core/utils/rbac";

import { OrganizationBillingPage } from "./OrganizationBillingPage";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
  useOrganizationPlan: jest.fn(),
}));

jest.mock("core/api", () => ({
  useGetOrganizationSubscriptionInfo: jest.fn(),
  useOrgInfo: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: { ManageOrganizationBilling: "ManageOrganizationBilling" },
  useGeneratedIntent: jest.fn(),
}));

jest.mock("area/organization/components/SetupBillingAlertsLink", () => ({
  SetupBillingAlertsLink: () => <div>Retool alerts link</div>,
}));
jest.mock("./BillingAlerts", () => ({ BillingAlerts: () => <div>Billing alerts section</div> }));
jest.mock("./AccountBalance", () => ({ AccountBalance: () => <div>Account balance</div> }));
jest.mock("./BillingBanners", () => ({ BillingBanners: () => null }));
jest.mock("./BillingInformation", () => ({ BillingInformation: () => <div>Billing information</div> }));
jest.mock("./Invoices", () => ({ Invoices: () => <div>Invoices</div> }));
jest.mock("./PaymentMethod", () => ({ PaymentMethod: () => <div>Payment method</div> }));
jest.mock("./SubscribeCards", () => ({ SubscribeCards: () => <div>Subscribe cards</div> }));
jest.mock("./Subscription", () => ({ Subscription: () => <div>Subscription</div> }));

type Plan = "standard" | "plus" | "flex" | "pro";

const setup = async ({
  plan = "standard",
  flagEnabled = true,
  canManageBilling = true,
  subscribed = true,
}: {
  plan?: Plan;
  flagEnabled?: boolean;
  canManageBilling?: boolean;
  subscribed?: boolean;
} = {}) => {
  mockExperiments({ "platform.billing-notification-settings": flagEnabled });
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
    billing: subscribed
      ? { subscriptionStatus: "subscribed", paymentStatus: "okay" }
      : { subscriptionStatus: "pre_subscription", paymentStatus: "uninitialized" },
  } as unknown as ReturnType<typeof useOrgInfo>);
  mocked(useGetOrganizationSubscriptionInfo).mockReturnValue({
    data: undefined,
  } as unknown as ReturnType<typeof useGetOrganizationSubscriptionInfo>);

  return render(<OrganizationBillingPage />);
};

describe("OrganizationBillingPage billing alerts", () => {
  it.each<Plan>(["standard", "plus"])(
    "shows the billing alerts section instead of the Retool link on %s",
    async (plan) => {
      const wrapper = await setup({ plan });

      expect(wrapper.queryByText("Billing alerts section")).toBeInTheDocument();
      expect(wrapper.queryByText("Retool alerts link")).not.toBeInTheDocument();
    }
  );

  it.each<Plan>(["pro", "flex"])("hides the billing alerts section and keeps the Retool link on %s", async (plan) => {
    const wrapper = await setup({ plan });

    expect(wrapper.queryByText("Billing alerts section")).not.toBeInTheDocument();
    expect(wrapper.queryByText("Retool alerts link")).toBeInTheDocument();
  });

  it("hides the billing alerts section and keeps the Retool link when the flag is off", async () => {
    const wrapper = await setup({ flagEnabled: false });

    expect(wrapper.queryByText("Billing alerts section")).not.toBeInTheDocument();
    expect(wrapper.queryByText("Retool alerts link")).toBeInTheDocument();
  });

  it("hides the billing alerts section when the user cannot manage billing", async () => {
    const wrapper = await setup({ canManageBilling: false });

    expect(wrapper.queryByText("Billing alerts section")).not.toBeInTheDocument();
  });

  it("shows neither the section nor the link while the subscribe cards are shown", async () => {
    const wrapper = await setup({ subscribed: false });

    expect(wrapper.queryByText("Subscribe cards")).toBeInTheDocument();
    expect(wrapper.queryByText("Billing alerts section")).not.toBeInTheDocument();
    expect(wrapper.queryByText("Retool alerts link")).not.toBeInTheDocument();
  });

  it("places the billing alerts section between the summary tiles and the invoices", async () => {
    const wrapper = await setup();

    const text = wrapper.container.textContent ?? "";
    expect(text.indexOf("Billing information")).toBeLessThan(text.indexOf("Billing alerts section"));
    expect(text.indexOf("Billing alerts section")).toBeLessThan(text.indexOf("Invoices"));
  });
});
