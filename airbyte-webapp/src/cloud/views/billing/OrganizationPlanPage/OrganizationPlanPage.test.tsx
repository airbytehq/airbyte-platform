import { mocked, render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useOrganizationPlan } from "area/organization/utils/useOrganizationPlan";
import { useGetOrganizationSubscriptionInfo, useOrgInfo } from "core/api";
import { useGeneratedIntent } from "core/utils/rbac";

import { OrganizationPlanPage } from "./OrganizationPlanPage";

jest.mock("area/organization/utils", () => ({
  isOrganizationSubscribed: (billing: { subscriptionStatus?: string; paymentStatus?: string } | undefined) =>
    billing?.subscriptionStatus === "subscribed" && billing.paymentStatus !== "uninitialized",
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
}));

jest.mock("area/organization/utils/useOrganizationPlan", () => ({
  useOrganizationPlan: jest.fn(),
}));

jest.mock("core/utils/rbac", () => ({
  Intent: {
    ManageOrganizationBilling: "ManageOrganizationBilling",
  },
  useGeneratedIntent: jest.fn(),
}));

jest.mock("core/api", () => ({
  useOrgInfo: jest.fn(),
  useGetOrganizationSubscriptionInfo: jest.fn(),
}));

jest.mock("cloud/area/billing/components/PlanCards", () => ({
  PricingComparisonLink: () => <div data-testid="pricing-comparison-link" />,
  PlusPlanCard: ({ disabled, isPaidPlan }: { disabled?: boolean; isPaidPlan?: boolean }) => (
    <div
      data-testid="plus-plan-card"
      data-disabled={disabled ? "true" : "false"}
      data-paid={isPaidPlan ? "true" : "false"}
    />
  ),
  ProPlanCard: () => <div data-testid="pro-plan-card" />,
  StandardPlanCard: ({ disabled, mode }: { disabled?: boolean; mode?: "subscribe" | "downgrade" }) => (
    <div data-testid="standard-plan-card" data-disabled={disabled ? "true" : "false"} data-mode={mode ?? "subscribe"} />
  ),
}));

jest.mock("./ActivePlanCard", () => ({
  ActivePlanCard: ({
    tier,
    planName,
    isLoading,
    isError,
  }: {
    tier?: string;
    planName?: string;
    isLoading?: boolean;
    isError?: boolean;
  }) => (
    <div
      data-testid="active-plan-card"
      data-tier={tier}
      data-plan-name={planName ?? ""}
      data-loading={isLoading ? "true" : "false"}
      data-error={isError ? "true" : "false"}
    />
  ),
}));

const planFlags = (overrides: Partial<ReturnType<typeof useOrganizationPlan>> = {}) =>
  ({
    isStiggPlanEnabled: false,
    isUnifiedTrialPlan: false,
    isStandardTrialPlan: false,
    isStandardPlan: false,
    isPlusPlan: false,
    isSmePlan: false,
    isFlexPlan: false,
    isProPlan: false,
    ...overrides,
  }) as ReturnType<typeof useOrganizationPlan>;

const billingState = (overrides: Partial<{ subscriptionStatus: string; paymentStatus: string }> = {}) =>
  ({
    billing: { subscriptionStatus: "subscribed", paymentStatus: "okay", ...overrides },
  }) as unknown as ReturnType<typeof useOrgInfo>;

const subscriptionInfo = (
  data: unknown,
  state: Partial<{ isLoading: boolean; isError: boolean }> = {}
): ReturnType<typeof useGetOrganizationSubscriptionInfo> =>
  ({
    data,
    isLoading: state.isLoading ?? false,
    isError: state.isError ?? false,
  }) as unknown as ReturnType<typeof useGetOrganizationSubscriptionInfo>;

beforeEach(() => {
  jest.clearAllMocks();
  mockExperiments({ "billing.selfServePlusPlan": true });
  mocked(useGeneratedIntent).mockReturnValue(true);
  mocked(useOrganizationPlan).mockReturnValue(planFlags());
  mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(subscriptionInfo(undefined));
});

describe("OrganizationPlanPage", () => {
  it("redirects away from the Plan page when the self-serve Plus experiment is disabled", async () => {
    mockExperiments({ "billing.selfServePlusPlan": false });
    mocked(useOrgInfo).mockReturnValue(billingState({ subscriptionStatus: "unsubscribed" }));

    const wrapper = await render(<OrganizationPlanPage />);

    expect(wrapper.queryByText("No Active Plan")).not.toBeInTheDocument();
    expect(wrapper.queryByTestId("standard-plan-card")).not.toBeInTheDocument();
    expect(wrapper.queryByTestId("plus-plan-card")).not.toBeInTheDocument();
    expect(wrapper.queryByTestId("pro-plan-card")).not.toBeInTheDocument();
  });

  it("renders the no-active-plan state with all three plan cards when the org is not subscribed", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState({ subscriptionStatus: "unsubscribed" }));

    const wrapper = await render(<OrganizationPlanPage />);

    expect(wrapper.container.textContent).toContain("No Active Plan");
    expect(wrapper.getByTestId("standard-plan-card")).toHaveAttribute("data-mode", "subscribe");
    expect(wrapper.getByTestId("plus-plan-card")).toHaveAttribute("data-paid", "false");
    expect(wrapper.getByTestId("pro-plan-card")).toBeInTheDocument();
    expect(wrapper.getByTestId("pricing-comparison-link")).toBeInTheDocument();
    expect(wrapper.queryByTestId("active-plan-card")).not.toBeInTheDocument();
  });

  it("renders the no-active-plan state when subscribed but payment status is uninitialized", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState({ paymentStatus: "uninitialized" }));

    const wrapper = await render(<OrganizationPlanPage />);

    expect(wrapper.container.textContent).toContain("No Active Plan");
    expect(wrapper.getByTestId("standard-plan-card")).toBeInTheDocument();
    expect(wrapper.queryByTestId("active-plan-card")).not.toBeInTheDocument();
  });

  it("treats trial users (standard / unified) as unsubscribed and shows three subscribe cards", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState({ subscriptionStatus: "unsubscribed" }));
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isStandardTrialPlan: true, isStiggPlanEnabled: true }));

    const wrapper = await render(<OrganizationPlanPage />);

    expect(wrapper.queryByTestId("active-plan-card")).not.toBeInTheDocument();
    expect(wrapper.getByTestId("standard-plan-card")).toHaveAttribute("data-mode", "subscribe");
    expect(wrapper.getByTestId("plus-plan-card")).toBeInTheDocument();
    expect(wrapper.getByTestId("pro-plan-card")).toBeInTheDocument();
  });

  it("renders ActivePlanCard + Plus/Pro upgrade cards for a subscribed Standard org", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(
      subscriptionInfo({ name: "Standard", entitlementPlan: { planName: "Airbyte Standard" } })
    );
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isStandardPlan: true }));

    const wrapper = await render(<OrganizationPlanPage />);

    const activeCard = wrapper.getByTestId("active-plan-card");
    expect(activeCard).toHaveAttribute("data-tier", "standard");
    expect(activeCard).toHaveAttribute("data-plan-name", "Airbyte Standard");
    expect(wrapper.queryByTestId("standard-plan-card")).not.toBeInTheDocument();
    const plusCard = wrapper.getByTestId("plus-plan-card");
    expect(plusCard).toHaveAttribute("data-paid", "true");
    expect(wrapper.getByTestId("pro-plan-card")).toBeInTheDocument();
  });

  it("renders ActivePlanCard + Standard (downgrade) + Pro cards for a Plus org", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(
      subscriptionInfo({ name: "Plus", entitlementPlan: { planName: "Airbyte Plus" } })
    );
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isPlusPlan: true }));

    const wrapper = await render(<OrganizationPlanPage />);

    expect(wrapper.getByTestId("active-plan-card")).toHaveAttribute("data-tier", "plus");
    const standardCard = wrapper.getByTestId("standard-plan-card");
    expect(standardCard).toHaveAttribute("data-mode", "downgrade");
    expect(wrapper.queryByTestId("plus-plan-card")).not.toBeInTheDocument();
    expect(wrapper.getByTestId("pro-plan-card")).toBeInTheDocument();
  });

  it.each([
    ["Pro", { isProPlan: true }],
    ["SME", { isSmePlan: true }],
    ["Flex", { isFlexPlan: true }],
  ])("hides upgrade cards for top-tier plan (%s)", async (_label, flags) => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(
      subscriptionInfo({ name: "Top", entitlementPlan: { planName: "Top Tier" } })
    );
    mocked(useOrganizationPlan).mockReturnValue(planFlags(flags));

    const wrapper = await render(<OrganizationPlanPage />);

    expect(wrapper.getByTestId("active-plan-card")).toHaveAttribute("data-tier", "pro");
    expect(wrapper.queryByTestId("standard-plan-card")).not.toBeInTheDocument();
    expect(wrapper.queryByTestId("plus-plan-card")).not.toBeInTheDocument();
    expect(wrapper.queryByTestId("pro-plan-card")).not.toBeInTheDocument();
    expect(wrapper.queryByTestId("pricing-comparison-link")).not.toBeInTheDocument();
  });

  it("passes disabled to upgrade cards when payment status is locked", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState({ paymentStatus: "locked" }));
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(
      subscriptionInfo({ name: "Standard", entitlementPlan: { planName: "Airbyte Standard" } })
    );
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isStandardPlan: true }));

    const wrapper = await render(<OrganizationPlanPage />);

    expect(wrapper.getByTestId("plus-plan-card")).toHaveAttribute("data-disabled", "true");
  });

  it("propagates loading state to ActivePlanCard", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(subscriptionInfo(undefined, { isLoading: true }));
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isStandardPlan: true }));

    const wrapper = await render(<OrganizationPlanPage />);

    expect(wrapper.getByTestId("active-plan-card")).toHaveAttribute("data-loading", "true");
  });

  it("propagates error state to ActivePlanCard", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(subscriptionInfo(undefined, { isError: true }));
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isPlusPlan: true }));

    const wrapper = await render(<OrganizationPlanPage />);

    expect(wrapper.getByTestId("active-plan-card")).toHaveAttribute("data-error", "true");
  });
});
