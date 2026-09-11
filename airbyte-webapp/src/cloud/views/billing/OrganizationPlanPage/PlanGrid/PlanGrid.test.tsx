import { screen, within } from "@testing-library/react";

import { mocked, render } from "test-utils";

import { useOrganizationPlan } from "area/organization/utils/useOrganizationPlan";
import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";
import { useGetOrganizationSubscriptionInfo, useOrgInfo } from "core/api";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useGeneratedIntent } from "core/utils/rbac";

import { PlanGrid } from "./PlanGrid";

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

jest.mock("cloud/area/billing/utils/useRedirectToCustomerPortal", () => ({
  useRedirectToCustomerPortal: jest.fn(),
}));

jest.mock("core/services/ConfirmationModal", () => ({
  ...jest.requireActual("core/services/ConfirmationModal"),
  useConfirmationModalService: jest.fn(),
}));

const CARD_TEST_IDS = ["standard-plan-card", "plus-plan-card", "pro-plan-card", "flex-plan-card"] as const;

const planFlags = (overrides: Partial<ReturnType<typeof useOrganizationPlan>> = {}) =>
  ({
    isStiggPlanEnabled: false,
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

const subscriptionInfo = (data: unknown): ReturnType<typeof useGetOrganizationSubscriptionInfo> =>
  ({
    data,
    isLoading: false,
    isError: false,
  }) as unknown as ReturnType<typeof useGetOrganizationSubscriptionInfo>;

const card = (testId: (typeof CARD_TEST_IDS)[number]) => within(screen.getByTestId(testId));

const expectCurrentPlan = (testId: (typeof CARD_TEST_IDS)[number], disabledButtonName: RegExp) => {
  expect(card(testId).getByTestId("current-plan-badge")).toHaveTextContent("Current plan");
  expect(card(testId).getByRole("button", { name: disabledButtonName })).toBeDisabled();
  expect(card(testId).queryByRole("link", { name: /Talk to Sales/i })).not.toBeInTheDocument();
};

const expectDisabledCta = (testId: (typeof CARD_TEST_IDS)[number], buttonName: RegExp) => {
  expect(card(testId).getByRole("button", { name: buttonName })).toBeDisabled();
  expect(card(testId).queryByRole("link", { name: /Talk to Sales/i })).not.toBeInTheDocument();
  expect(card(testId).queryByTestId("current-plan-badge")).not.toBeInTheDocument();
};

beforeEach(() => {
  jest.clearAllMocks();
  mocked(useGeneratedIntent).mockReturnValue(true);
  mocked(useOrganizationPlan).mockReturnValue(planFlags());
  mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(subscriptionInfo(undefined));
  mocked(useRedirectToCustomerPortal).mockReturnValue({ goToCustomerPortal: jest.fn(), redirecting: false });
  mocked(useConfirmationModalService).mockReturnValue({
    openConfirmationModal: jest.fn(),
    closeConfirmationModal: jest.fn(),
  });
});

describe("PlanGrid", () => {
  it("renders all four cards with subscribe CTAs and no current plan when the org is not subscribed", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState({ subscriptionStatus: "unsubscribed" }));

    await render(<PlanGrid />);

    CARD_TEST_IDS.forEach((testId) => expect(screen.getByTestId(testId)).toBeInTheDocument());
    expect(screen.queryAllByTestId("current-plan-badge")).toHaveLength(0);
    expect(card("standard-plan-card").getByRole("button", { name: /Subscribe/i })).toBeEnabled();
    expect(card("plus-plan-card").getByRole("button", { name: /Subscribe/i })).toBeEnabled();
    expect(card("pro-plan-card").getByRole("link", { name: /Talk to Sales/i })).toBeInTheDocument();
    expect(card("flex-plan-card").getByRole("link", { name: /Talk to Sales/i })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /See full pricing & feature comparison/i })).toBeInTheDocument();
    expect(screen.queryByText("No Active Plan")).not.toBeInTheDocument();
    expect(useGetOrganizationSubscriptionInfo).toHaveBeenCalledWith("test-organization-id", false);
  });

  it("does not mark a plan current when billing reports no subscription, even if the plan id is set", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState({ subscriptionStatus: "unsubscribed" }));
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isPlusPlan: true }));

    await render(<PlanGrid />);

    expect(screen.queryAllByTestId("current-plan-badge")).toHaveLength(0);
    expect(card("standard-plan-card").getByRole("button", { name: /Subscribe/i })).toBeInTheDocument();
    expect(card("plus-plan-card").getByRole("button", { name: /Subscribe/i })).toBeInTheDocument();
    expect(useGetOrganizationSubscriptionInfo).toHaveBeenCalledWith("test-organization-id", false);
  });

  it("treats a subscribed org with an uninitialized payment status as unsubscribed", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState({ paymentStatus: "uninitialized" }));
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isStandardPlan: true }));

    await render(<PlanGrid />);

    expect(screen.queryAllByTestId("current-plan-badge")).toHaveLength(0);
    expect(card("standard-plan-card").getByRole("button", { name: /Subscribe/i })).toBeInTheDocument();
    expect(useGetOrganizationSubscriptionInfo).toHaveBeenCalledWith("test-organization-id", false);
  });

  it("treats trial users as unsubscribed", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState({ subscriptionStatus: "unsubscribed" }));
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isStandardTrialPlan: true, isStiggPlanEnabled: true }));

    await render(<PlanGrid />);

    expect(screen.queryAllByTestId("current-plan-badge")).toHaveLength(0);
    expect(card("standard-plan-card").getByRole("button", { name: /Subscribe/i })).toBeInTheDocument();
    expect(card("plus-plan-card").getByRole("button", { name: /Subscribe/i })).toBeInTheDocument();
  });

  it("does not mark any card current when subscribed on an unrecognized plan", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());

    await render(<PlanGrid />);

    expect(screen.queryAllByTestId("current-plan-badge")).toHaveLength(0);
    expect(card("standard-plan-card").getByRole("button", { name: /Subscribe/i })).toBeInTheDocument();
    expect(card("plus-plan-card").getByRole("button", { name: /Subscribe/i })).toBeInTheDocument();
  });

  it("marks Standard as current and offers Plus as an upgrade for a Standard org", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isStandardPlan: true }));

    await render(<PlanGrid />);

    expectCurrentPlan("standard-plan-card", /Current plan/i);
    expect(card("standard-plan-card").queryByRole("button", { name: /Subscribe/i })).not.toBeInTheDocument();
    expect(card("plus-plan-card").getByRole("button", { name: /Upgrade to Plus/i })).toBeEnabled();
    expect(card("pro-plan-card").getByRole("link", { name: /Talk to Sales/i })).toBeInTheDocument();
    expect(card("flex-plan-card").getByRole("link", { name: /Talk to Sales/i })).toBeInTheDocument();
    expect(useGetOrganizationSubscriptionInfo).toHaveBeenCalledWith("test-organization-id", true);
  });

  it("marks Plus as current and offers Standard as a downgrade for a Plus org", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isPlusPlan: true }));

    await render(<PlanGrid />);

    expectCurrentPlan("plus-plan-card", /Current plan/i);
    expect(card("standard-plan-card").getByRole("button", { name: /Downgrade/i })).toBeEnabled();
    expect(card("pro-plan-card").getByRole("link", { name: /Talk to Sales/i })).toBeInTheDocument();
    expect(card("flex-plan-card").getByRole("link", { name: /Talk to Sales/i })).toBeInTheDocument();
  });

  it("marks the Plus tier from the subscription current and offers the next tier up", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isPlusPlan: true }));
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(
      subscriptionInfo({ name: "Plus", selfServePlan: "plus_500" })
    );

    await render(<PlanGrid />);

    expect(card("plus-plan-card").getByTestId("current-plan-badge")).toHaveTextContent("Current plan");
    expect(card("plus-plan-card").getByRole("button", { name: "1,000 credits · $3,199/month" })).toBeEnabled();
    expect(card("plus-plan-card").getByText("$3,199")).toBeInTheDocument();
    expect(card("plus-plan-card").getByRole("button", { name: "Upgrade" })).toBeEnabled();
    expect(card("standard-plan-card").getByRole("button", { name: /Downgrade/i })).toBeEnabled();
  });

  it("prefers the subscription's self-serve plan over the entitlement plan when they disagree", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isStandardPlan: true }));
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(
      subscriptionInfo({ name: "Plus", selfServePlan: "plus_100" })
    );

    await render(<PlanGrid />);

    expect(screen.getAllByTestId("current-plan-badge")).toHaveLength(1);
    expect(card("plus-plan-card").getByTestId("current-plan-badge")).toBeInTheDocument();
    expect(card("plus-plan-card").getByRole("button", { name: "250 credits · $999/month" })).toBeEnabled();
    expect(card("standard-plan-card").getByRole("button", { name: /Downgrade/i })).toBeEnabled();
  });

  it("marks Standard current from the subscription even when the entitlement plan says Plus", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isPlusPlan: true }));
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(
      subscriptionInfo({ name: "Standard", selfServePlan: "standard" })
    );

    await render(<PlanGrid />);

    expectCurrentPlan("standard-plan-card", /Current plan/i);
    expect(card("plus-plan-card").getByRole("button", { name: /Upgrade to Plus/i })).toBeEnabled();
    expect(card("plus-plan-card").queryByTestId("current-plan-badge")).not.toBeInTheDocument();
  });

  it("fetches the subscription for any subscribed org, even without a recognized entitlement plan", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());

    await render(<PlanGrid />);

    expect(useGetOrganizationSubscriptionInfo).toHaveBeenCalledWith("test-organization-id", true);
  });

  it.each([
    ["Pro", { isProPlan: true }],
    ["SME", { isSmePlan: true }],
  ])("marks Pro as current and disables the self-serve subscribe buttons for a %s org", async (_label, flags) => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useOrganizationPlan).mockReturnValue(planFlags(flags));

    await render(<PlanGrid />);

    expectCurrentPlan("pro-plan-card", /Talk to Sales/i);
    expectDisabledCta("standard-plan-card", /Subscribe/i);
    expectDisabledCta("plus-plan-card", /Subscribe/i);
    expect(card("plus-plan-card").getByRole("button", { name: /credits/i })).toBeDisabled();
    expect(card("flex-plan-card").getByRole("link", { name: /Talk to Sales/i })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /See full pricing & feature comparison/i })).toBeInTheDocument();
  });

  it("marks Flex as current and disables every other CTA for a Flex org", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isFlexPlan: true }));

    await render(<PlanGrid />);

    expect(screen.getAllByTestId("current-plan-badge")).toHaveLength(1);
    expectCurrentPlan("flex-plan-card", /Talk to Sales/i);
    expectDisabledCta("standard-plan-card", /Subscribe/i);
    expectDisabledCta("plus-plan-card", /Subscribe/i);
    expectDisabledCta("pro-plan-card", /Talk to Sales/i);
  });

  it("disables the self-serve CTAs when payment status is locked", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState({ subscriptionStatus: "unsubscribed", paymentStatus: "locked" }));

    await render(<PlanGrid />);

    expect(card("standard-plan-card").getByRole("button", { name: /Subscribe/i })).toBeDisabled();
    expect(card("plus-plan-card").getByRole("button", { name: /Subscribe/i })).toBeDisabled();
  });

  it("shows the cancellation badge on the current plan card only", async () => {
    mocked(useOrgInfo).mockReturnValue(billingState());
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isPlusPlan: true }));
    mocked(useGetOrganizationSubscriptionInfo).mockReturnValue(
      subscriptionInfo({ name: "Plus", cancellationDate: "2030-01-15T00:00:00Z" })
    );

    await render(<PlanGrid />);

    expect(card("plus-plan-card").getByText(/Cancels/)).toBeInTheDocument();
    expect(card("standard-plan-card").queryByText(/Cancels/)).not.toBeInTheDocument();
    expect(card("pro-plan-card").queryByText(/Cancels/)).not.toBeInTheDocument();
    expect(card("flex-plan-card").queryByText(/Cancels/)).not.toBeInTheDocument();
  });
});
