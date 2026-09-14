import { screen } from "@testing-library/react";

import { mocked, render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useOrganizationPlan } from "area/organization/utils";

import { PlusUpgradePromoBanner, PLUS_UPGRADE_PROMO_BANNER_END } from "./PlusUpgradePromoBanner";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn().mockReturnValue("test-organization-id"),
  useOrganizationPlan: jest.fn(),
}));

jest.useFakeTimers();

const planFlags = (
  overrides: Partial<ReturnType<typeof useOrganizationPlan>> = {}
): ReturnType<typeof useOrganizationPlan> => ({
  isStiggPlanEnabled: true,
  isStandardTrialPlan: false,
  isStandardPlan: false,
  isPlusPlan: false,
  isSmePlan: false,
  isFlexPlan: false,
  isProPlan: false,
  ...overrides,
});

describe("PlusUpgradePromoBanner", () => {
  beforeEach(() => {
    jest.setSystemTime(new Date("2026-09-14T12:00:00-07:00"));
    mockExperiments({ "plan-page-redesign-ui": true });
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isStandardPlan: true }));
  });

  afterAll(() => {
    jest.useRealTimers();
  });

  it("renders the banner with a link to the organization plan page for a Standard org", async () => {
    await render(<PlusUpgradePromoBanner />);

    expect(screen.getByTestId("plus-upgrade-promo-banner")).toHaveTextContent(
      "Upgrade to the Plus plan by September 29 and get up to 500 free overage credits. Conditions apply."
    );
    expect(screen.getByRole("link", { name: "View plans and pricing →" })).toHaveAttribute(
      "href",
      "/organization/test-organization-id/settings/plan"
    );
  });

  it("renders for a Standard Trial org as well", async () => {
    mocked(useOrganizationPlan).mockReturnValue(planFlags({ isStandardTrialPlan: true }));

    await render(<PlusUpgradePromoBanner />);

    expect(screen.getByTestId("plus-upgrade-promo-banner")).toBeInTheDocument();
  });

  it("does not render when the plan page redesign flag is off", async () => {
    mockExperiments({ "plan-page-redesign-ui": false });

    await render(<PlusUpgradePromoBanner />);

    expect(screen.queryByTestId("plus-upgrade-promo-banner")).not.toBeInTheDocument();
  });

  it.each([
    ["Plus", planFlags({ isPlusPlan: true })],
    ["SME", planFlags({ isSmePlan: true })],
    ["Flex", planFlags({ isFlexPlan: true })],
    ["Pro", planFlags({ isProPlan: true })],
    ["no Stigg plan", planFlags({ isStiggPlanEnabled: false })],
  ])("does not render for an org on the %s plan", async (_plan, flags) => {
    mocked(useOrganizationPlan).mockReturnValue(flags);

    await render(<PlusUpgradePromoBanner />);

    expect(screen.queryByTestId("plus-upgrade-promo-banner")).not.toBeInTheDocument();
  });

  it("still renders one second before the cutoff", async () => {
    jest.setSystemTime(new Date("2026-09-28T23:59:59-07:00"));

    await render(<PlusUpgradePromoBanner />);

    expect(screen.getByTestId("plus-upgrade-promo-banner")).toBeInTheDocument();
  });

  it("stops rendering at midnight Pacific on September 29, 2026", async () => {
    jest.setSystemTime(new Date(PLUS_UPGRADE_PROMO_BANNER_END));

    await render(<PlusUpgradePromoBanner />);

    expect(screen.queryByTestId("plus-upgrade-promo-banner")).not.toBeInTheDocument();
  });

  it("does not render after the cutoff has passed", async () => {
    jest.setSystemTime(new Date("2026-10-01T09:00:00-07:00"));

    await render(<PlusUpgradePromoBanner />);

    expect(screen.queryByTestId("plus-upgrade-promo-banner")).not.toBeInTheDocument();
  });
});
