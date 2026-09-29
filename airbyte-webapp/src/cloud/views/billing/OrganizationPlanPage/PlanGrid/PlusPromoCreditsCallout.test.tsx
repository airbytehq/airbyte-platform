import { screen, within } from "@testing-library/react";

import { render } from "test-utils";

import { PlusPromoCreditsCallout, PLUS_PROMO_CREDITS_CALLOUT_END } from "./PlusPromoCreditsCallout";

jest.useFakeTimers();

describe("PlusPromoCreditsCallout", () => {
  beforeEach(() => {
    jest.setSystemTime(new Date("2026-09-14T12:00:00-07:00"));
  });

  afterAll(() => {
    jest.useRealTimers();
  });

  it("renders the promo terms and one line per Plus tier", async () => {
    await render(<PlusPromoCreditsCallout />);

    const callout = screen.getByTestId("plus-promo-credits-callout");
    expect(callout).toHaveTextContent(
      "Upgrade to Plus by October 21 to get free overage credits. All unused credits roll over for up to 3 months. Downgrading to a lower tier revokes promotional credits."
    );

    const tiers = within(callout).getAllByRole("listitem");
    expect(tiers.map((tier) => tier.textContent)).toEqual([
      "Plus 40: 40 free overage credits.",
      "Plus 100: 100 free overage credits.",
      "Plus 250: 250 free overage credits.",
      "Plus 500: 400 free overage credits.",
      "Plus 1000: 500 free overage credits.",
      "Plus 2000: 500 free overage credits only if your spend increases this month.",
    ]);
    tiers.forEach((tier, index) => {
      expect(
        within(tier).getByText(["Plus 40", "Plus 100", "Plus 250", "Plus 500", "Plus 1000", "Plus 2000"][index])
      ).toBeInTheDocument();
    });
  });

  it("still renders one second before the cutoff", async () => {
    jest.setSystemTime(new Date("2026-10-21T23:59:59-07:00"));

    await render(<PlusPromoCreditsCallout />);

    expect(screen.getByTestId("plus-promo-credits-callout")).toBeInTheDocument();
  });

  it("stops rendering at midnight Pacific at the end of October 21, 2026", async () => {
    jest.setSystemTime(new Date(PLUS_PROMO_CREDITS_CALLOUT_END));

    await render(<PlusPromoCreditsCallout />);

    expect(screen.queryByTestId("plus-promo-credits-callout")).not.toBeInTheDocument();
  });

  it("does not render after the cutoff has passed", async () => {
    jest.setSystemTime(new Date("2026-10-22T09:00:00-07:00"));

    await render(<PlusPromoCreditsCallout />);

    expect(screen.queryByTestId("plus-promo-credits-callout")).not.toBeInTheDocument();
  });
});
