import { screen, within } from "@testing-library/react";

import { render } from "test-utils";

import { PlusPromoCreditsCallout } from "./PlusPromoCreditsCallout";

describe("PlusPromoCreditsCallout", () => {
  it("renders the promo terms and one line per Plus tier", async () => {
    await render(<PlusPromoCreditsCallout />);

    const callout = screen.getByTestId("plus-promo-credits-callout");
    expect(callout).toHaveTextContent(
      "Upgrade to Plus by September 29 to get free overage credits. All unused credits roll over for up to 3 months. Downgrading to a lower tier revokes promotional credits."
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
});
