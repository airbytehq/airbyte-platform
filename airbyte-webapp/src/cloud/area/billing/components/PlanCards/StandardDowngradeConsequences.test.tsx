import { screen, within } from "@testing-library/react";

import { render } from "test-utils";

import { StandardDowngradeConsequences } from "./StandardDowngradeConsequences";

describe("StandardDowngradeConsequences", () => {
  it("lists everything a Plus org loses and when the change takes effect", async () => {
    await render(<StandardDowngradeConsequences />);

    expect(screen.getByText("If you downgrade to Standard, you will lose access to:")).toBeInTheDocument();

    const items = within(screen.getByTestId("downgrade-consequences"))
      .getAllByRole("listitem")
      .map((item) => item.textContent);
    expect(items).toEqual([
      "15-minute sync frequency (become hourly)",
      "Mappings (connections with mappings are disabled)",
      "SSO (members must reset their passwords to log in again)",
      "Multiple workspaces (they merge into one, and your connections are migrated)",
      "Premium support",
    ]);

    expect(
      screen.getByText(
        "Your plan will change to Standard at the end of your billing period. You can continue using Plus until then."
      )
    ).toBeInTheDocument();
  });
});
