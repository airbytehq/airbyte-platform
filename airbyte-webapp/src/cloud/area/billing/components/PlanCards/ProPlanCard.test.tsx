import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { ProPlanCard } from "./ProPlanCard";

describe("ProPlanCard", () => {
  it("lists the Pro support level as the last feature", async () => {
    await render(<ProPlanCard />);

    const features = screen.getAllByRole("listitem");
    expect(features[features.length - 1]).toHaveTextContent("Support, 24/5 · 12-hour critical response");
    expect(screen.getByRole("link", { name: /Talk to Sales/i })).toBeInTheDocument();
  });
});
