import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { links } from "core/utils/links";

import { FlexPlanGridCard } from "./FlexPlanGridCard";

describe("FlexPlanGridCard", () => {
  it("renders a custom price, the feature list, and a Talk to Sales link", async () => {
    await render(<FlexPlanGridCard />);

    expect(screen.getByText("Flex")).toBeInTheDocument();
    expect(screen.getByText("Custom")).toBeInTheDocument();
    expect(screen.queryByText("/ month")).not.toBeInTheDocument();

    const features = screen.getAllByRole("listitem");
    expect(features).toHaveLength(5);
    expect(features[0]).toHaveTextContent("Everything in Pro");
    expect(features[features.length - 1]).toHaveTextContent("Full data sovereignty");

    const link = screen.getByRole("link", { name: /Talk to Sales/i });
    expect(link).toHaveAttribute("href", links.contactSales);
    expect(link).toHaveAttribute("target", "_blank");
  });

  it("renders a disabled Talk to Sales button without the link when disabled", async () => {
    await render(<FlexPlanGridCard disabled />);

    expect(screen.getByRole("button", { name: /Talk to Sales/i })).toBeDisabled();
    expect(screen.queryByRole("link", { name: /Talk to Sales/i })).not.toBeInTheDocument();
    expect(screen.queryByTestId("current-plan-badge")).not.toBeInTheDocument();
  });

  it("renders the current plan badge and a disabled Talk to Sales button instead of the sales link", async () => {
    await render(<FlexPlanGridCard isCurrentPlan cancellationDate="2030-01-15T00:00:00Z" />);

    expect(screen.getByTestId("current-plan-badge")).toHaveTextContent("Current plan");
    expect(screen.getByRole("button", { name: /Talk to Sales/i })).toBeDisabled();
    expect(screen.getByText(/Cancels/)).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /Talk to Sales/i })).not.toBeInTheDocument();
  });
});
