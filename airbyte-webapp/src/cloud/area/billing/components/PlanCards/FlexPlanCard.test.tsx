import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { links } from "core/utils/links";

import { FlexPlanCard } from "./FlexPlanCard";

describe("FlexPlanCard", () => {
  it("renders the Flex features and a Talk to Sales link", async () => {
    await render(<FlexPlanCard />);

    expect(screen.getByText("Flex")).toBeInTheDocument();
    const features = screen.getAllByRole("listitem");
    expect(features).toHaveLength(5);
    expect(features[0]).toHaveTextContent("Everything in Pro");
    expect(features[features.length - 1]).toHaveTextContent("Full data sovereignty");
    expect(screen.getByRole("link", { name: /Talk to Sales/i })).toHaveAttribute("href", links.contactSales);
  });
});
