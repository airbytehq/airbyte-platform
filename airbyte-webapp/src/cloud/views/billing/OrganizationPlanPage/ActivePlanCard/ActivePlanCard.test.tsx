import { render } from "test-utils";

import { ActivePlanCard } from "./ActivePlanCard";

describe("ActivePlanCard", () => {
  it("renders the plan name with the active subscription badge", async () => {
    const wrapper = await render(<ActivePlanCard tier="standard" planName="Airbyte Standard" />);

    expect(wrapper.getByTestId("active-plan-card")).toHaveAttribute("data-tier", "standard");
    expect(wrapper.container.textContent).toContain("Airbyte Standard");
    expect(wrapper.container.textContent).toContain("Active subscription");
  });

  it("applies the standard accent class for the standard tier", async () => {
    const wrapper = await render(<ActivePlanCard tier="standard" planName="Airbyte Standard" />);
    const card = wrapper.getByTestId("active-plan-card");
    expect(card.className).toMatch(/activePlanCard--standard/);
  });

  it("applies the plus accent class for the plus tier", async () => {
    const wrapper = await render(<ActivePlanCard tier="plus" planName="Airbyte Plus" />);
    const card = wrapper.getByTestId("active-plan-card");
    expect(card.className).toMatch(/activePlanCard--plus/);
  });

  it("applies the pro accent class for the pro tier", async () => {
    const wrapper = await render(<ActivePlanCard tier="pro" planName="Airbyte Pro" />);
    const card = wrapper.getByTestId("active-plan-card");
    expect(card.className).toMatch(/activePlanCard--pro/);
  });

  it("renders a loading skeleton when isLoading is true", async () => {
    const wrapper = await render(<ActivePlanCard tier="standard" isLoading />);
    expect(wrapper.container.querySelector('[class*="LoadingSkeleton"], [class*="loadingSkeleton"]')).toBeTruthy();
  });

  it("renders an error message when isError is true", async () => {
    const wrapper = await render(<ActivePlanCard tier="standard" isError planName="Airbyte Standard" />);
    expect(wrapper.container.textContent).not.toContain("Airbyte Standard");
  });

  it("shows a cancellation badge when cancellationDate is provided", async () => {
    const wrapper = await render(
      <ActivePlanCard tier="plus" planName="Airbyte Plus" cancellationDate="2030-01-15T00:00:00Z" />
    );
    expect(wrapper.container.textContent).toMatch(/Cancels/);
  });
});
