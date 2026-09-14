import { act, fireEvent, screen, within } from "@testing-library/react";

import { render } from "test-utils";

import { PricingCalculator } from "./PricingCalculator";

jest.useFakeTimers();

const slider = () => screen.getByRole("slider", { name: "Monthly credits" });
const creditsField = () => screen.getByRole("spinbutton", { name: "Monthly credits" });
const tile = (name: "standard-price" | "plus-price" | "rows" | "data-volume") =>
  screen.getByTestId(`pricing-calculator-${name}`);
const recommendation = () => screen.getByTestId("pricing-calculator-recommendation");

const setSlider = (credits: number) => fireEvent.change(slider(), { target: { value: String(credits) } });
const typeCredits = (value: string) => fireEvent.change(creditsField(), { target: { value } });
const settleDebounce = () => act(() => jest.advanceTimersByTime(300));

describe("PricingCalculator", () => {
  afterAll(() => {
    jest.useRealTimers();
  });

  it("starts at 5 credits with Standard as the cheaper plan", async () => {
    await render(<PricingCalculator />);

    expect(screen.getByRole("heading", { name: "Pricing calculator" })).toBeInTheDocument();
    expect(slider()).toHaveValue("5");
    expect(creditsField()).toHaveValue(5);
    expect(tile("standard-price")).toHaveTextContent("$20/ mo");
    expect(tile("standard-price")).toHaveAttribute("data-highlighted", "true");
    expect(tile("plus-price")).toHaveTextContent("$189/ mo");
    expect(tile("plus-price")).toHaveAttribute("data-highlighted", "false");
    expect(tile("rows")).toHaveTextContent("830Krows / mo");
    expect(tile("data-volume")).toHaveTextContent("1GB / mo");
    expect(recommendation()).toHaveTextContent("Standard is the most cost-effective choice.");
  });

  it("updates the controls immediately but recomputes prices only after the 300ms debounce", async () => {
    await render(<PricingCalculator />);

    setSlider(1000);

    expect(creditsField()).toHaveValue(1000);
    expect(tile("standard-price")).toHaveTextContent("$20");

    settleDebounce();

    expect(tile("standard-price")).toHaveTextContent("$4,995/ mo");
    expect(tile("standard-price")).toHaveAttribute("data-highlighted", "false");
    expect(tile("plus-price")).toHaveTextContent("$3,199/ mo");
    expect(tile("plus-price")).toHaveAttribute("data-highlighted", "true");
    expect(tile("rows")).toHaveTextContent("166Mrows / mo");
    expect(tile("data-volume")).toHaveTextContent("250GB / mo");
    expect(recommendation()).toHaveTextContent("Plus with 1000 credits is the most cost-effective choice.");
  });

  it("recommends Pro at 2000 credits while still quoting both self-serve plans", async () => {
    await render(<PricingCalculator />);

    setSlider(2000);
    settleDebounce();

    expect(tile("standard-price")).toHaveTextContent("$9,995/ mo");
    expect(tile("plus-price")).toHaveTextContent("$4,999/ mo");
    expect(tile("plus-price")).toHaveAttribute("data-highlighted", "true");
    expect(tile("rows")).toHaveTextContent("332Mrows / mo");
    expect(tile("data-volume")).toHaveTextContent("500GB / mo");
    expect(recommendation()).toHaveTextContent(
      "Above 2000 credits per month, Pro is a more cost-effective choice. Talk to Sales."
    );
    const salesLink = within(recommendation()).getByRole("link", { name: "Talk to Sales" });
    expect(salesLink).toHaveAttribute("href", "https://airbyte.com/talk-to-sales");
    expect(salesLink).toHaveAttribute("target", "_blank");
  });

  it("does not render a Talk to Sales link below 2000 credits", async () => {
    await render(<PricingCalculator />);

    setSlider(1000);
    settleDebounce();

    expect(within(recommendation()).queryByRole("link")).not.toBeInTheDocument();
  });

  it("accepts typed credits and shows cents when the Plus quote has them", async () => {
    await render(<PricingCalculator />);

    typeCredits("750");
    settleDebounce();

    expect(slider()).toHaveValue("750");
    expect(tile("plus-price")).toHaveTextContent("$2,836.50/ mo");
    expect(recommendation()).toHaveTextContent("Plus with 500 credits is the most cost-effective choice.");
  });

  it("clamps typed credits to the slider range", async () => {
    await render(<PricingCalculator />);

    typeCredits("5000");
    expect(slider()).toHaveValue("2000");
    expect(creditsField()).toHaveValue(2000);

    typeCredits("-3");
    settleDebounce();
    expect(slider()).toHaveValue("0");
    expect(tile("standard-price")).toHaveTextContent("$20/ mo");
    expect(tile("rows")).toHaveTextContent("0rows / mo");
    expect(tile("data-volume")).toHaveTextContent("0MB / mo");
  });

  it("shows megabytes below one gigabyte", async () => {
    await render(<PricingCalculator />);

    setSlider(3);
    settleDebounce();

    expect(tile("data-volume")).toHaveTextContent("750MB / mo");
  });

  it("keeps the last valid credits while the field is empty and restores them on blur", async () => {
    await render(<PricingCalculator />);

    setSlider(60);
    typeCredits("");

    expect(creditsField()).toHaveValue(null);
    expect(slider()).toHaveValue("60");

    fireEvent.blur(creditsField());
    settleDebounce();

    expect(creditsField()).toHaveValue(60);
    expect(tile("plus-price")).toHaveTextContent("$289/ mo");
    expect(recommendation()).toHaveTextContent("Plus with 40 credits is the most cost-effective choice.");
  });
});
