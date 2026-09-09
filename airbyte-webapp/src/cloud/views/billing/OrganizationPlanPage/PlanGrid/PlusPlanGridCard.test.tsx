import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { links } from "core/utils/links";

import { PlusPlanGridCard } from "./PlusPlanGridCard";

jest.mock("cloud/area/billing/utils/useRedirectToCustomerPortal", () => ({
  useRedirectToCustomerPortal: jest.fn(),
}));

jest.mock("core/services/ConfirmationModal", () => ({
  ...jest.requireActual("core/services/ConfirmationModal"),
  useConfirmationModalService: jest.fn(),
}));

const goToCustomerPortal = jest.fn();
const openConfirmationModal = jest.fn();
const closeConfirmationModal = jest.fn();

const selectCredits = async (optionLabel: string) => {
  await userEvent.click(screen.getByRole("button", { name: /credits$/i }));
  await userEvent.click(await screen.findByRole("option", { name: optionLabel }));
};

const featureTexts = () => screen.getAllByRole("listitem").map((item) => item.textContent);

beforeEach(() => {
  jest.clearAllMocks();
  mocked(useRedirectToCustomerPortal).mockReturnValue({
    goToCustomerPortal,
    redirecting: false,
  });
  mocked(useConfirmationModalService).mockReturnValue({
    openConfirmationModal,
    closeConfirmationModal,
  });
});

describe("PlusPlanGridCard", () => {
  it("renders the 100 credit tier by default", async () => {
    await render(<PlusPlanGridCard />);

    expect(screen.getByText("$500")).toBeInTheDocument();
    expect(screen.getByText("/ month")).toBeInTheDocument();
    expect(screen.getByText(/For teams who need higher throughput/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "100 credits" })).toBeInTheDocument();

    const features = featureTexts();
    expect(features).toHaveLength(7);
    expect(features[0]).toBe("100 credits per month");
    expect(features[1]).toBe("Overage at $5/credit");
    expect(features).toContain("Basic mappings");
    expect(features[features.length - 1]).toBe("Cancel any time");
    expect(screen.getByRole("link", { name: "credits" })).toHaveAttribute("href", links.creditDescription);
  });

  it("offers the four credit tiers in the dropdown", async () => {
    await render(<PlusPlanGridCard />);

    await userEvent.click(screen.getByRole("button", { name: "100 credits" }));

    const options = await screen.findAllByRole("option");
    expect(options.map((option) => option.textContent)).toEqual([
      "100 credits",
      "250 credits",
      "500 credits",
      "1,000 credits",
    ]);
  });

  it.each([
    ["250 credits", "$1,000", "250 credits per month", "Overage at $4/credit"],
    ["500 credits", "$1,800", "500 credits per month", "Overage at $3.60/credit"],
    ["1,000 credits", "$3,000", "1,000 credits per month", "Overage at $3/credit"],
  ])("updates the price and features when %s is selected", async (option, price, creditsFeature, overageFeature) => {
    await render(<PlusPlanGridCard />);

    await selectCredits(option);

    expect(screen.getByText(price)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: option })).toBeInTheDocument();
    const features = featureTexts();
    expect(features[0]).toBe(creditsFeature);
    expect(features[1]).toBe(overageFeature);
  });

  it("uses the plus setup flow", async () => {
    await render(<PlusPlanGridCard />);

    expect(useRedirectToCustomerPortal).toHaveBeenCalledWith("setup", "plus");
  });

  it("starts setup directly when getting Plus without an active paid plan", async () => {
    await render(<PlusPlanGridCard />);

    await userEvent.click(screen.getByRole("button", { name: /Subscribe/i }));

    expect(openConfirmationModal).not.toHaveBeenCalled();
    expect(goToCustomerPortal).toHaveBeenCalledTimes(1);
  });

  it("opens a proration confirmation before upgrading an active paid plan to Plus", async () => {
    await render(<PlusPlanGridCard isPaidPlan />);

    await userEvent.click(screen.getByRole("button", { name: /Upgrade/i }));

    expect(openConfirmationModal).toHaveBeenCalledTimes(1);
    expect(goToCustomerPortal).not.toHaveBeenCalled();

    const modalOptions = openConfirmationModal.mock.calls[0][0];
    expect(modalOptions.submitButtonText).toBe("plans.plus.upgrade.confirmSubmit");
    expect(modalOptions.cancelButtonText).toBe("plans.plus.upgrade.confirmCancel");

    await act(async () => {
      await modalOptions.onSubmit();
    });
    expect(closeConfirmationModal).toHaveBeenCalledTimes(1);
    expect(goToCustomerPortal).toHaveBeenCalledTimes(1);
  });

  it("disables the subscribe button and the credits dropdown when disabled is true", async () => {
    await render(<PlusPlanGridCard disabled />);

    expect(screen.getByRole("button", { name: /Subscribe/i })).toBeDisabled();
    expect(screen.getByRole("button", { name: "100 credits" })).toBeDisabled();
  });

  it("renders the current plan badge and a disabled Current plan button without the credits dropdown", async () => {
    await render(<PlusPlanGridCard isCurrentPlan />);

    expect(screen.getByTestId("current-plan-badge")).toHaveTextContent("Current plan");
    expect(screen.getByRole("button", { name: /Current plan/i })).toBeDisabled();
    expect(screen.queryByRole("button", { name: /Subscribe/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Upgrade/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /credits$/i })).not.toBeInTheDocument();
  });

  it("shows the cancellation badge only when this is the current plan", async () => {
    const { unmount } = await render(<PlusPlanGridCard isCurrentPlan cancellationDate="2030-01-15T00:00:00Z" />);
    expect(screen.getByText(/Cancels/)).toBeInTheDocument();
    unmount();

    await render(<PlusPlanGridCard cancellationDate="2030-01-15T00:00:00Z" />);
    expect(screen.queryByText(/Cancels/)).not.toBeInTheDocument();
  });
});
