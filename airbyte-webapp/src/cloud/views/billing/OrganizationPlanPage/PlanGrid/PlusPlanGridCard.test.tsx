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
  await userEvent.click(screen.getByRole("button", { name: /credits/i }));
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
  it("renders the 40 credit tier by default", async () => {
    await render(<PlusPlanGridCard />);

    expect(screen.getByText("$189")).toBeInTheDocument();
    expect(screen.getByText("/ month")).toBeInTheDocument();
    expect(screen.getByText(/For teams who need higher throughput/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "40 credits · $189/month" })).toBeInTheDocument();

    const features = featureTexts();
    expect(features).toHaveLength(8);
    expect(features[0]).toBe("40 credits per month");
    expect(features[1]).toBe("Overage at $5/credit");
    expect(features).toContain("Basic mappers");
    expect(features).toContain("Up to 2 workspaces");
    expect(features[features.length - 1]).toBe("Cancel any time");
    expect(screen.getByRole("link", { name: "credits" })).toHaveAttribute("href", links.creditDescription);
  });

  it("offers the six credit tiers with per-credit prices in the dropdown", async () => {
    await render(<PlusPlanGridCard />);

    await userEvent.click(screen.getByRole("button", { name: "40 credits · $189/month" }));

    const options = await screen.findAllByRole("option");
    expect(options.map((option) => option.textContent)).toEqual([
      "40 credits · $189/month",
      "100 credits · $449/month",
      "250 credits · $999/month",
      "500 credits · $1,799/month",
      "1,000 credits · $3,199/month",
      "2,000 credits · $4,999/month",
    ]);
    expect(options.some((option) => option.querySelector('[data-icon="check-circle"]'))).toBe(false);
  });

  it.each([
    ["100 credits · $449/month", "$449", "100 credits per month", "Overage at $5/credit"],
    ["250 credits · $999/month", "$999", "250 credits per month", "Overage at $4.50/credit"],
    ["500 credits · $1,799/month", "$1,799", "500 credits per month", "Overage at $4.15/credit"],
    ["1,000 credits · $3,199/month", "$3,199", "1,000 credits per month", "Overage at $3.75/credit"],
    ["2,000 credits · $4,999/month", "$4,999", "2,000 credits per month", "Overage at $2.50/credit"],
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

    expect(useRedirectToCustomerPortal).toHaveBeenCalledWith("setup", "plus_40");
  });

  it("sends the selected tier to the setup flow", async () => {
    await render(<PlusPlanGridCard />);

    await selectCredits("500 credits · $1,799/month");

    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_500");
  });

  it("starts setup directly when getting Plus without an active paid plan", async () => {
    await render(<PlusPlanGridCard />);

    await userEvent.click(screen.getByRole("button", { name: /Subscribe/i }));

    expect(openConfirmationModal).not.toHaveBeenCalled();
    expect(goToCustomerPortal).toHaveBeenCalledTimes(1);
  });

  it("opens a confirmation before upgrading an active paid plan to Plus", async () => {
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
    expect(screen.getByRole("button", { name: "40 credits · $189/month" })).toBeDisabled();
  });

  it("renders the current plan badge and a disabled Current plan button without the credits dropdown", async () => {
    await render(<PlusPlanGridCard isCurrentPlan />);

    expect(screen.getByTestId("current-plan-badge")).toHaveTextContent("Current plan");
    expect(screen.getByRole("button", { name: /Current plan/i })).toBeDisabled();
    expect(screen.queryByRole("button", { name: /Subscribe/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Upgrade/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /credits/i })).not.toBeInTheDocument();
  });

  it("shows the cancellation badge only when this is the current plan", async () => {
    const { unmount } = await render(<PlusPlanGridCard isCurrentPlan cancellationDate="2030-01-15T00:00:00Z" />);
    expect(screen.getByText(/Cancels/)).toBeInTheDocument();
    unmount();

    await render(<PlusPlanGridCard cancellationDate="2030-01-15T00:00:00Z" />);
    expect(screen.queryByText(/Cancels/)).not.toBeInTheDocument();
  });

  it("disables the Downgrade CTA when a cancellation is pending", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_2000" cancellationDate="2030-01-15T00:00:00Z" />);

    await selectCredits("1,000 credits · $3,199/month");
    expect(screen.getByRole("button", { name: "Downgrade" })).toBeDisabled();
  });

  it("keeps the Subscribe CTA and the credits dropdown enabled when a cancellation is pending", async () => {
    await render(<PlusPlanGridCard cancellationDate="2030-01-15T00:00:00Z" />);

    expect(screen.getByRole("button", { name: /Subscribe/i })).toBeEnabled();
    expect(screen.getByRole("button", { name: "40 credits · $189/month" })).toBeEnabled();
  });

  it("preselects the active Plus tier with a Current plan button and badge", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_40" />);

    expect(screen.getByTestId("current-plan-badge")).toHaveTextContent("Current plan");
    expect(screen.getByRole("button", { name: "40 credits · $189/month" })).toBeEnabled();
    expect(screen.getByText("$189")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Current plan" })).toBeDisabled();
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_40");

    await userEvent.click(screen.getByRole("button", { name: "40 credits · $189/month" }));
    const currentOption = await screen.findByRole("option", { name: "40 credits · $189/month (current plan)" });
    expect(currentOption).not.toHaveAttribute("aria-disabled", "true");
    expect(currentOption.querySelector('[data-icon="check-circle"]')).toBeInTheDocument();
    expect(
      screen.getAllByRole("option").filter((option) => option.querySelector('[data-icon="check-circle"]'))
    ).toEqual([currentOption]);
  });

  it("preselects the active top tier and offers a downgrade after another tier is selected", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_2000" />);

    expect(screen.getByRole("button", { name: "2,000 credits · $4,999/month" })).toBeEnabled();
    expect(screen.getByText("$4,999")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Current plan" })).toBeDisabled();
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_2000");

    await selectCredits("1,000 credits · $3,199/month");
    expect(screen.queryByTestId("current-plan-badge")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Downgrade" })).toBeEnabled();
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_1000");
  });

  it("confirms an immediate upgrade when a higher tier is selected on a Plus org", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_250" />);

    await selectCredits("1,000 credits · $3,199/month");
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_1000");
    expect(screen.queryByTestId("current-plan-badge")).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Upgrade" }));

    expect(goToCustomerPortal).not.toHaveBeenCalled();
    const modalOptions = openConfirmationModal.mock.calls[0][0];
    expect(modalOptions.submitButtonText).toBe("plans.plus.tierUpgrade.confirmSubmit");
    expect(modalOptions.cancelButtonText).toBe("plans.plus.tierUpgrade.confirmCancel");

    await act(async () => {
      await modalOptions.onSubmit();
    });
    expect(closeConfirmationModal).toHaveBeenCalledTimes(1);
    expect(goToCustomerPortal).toHaveBeenCalledTimes(1);
  });

  it("offers a downgrade from Plus 100 to Plus 40", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_100" />);

    await selectCredits("40 credits · $189/month");
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_40");
    expect(screen.queryByTestId("current-plan-badge")).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Downgrade" }));

    expect(goToCustomerPortal).not.toHaveBeenCalled();
    const modalOptions = openConfirmationModal.mock.calls[0][0];
    expect(modalOptions.submitButtonText).toBe("plans.plus.tierDowngrade.confirmSubmit");
    expect(modalOptions.cancelButtonText).toBe("plans.plus.tierDowngrade.confirmCancel");
  });

  it("restores the Current plan button and badge when selecting the active tier again", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_500" />);

    await selectCredits("1,000 credits · $3,199/month");
    expect(screen.queryByTestId("current-plan-badge")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Upgrade" })).toBeEnabled();

    await userEvent.click(screen.getByRole("button", { name: "1,000 credits · $3,199/month" }));
    expect(
      screen
        .getByRole("option", { name: "500 credits · $1,799/month (current plan)" })
        .querySelector('[data-icon="check-circle"]')
    ).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "1,000 credits · $3,199/month" }).querySelector('[data-icon="check-circle"]')
    ).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("option", { name: "500 credits · $1,799/month (current plan)" }));

    expect(screen.getByTestId("current-plan-badge")).toHaveTextContent("Current plan");
    expect(screen.getByRole("button", { name: "500 credits · $1,799/month" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Current plan" })).toBeDisabled();
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_500");
  });
});
