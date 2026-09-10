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

    expect(screen.getByText("$449")).toBeInTheDocument();
    expect(screen.getByText("/ month")).toBeInTheDocument();
    expect(screen.getByText(/For teams who need higher throughput/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "100 credits" })).toBeInTheDocument();

    const features = featureTexts();
    expect(features).toHaveLength(8);
    expect(features[0]).toBe("100 credits per month");
    expect(features[1]).toBe("Overage at $5/credit");
    expect(features).toContain("Basic mappers");
    expect(features).toContain("Up to 2 workspaces");
    expect(features[features.length - 1]).toBe("Cancel any time");
    expect(screen.getByRole("link", { name: "credits" })).toHaveAttribute("href", links.creditDescription);
  });

  it("offers the six credit tiers in the dropdown", async () => {
    await render(<PlusPlanGridCard />);

    await userEvent.click(screen.getByRole("button", { name: "100 credits" }));

    const options = await screen.findAllByRole("option");
    expect(options.map((option) => option.textContent)).toEqual([
      "40 credits",
      "100 credits",
      "250 credits",
      "500 credits",
      "1,000 credits",
      "2,000 credits",
    ]);
  });

  it.each([
    ["40 credits", "$189", "40 credits per month", "Overage at $5/credit"],
    ["250 credits", "$999", "250 credits per month", "Overage at $4.50/credit"],
    ["500 credits", "$1,799", "500 credits per month", "Overage at $4.15/credit"],
    ["1,000 credits", "$3,199", "1,000 credits per month", "Overage at $3.75/credit"],
    ["2,000 credits", "$4,999", "2,000 credits per month", "Overage at $2.50/credit"],
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

    expect(useRedirectToCustomerPortal).toHaveBeenCalledWith("setup", "plus_100");
  });

  it("sends the selected tier to the setup flow", async () => {
    await render(<PlusPlanGridCard />);

    await selectCredits("500 credits");

    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_500");
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

  it("preselects the next tier up and disables the current tier for a Plus org", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_250" />);

    expect(screen.getByTestId("current-plan-badge")).toHaveTextContent("Current plan");
    expect(screen.getByRole("button", { name: "500 credits" })).toBeEnabled();
    expect(screen.getByText("$1,799")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Upgrade" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: /Current plan/i })).not.toBeInTheDocument();
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_500");

    await userEvent.click(screen.getByRole("button", { name: "500 credits" }));
    const currentOption = await screen.findByRole("option", { name: "250 credits (current plan)" });
    expect(currentOption).toHaveAttribute("aria-disabled", "true");
    expect(screen.getByRole("option", { name: "1,000 credits" })).not.toHaveAttribute("aria-disabled", "true");
  });

  it("preselects the next tier down when the org is on the top tier", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_2000" />);

    expect(screen.getByRole("button", { name: "1,000 credits" })).toBeEnabled();
    expect(screen.getByText("$3,199")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Downgrade" })).toBeEnabled();
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_1000");
  });

  it("confirms an immediate upgrade when a higher tier is selected on a Plus org", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_250" />);

    await selectCredits("1,000 credits");
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_1000");

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

  it("confirms an end-of-term downgrade when a lower tier is selected on a Plus org", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_500" />);

    await selectCredits("100 credits");
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_100");

    await userEvent.click(screen.getByRole("button", { name: "Downgrade" }));

    expect(goToCustomerPortal).not.toHaveBeenCalled();
    const modalOptions = openConfirmationModal.mock.calls[0][0];
    expect(modalOptions.submitButtonText).toBe("plans.plus.tierDowngrade.confirmSubmit");
    expect(modalOptions.cancelButtonText).toBe("plans.plus.tierDowngrade.confirmCancel");
  });

  it("does not let the current tier be selected", async () => {
    await render(<PlusPlanGridCard isCurrentPlan currentPlan="plus_500" />);

    await userEvent.click(screen.getByRole("button", { name: "1,000 credits" }));
    await userEvent.click(await screen.findByRole("option", { name: "500 credits (current plan)" }));
    await userEvent.keyboard("{Escape}");

    expect(screen.getByRole("button", { name: "1,000 credits" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Upgrade" })).toBeEnabled();
    expect(useRedirectToCustomerPortal).toHaveBeenLastCalledWith("setup", "plus_1000");
  });
});
