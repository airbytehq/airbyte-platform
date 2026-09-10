import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { links } from "core/utils/links";

import { StandardPlanGridCard } from "./StandardPlanGridCard";

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

describe("StandardPlanGridCard", () => {
  it("renders the price, description, and feature list", async () => {
    await render(<StandardPlanGridCard disabled={false} />);

    expect(screen.getByText("$10")).toBeInTheDocument();
    expect(screen.getByText("/ month")).toBeInTheDocument();
    expect(screen.getByText(/For practitioners looking for fully managed software/)).toBeInTheDocument();

    const features = screen.getAllByRole("listitem");
    expect(features).toHaveLength(6);
    expect(features[0]).toHaveTextContent("4 credits per month");
    expect(features[1]).toHaveTextContent("Buy credits from $2.50");
    expect(features[features.length - 1]).toHaveTextContent("Cancel any time");
    expect(screen.getByRole("link", { name: "credits" })).toHaveAttribute("href", links.creditDescription);
  });

  it("renders the Subscribe label and uses the setup flow by default", async () => {
    await render(<StandardPlanGridCard disabled={false} />);

    expect(useRedirectToCustomerPortal).toHaveBeenCalledWith("setup", undefined);
    expect(screen.getByRole("button", { name: /Subscribe/i })).toBeInTheDocument();
    expect(screen.queryByTestId("current-plan-badge")).not.toBeInTheDocument();
  });

  it("invokes goToCustomerPortal directly in subscribe mode (no confirmation modal)", async () => {
    await render(<StandardPlanGridCard disabled={false} />);

    await userEvent.click(screen.getByRole("button", { name: /Subscribe/i }));

    expect(openConfirmationModal).not.toHaveBeenCalled();
    expect(goToCustomerPortal).toHaveBeenCalledTimes(1);
  });

  it("renders the Downgrade label and uses the standard setup flow in downgrade mode", async () => {
    await render(<StandardPlanGridCard disabled={false} mode="downgrade" />);

    expect(useRedirectToCustomerPortal).toHaveBeenCalledWith("setup", "standard");
    expect(screen.getByRole("button", { name: /Downgrade/i })).toBeInTheDocument();
  });

  it("opens a confirmation modal before redirecting on downgrade click", async () => {
    await render(<StandardPlanGridCard disabled={false} mode="downgrade" />);

    await userEvent.click(screen.getByRole("button", { name: /Downgrade/i }));

    expect(openConfirmationModal).toHaveBeenCalledTimes(1);
    expect(goToCustomerPortal).not.toHaveBeenCalled();

    const modalOptions = openConfirmationModal.mock.calls[0][0];
    expect(modalOptions.submitButtonText).toBe("plans.standard.downgrade.confirmSubmit");
    expect(modalOptions.cancelButtonText).toBe("plans.standard.downgrade.confirmCancel");

    await act(async () => {
      await modalOptions.onSubmit();
    });
    expect(closeConfirmationModal).toHaveBeenCalledTimes(1);
    expect(goToCustomerPortal).toHaveBeenCalledTimes(1);
  });

  it("disables the action button when disabled is true", async () => {
    await render(<StandardPlanGridCard disabled mode="downgrade" />);

    expect(screen.getByRole("button", { name: /Downgrade/i })).toBeDisabled();
  });

  it("renders the current plan badge and a disabled Current plan button instead of a CTA", async () => {
    await render(<StandardPlanGridCard disabled={false} isCurrentPlan />);

    expect(screen.getByTestId("current-plan-badge")).toHaveTextContent("Current plan");
    expect(screen.getByRole("button", { name: /Current plan/i })).toBeDisabled();
    expect(screen.queryByRole("button", { name: /Subscribe/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Downgrade/i })).not.toBeInTheDocument();
  });

  it("shows the cancellation badge only when this is the current plan", async () => {
    const { unmount } = await render(
      <StandardPlanGridCard disabled={false} isCurrentPlan cancellationDate="2030-01-15T00:00:00Z" />
    );
    expect(screen.getByText(/Cancels/)).toBeInTheDocument();
    unmount();

    await render(<StandardPlanGridCard disabled={false} cancellationDate="2030-01-15T00:00:00Z" />);
    expect(screen.queryByText(/Cancels/)).not.toBeInTheDocument();
  });
});
