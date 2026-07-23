import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";
import { useConfirmationModalService } from "core/services/ConfirmationModal";

import { StandardPlanCard } from "./StandardPlanCard";

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

describe("StandardPlanCard", () => {
  it("renders the Subscribe label and uses the setup flow by default", async () => {
    await render(<StandardPlanCard disabled={false} />);

    expect(useRedirectToCustomerPortal).toHaveBeenCalledWith("setup", undefined);
    expect(screen.getByRole("button", { name: /Subscribe/i })).toBeInTheDocument();
  });

  it("invokes goToCustomerPortal directly in subscribe mode (no confirmation modal)", async () => {
    await render(<StandardPlanCard disabled={false} />);

    await userEvent.click(screen.getByRole("button", { name: /Subscribe/i }));

    expect(openConfirmationModal).not.toHaveBeenCalled();
    expect(goToCustomerPortal).toHaveBeenCalledTimes(1);
  });

  it("renders the Downgrade label and uses the standard setup flow in downgrade mode", async () => {
    await render(<StandardPlanCard disabled={false} mode="downgrade" />);

    expect(useRedirectToCustomerPortal).toHaveBeenCalledWith("setup", "standard");
    expect(screen.getByRole("button", { name: /Downgrade/i })).toBeInTheDocument();
  });

  it("opens a confirmation modal before redirecting on downgrade click", async () => {
    await render(<StandardPlanCard disabled={false} mode="downgrade" />);

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
    await render(<StandardPlanCard disabled mode="downgrade" />);

    expect(screen.getByRole("button", { name: /Downgrade/i })).toBeDisabled();
  });
});
