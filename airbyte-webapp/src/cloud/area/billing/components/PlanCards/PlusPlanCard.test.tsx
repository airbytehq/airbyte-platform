import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";
import { useConfirmationModalService } from "core/services/ConfirmationModal";

import { PlusPlanCard } from "./PlusPlanCard";

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

describe("PlusPlanCard", () => {
  it("uses the plus setup flow", async () => {
    await render(<PlusPlanCard />);

    expect(useRedirectToCustomerPortal).toHaveBeenCalledWith("setup", "plus");
  });

  it("starts setup directly when getting Plus without an active paid plan", async () => {
    await render(<PlusPlanCard />);

    await userEvent.click(screen.getByRole("button", { name: /Subscribe/i }));

    expect(openConfirmationModal).not.toHaveBeenCalled();
    expect(goToCustomerPortal).toHaveBeenCalledTimes(1);
  });

  it("opens a proration confirmation before upgrading an active paid plan to Plus", async () => {
    await render(<PlusPlanCard isPaidPlan />);

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
});
