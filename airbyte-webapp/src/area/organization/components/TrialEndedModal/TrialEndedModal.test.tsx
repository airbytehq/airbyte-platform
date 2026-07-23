import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useRedirectToCustomerPortal } from "cloud/area/billing/utils/useRedirectToCustomerPortal";

import { TrialEndedModal } from "./TrialEndedModal";

const mockNavigate = jest.fn();

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useNavigate: () => mockNavigate,
}));

jest.mock("cloud/area/billing/utils/useLinkToPlanPage", () => ({
  useLinkToPlanPage: jest.fn().mockReturnValue("/organization/test-organization-id/settings/plan"),
}));

jest.mock("cloud/area/billing/utils/useRedirectToCustomerPortal", () => ({
  useRedirectToCustomerPortal: jest.fn(),
}));

jest.mock("core/api", () => ({}));

describe("TrialEndedModal", () => {
  const mockGoToCustomerPortal = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    mockExperiments({ "billing.selfServePlusPlan": false });
    mockGoToCustomerPortal.mockResolvedValue(undefined);
    mocked(useRedirectToCustomerPortal).mockReturnValue({
      goToCustomerPortal: mockGoToCustomerPortal,
      redirecting: false,
    });
  });

  it("keeps the existing Standard setup flow when self-serve Plus is disabled", async () => {
    const onComplete = jest.fn();
    const wrapper = await render(<TrialEndedModal onCancel={jest.fn()} onComplete={onComplete} />);

    await userEvent.click(wrapper.getByRole("button", { name: "Choose Standard" }));

    expect(mockGoToCustomerPortal).toHaveBeenCalledTimes(1);
    expect(mockNavigate).not.toHaveBeenCalled();
    expect(onComplete).toHaveBeenCalledWith({ action: "standard" });
  });

  it("links Standard to the Plan page when self-serve Plus is enabled", async () => {
    mockExperiments({ "billing.selfServePlusPlan": true });
    const onComplete = jest.fn();
    const wrapper = await render(<TrialEndedModal onCancel={jest.fn()} onComplete={onComplete} />);

    await userEvent.click(wrapper.getByRole("button", { name: "Choose Standard" }));

    expect(mockNavigate).toHaveBeenCalledWith("/organization/test-organization-id/settings/plan");
    expect(mockGoToCustomerPortal).not.toHaveBeenCalled();
    expect(onComplete).toHaveBeenCalledWith({ action: "standard" });
  });
});
