import { renderHook, act } from "@testing-library/react";
import { useTrialEndedModal } from "../src/packages/cloud/area/billing/utils/useTrialEndedModal";

jest.mock("components/TrialEndedModal/TrialEndedModal", () => ({
  TrialEndedModal: () => "MockedTrialEndedModal",
}));

jest.mock("hooks/services/Modal", () => ({
  useModalService: jest.fn(),
}));

jest.mock("core/utils/useOrganizationSubscriptionStatus", () => ({
  useOrganizationSubscriptionStatus: jest.fn(),
}));

const { useModalService } = jest.requireMock("hooks/services/Modal");
const { useOrganizationSubscriptionStatus } = jest.requireMock(
  "core/utils/useOrganizationSubscriptionStatus"
);

describe("useTrialEndedModal", () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("abre el modal y configura un intervalo cuando la prueba ha terminado", () => {
    const mockOpenModal = jest.fn();
    (useModalService as jest.Mock).mockReturnValue({ openModal: mockOpenModal });
    (useOrganizationSubscriptionStatus as jest.Mock).mockReturnValue({
      trialStatus: "post_trial",
      isUnifiedTrialPlan: true,
    });

    renderHook(() => useTrialEndedModal());

    // El modal se abre inmediatamente
    expect(mockOpenModal).toHaveBeenCalledTimes(1);
    expect(mockOpenModal).toHaveBeenCalledWith({ content: expect.any(Function) });


    act(() => {
      jest.advanceTimersByTime(60 * 60 * 1000);
    });


    expect(mockOpenModal).toHaveBeenCalledTimes(2);
  });

  it("no abre el modal si no es un plan unificado o no está en post_trial", () => {
    const mockOpenModal = jest.fn();
    (useModalService as jest.Mock).mockReturnValue({ openModal: mockOpenModal });
    (useOrganizationSubscriptionStatus as jest.Mock).mockReturnValue({
      trialStatus: "active",
      isUnifiedTrialPlan: false,
    });

    renderHook(() => useTrialEndedModal());


    expect(mockOpenModal).not.toHaveBeenCalled();
  });

  it("limpia el intervalo al desmontar o cambiar condiciones", () => {
    const mockOpenModal = jest.fn();
    (useModalService as jest.Mock).mockReturnValue({ openModal: mockOpenModal });
    (useOrganizationSubscriptionStatus as jest.Mock).mockReturnValue({
      trialStatus: "post_trial",
      isUnifiedTrialPlan: true,
    });

    const { unmount } = renderHook(() => useTrialEndedModal());
    expect(mockOpenModal).toHaveBeenCalledTimes(1);


    unmount();

    act(() => {
      jest.advanceTimersByTime(60 * 60 * 1000);
    });


    expect(mockOpenModal).toHaveBeenCalledTimes(1);
  });
});
