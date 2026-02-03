import { render, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useEffectOnce } from "react-use";

import { TestWrapper, useMockIntersectionObserver } from "test-utils/testutils";

import { useModalService } from "./ModalService";
import { ModalResult } from "./types";

const TestComponent: React.FC<{ onModalResult?: (result: ModalResult<unknown>) => void }> = ({ onModalResult }) => {
  const { openModal } = useModalService();
  useEffectOnce(() => {
    openModal({
      title: "Test Modal Title",
      content: ({ onComplete, onCancel }) => (
        <div data-testid="testModalContent">
          <button onClick={onCancel} data-testid="cancel">
            Cancel
          </button>
          <button onClick={() => onComplete("reason1")} data-testid="close-reason1">
            Close Reason 1
          </button>
          <button onClick={() => onComplete("reason2")} data-testid="close-reason2">
            Close Reason 2
          </button>
        </div>
      ),
    }).then(onModalResult);
  });
  return null;
};

const renderModal = (resultCallback?: (reason: unknown) => void) => {
  return render(
    <TestWrapper>
      <TestComponent onModalResult={resultCallback} />
    </TestWrapper>
  );
};

describe("ModalService", () => {
  beforeEach(() => {
    // IntersectionObserver isn't available in test environment but is used by headless-ui dialog
    useMockIntersectionObserver();
  });
  it("should open a modal on openModal", () => {
    const rendered = renderModal();

    expect(rendered.getByText("Test Modal Title")).toBeTruthy();
    expect(rendered.getByTestId("testModalContent")).toBeTruthy();
  });

  it("should close the modal with escape and emit a cancel result", async () => {
    const resultCallback = jest.fn();

    const rendered = renderModal(resultCallback);

    await waitFor(() => userEvent.keyboard("{Escape}"));

    expect(rendered.queryByTestId("testModalContent")).toBeFalsy();
    expect(resultCallback).toHaveBeenCalledWith({ type: "canceled" });
  });

  it("should allow cancelling the modal from inside", async () => {
    const resultCallback = jest.fn();

    const rendered = renderModal(resultCallback);

    await waitFor(() => userEvent.click(rendered.getByTestId("cancel")));

    expect(rendered.queryByTestId("testModalContent")).toBeFalsy();
    expect(resultCallback).toHaveBeenCalledWith({ type: "canceled" });
  });

  it("should allow closing the button with a reason and return that reason", async () => {
    const resultCallback = jest.fn();

    let rendered = renderModal(resultCallback);

    await waitFor(() => userEvent.click(rendered.getByTestId("close-reason1")));

    expect(rendered.queryByTestId("testModalContent")).toBeFalsy();
    expect(resultCallback).toHaveBeenCalledWith({ type: "completed", reason: "reason1" });

    resultCallback.mockReset();
    rendered = renderModal(resultCallback);

    await waitFor(() => userEvent.click(rendered.getByTestId("close-reason2")));

    expect(rendered.queryByTestId("testModalContent")).toBeFalsy();
    expect(resultCallback).toHaveBeenCalledWith({ type: "completed", reason: "reason2" });
  });

  it("should return undefined when no modal is open", () => {
    const TestComponentWithNoModal: React.FC = () => {
      const modalService = useModalService();

      expect(modalService.getCurrentModalTitle()).toBeUndefined();

      return null;
    };

    render(
      <TestWrapper>
        <TestComponentWithNoModal />
      </TestWrapper>
    );
  });

  it("should return the current modal title when modal is open", async () => {
    let capturedTitle: string | undefined;

    const ModalContentComponent: React.FC<{ onComplete: (result: string) => void }> = ({ onComplete }) => {
      const modalService = useModalService();

      useEffectOnce(() => {
        // Capture title after component mounts (modal is fully rendered)
        capturedTitle = modalService.getCurrentModalTitle() as string | undefined;
      });

      return (
        <div data-testid="modal-content-with-title">
          <button onClick={() => onComplete("done")} data-testid="complete-btn">
            Complete
          </button>
        </div>
      );
    };

    const TestComponentWithModalTitle: React.FC = () => {
      const modalService = useModalService();

      useEffectOnce(() => {
        modalService.openModal({
          title: "Current Modal Title",
          content: ModalContentComponent,
        });
      });

      return null;
    };

    const rendered = render(
      <TestWrapper>
        <TestComponentWithModalTitle />
      </TestWrapper>
    );

    // Wait for modal to render
    await waitFor(() => {
      expect(rendered.getByText("Current Modal Title")).toBeTruthy();
    });

    // Wait a bit more for the useEffectOnce in ModalContentComponent to execute
    await waitFor(() => {
      expect(capturedTitle).toBe("Current Modal Title");
    });
  });
});
