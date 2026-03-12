import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { TestWrapper } from "test-utils";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { CapacityReachedMessage } from "./CapacityReachedMessage";

const mockSetDismissedByWorkspace = jest.fn();
let mockDismissedByWorkspace: Record<string, boolean> = {};
let mockStatusCounts: { queued: number } | undefined;

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => mockWorkspace,
  useGetConnectionStatusesCounts: () => ({ data: mockStatusCounts }),
}));

jest.mock("core/utils/useLocalStorage", () => ({
  useLocalStorage: () => [mockDismissedByWorkspace, mockSetDismissedByWorkspace],
}));

describe("CapacityReachedMessage", () => {
  beforeEach(() => {
    mockDismissedByWorkspace = {};
    mockStatusCounts = { queued: 0 };
    mockSetDismissedByWorkspace.mockClear();
  });

  const renderComponent = () => {
    return render(
      <TestWrapper>
        <CapacityReachedMessage />
      </TestWrapper>
    );
  };

  it("shows banner when queuedCount > 0 and not dismissed", () => {
    mockStatusCounts = { queued: 5 };
    mockDismissedByWorkspace = {};

    renderComponent();

    expect(screen.getByTestId("capacity-reached-banner")).toBeInTheDocument();
  });

  it("does not show banner when queuedCount is 0", () => {
    mockStatusCounts = { queued: 0 };
    mockDismissedByWorkspace = {};

    renderComponent();

    expect(screen.queryByTestId("capacity-reached-banner")).not.toBeInTheDocument();
  });

  it("does not show banner when dismissed for current workspace", () => {
    mockStatusCounts = { queued: 5 };
    mockDismissedByWorkspace = { [mockWorkspace.workspaceId]: true };

    renderComponent();

    expect(screen.queryByTestId("capacity-reached-banner")).not.toBeInTheDocument();
  });

  it("shows banner when dismissed for different workspace but not current", () => {
    mockStatusCounts = { queued: 5 };
    mockDismissedByWorkspace = { "different-workspace-id": true };

    renderComponent();

    expect(screen.getByTestId("capacity-reached-banner")).toBeInTheDocument();
  });

  it("calls setDismissedByWorkspace when close button is clicked", async () => {
    mockStatusCounts = { queued: 5 };
    mockDismissedByWorkspace = {};

    renderComponent();

    const closeButton = screen.getByRole("button");
    await userEvent.click(closeButton);

    expect(mockSetDismissedByWorkspace).toHaveBeenCalledWith(expect.any(Function));

    // Verify the updater function sets the correct workspace
    const updaterFn = mockSetDismissedByWorkspace.mock.calls[0][0];
    const result = updaterFn({});
    expect(result).toEqual({ [mockWorkspace.workspaceId]: true });
  });

  it("resets dismissed state when queuedCount becomes 0 and was previously dismissed", () => {
    mockStatusCounts = { queued: 0 };
    mockDismissedByWorkspace = { [mockWorkspace.workspaceId]: true };

    renderComponent();

    expect(mockSetDismissedByWorkspace).toHaveBeenCalledWith(expect.any(Function));

    // Verify the updater function resets to false
    const updaterFn = mockSetDismissedByWorkspace.mock.calls[0][0];
    const result = updaterFn({ [mockWorkspace.workspaceId]: true });
    expect(result).toEqual({ [mockWorkspace.workspaceId]: false });
  });

  it("does not reset dismissed state when data is not loaded yet", () => {
    mockStatusCounts = undefined; // Data not loaded
    mockDismissedByWorkspace = { [mockWorkspace.workspaceId]: true };

    renderComponent();

    // Should not call setDismissedByWorkspace because data isn't loaded
    expect(mockSetDismissedByWorkspace).not.toHaveBeenCalled();
  });

  it("does not reset dismissed state when queuedCount is 0 but was not dismissed", () => {
    mockStatusCounts = { queued: 0 };
    mockDismissedByWorkspace = { [mockWorkspace.workspaceId]: false };

    renderComponent();

    expect(mockSetDismissedByWorkspace).not.toHaveBeenCalled();
  });
});
