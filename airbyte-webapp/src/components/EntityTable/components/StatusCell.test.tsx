import { render, waitFor } from "@testing-library/react";

import { TestWrapper, TestSuspenseBoundary, mockConnection } from "test-utils";

import { StatusCell } from "./StatusCell";

jest.mock("core/api", () => ({
  useConnectionList: jest.fn(() => ({
    connections: [],
  })),
  useSyncConnection: jest.fn(() => ({
    mutateAsync: jest.fn(),
  })),
  useUpdateConnection: jest.fn(() => ({
    mutateAsync: jest.fn(),
    isLoading: false,
  })),
}));

const mockId = "mock-id";

describe("<StatusCell />", () => {
  it("renders switch when connection has schedule", () => {
    const { getByTestId } = render(
      <TestSuspenseBoundary>
        <StatusCell id={mockId} connection={mockConnection} enabled />
      </TestSuspenseBoundary>,
      {
        wrapper: TestWrapper,
      }
    );

    const switchElement = getByTestId("enable-connection-switch");

    expect(switchElement).toBeEnabled();
    expect(switchElement).toBeChecked();
  });

  it("renders button when connection does not have schedule", async () => {
    const { getByTestId } = render(
      <TestSuspenseBoundary>
        <StatusCell id={mockId} connection={mockConnection} enabled isManual />
      </TestSuspenseBoundary>,
      {
        wrapper: TestWrapper,
      }
    );

    await waitFor(() => expect(getByTestId("manual-sync-button")).toBeEnabled());
  });

  it("disables switch when hasBreakingChange is true", () => {
    const { getByTestId } = render(
      <TestSuspenseBoundary>
        <StatusCell id={mockId} connection={mockConnection} hasBreakingChange />
      </TestSuspenseBoundary>,
      {
        wrapper: TestWrapper,
      }
    );

    expect(getByTestId("enable-connection-switch")).toBeDisabled();
  });

  it("disables manual sync button when hasBreakingChange is true", () => {
    const { getByTestId } = render(
      <TestSuspenseBoundary>
        <StatusCell id={mockId} connection={mockConnection} hasBreakingChange enabled isManual />
      </TestSuspenseBoundary>,
      {
        wrapper: TestWrapper,
      }
    );

    expect(getByTestId("manual-sync-button")).toBeDisabled();
  });
});
