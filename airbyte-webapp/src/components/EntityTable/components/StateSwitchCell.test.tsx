import { render } from "@testing-library/react";

import { TestSuspenseBoundary, TestWrapper } from "test-utils";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { StateSwitchCell } from "./StateSwitchCell";

jest.mock("core/api", () => ({
  useCurrentWorkspace: jest.fn(() => mockWorkspace),
  useUpdateConnection: jest.fn(() => ({
    mutateAsync: jest.fn(),
    isLoading: false,
  })),
}));

jest.mock("core/utils/rbac", () => ({
  useIntent: jest.fn(() => true),
}));

const mockId = "mock-id";

describe(`${StateSwitchCell.name}`, () => {
  it("renders enabled switch", () => {
    const { getByTestId } = render(
      <TestSuspenseBoundary>
        <StateSwitchCell connectionId={mockId} enabled />
      </TestSuspenseBoundary>,
      {
        wrapper: TestWrapper,
      }
    );

    const switchElement = getByTestId("connection-state-switch-mock-id");

    expect(switchElement).toBeEnabled();
    expect(switchElement).toBeChecked();
  });

  it("renders disabled switch when connection has `breaking` changes", () => {
    const { getByTestId } = render(
      <TestSuspenseBoundary>
        <StateSwitchCell connectionId={mockId} schemaChange="breaking" />
      </TestSuspenseBoundary>,
      {
        wrapper: TestWrapper,
      }
    );

    expect(getByTestId("connection-state-switch-mock-id")).toBeDisabled();
  });

  it("renders disabled switch when connection is in loading state", () => {
    jest.doMock("core/api", () => ({
      useUpdateConnection: jest.fn(() => ({
        isLoading: true,
      })),
    }));

    const { getByTestId } = render(
      <TestSuspenseBoundary>
        <StateSwitchCell connectionId={mockId} schemaChange="breaking" />
      </TestSuspenseBoundary>,
      {
        wrapper: TestWrapper,
      }
    );

    expect(getByTestId("connection-state-switch-mock-id")).toBeDisabled();
  });
});
