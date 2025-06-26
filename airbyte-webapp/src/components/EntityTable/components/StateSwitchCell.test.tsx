import { CellContext } from "@tanstack/react-table";
import { render } from "@testing-library/react";

import { TestSuspenseBoundary, TestWrapper } from "test-utils";
import { mockWebappConfig } from "test-utils/mock-data/mockWebappConfig";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { StateSwitchCell } from "./StateSwitchCell";
import { ConnectionTableDataItem } from "../types";

jest.mock("core/api", () => ({
  useCurrentWorkspace: jest.fn(() => mockWorkspace),
  useUpdateConnection: jest.fn(() => ({
    mutateAsync: jest.fn(),
    isLoading: false,
  })),
  useGetWebappConfig: () => mockWebappConfig,
}));

jest.mock("core/utils/rbac", () => ({
  useIntent: jest.fn(() => true),
}));

const mockId = "mock-id";

const makeCellProps = (
  connectionId: string,
  enabled = false,
  schemaChange: ConnectionTableDataItem["schemaChange"] = "no_change"
): CellContext<ConnectionTableDataItem, boolean> =>
  ({
    row: {
      original: {
        connectionId,
        schemaChange,
      },
    },
    cell: {
      getValue: () => enabled,
    },
  }) as unknown as CellContext<ConnectionTableDataItem, boolean>;

describe("StateSwitchCell", () => {
  it("renders enabled switch", () => {
    const { getByTestId } = render(
      <TestSuspenseBoundary>
        <StateSwitchCell {...makeCellProps(mockId, true)} />
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
        <StateSwitchCell {...makeCellProps(mockId, false, "breaking")} />
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
        <StateSwitchCell {...makeCellProps(mockId, false, "breaking")} />
      </TestSuspenseBoundary>,
      {
        wrapper: TestWrapper,
      }
    );

    expect(getByTestId("connection-state-switch-mock-id")).toBeDisabled();
  });
});
