import { CellContext } from "@tanstack/react-table";
import { render, screen } from "@testing-library/react";
import React from "react";

import { TestSuspenseBoundary, TestWrapper } from "test-utils";
import { mockWorkspace } from "test-utils/mock-data/mockWorkspace";

import { Tag } from "core/api/types/AirbyteClient";
import { defaultOssFeatures, FeatureItem } from "core/services/features";

import { TagsCell } from "./TagsCell";
import { ConnectionTableDataItem } from "../types";

const mockTags: Tag[] = [
  { tagId: "tag-1", name: "Production", color: "FF0000", workspaceId: "workspace-1" },
  { tagId: "tag-2", name: "Staging", color: "00FF00", workspaceId: "workspace-1" },
];

jest.mock("core/api", () => ({
  useCurrentWorkspace: jest.fn(() => mockWorkspace),
  useTagsList: jest.fn(() => mockTags),
  useCreateTag: jest.fn(() => ({
    mutateAsync: jest.fn(),
  })),
  useUpdateConnectionOptimistically: jest.fn(() => ({
    mutateAsync: jest.fn(),
  })),
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceId: jest.fn(() => "workspace-1"),
}));

const makeCellProps = (
  connectionId: string,
  tags: Tag[] = [],
  onDemandEnabled = false
): CellContext<ConnectionTableDataItem, Tag[]> =>
  ({
    row: {
      original: {
        connectionId,
        tags,
        connection: {
          connectionId,
          onDemandEnabled,
        },
      },
    },
    getValue: () => tags,
  }) as unknown as CellContext<ConnectionTableDataItem, Tag[]>;

// Wrapper with OnDemandCapacity feature enabled
const WrapperWithOnDemand: React.FC<React.PropsWithChildren> = ({ children }) => (
  <TestWrapper features={[...defaultOssFeatures, FeatureItem.OnDemandCapacity]}>{children}</TestWrapper>
);

// Wrapper without OnDemandCapacity feature
const WrapperWithoutOnDemand: React.FC<React.PropsWithChildren> = ({ children }) => (
  <TestWrapper features={defaultOssFeatures}>{children}</TestWrapper>
);

describe("TagsCell", () => {
  it("renders user tags", () => {
    render(
      <TestSuspenseBoundary>
        <TagsCell {...makeCellProps("conn-1", mockTags)} />
      </TestSuspenseBoundary>,
      { wrapper: WrapperWithoutOnDemand }
    );

    expect(screen.getByText("Production")).toBeInTheDocument();
    expect(screen.getByText("Staging")).toBeInTheDocument();
  });

  it("renders Burst tag when onDemandEnabled is true and feature flag is enabled", () => {
    render(
      <TestSuspenseBoundary>
        <TagsCell {...makeCellProps("conn-1", [], true)} />
      </TestSuspenseBoundary>,
      { wrapper: WrapperWithOnDemand }
    );

    expect(screen.getByText("Burst")).toBeInTheDocument();
  });

  it("does NOT render Burst tag when onDemandEnabled is false", () => {
    render(
      <TestSuspenseBoundary>
        <TagsCell {...makeCellProps("conn-1", [], false)} />
      </TestSuspenseBoundary>,
      { wrapper: WrapperWithOnDemand }
    );

    expect(screen.queryByText("Burst")).not.toBeInTheDocument();
  });

  it("does NOT render Burst tag when feature flag is disabled", () => {
    render(
      <TestSuspenseBoundary>
        <TagsCell {...makeCellProps("conn-1", [], true)} />
      </TestSuspenseBoundary>,
      { wrapper: WrapperWithoutOnDemand }
    );

    expect(screen.queryByText("Burst")).not.toBeInTheDocument();
  });

  it("renders both Burst tag and user tags together", () => {
    render(
      <TestSuspenseBoundary>
        <TagsCell {...makeCellProps("conn-1", mockTags, true)} />
      </TestSuspenseBoundary>,
      { wrapper: WrapperWithOnDemand }
    );

    expect(screen.getByText("Burst")).toBeInTheDocument();
    expect(screen.getByText("Production")).toBeInTheDocument();
    expect(screen.getByText("Staging")).toBeInTheDocument();
  });
});
