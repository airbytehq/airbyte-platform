import { screen, within } from "@testing-library/react";

import { render } from "test-utils";

import { RegionDataBar } from "./calculateGraphData";
import { GraphTooltip } from "./GraphTooltip";

const top10Workspaces = [
  { id: "workspace-alpha", name: "Alpha" },
  { id: "workspace-beta", name: "Beta" },
  { id: "workspace-zero", name: "Zero" },
];

const graphData: RegionDataBar = {
  formattedDate: "2025-01-15",
  regionUsage: 1.45,
  maxWorkspaceUsage: 0.67,
  workspaceUsage: {
    "workspace-alpha": 0.43,
    "workspace-beta": 0.67,
    "workspace-zero": 0.04,
    other: 0.31,
  },
};

describe("GraphTooltip", () => {
  it("renders nothing when inactive", async () => {
    await render(
      <GraphTooltip
        active={false}
        payload={[{ payload: graphData }]}
        regionName="US East (N. Virginia)"
        top10Workspaces={top10Workspaces}
        hasOtherCategory
        barColor="#605cff"
        granularity="day"
      />
    );

    expect(screen.queryByText("Region max")).not.toBeInTheDocument();
  });

  it("renders the date, region aggregate, per-workspace copy, sorted usage, and Other pinned last", async () => {
    await render(
      <GraphTooltip
        active
        payload={[{ payload: graphData }]}
        regionName="US East (N. Virginia)"
        top10Workspaces={top10Workspaces}
        hasOtherCategory
        barColor="#605cff"
        granularity="day"
      />
    );

    expect(screen.getByText("Wed, Jan 15")).toBeInTheDocument();
    expect(screen.queryByText(/\d{1,2}:\d{2}/)).not.toBeInTheDocument();
    expect(screen.getByText("Region max")).toBeInTheDocument();
    expect(screen.getByText("US East (N. Virginia)")).toBeInTheDocument();
    expect(screen.getByText("1.4 DW")).toBeInTheDocument();
    expect(screen.getByText("Per-workspace max")).toBeInTheDocument();
    expect(screen.getByText("top 10")).toBeInTheDocument();
    expect(
      screen.getByText("Peaks can occur at different times, so they may total more than the region max.")
    ).toBeInTheDocument();

    const workspaceList = screen.getByRole("list", { name: "Per-workspace max" });
    expect(
      within(workspaceList)
        .getAllByRole("listitem")
        .map((row) => row.textContent)
    ).toEqual(["Beta0.7", "Alpha0.4", "Other0.3"]);
    expect(within(workspaceList).queryByText("Zero")).not.toBeInTheDocument();
  });

  it("omits Other when there is no remaining workspace category", async () => {
    await render(
      <GraphTooltip
        active
        payload={[{ payload: graphData }]}
        regionName="US East (N. Virginia)"
        top10Workspaces={top10Workspaces}
        hasOtherCategory={false}
        barColor="#605cff"
        granularity="day"
      />
    );

    const workspaceList = screen.getByRole("list", { name: "Per-workspace max" });
    expect(within(workspaceList).queryByText("Other")).not.toBeInTheDocument();
    expect(within(workspaceList).queryByText("Zero")).not.toBeInTheDocument();
  });

  it("includes the local time for an hourly bucket", async () => {
    expect(process.env.TZ).toBe("US/Pacific");

    await render(
      <GraphTooltip
        active
        payload={[
          {
            payload: {
              ...graphData,
              formattedDate: "2025-01-15T18:00:00.000Z",
            },
          },
        ]}
        regionName="US East (N. Virginia)"
        top10Workspaces={top10Workspaces}
        hasOtherCategory
        barColor="#605cff"
        granularity="hour"
      />
    );

    expect(screen.getByText("Wed, Jan 15, 10:00 AM")).toBeInTheDocument();
  });

  it("shows only the weekly bucket start date for a weekly bucket", async () => {
    await render(
      <GraphTooltip
        active
        payload={[
          {
            payload: {
              ...graphData,
              formattedDate: "2026-08-02T07:00:00.000Z",
            },
          },
        ]}
        regionName="US East (N. Virginia)"
        top10Workspaces={top10Workspaces}
        hasOtherCategory
        barColor="#605cff"
        granularity="week"
      />
    );

    expect(screen.getByText("Aug 2, 2026")).toBeInTheDocument();
    expect(screen.queryByText(/Aug 9/)).not.toBeInTheDocument();
  });
});
