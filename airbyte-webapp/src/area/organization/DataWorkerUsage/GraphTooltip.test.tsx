import { screen, within } from "@testing-library/react";

import { render } from "test-utils";

import { RegionDataBar } from "./calculateGraphData";
import { GraphTooltip, RegionComparisonDataBar } from "./GraphTooltip";

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
    "workspace-zero": 0.004,
    other: 0.31,
  },
};

const comparisonGraphData: RegionComparisonDataBar = {
  formattedDate: "2025-01-15T18:00:00.000Z",
  currentDate: "2025-01-15T18:00:00.000Z",
  previousDate: "2025-01-08T18:00:00.000Z",
  currentUsage: 1,
  previousUsage: 0,
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
    expect(screen.getByText("0.67 DW")).toBeInTheDocument();
    expect(screen.queryByText("1.45 DW")).not.toBeInTheDocument();
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
    ).toEqual(["Beta0.67", "Alpha0.43", "Other0.31"]);
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

  it.each([
    ["1d", "Comparing vs previous day"],
    ["1w", "Comparing vs previous week"],
    ["1m", "Comparing vs previous month"],
    ["1q", "Comparing vs previous quarter"],
    ["1y", "Comparing vs previous year"],
  ] as const)("renders the %s comparison heading", async (selectedTimeRange, heading) => {
    await render(
      <GraphTooltip
        active
        payload={[{ payload: comparisonGraphData }]}
        barColor="#605cff"
        granularity="day"
        comparison={{ comparisonBarColor: "#00aabb", selectedTimeRange }}
      />
    );

    expect(screen.getByText(heading)).toBeInTheDocument();
  });

  it("renders comparison dates, two-decimal values, themed swatches, and no workspace breakdown", async () => {
    const { container } = await render(
      <GraphTooltip
        active
        payload={[{ payload: comparisonGraphData }]}
        barColor="#605cff"
        granularity="hour"
        comparison={{ comparisonBarColor: "#00aabb", selectedTimeRange: "1d" }}
      />
    );

    expect(screen.getByText("Region max")).toBeInTheDocument();
    expect(screen.getByText("Current")).toBeInTheDocument();
    expect(screen.getByText("Previous")).toBeInTheDocument();
    expect(screen.getByText("Wed, Jan 15, 10:00 AM PST")).toBeInTheDocument();
    expect(screen.getByText("Wed, Jan 8, 10:00 AM PST")).toBeInTheDocument();
    expect(screen.getByText("1.00 DW")).toBeInTheDocument();
    expect(screen.getByText("0.00 DW")).toBeInTheDocument();

    const swatches = container.querySelectorAll('span[aria-hidden="true"]');
    expect(swatches).toHaveLength(2);
    expect(swatches[0]).toHaveStyle({ backgroundColor: "#605cff" });
    expect(swatches[1]).toHaveStyle({ backgroundColor: "#00aabb" });

    expect(screen.queryByText("US East (N. Virginia)")).not.toBeInTheDocument();
    expect(screen.queryByText("Per-workspace max")).not.toBeInTheDocument();
    expect(screen.queryByText("top 10")).not.toBeInTheDocument();
    expect(screen.queryByText("Other")).not.toBeInTheDocument();
  });

  it("renders N/A for a missing comparison metric while preserving the present side's date and value", async () => {
    await render(
      <GraphTooltip
        active
        payload={[
          {
            payload: {
              ...comparisonGraphData,
              currentDate: undefined,
              currentUsage: null,
            },
          },
        ]}
        barColor="#605cff"
        granularity="hour"
        comparison={{ comparisonBarColor: "#00aabb", selectedTimeRange: "1d" }}
      />
    );

    const [currentRow, previousRow] = screen.getAllByRole("listitem");
    expect(within(currentRow).getByText("N/A")).toBeInTheDocument();
    expect(within(previousRow).getByText("Wed, Jan 8, 10:00 AM PST")).toBeInTheDocument();
    expect(within(previousRow).getByText("0.00 DW")).toBeInTheDocument();
  });

  it("distinguishes repeated fall-back hours with time-zone names", async () => {
    expect(process.env.TZ).toBe("US/Pacific");

    await render(
      <GraphTooltip
        active
        payload={[
          {
            payload: {
              ...comparisonGraphData,
              currentDate: "2026-11-01T08:00:00.000Z",
              previousDate: "2026-11-01T09:00:00.000Z",
            },
          },
        ]}
        barColor="#605cff"
        granularity="hour"
        comparison={{ comparisonBarColor: "#00aabb", selectedTimeRange: "1d" }}
      />
    );

    expect(screen.getByText("Sun, Nov 1, 1:00 AM PDT")).toBeInTheDocument();
    expect(screen.getByText("Sun, Nov 1, 1:00 AM PST")).toBeInTheDocument();
  });

  it.each([
    ["day", "Wed, Jan 15", "Wed, Jan 8"],
    ["week", "Jan 15, 2025", "Jan 8, 2025"],
  ] as const)("formats %s comparison dates", async (granularity, currentDate, previousDate) => {
    await render(
      <GraphTooltip
        active
        payload={[{ payload: comparisonGraphData }]}
        barColor="#605cff"
        granularity={granularity}
        comparison={{ comparisonBarColor: "#00aabb", selectedTimeRange: "1m" }}
      />
    );

    expect(screen.getByText(currentDate)).toBeInTheDocument();
    expect(screen.getByText(previousDate)).toBeInTheDocument();
  });

  it("renders nothing for an inactive comparison or an empty comparison payload", async () => {
    const { rerender } = await render(
      <GraphTooltip
        active={false}
        payload={[{ payload: comparisonGraphData }]}
        barColor="#605cff"
        granularity="day"
        comparison={{ comparisonBarColor: "#00aabb", selectedTimeRange: "1m" }}
      />
    );

    expect(screen.queryByText("Comparing vs previous month")).not.toBeInTheDocument();

    rerender(
      <GraphTooltip
        active
        payload={[]}
        barColor="#605cff"
        granularity="day"
        comparison={{ comparisonBarColor: "#00aabb", selectedTimeRange: "1m" }}
      />
    );

    expect(screen.queryByText("Comparing vs previous month")).not.toBeInTheDocument();
  });
});
