import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { useOrganizationWorkerUsage } from "core/api";

import { GraphTooltip } from "./GraphTooltip";
import { UsageByWorkspaceGraph, UsageTimeRange } from "./UsageByWorkspaceGraph";

interface MockDataWorkerUsageBarChartProps {
  data: Array<Record<string, unknown>>;
  xAxisDataKey: string;
  barDataKey: string;
  xAxisTicks?: string[];
  xAxisTickFormatter?: (value: unknown, index: number) => string;
  renderTooltipContent: (barColor: string) => React.ReactElement;
  [key: string]: unknown;
}

const mockDataWorkerUsageBarChart = jest.fn();

jest.mock("core/api", () => ({
  useOrganizationWorkerUsage: jest.fn(() => ({
    regions: [
      {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [{ date: "2026-08-24T18:00:00Z", used: 1 }],
          },
        ],
      },
    ],
  })),
}));

jest.mock("./DataWorkerUsageBarChart", () => {
  const React = jest.requireActual<typeof import("react")>("react");

  return {
    DataWorkerUsageBarChart: (props: MockDataWorkerUsageBarChartProps) => {
      mockDataWorkerUsageBarChart(props);
      return React.createElement("div", { "data-testid": "data-worker-bar-chart" });
    },
  };
});

const lastChartProps = (): MockDataWorkerUsageBarChartProps =>
  mockDataWorkerUsageBarChart.mock.calls[mockDataWorkerUsageBarChart.mock.calls.length - 1][0];

const rangeProps: Record<UsageTimeRange, { requestDateRange: [string, string]; displayRange: [string, string] }> = {
  "1d": {
    requestDateRange: ["2026-08-23", "2026-08-24"],
    displayRange: ["2026-08-23T20:00:00.000Z", "2026-08-24T20:00:00.000Z"],
  },
  "1w": {
    requestDateRange: ["2026-08-17", "2026-08-24"],
    displayRange: ["2026-08-17T20:00:00.000Z", "2026-08-24T20:00:00.000Z"],
  },
  "1m": {
    requestDateRange: ["2026-07-26", "2026-08-25"],
    displayRange: ["2026-07-26T07:00:00.000Z", "2026-08-25T07:00:00.000Z"],
  },
};

const renderGraph = (selectedTimeRange: UsageTimeRange, committedDataWorkers: number | null = 4) =>
  render(
    <UsageByWorkspaceGraph
      selectedRegionId="region-1"
      selectedTimeRange={selectedTimeRange}
      committedDataWorkers={committedDataWorkers}
      {...rangeProps[selectedTimeRange]}
    />
  );

describe(`${UsageByWorkspaceGraph.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renders the 1D range with 24 hourly bars and time ticks", async () => {
    await renderGraph("1d");

    expect(useOrganizationWorkerUsage).toHaveBeenCalledWith({ startDate: "2026-08-23", endDate: "2026-08-24" });
    expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument();

    const chartProps = lastChartProps();
    expect(chartProps.data).toHaveLength(24);
    expect(chartProps.xAxisTicks).toHaveLength(4);
    expect(chartProps.xAxisTickFormatter?.(chartProps.xAxisTicks?.[0], 0)).toBe("1 PM");
    expect(chartProps).toEqual(
      expect.objectContaining({
        xAxisDataKey: "formattedDate",
        barDataKey: "maxWorkspaceUsage",
        chartKey: "region-1-1d",
        xAxisInterval: 0,
        barSize: 16,
        referenceLine: { value: 4, label: "Contracted capacity" },
      })
    );

    const tooltip = chartProps.renderTooltipContent("#605cff");
    expect(tooltip.type).toBe(GraphTooltip);
    expect(tooltip.props).toEqual(
      expect.objectContaining({
        granularity: "hour",
        barColor: "#605cff",
        regionName: "Region 1",
        hasOtherCategory: false,
      })
    );
  });

  it("renders the 1W range with 168 hourly bars and daily ticks", async () => {
    await renderGraph("1w");

    const chartProps = lastChartProps();
    expect(chartProps.data).toHaveLength(168);
    expect(chartProps.xAxisTicks).toHaveLength(7);
    expect(chartProps.xAxisTickFormatter?.(chartProps.xAxisTicks?.[0], 0)).toBe("Aug 17");
    expect(chartProps.barSize).toBe(4);
    expect(chartProps.renderTooltipContent("#605cff").props.granularity).toBe("hour");
  });

  it("renders the 1M range with 30 daily peak bars and spaced date ticks", async () => {
    await renderGraph("1m");

    expect(useOrganizationWorkerUsage).toHaveBeenCalledWith({ startDate: "2026-07-26", endDate: "2026-08-25" });

    const chartProps = lastChartProps();
    expect(chartProps.data).toHaveLength(30);
    expect(chartProps.xAxisTicks).toHaveLength(6);
    expect(chartProps.xAxisTickFormatter?.(chartProps.xAxisTicks?.[0], 0)).toBe("Jul 26");
    expect(chartProps.barSize).toBe(16);
    expect(chartProps.renderTooltipContent("#605cff").props.granularity).toBe("day");
  });

  it("retains the no-data state when the selected region has no workspaces", async () => {
    (useOrganizationWorkerUsage as jest.Mock).mockReturnValueOnce({
      regions: [{ id: "region-1", name: "Region 1", workspaces: [] }],
    });

    await renderGraph("1w");

    expect(screen.getByText("No usage data found for the selected date range.")).toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
  });

  it.each([null, 0])("omits the capacity line when committed capacity is %s", async (committedDataWorkers) => {
    await renderGraph("1w", committedDataWorkers);

    expect(lastChartProps().referenceLine).toBeUndefined();
  });

  it("retains the no-data state when every workspace data point falls outside the display range", async () => {
    (useOrganizationWorkerUsage as jest.Mock).mockReturnValueOnce({
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-1",
              name: "Workspace 1",
              dataWorkers: [{ date: "2026-08-17T19:59:00Z", used: 5 }],
            },
          ],
        },
      ],
    });

    await renderGraph("1w");

    expect(screen.getByText("No usage data found for the selected date range.")).toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
  });

  it("excludes workspaces whose usage is entirely out of range from the top 10 ranking", async () => {
    (useOrganizationWorkerUsage as jest.Mock).mockReturnValueOnce({
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-a",
              name: "Workspace A",
              dataWorkers: [{ date: "2026-08-17T19:59:00Z", used: 1000 }],
            },
            {
              id: "workspace-b",
              name: "Workspace B",
              dataWorkers: [{ date: "2026-08-18T00:00:00Z", used: 1 }],
            },
          ],
        },
      ],
    });

    await renderGraph("1w");

    const tooltip = lastChartProps().renderTooltipContent("#605cff");
    const top10Workspaces = tooltip.props.top10Workspaces as Array<{ id: string }>;
    expect(top10Workspaces.map(({ id }) => id)).toEqual(["workspace-b"]);
  });
});
