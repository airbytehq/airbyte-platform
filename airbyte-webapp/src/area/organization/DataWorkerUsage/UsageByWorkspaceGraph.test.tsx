import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { useOrganizationHistoricalWorkerUsage, useOrganizationWorkerUsage } from "core/api";

import { GraphTooltip } from "./GraphTooltip";
import { UsageByWorkspaceGraph, UsageTimeRange } from "./UsageByWorkspaceGraph";

interface MockDataWorkerUsageBarChartProps {
  data: Array<Record<string, unknown>>;
  xAxisDataKey: string;
  barDataKey: string;
  xAxisTicks?: string[];
  xAxisTickFormatter?: (value: unknown, index: number) => string;
  comparisonBarDataKey?: string;
  renderTooltipContent: (barColor: string, comparisonBarColor?: string) => React.ReactElement;
  [key: string]: unknown;
}

const mockDataWorkerUsageBarChart = jest.fn();
let mockCurrentUsage = {
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
};
let mockHistoricalUsage: typeof mockCurrentUsage | undefined;
let mockHistoricalUsageError = false;

jest.mock("core/api", () => ({
  useOrganizationWorkerUsage: jest.fn(() => mockCurrentUsage),
  useOrganizationHistoricalWorkerUsage: jest.fn(
    (_params: { startDate: string; endDate: string }, options: { enabled: boolean }) => ({
      data: mockHistoricalUsage,
      isError: options.enabled && mockHistoricalUsageError,
      isInitialLoading: options.enabled && mockHistoricalUsage === undefined && !mockHistoricalUsageError,
    })
  ),
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

const rangeProps: Record<
  UsageTimeRange,
  {
    requestDateRange: [string, string];
    displayRange: [string, string];
    historicalRequestDateRange: [string, string];
    historicalDisplayRange: [string, string];
  }
> = {
  "1d": {
    requestDateRange: ["2026-08-23", "2026-08-24"],
    displayRange: ["2026-08-23T20:00:00.000Z", "2026-08-24T20:00:00.000Z"],
    historicalRequestDateRange: ["2026-08-22", "2026-08-23"],
    historicalDisplayRange: ["2026-08-22T20:00:00.000Z", "2026-08-23T20:00:00.000Z"],
  },
  "1w": {
    requestDateRange: ["2026-08-17", "2026-08-24"],
    displayRange: ["2026-08-17T20:00:00.000Z", "2026-08-24T20:00:00.000Z"],
    historicalRequestDateRange: ["2026-08-10", "2026-08-17"],
    historicalDisplayRange: ["2026-08-10T20:00:00.000Z", "2026-08-17T20:00:00.000Z"],
  },
  "1m": {
    requestDateRange: ["2026-07-25", "2026-08-25"],
    displayRange: ["2026-07-25T07:00:00.000Z", "2026-08-25T07:00:00.000Z"],
    historicalRequestDateRange: ["2026-06-25", "2026-07-25"],
    historicalDisplayRange: ["2026-06-25T07:00:00.000Z", "2026-07-25T07:00:00.000Z"],
  },
  "1q": {
    requestDateRange: ["2026-05-25", "2026-08-25"],
    displayRange: ["2026-05-25T07:00:00.000Z", "2026-08-25T07:00:00.000Z"],
    historicalRequestDateRange: ["2026-02-25", "2026-05-25"],
    historicalDisplayRange: ["2026-02-25T08:00:00.000Z", "2026-05-25T07:00:00.000Z"],
  },
  "1y": {
    requestDateRange: ["2025-08-25", "2026-08-25"],
    displayRange: ["2025-08-25T07:00:00.000Z", "2026-08-25T07:00:00.000Z"],
    historicalRequestDateRange: ["2024-08-25", "2025-08-25"],
    historicalDisplayRange: ["2024-08-25T07:00:00.000Z", "2025-08-25T07:00:00.000Z"],
  },
};

const renderGraph = (
  selectedTimeRange: UsageTimeRange,
  committedDataWorkers: number | null = 4,
  comparisonEnabled = false,
  props = rangeProps[selectedTimeRange]
) =>
  render(
    <UsageByWorkspaceGraph
      selectedRegionId="region-1"
      selectedTimeRange={selectedTimeRange}
      comparisonEnabled={comparisonEnabled}
      committedDataWorkers={committedDataWorkers}
      {...props}
    />
  );

describe(`${UsageByWorkspaceGraph.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockCurrentUsage = {
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
    };
    mockHistoricalUsage = undefined;
    mockHistoricalUsageError = false;
  });

  it("renders the 1D range with 24 hourly bars and time ticks", async () => {
    await renderGraph("1d");

    expect(useOrganizationWorkerUsage).toHaveBeenCalledWith({ startDate: "2026-08-23", endDate: "2026-08-24" }, 60_000);
    expect(useOrganizationHistoricalWorkerUsage).toHaveBeenCalledWith(
      { startDate: "2026-08-22", endDate: "2026-08-23" },
      { enabled: false }
    );
    expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument();

    const chartProps = lastChartProps();
    expect(chartProps.data).toHaveLength(24);
    expect(chartProps.xAxisTicks).toHaveLength(4);
    expect(chartProps.xAxisTickFormatter?.(chartProps.xAxisTicks?.[0], 0)).toBe("1 PM");
    expect(chartProps).toEqual(
      expect.objectContaining({
        xAxisDataKey: "formattedDate",
        barDataKey: "maxWorkspaceUsage",
        comparisonBarDataKey: undefined,
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

  it("renders the calendar 1M range with daily peak bars and spaced date ticks", async () => {
    await renderGraph("1m");

    expect(useOrganizationWorkerUsage).toHaveBeenCalledWith({ startDate: "2026-07-25", endDate: "2026-08-25" }, 60_000);

    const chartProps = lastChartProps();
    expect(chartProps.data).toHaveLength(31);
    expect(chartProps.xAxisTicks).toHaveLength(7);
    expect(chartProps.xAxisTickFormatter?.(chartProps.xAxisTicks?.[0], 0)).toBe("Jul 25");
    expect(chartProps.barSize).toBe(16);
    expect(chartProps.renderTooltipContent("#605cff").props.granularity).toBe("day");
  });

  it("renders the 1Q range with daily bars and five-minute polling", async () => {
    await renderGraph("1q");

    expect(useOrganizationWorkerUsage).toHaveBeenCalledWith(
      { startDate: "2026-05-25", endDate: "2026-08-25" },
      300_000
    );

    const chartProps = lastChartProps();
    expect(chartProps.data).toHaveLength(92);
    expect(chartProps.xAxisTicks).toHaveLength(7);
    expect(chartProps.xAxisTickFormatter?.(chartProps.xAxisTicks?.[0], 0)).toBe("May 25");
    expect(chartProps.barSize).toBe(4);
    expect(chartProps.renderTooltipContent("#605cff").props.granularity).toBe("day");
  });

  it("renders the 1Y range with Sunday-starting weekly bars and month ticks", async () => {
    await renderGraph("1y");

    expect(useOrganizationWorkerUsage).toHaveBeenCalledWith(
      { startDate: "2025-08-25", endDate: "2026-08-25" },
      300_000
    );

    const chartProps = lastChartProps();
    expect(chartProps.data).toHaveLength(53);
    expect(chartProps.data[0]).toEqual(expect.objectContaining({ formattedDate: "2025-08-24T07:00:00.000Z" }));
    expect(chartProps.data.at(-1)).toEqual(expect.objectContaining({ formattedDate: "2026-08-23T07:00:00.000Z" }));
    expect(chartProps.xAxisTicks).toHaveLength(13);
    expect(chartProps.xAxisTickFormatter?.(chartProps.xAxisTicks?.[0], 0)).toBe("Aug");
    expect(chartProps.barSize).toBe(8);
    expect(chartProps.renderTooltipContent("#605cff").props.granularity).toBe("week");
  });

  it("keeps the current single-period chart visible while historical usage loads", async () => {
    await renderGraph("1w", 4, true);

    expect(useOrganizationHistoricalWorkerUsage).toHaveBeenCalledWith(
      { startDate: "2026-08-10", endDate: "2026-08-17" },
      { enabled: true }
    );
    expect(lastChartProps()).toEqual(
      expect.objectContaining({
        barDataKey: "maxWorkspaceUsage",
        comparisonBarDataKey: undefined,
        barSize: 4,
        chartKey: "region-1-1w",
      })
    );
  });

  it("shows a loader instead of no data while historical usage loads for an empty current period", async () => {
    mockCurrentUsage = { regions: [] };

    await renderGraph("1w", 4, true);

    expect(screen.getByText("Loading usage data...")).toBeInTheDocument();
    expect(screen.queryByText("No usage data found for the selected date range.")).not.toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
  });

  it("shows a comparison error while retaining the current-period chart", async () => {
    mockHistoricalUsageError = true;

    await renderGraph("1w", 4, true);

    expect(screen.getByText("Unable to load comparison data.")).toBeInTheDocument();
    expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument();
    expect(lastChartProps()).toEqual(
      expect.objectContaining({
        barDataKey: "maxWorkspaceUsage",
        comparisonBarDataKey: undefined,
        chartKey: "region-1-1w",
      })
    );
  });

  it.each([
    { selectedTimeRange: "1w" as const, expectedBarSize: 2 },
    { selectedTimeRange: "1q" as const, expectedBarSize: 4 },
  ])("uses readable comparison bars for $selectedTimeRange", async ({ selectedTimeRange, expectedBarSize }) => {
    mockHistoricalUsage = { regions: [] };

    await renderGraph(selectedTimeRange, 4, true);

    expect(lastChartProps().barSize).toBe(expectedBarSize);
  });

  it("pairs Current and Previous hourly regional totals by ordinal bucket", async () => {
    mockHistoricalUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-previous",
              name: "Previous workspace",
              dataWorkers: [{ date: "2026-08-23T18:00:00Z", used: 2 }],
            },
          ],
        },
      ],
    };

    await renderGraph("1d", 4, true);

    const chartProps = lastChartProps();
    expect(chartProps).toEqual(
      expect.objectContaining({
        barDataKey: "currentUsage",
        comparisonBarDataKey: "previousUsage",
        barSize: 8,
        chartKey: "region-1-1d-comparison",
        referenceLine: { value: 4, label: "Contracted capacity" },
      })
    );
    expect(chartProps.data).toHaveLength(24);
    expect(chartProps.data[22]).toEqual(
      expect.objectContaining({
        formattedDate: "2026-08-24T18:00:00.000Z",
        currentDate: "2026-08-24T18:00:00.000Z",
        previousDate: "2026-08-23T18:00:00.000Z",
        currentUsage: 1,
        previousUsage: 2,
      })
    );
    const tooltip = chartProps.renderTooltipContent("#605cff", "#00aabb");
    expect(tooltip.type).toBe(GraphTooltip);
    expect(tooltip.props).toEqual(
      expect.objectContaining({
        barColor: "#605cff",
        granularity: "hour",
        comparison: { comparisonBarColor: "#00aabb", selectedTimeRange: "1d" },
      })
    );
    expect(tooltip.props.regionName).toBeUndefined();
    expect(tooltip.props.top10Workspaces).toBeUndefined();
    expect(tooltip.props.hasOtherCategory).toBeUndefined();
  });

  it("uses regional totals for comparison bars", async () => {
    mockCurrentUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-current-a",
              name: "Current workspace A",
              dataWorkers: [{ date: "2026-08-24T18:00:00Z", used: 2 }],
            },
            {
              id: "workspace-current-b",
              name: "Current workspace B",
              dataWorkers: [{ date: "2026-08-24T18:00:00Z", used: 3 }],
            },
          ],
        },
      ],
    };
    mockHistoricalUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-previous-a",
              name: "Previous workspace A",
              dataWorkers: [{ date: "2026-08-23T18:00:00Z", used: 4 }],
            },
            {
              id: "workspace-previous-b",
              name: "Previous workspace B",
              dataWorkers: [{ date: "2026-08-23T18:00:00Z", used: 1 }],
            },
          ],
        },
      ],
    };

    await renderGraph("1d", 4, true);

    expect(lastChartProps()).toEqual(
      expect.objectContaining({
        barDataKey: "currentUsage",
        comparisonBarDataKey: "previousUsage",
      })
    );
    expect(lastChartProps().data[22]).toEqual(
      expect.objectContaining({
        currentUsage: 5,
        previousUsage: 5,
      })
    );
  });

  it("null-fills missing daily buckets and retains Previous overflow", async () => {
    mockCurrentUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-1",
              name: "Workspace 1",
              dataWorkers: [{ date: "2026-03-01T18:00:00Z", used: 3 }],
            },
          ],
        },
      ],
    };
    mockHistoricalUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-1",
              name: "Workspace 1",
              dataWorkers: [{ date: "2026-02-01T18:00:00Z", used: 4 }],
            },
          ],
        },
      ],
    };
    const longerCurrentPeriod = {
      requestDateRange: ["2026-03-01", "2026-04-01"] as [string, string],
      displayRange: ["2026-03-01T08:00:00.000Z", "2026-04-01T07:00:00.000Z"] as [string, string],
      historicalRequestDateRange: ["2026-02-01", "2026-03-01"] as [string, string],
      historicalDisplayRange: ["2026-02-01T08:00:00.000Z", "2026-03-01T08:00:00.000Z"] as [string, string],
    };

    await renderGraph("1m", 4, true, longerCurrentPeriod);

    const longerCurrentData = lastChartProps().data;
    expect(longerCurrentData).toHaveLength(31);
    expect(longerCurrentData[0]).toEqual(
      expect.objectContaining({ currentUsage: 3, previousUsage: 4, previousDate: "2026-02-01T08:00:00.000Z" })
    );
    expect(longerCurrentData.slice(28)).toEqual([
      expect.objectContaining({ currentUsage: 0, previousUsage: null, previousDate: undefined }),
      expect.objectContaining({ currentUsage: 0, previousUsage: null, previousDate: undefined }),
      expect.objectContaining({ currentUsage: 0, previousUsage: null, previousDate: undefined }),
    ]);

    mockCurrentUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-1",
              name: "Workspace 1",
              dataWorkers: [{ date: "2026-02-01T18:00:00Z", used: 3 }],
            },
          ],
        },
      ],
    };
    mockHistoricalUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-1",
              name: "Workspace 1",
              dataWorkers: [{ date: "2026-01-31T18:00:00Z", used: 9 }],
            },
          ],
        },
      ],
    };
    const shorterCurrentPeriod = {
      requestDateRange: ["2026-02-01", "2026-03-01"] as [string, string],
      displayRange: ["2026-02-01T08:00:00.000Z", "2026-03-01T08:00:00.000Z"] as [string, string],
      historicalRequestDateRange: ["2026-01-01", "2026-02-01"] as [string, string],
      historicalDisplayRange: ["2026-01-01T08:00:00.000Z", "2026-02-01T08:00:00.000Z"] as [string, string],
    };

    await renderGraph("1m", 4, true, shorterCurrentPeriod);

    expect(lastChartProps().data).toHaveLength(31);
    expect(lastChartProps().data.at(-1)).toEqual(
      expect.objectContaining({
        formattedDate: "2026-01-31T08:00:00.000Z",
        currentDate: undefined,
        previousDate: "2026-01-31T08:00:00.000Z",
        currentUsage: null,
        previousUsage: 9,
      })
    );
  });

  it("preserves repeated DST hours in both comparison periods", async () => {
    mockCurrentUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-1",
              name: "Workspace 1",
              dataWorkers: [
                { date: "2026-11-01T08:30:00Z", used: 1 },
                { date: "2026-11-01T09:30:00Z", used: 2 },
              ],
            },
          ],
        },
      ],
    };
    mockHistoricalUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-1",
              name: "Workspace 1",
              dataWorkers: [{ date: "2026-10-25T09:30:00Z", used: 4 }],
            },
          ],
        },
      ],
    };

    await renderGraph("1d", 4, true, {
      requestDateRange: ["2026-11-01", "2026-11-01"],
      displayRange: ["2026-11-01T08:00:00.000Z", "2026-11-01T11:00:00.000Z"],
      historicalRequestDateRange: ["2026-10-25", "2026-10-25"],
      historicalDisplayRange: ["2026-10-25T08:00:00.000Z", "2026-10-25T11:00:00.000Z"],
    });

    expect(lastChartProps().data).toEqual([
      expect.objectContaining({
        currentDate: "2026-11-01T08:00:00.000Z",
        previousDate: "2026-10-25T08:00:00.000Z",
        currentUsage: 1,
        previousUsage: 0,
      }),
      expect.objectContaining({
        currentDate: "2026-11-01T09:00:00.000Z",
        previousDate: "2026-10-25T09:00:00.000Z",
        currentUsage: 2,
        previousUsage: 4,
      }),
      expect.objectContaining({
        currentDate: "2026-11-01T10:00:00.000Z",
        previousDate: "2026-10-25T10:00:00.000Z",
        currentUsage: 0,
        previousUsage: 0,
      }),
    ]);
  });

  it("uses weekly regional peaks for yearly comparison", async () => {
    mockCurrentUsage.regions[0].workspaces[0].dataWorkers = [{ date: "2025-08-26T18:00:00Z", used: 3 }];
    mockHistoricalUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-1",
              name: "Workspace 1",
              dataWorkers: [{ date: "2024-08-26T18:00:00Z", used: 2 }],
            },
          ],
        },
      ],
    };

    await renderGraph("1y", 4, true);

    expect(lastChartProps().data[0]).toEqual(expect.objectContaining({ currentUsage: 3, previousUsage: 2 }));
    expect(lastChartProps().renderTooltipContent("#605cff", "#00aabb").props.granularity).toBe("week");
    expect(lastChartProps().barSize).toBe(4);
  });

  it("renders comparison when only Current, only Previous, or an explicit zero sample exists", async () => {
    mockHistoricalUsage = { regions: [] };
    await renderGraph("1d", 4, true);
    expect(screen.getAllByTestId("data-worker-bar-chart")).toHaveLength(1);

    mockCurrentUsage = { regions: [] };
    mockHistoricalUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-1",
              name: "Workspace 1",
              dataWorkers: [{ date: "2026-08-23T18:00:00Z", used: 2 }],
            },
          ],
        },
      ],
    };
    await renderGraph("1d", 4, true);
    expect(screen.getAllByTestId("data-worker-bar-chart")).toHaveLength(2);

    mockCurrentUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-1",
              name: "Workspace 1",
              dataWorkers: [{ date: "2026-08-24T18:00:00Z", used: 0 }],
            },
          ],
        },
      ],
    };
    mockHistoricalUsage = { regions: [] };
    await renderGraph("1d", 4, true);
    expect(screen.getAllByTestId("data-worker-bar-chart")).toHaveLength(3);
    expect(lastChartProps().data[22]).toEqual(expect.objectContaining({ currentUsage: 0, previousUsage: 0 }));
  });

  it("shows no data when neither comparison period contains a selected-region sample", async () => {
    mockCurrentUsage = { regions: [] };
    mockHistoricalUsage = { regions: [] };

    await renderGraph("1d", 4, true);

    expect(screen.getByText("No usage data found for the selected date range.")).toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
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
