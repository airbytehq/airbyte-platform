import { fireEvent, screen, waitFor } from "@testing-library/react";

import { render } from "test-utils";

import { useCurrentWorkspace, useOrganizationHistoricalWorkerUsage, useOrganizationWorkerUsage } from "core/api";

import { WorkspaceDataWorkerGraphTooltip } from "./WorkspaceDataWorkerGraphTooltip";
import { WorkspaceDataWorkerUsageGraph } from "./WorkspaceDataWorkerUsageGraph";

interface MockOrganizationUsage {
  committedDataWorkers?: number | null;
  regions: Array<{
    id: string;
    name: string;
    workspaces: Array<{
      id: string;
      name: string;
      dataWorkers: Array<{ date: string; used: number }>;
    }>;
  }>;
}

interface MockDataWorkerUsageBarChartProps {
  data: Array<{
    date: string;
    used?: number;
    currentDate?: string;
    previousDate?: string;
    currentUsage?: number | null;
    previousUsage?: number | null;
  }>;
  xAxisDataKey: string;
  barDataKey: string;
  comparisonBarDataKey?: string;
  xAxisTicks?: Array<string | number>;
  xAxisTickFormatter?: (value: unknown, index: number) => string;
  xAxisInterval?: number;
  xAxisPadding?: { left?: number; right?: number };
  chartKey?: React.Key;
  chartMargin?: { top?: number; right?: number; bottom?: number; left?: number };
  renderTooltipContent: (barColor: string, comparisonBarColor?: string) => React.ReactElement;
  barSize?: number;
  referenceLine?: unknown;
}

const mockCurrentWorkspace = { workspaceId: "workspace-current", name: "Current workspace" };
let mockOrganizationUsage: MockOrganizationUsage = { regions: [] };
let mockHistoricalOrganizationUsage: MockOrganizationUsage | undefined;
let mockHistoricalOrganizationUsageError = false;
let mockPendingOrganizationUsageRequest: Promise<void> | null = null;
const mockDataWorkerUsageBarChart = jest.fn();

jest.mock("core/api", () => ({
  useCurrentWorkspace: jest.fn(() => mockCurrentWorkspace),
  useOrganizationWorkerUsage: jest.fn((params: { startDate: string }) => {
    if (params.startDate === "2025-08-25" && mockPendingOrganizationUsageRequest) {
      throw mockPendingOrganizationUsageRequest;
    }

    return mockOrganizationUsage;
  }),
  useOrganizationHistoricalWorkerUsage: jest.fn(
    (_params: { startDate: string; endDate: string }, options: { enabled: boolean }) => ({
      data: options.enabled ? mockHistoricalOrganizationUsage : undefined,
      isError: options.enabled && mockHistoricalOrganizationUsageError,
      isInitialLoading:
        options.enabled && mockHistoricalOrganizationUsage === undefined && !mockHistoricalOrganizationUsageError,
    })
  ),
}));

jest.mock("area/organization/DataWorkerUsage/DataWorkerUsageBarChart", () => {
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

const workspaceUsage = (dataWorkers: Array<{ date: string; used: number }>): MockOrganizationUsage => ({
  regions: [
    {
      id: "region-1",
      name: "Region 1",
      workspaces: [{ id: "workspace-current", name: "Current workspace", dataWorkers }],
    },
  ],
});

jest.useFakeTimers();

describe(`${WorkspaceDataWorkerUsageGraph.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.setSystemTime(new Date("2026-08-24T12:34:00-07:00"));
    mockOrganizationUsage = { regions: [] };
    mockHistoricalOrganizationUsage = undefined;
    mockHistoricalOrganizationUsageError = false;
    mockPendingOrganizationUsageRequest = null;
  });

  afterAll(() => {
    jest.useRealTimers();
  });

  it("defaults to 1W with exactly 168 hourly buckets and clips the date-only response", async () => {
    mockOrganizationUsage = {
      committedDataWorkers: 4,
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [
                { date: "2026-08-17T19:59:00Z", used: 100 },
                { date: "2026-08-18T16:15:00Z", used: 1.2 },
                { date: "2026-08-24T19:15:00Z", used: 0.5 },
              ],
            },
            {
              id: "workspace-other",
              name: "Other workspace",
              dataWorkers: [{ date: "2026-08-18T16:00:00Z", used: 99 }],
            },
          ],
        },
        {
          id: "region-2",
          name: "Region 2",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [{ date: "2026-08-18T16:45:00Z", used: 0.8 }],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(screen.getByRole("group", { name: "Usage time range" })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "1D" })).not.toBeChecked();
    expect(screen.getByRole("radio", { name: "1W" })).toBeChecked();
    expect(screen.getByRole("radio", { name: "1M" })).not.toBeChecked();
    expect(screen.getByRole("radio", { name: "1Q" })).not.toBeChecked();
    expect(screen.getByRole("radio", { name: "1Y" })).not.toBeChecked();
    expect(screen.getByRole("checkbox", { name: "Compare to previous period" })).not.toBeChecked();
    expect(useOrganizationHistoricalWorkerUsage).toHaveBeenLastCalledWith(
      { startDate: "2026-08-10", endDate: "2026-08-17" },
      { enabled: false }
    );
    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      {
        startDate: "2026-08-17",
        endDate: "2026-08-24",
      },
      60_000
    );
    expect(useCurrentWorkspace).toHaveBeenCalled();
    expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument();

    const chartProps = lastChartProps();
    expect(chartProps.data).toHaveLength(168);
    expect(chartProps.data[0]).toEqual({ date: "2026-08-17T20:00:00.000Z", used: 0 });
    expect(chartProps.data.at(-1)).toEqual({ date: "2026-08-24T19:00:00.000Z", used: 0.5 });
    expect(chartProps.data).toEqual(expect.arrayContaining([{ date: "2026-08-18T16:00:00.000Z", used: 2 }]));
    expect(chartProps.data).not.toContainEqual({ date: "2026-08-17T19:00:00.000Z", used: 100 });
    expect(chartProps).toEqual(
      expect.objectContaining({
        xAxisDataKey: "date",
        barDataKey: "used",
        comparisonBarDataKey: undefined,
        xAxisInterval: 0,
        xAxisPadding: { left: 20, right: 20 },
        chartKey: "1w",
        chartMargin: { top: 0, right: 20, left: 0, bottom: 0 },
        barSize: 4,
        referenceLine: { value: 4, label: "Contracted capacity" },
      })
    );
    expect(chartProps.xAxisTicks).toHaveLength(7);
    expect(chartProps.xAxisTicks?.map((tick, index) => chartProps.xAxisTickFormatter?.(tick, index))).toEqual([
      "Aug 17",
      "Aug 18",
      "Aug 19",
      "Aug 20",
      "Aug 21",
      "Aug 22",
      "Aug 23",
    ]);

    const tooltip = chartProps.renderTooltipContent("#605cff");
    expect(tooltip.type).toBe(WorkspaceDataWorkerGraphTooltip);
    expect(tooltip.props).toEqual(expect.objectContaining({ workspaceName: "Current workspace", granularity: "hour" }));
  });

  it("switches to aligned 1D and 1M windows and uses daily workspace peaks for 1M", async () => {
    mockOrganizationUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [
                { date: "2026-08-23T16:15:00Z", used: 1.2 },
                { date: "2026-08-23T20:15:00Z", used: 1.5 },
                { date: "2026-08-24T19:15:00Z", used: 0.5 },
                { date: "2026-08-25T07:00:00Z", used: 100 },
              ],
            },
          ],
        },
        {
          id: "region-2",
          name: "Region 2",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [{ date: "2026-08-23T16:45:00Z", used: 0.8 }],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    fireEvent.click(screen.getByRole("radio", { name: "1D" }));

    expect(screen.getByRole("radio", { name: "1D" })).toBeChecked();
    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      { startDate: "2026-08-23", endDate: "2026-08-24" },
      60_000
    );
    expect(lastChartProps().data).toHaveLength(24);
    expect(lastChartProps().data[0]).toEqual({ date: "2026-08-23T20:00:00.000Z", used: 1.5 });
    expect(lastChartProps().data.at(-1)).toEqual({ date: "2026-08-24T19:00:00.000Z", used: 0.5 });
    expect(lastChartProps().xAxisTicks).toHaveLength(4);
    expect(lastChartProps().xAxisTickFormatter?.(lastChartProps().xAxisTicks?.[0], 0)).toBe("1 PM");
    expect(lastChartProps().barSize).toBe(16);
    expect(lastChartProps().chartKey).toBe("1d");
    expect(lastChartProps().renderTooltipContent("#605cff").props.granularity).toBe("hour");

    fireEvent.click(screen.getByRole("radio", { name: "1M" }));

    expect(screen.getByRole("radio", { name: "1M" })).toBeChecked();
    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      { startDate: "2026-07-25", endDate: "2026-08-25" },
      60_000
    );
    expect(lastChartProps().data).toHaveLength(31);
    expect(lastChartProps().data[0]).toEqual({ date: "2026-07-25T07:00:00.000Z", used: 0 });
    expect(lastChartProps().data.at(-1)).toEqual({ date: "2026-08-24T07:00:00.000Z", used: 0.5 });
    expect(lastChartProps().data).toEqual(expect.arrayContaining([{ date: "2026-08-23T07:00:00.000Z", used: 2 }]));
    expect(lastChartProps().data).not.toContainEqual({ date: "2026-08-25T07:00:00.000Z", used: 100 });
    expect(lastChartProps().xAxisTicks).toHaveLength(7);
    expect(lastChartProps().xAxisTickFormatter?.(lastChartProps().xAxisTicks?.[0], 0)).toBe("Jul 25");
    expect(lastChartProps().barSize).toBe(16);
    expect(lastChartProps().chartKey).toBe("1m");
    expect(lastChartProps().renderTooltipContent("#605cff").props.granularity).toBe("day");
    expect(window.location.search).toBe("");
  });

  it("uses daily quarter buckets and Sunday-starting weekly year buckets with five-minute polling", async () => {
    mockOrganizationUsage = workspaceUsage([
      { date: "2026-08-18T16:15:00Z", used: 3 },
      { date: "2026-08-23T16:15:00Z", used: 1.2 },
      { date: "2026-08-23T20:15:00Z", used: 1.5 },
      { date: "2026-08-24T19:15:00Z", used: 0.5 },
      { date: "2026-08-25T07:00:00Z", used: 100 },
    ]);

    await render(<WorkspaceDataWorkerUsageGraph />);

    fireEvent.click(screen.getByRole("radio", { name: "1Q" }));

    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      { startDate: "2026-05-25", endDate: "2026-08-25" },
      300_000
    );
    expect(lastChartProps().data).toHaveLength(92);
    expect(lastChartProps().data[0]).toEqual({ date: "2026-05-25T07:00:00.000Z", used: 0 });
    expect(lastChartProps().data.at(-1)).toEqual({ date: "2026-08-24T07:00:00.000Z", used: 0.5 });
    expect(lastChartProps().data).toEqual(expect.arrayContaining([{ date: "2026-08-23T07:00:00.000Z", used: 1.5 }]));
    expect(lastChartProps().xAxisTicks).toHaveLength(7);
    expect(lastChartProps().xAxisTickFormatter?.(lastChartProps().xAxisTicks?.[0], 0)).toBe("May 25");
    expect(lastChartProps().barSize).toBe(4);
    expect(lastChartProps().renderTooltipContent("#605cff").props.granularity).toBe("day");

    fireEvent.click(screen.getByRole("radio", { name: "1Y" }));

    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      { startDate: "2025-08-25", endDate: "2026-08-25" },
      300_000
    );
    expect(lastChartProps().data).toHaveLength(53);
    expect(lastChartProps().data[0]).toEqual({ date: "2025-08-24T07:00:00.000Z", used: 0 });
    expect(lastChartProps().data.at(-1)).toEqual({ date: "2026-08-23T07:00:00.000Z", used: 1.5 });
    expect(lastChartProps().data).toEqual(expect.arrayContaining([{ date: "2026-08-16T07:00:00.000Z", used: 3 }]));
    expect(lastChartProps().data).not.toContainEqual({ date: "2026-08-30T07:00:00.000Z", used: 100 });
    expect(lastChartProps().xAxisTicks).toHaveLength(13);
    expect(lastChartProps().xAxisTickFormatter?.(lastChartProps().xAxisTicks?.[0], 0)).toBe("Aug");
    expect(lastChartProps().barSize).toBe(8);
    expect(lastChartProps().renderTooltipContent("#605cff").props.granularity).toBe("week");
  });

  it("requests every preceding period only when comparison is enabled", async () => {
    mockOrganizationUsage = workspaceUsage([{ date: "2026-08-24T19:15:00Z", used: 1 }]);
    mockHistoricalOrganizationUsage = { regions: [] };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(useOrganizationHistoricalWorkerUsage).toHaveBeenLastCalledWith(
      { startDate: "2026-08-10", endDate: "2026-08-17" },
      { enabled: false }
    );

    fireEvent.click(screen.getByText("Compare to previous period"));

    expect(screen.getByRole("checkbox", { name: "Compare to previous period" })).toBeChecked();
    expect(useOrganizationHistoricalWorkerUsage).toHaveBeenLastCalledWith(
      { startDate: "2026-08-10", endDate: "2026-08-17" },
      { enabled: true }
    );

    const expectedRanges: Array<{ label: string; startDate: string; endDate: string }> = [
      { label: "1D", startDate: "2026-08-22", endDate: "2026-08-23" },
      { label: "1M", startDate: "2026-06-25", endDate: "2026-07-25" },
      { label: "1Q", startDate: "2026-02-25", endDate: "2026-05-25" },
      { label: "1Y", startDate: "2024-08-25", endDate: "2025-08-25" },
    ];

    for (const { label, startDate, endDate } of expectedRanges) {
      fireEvent.click(screen.getByRole("radio", { name: label }));
      await waitFor(() =>
        expect(useOrganizationHistoricalWorkerUsage).toHaveBeenLastCalledWith({ startDate, endDate }, { enabled: true })
      );
    }
  });

  it.each([
    { label: "1W", expectedBarSize: 2 },
    { label: "1Q", expectedBarSize: 4 },
  ])("uses readable comparison bars for $label", async ({ label, expectedBarSize }) => {
    mockOrganizationUsage = workspaceUsage([{ date: "2026-08-24T19:15:00Z", used: 1 }]);
    mockHistoricalOrganizationUsage = { regions: [] };

    await render(<WorkspaceDataWorkerUsageGraph />);
    fireEvent.click(screen.getByText("Compare to previous period"));
    fireEvent.click(screen.getByRole("radio", { name: label }));

    expect(lastChartProps().barSize).toBe(expectedBarSize);
  });

  it("renders grouped cross-region workspace peaks and the workspace comparison tooltip contract", async () => {
    mockOrganizationUsage = {
      committedDataWorkers: 4,
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [{ date: "2026-08-18T16:15:00Z", used: 1.2 }],
            },
          ],
        },
        {
          id: "region-2",
          name: "Region 2",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [{ date: "2026-08-18T16:45:00Z", used: 0.8 }],
            },
          ],
        },
      ],
    };
    mockHistoricalOrganizationUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [{ date: "2026-08-11T16:15:00Z", used: 0.5 }],
            },
          ],
        },
        {
          id: "region-2",
          name: "Region 2",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [{ date: "2026-08-11T16:45:00Z", used: 1 }],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);
    fireEvent.click(screen.getByText("Compare to previous period"));

    const chartProps = lastChartProps();
    expect(chartProps.data).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          date: "2026-08-18T16:00:00.000Z",
          currentDate: "2026-08-18T16:00:00.000Z",
          previousDate: "2026-08-11T16:00:00.000Z",
          currentUsage: 2,
          previousUsage: 1.5,
        }),
      ])
    );
    expect(chartProps).toEqual(
      expect.objectContaining({
        barDataKey: "currentUsage",
        comparisonBarDataKey: "previousUsage",
        barSize: 2,
        chartKey: "1w-comparison",
        referenceLine: { value: 4, label: "Contracted capacity" },
      })
    );

    const tooltip = chartProps.renderTooltipContent("#605cff", "#00aabb");
    expect(tooltip.type).toBe(WorkspaceDataWorkerGraphTooltip);
    expect(tooltip.props).toEqual(
      expect.objectContaining({
        granularity: "hour",
        comparison: {
          barColor: "#605cff",
          comparisonBarColor: "#00aabb",
          selectedTimeRange: "1w",
        },
      })
    );
    expect(tooltip.props.workspaceName).toBeUndefined();

    fireEvent.click(screen.getByText("Compare to previous period"));

    expect(screen.getByRole("checkbox", { name: "Compare to previous period" })).not.toBeChecked();
    expect(lastChartProps()).toEqual(
      expect.objectContaining({
        barDataKey: "used",
        comparisonBarDataKey: undefined,
        barSize: 4,
        chartKey: "1w",
      })
    );
  });

  it("uses null for a missing previous calendar-month bucket", async () => {
    mockOrganizationUsage = workspaceUsage([{ date: "2026-08-24T19:15:00Z", used: 1 }]);
    mockHistoricalOrganizationUsage = workspaceUsage([{ date: "2026-07-24T19:15:00Z", used: 2 }]);

    await render(<WorkspaceDataWorkerUsageGraph />);
    fireEvent.click(screen.getByText("Compare to previous period"));
    fireEvent.click(screen.getByRole("radio", { name: "1M" }));

    expect(lastChartProps().data).toHaveLength(31);
    expect(lastChartProps().data.at(-1)).toEqual(
      expect.objectContaining({
        date: "2026-08-24T07:00:00.000Z",
        currentDate: "2026-08-24T07:00:00.000Z",
        currentUsage: 1,
        previousDate: undefined,
        previousUsage: null,
      })
    );
  });

  it("retains surplus previous calendar-month buckets", async () => {
    jest.setSystemTime(new Date("2026-03-28T12:34:00-07:00"));
    mockOrganizationUsage = workspaceUsage([{ date: "2026-03-28T19:15:00Z", used: 1 }]);
    mockHistoricalOrganizationUsage = workspaceUsage([{ date: "2026-02-27T19:15:00Z", used: 2 }]);

    await render(<WorkspaceDataWorkerUsageGraph />);
    fireEvent.click(screen.getByText("Compare to previous period"));
    fireEvent.click(screen.getByRole("radio", { name: "1M" }));

    expect(lastChartProps().data).toHaveLength(31);
    expect(lastChartProps().data[28]).toEqual(
      expect.objectContaining({
        date: "2026-03-28T07:00:00.000Z",
        previousDate: "2026-02-25T08:00:00.000Z",
      })
    );
    expect(lastChartProps().data.at(-1)).toEqual(
      expect.objectContaining({
        date: "2026-02-27T08:00:00.000Z",
        currentDate: undefined,
        currentUsage: null,
        previousDate: "2026-02-27T08:00:00.000Z",
        previousUsage: 2,
      })
    );
  });

  it("uses daily and weekly workspace peaks for both comparison periods", async () => {
    mockOrganizationUsage = workspaceUsage([
      { date: "2026-08-18T16:15:00Z", used: 3 },
      { date: "2026-08-23T16:15:00Z", used: 1.2 },
      { date: "2026-08-23T20:15:00Z", used: 1.5 },
    ]);
    mockHistoricalOrganizationUsage = workspaceUsage([
      { date: "2026-07-24T16:15:00Z", used: 0.7 },
      { date: "2026-07-24T20:15:00Z", used: 1.1 },
      { date: "2025-08-18T16:15:00Z", used: 2.5 },
      { date: "2025-08-23T20:15:00Z", used: 1 },
    ]);

    await render(<WorkspaceDataWorkerUsageGraph />);
    fireEvent.click(screen.getByText("Compare to previous period"));
    fireEvent.click(screen.getByRole("radio", { name: "1M" }));

    expect(lastChartProps().data).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          date: "2026-08-23T07:00:00.000Z",
          previousDate: "2026-07-24T07:00:00.000Z",
          currentUsage: 1.5,
          previousUsage: 1.1,
        }),
      ])
    );

    fireEvent.click(screen.getByRole("radio", { name: "1Y" }));

    expect(lastChartProps().data).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          date: "2026-08-16T07:00:00.000Z",
          previousDate: "2025-08-17T07:00:00.000Z",
          currentUsage: 3,
          previousUsage: 2.5,
        }),
      ])
    );
  });

  it.each([
    {
      name: "current-only usage",
      currentUsage: [{ date: "2026-08-18T16:15:00Z", used: 1 }],
      previousUsage: [],
      expectedCurrent: 1,
      expectedPrevious: 0,
    },
    {
      name: "previous-only usage",
      currentUsage: [],
      previousUsage: [{ date: "2026-08-11T16:15:00Z", used: 2 }],
      expectedCurrent: 0,
      expectedPrevious: 2,
    },
    {
      name: "an explicit current zero sample",
      currentUsage: [{ date: "2026-08-18T16:15:00Z", used: 0 }],
      previousUsage: [],
      expectedCurrent: 0,
      expectedPrevious: 0,
    },
  ])("handles $name in comparison mode", async ({ currentUsage, previousUsage, expectedCurrent, expectedPrevious }) => {
    mockOrganizationUsage = workspaceUsage(currentUsage);
    mockHistoricalOrganizationUsage = workspaceUsage(previousUsage);

    await render(<WorkspaceDataWorkerUsageGraph />);
    fireEvent.click(screen.getByText("Compare to previous period"));

    expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument();
    expect(lastChartProps().data[20]).toEqual(
      expect.objectContaining({ currentUsage: expectedCurrent, previousUsage: expectedPrevious })
    );
  });

  it("renders the no-data state when both comparison periods are empty", async () => {
    mockOrganizationUsage = workspaceUsage([]);
    mockHistoricalOrganizationUsage = workspaceUsage([]);

    await render(<WorkspaceDataWorkerUsageGraph />);
    fireEvent.click(screen.getByText("Compare to previous period"));

    expect(screen.getByText("No usage data found for the selected date range.")).toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
  });

  it("shows a loader instead of no data while historical usage loads for an empty current period", async () => {
    mockOrganizationUsage = workspaceUsage([]);

    await render(<WorkspaceDataWorkerUsageGraph />);
    fireEvent.click(screen.getByText("Compare to previous period"));

    expect(screen.getByText("Loading usage data...")).toBeInTheDocument();
    expect(screen.queryByText("No usage data found for the selected date range.")).not.toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
  });

  it("shows a comparison error while retaining the current-period chart", async () => {
    mockOrganizationUsage = workspaceUsage([{ date: "2026-08-24T19:15:00Z", used: 1 }]);
    mockHistoricalOrganizationUsageError = true;

    await render(<WorkspaceDataWorkerUsageGraph />);
    fireEvent.click(screen.getByText("Compare to previous period"));

    expect(screen.getByText("Unable to load comparison data.")).toBeInTheDocument();
    expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument();
    expect(lastChartProps()).toEqual(
      expect.objectContaining({
        barDataKey: "used",
        comparisonBarDataKey: undefined,
        chartKey: "1w",
      })
    );
  });

  it("shows the selected range and chart loader while a new range query is pending", async () => {
    mockOrganizationUsage = workspaceUsage([{ date: "2026-08-24T19:15:00Z", used: 1 }]);

    await render(<WorkspaceDataWorkerUsageGraph />);
    expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument();

    let resolvePendingOrganizationUsageRequest!: () => void;
    mockPendingOrganizationUsageRequest = new Promise<void>((resolve) => {
      resolvePendingOrganizationUsageRequest = resolve;
    });

    fireEvent.click(screen.getByRole("radio", { name: "1Y" }));

    expect(screen.getByRole("radio", { name: "1Y" })).toBeChecked();
    expect(screen.getByText("Loading usage data...")).toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
    await waitFor(() =>
      expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
        { startDate: "2025-08-25", endDate: "2026-08-25" },
        300_000
      )
    );

    mockPendingOrganizationUsageRequest = null;
    resolvePendingOrganizationUsageRequest();

    await waitFor(() => expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument());
    expect(lastChartProps().chartKey).toBe("1y");
  });

  it("renders the no-data state and range controls when the current workspace has no usage", async () => {
    mockOrganizationUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-other",
              name: "Other workspace",
              dataWorkers: [{ date: "2026-08-24T19:00:00Z", used: 1 }],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(screen.getByRole("group", { name: "Usage time range" })).toBeInTheDocument();
    expect(screen.getByText("No usage data found for the selected date range.")).toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
    expect(mockDataWorkerUsageBarChart).not.toHaveBeenCalled();
  });

  it("renders the no-data state when the current workspace has only out-of-range usage", async () => {
    mockOrganizationUsage = workspaceUsage([{ date: "2026-08-17T19:59:00Z", used: 1 }]);

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(screen.getByText("No usage data found for the selected date range.")).toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
    expect(mockDataWorkerUsageBarChart).not.toHaveBeenCalled();
  });

  it("renders a zero-usage hourly point as chart data", async () => {
    mockOrganizationUsage = workspaceUsage([{ date: "2026-08-24T19:15:00Z", used: 0 }]);

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument();
    expect(lastChartProps().data).toHaveLength(168);
    expect(lastChartProps().data).toEqual(expect.arrayContaining([{ date: "2026-08-24T19:00:00.000Z", used: 0 }]));
    expect(lastChartProps().referenceLine).toBeUndefined();
    expect(screen.queryByText("No usage data found for the selected date range.")).not.toBeInTheDocument();
  });

  it("requests and includes the next UTC date after local time crosses UTC midnight", async () => {
    jest.setSystemTime(new Date("2026-08-24T18:34:00-07:00"));
    mockOrganizationUsage = workspaceUsage([{ date: "2026-08-25T00:15:00Z", used: 2 }]);

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(useOrganizationWorkerUsage).toHaveBeenLastCalledWith(
      {
        startDate: "2026-08-18",
        endDate: "2026-08-25",
      },
      60_000
    );
    expect(lastChartProps().data).toHaveLength(168);
    expect(lastChartProps().data).toEqual(expect.arrayContaining([{ date: "2026-08-25T00:00:00.000Z", used: 2 }]));
    expect(lastChartProps().data.at(-1)).toEqual({ date: "2026-08-25T01:00:00.000Z", used: 0 });
  });

  it("omits the contracted capacity line when committed capacity is zero", async () => {
    mockOrganizationUsage = {
      ...workspaceUsage([{ date: "2026-08-24T19:15:00Z", used: 1 }]),
      committedDataWorkers: 0,
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(lastChartProps().referenceLine).toBeUndefined();
  });

  it("keeps both occurrences of a repeated fall-back hour as distinct buckets", async () => {
    jest.setSystemTime(new Date("2026-11-01T02:30:00-08:00"));
    mockOrganizationUsage = workspaceUsage([
      { date: "2026-11-01T08:15:00Z", used: 1 },
      { date: "2026-11-01T09:15:00Z", used: 2 },
    ]);

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(lastChartProps().data).toHaveLength(168);
    expect(lastChartProps().data).toEqual(
      expect.arrayContaining([
        { date: "2026-11-01T08:00:00.000Z", used: 1 },
        { date: "2026-11-01T09:00:00.000Z", used: 2 },
      ])
    );
    expect(lastChartProps().data.at(-1)).toEqual({ date: "2026-11-01T10:00:00.000Z", used: 0 });
  });

  it("pairs both occurrences of a repeated fall-back hour with distinct previous buckets", async () => {
    jest.setSystemTime(new Date("2026-11-01T02:30:00-08:00"));
    mockOrganizationUsage = workspaceUsage([
      { date: "2026-11-01T08:15:00Z", used: 1 },
      { date: "2026-11-01T09:15:00Z", used: 2 },
    ]);
    mockHistoricalOrganizationUsage = workspaceUsage([
      { date: "2026-10-25T08:15:00Z", used: 3 },
      { date: "2026-10-25T09:15:00Z", used: 4 },
    ]);

    await render(<WorkspaceDataWorkerUsageGraph />);
    fireEvent.click(screen.getByText("Compare to previous period"));

    expect(lastChartProps().data).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          currentDate: "2026-11-01T08:00:00.000Z",
          previousDate: "2026-10-25T08:00:00.000Z",
          currentUsage: 1,
          previousUsage: 3,
        }),
        expect.objectContaining({
          currentDate: "2026-11-01T09:00:00.000Z",
          previousDate: "2026-10-25T09:00:00.000Z",
          currentUsage: 2,
          previousUsage: 4,
        }),
      ])
    );
  });
});
