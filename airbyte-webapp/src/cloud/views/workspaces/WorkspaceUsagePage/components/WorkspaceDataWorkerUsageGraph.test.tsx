import { fireEvent, screen } from "@testing-library/react";

import { render } from "test-utils";

import { useCurrentWorkspace, useOrganizationWorkerUsage } from "core/api";

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
  data: Array<{ date: string; used: number }>;
  xAxisDataKey: string;
  barDataKey: string;
  xAxisTicks?: Array<string | number>;
  xAxisTickFormatter?: (value: unknown, index: number) => string;
  xAxisInterval?: number;
  xAxisPadding?: { left?: number; right?: number };
  chartKey?: React.Key;
  chartMargin?: { top?: number; right?: number; bottom?: number; left?: number };
  renderTooltipContent: (barColor: string) => React.ReactElement;
  barSize?: number;
  referenceLine?: unknown;
}

const mockCurrentWorkspace = { workspaceId: "workspace-current", name: "Current workspace" };
let mockOrganizationUsage: MockOrganizationUsage = { regions: [] };
const mockDataWorkerUsageBarChart = jest.fn();

jest.mock("core/api", () => ({
  useCurrentWorkspace: jest.fn(() => mockCurrentWorkspace),
  useOrganizationWorkerUsage: jest.fn(() => mockOrganizationUsage),
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
});
