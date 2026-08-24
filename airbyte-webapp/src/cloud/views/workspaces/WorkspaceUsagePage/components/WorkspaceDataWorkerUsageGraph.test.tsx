import { screen } from "@testing-library/react";

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
  xAxisPadding?: { left?: number; right?: number };
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

  it("zero-fills the requested hourly range, aggregates normalized hours, and includes a tick for every day", async () => {
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
                { date: "2026-08-16T16:00:00Z", used: 100 },
                { date: "2026-08-18T16:15:00Z", used: 1.2 },
                { date: "2026-08-17T20:00:00Z", used: 0.5 },
                { date: "2026-08-17T16:00:00Z", used: 0.25 },
              ],
            },
            {
              id: "workspace-other",
              name: "Other workspace",
              dataWorkers: [{ date: "2026-08-17T16:00:00Z", used: 99 }],
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
              dataWorkers: [
                { date: "2026-08-18T16:45:00Z", used: 0.8 },
                { date: "2026-08-17T16:00:00Z", used: 0.75 },
              ],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(useOrganizationWorkerUsage).toHaveBeenCalledWith({
      startDate: "2026-08-17",
      endDate: "2026-08-24",
    });
    expect(useCurrentWorkspace).toHaveBeenCalled();
    expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument();

    const chartProps = lastChartProps();
    expect(chartProps.data).toHaveLength(7 * 24 + 13);
    expect(chartProps.data.at(-1)).toEqual({ date: "2026-08-24T19:00:00.000Z", used: 0 });
    expect(chartProps.data).toEqual(
      expect.arrayContaining([
        { date: "2026-08-17T16:00:00.000Z", used: 1 },
        { date: "2026-08-17T20:00:00.000Z", used: 0.5 },
        { date: "2026-08-18T16:00:00.000Z", used: 2 },
        { date: "2026-08-20T19:00:00.000Z", used: 0 },
      ])
    );
    expect(chartProps.data).not.toContainEqual({ date: "2026-08-16T16:00:00.000Z", used: 100 });
    expect(chartProps).toEqual(
      expect.objectContaining({
        xAxisDataKey: "date",
        barDataKey: "used",
        xAxisPadding: { left: 20, right: 20 },
        chartMargin: { top: 0, right: 20, left: 0, bottom: 0 },
      })
    );
    expect(chartProps.xAxisTicks).toHaveLength(8);
    expect(chartProps.xAxisTicks?.map((tick, index) => chartProps.xAxisTickFormatter?.(tick, index))).toEqual([
      "Aug 17",
      "Aug 18",
      "Aug 19",
      "Aug 20",
      "Aug 21",
      "Aug 22",
      "Aug 23",
      "Aug 24",
    ]);
    expect(chartProps.barSize).toBeUndefined();
    expect(chartProps.referenceLine).toEqual({ value: 4, label: "Contracted capacity" });

    const tooltip = chartProps.renderTooltipContent("#605cff");
    expect(tooltip.type).toBe(WorkspaceDataWorkerGraphTooltip);
    expect(tooltip.props).toEqual(expect.objectContaining({ workspaceName: "Current workspace" }));
  });

  it("renders the existing no-data state when the current workspace has no usage", async () => {
    mockOrganizationUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-other",
              name: "Other workspace",
              dataWorkers: [{ date: "2026-08-17T16:00:00Z", used: 1 }],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(screen.getByText("No usage data found for the selected date range.")).toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
    expect(mockDataWorkerUsageBarChart).not.toHaveBeenCalled();
  });

  it("renders the no-data state when the current workspace has only out-of-range usage", async () => {
    mockOrganizationUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [{ date: "2026-08-16T16:00:00Z", used: 1 }],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(screen.getByText("No usage data found for the selected date range.")).toBeInTheDocument();
    expect(screen.queryByTestId("data-worker-bar-chart")).not.toBeInTheDocument();
    expect(mockDataWorkerUsageBarChart).not.toHaveBeenCalled();
  });

  it("renders a zero-usage hourly point as a bar chart data point", async () => {
    mockOrganizationUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [{ date: "2026-08-17T16:00:00Z", used: 0 }],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(screen.getByTestId("data-worker-bar-chart")).toBeInTheDocument();
    expect(lastChartProps().data).toHaveLength(7 * 24 + 13);
    expect(lastChartProps().data).toEqual(expect.arrayContaining([{ date: "2026-08-17T16:00:00.000Z", used: 0 }]));
    expect(lastChartProps().referenceLine).toBeUndefined();
    expect(screen.queryByText("No usage data found for the selected date range.")).not.toBeInTheDocument();
  });

  it("requests and includes the next UTC date after local time crosses UTC midnight", async () => {
    jest.setSystemTime(new Date("2026-08-24T18:34:00-07:00"));
    mockOrganizationUsage = {
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [{ date: "2026-08-25T00:15:00Z", used: 2 }],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(useOrganizationWorkerUsage).toHaveBeenCalledWith({
      startDate: "2026-08-17",
      endDate: "2026-08-25",
    });
    expect(lastChartProps().data).toEqual(expect.arrayContaining([{ date: "2026-08-25T00:00:00.000Z", used: 2 }]));
    expect(lastChartProps().data.at(-1)).toEqual({ date: "2026-08-25T01:00:00.000Z", used: 0 });
  });

  it("omits the contracted capacity line when committed capacity is zero", async () => {
    mockOrganizationUsage = {
      committedDataWorkers: 0,
      regions: [
        {
          id: "region-1",
          name: "Region 1",
          workspaces: [
            {
              id: "workspace-current",
              name: "Current workspace",
              dataWorkers: [{ date: "2026-08-17T16:00:00Z", used: 1 }],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(lastChartProps().referenceLine).toBeUndefined();
  });

  it("keeps both occurrences of a repeated fall-back hour as distinct buckets", async () => {
    jest.setSystemTime(new Date("2026-11-01T02:30:00-08:00"));
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
                { date: "2026-11-01T08:15:00Z", used: 1 },
                { date: "2026-11-01T09:15:00Z", used: 2 },
              ],
            },
          ],
        },
      ],
    };

    await render(<WorkspaceDataWorkerUsageGraph />);

    expect(lastChartProps().data).toEqual(
      expect.arrayContaining([
        { date: "2026-11-01T08:00:00.000Z", used: 1 },
        { date: "2026-11-01T09:00:00.000Z", used: 2 },
      ])
    );
    expect(lastChartProps().data.at(-1)).toEqual({ date: "2026-11-01T10:00:00.000Z", used: 0 });
  });
});
