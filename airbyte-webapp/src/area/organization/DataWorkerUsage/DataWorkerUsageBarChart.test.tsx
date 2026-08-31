import type { ReactNode } from "react";

import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { DataWorkerUsageBarChart } from "./DataWorkerUsageBarChart";

const mockBar = jest.fn();
const mockBarChart = jest.fn();
const mockCartesianGrid = jest.fn();
const mockReferenceLine = jest.fn();
const mockResponsiveContainer = jest.fn();
const mockTooltip = jest.fn();
const mockXAxis = jest.fn();
const mockYAxis = jest.fn();

const mockColorValues = {
  gridLine: "#grid",
  barColor: "#bar",
  comparisonBarColor: "#comparison",
  barHover: "#hover",
  committedLine: "#committed",
  tickColor: "#tick",
};

jest.mock("core/utils/useAirbyteTheme", () => ({
  useAirbyteTheme: () => ({ colorValues: mockColorValues }),
}));

jest.mock("recharts", () => {
  const React = jest.requireActual<typeof import("react")>("react");
  const Recharts = jest.requireActual<typeof import("recharts")>("recharts");

  class MockBar extends Recharts.Bar {
    override render(): JSX.Element {
      mockBar(this.props);
      return React.createElement(React.Fragment);
    }
  }

  return {
    Bar: MockBar,
    BarChart: ({ children, ...props }: { children: ReactNode }) => {
      mockBarChart(props);
      return React.createElement(React.Fragment, null, children);
    },
    CartesianGrid: (props: unknown) => {
      mockCartesianGrid(props);
      return null;
    },
    ReferenceLine: (props: unknown) => {
      mockReferenceLine(props);
      return null;
    },
    ResponsiveContainer: ({ children, ...props }: { children: ReactNode }) => {
      mockResponsiveContainer(props);
      return React.createElement(React.Fragment, null, children);
    },
    Tooltip: ({ content, ...props }: { content?: ReactNode }) => {
      mockTooltip({ content, ...props });
      return React.createElement(React.Fragment, null, content);
    },
    XAxis: (props: unknown) => {
      mockXAxis(props);
      return null;
    },
    YAxis: (props: unknown) => {
      mockYAxis(props);
      return null;
    },
  };
});

const lastProps = <Props,>(mockComponent: jest.Mock): Props =>
  mockComponent.mock.calls[mockComponent.mock.calls.length - 1][0];

const organizationData = [{ formattedDate: "2026-06-01", maxWorkspaceUsage: 2 }];

describe(`${DataWorkerUsageBarChart.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renders the organization chart configuration through the shared Recharts layer", async () => {
    const xAxisTickFormatter = jest.fn((value: string) => value);
    const yAxisTickFormatter = jest.fn((value: number) => `${value} DW`);
    const renderTooltipContent = jest.fn((barColor: string) => <span data-testid="tooltip-content">{barColor}</span>);

    await render(
      <DataWorkerUsageBarChart
        data={organizationData}
        xAxisDataKey="formattedDate"
        barDataKey="maxWorkspaceUsage"
        xAxisTickFormatter={xAxisTickFormatter}
        xAxisInterval={1}
        yAxisTickFormatter={yAxisTickFormatter}
        renderTooltipContent={renderTooltipContent}
        chartKey="region-1"
        chartMargin={{ top: 0, right: 0, bottom: 0, left: 0 }}
        tooltipPosition={{ y: 20 }}
        barSize={16}
        referenceLine={{ value: 12, label: "Contracted capacity" }}
      />
    );

    expect(lastProps(mockResponsiveContainer)).toEqual(expect.objectContaining({ width: "99%", height: 250 }));
    expect(lastProps(mockBarChart)).toEqual({
      data: organizationData,
      margin: { top: 0, right: 0, bottom: 0, left: 0 },
    });
    expect(lastProps(mockXAxis)).toEqual(
      expect.objectContaining({
        dataKey: "formattedDate",
        tickFormatter: xAxisTickFormatter,
        interval: 1,
        axisLine: false,
        tickLine: false,
        stroke: "#tick",
      })
    );
    expect(lastProps(mockYAxis)).toEqual(
      expect.objectContaining({ tickFormatter: yAxisTickFormatter, allowDecimals: false, stroke: "#tick" })
    );
    expect(renderTooltipContent).toHaveBeenLastCalledWith("#bar", "#comparison");
    expect(screen.getByTestId("tooltip-content")).toHaveTextContent("#bar");
    expect(lastProps(mockTooltip)).toEqual(
      expect.objectContaining({
        position: { y: 20 },
        cursor: { fill: "#hover" },
        allowEscapeViewBox: { x: false, y: true },
        isAnimationActive: false,
      })
    );
    expect(lastProps(mockCartesianGrid)).toEqual({ stroke: "#grid", vertical: false });
    expect(lastProps(mockReferenceLine)).toEqual(
      expect.objectContaining({
        y: 12,
        stroke: "#committed",
        ifOverflow: "extendDomain",
        label: expect.objectContaining({ value: "Contracted capacity", fill: "#committed" }),
      })
    );
    expect(lastProps(mockBar)).toEqual(
      expect.objectContaining({
        dataKey: "maxWorkspaceUsage",
        fill: "#bar",
        barSize: 16,
        animationDuration: 300,
        animationEasing: "linear",
      })
    );
    expect(new Set(mockBar.mock.calls.map(([props]) => props.dataKey))).toEqual(new Set(["maxWorkspaceUsage"]));
  });

  it("omits the optional capacity line", async () => {
    await render(
      <DataWorkerUsageBarChart
        data={organizationData}
        xAxisDataKey="formattedDate"
        barDataKey="maxWorkspaceUsage"
        renderTooltipContent={() => <span>Tooltip</span>}
      />
    );

    expect(mockReferenceLine).not.toHaveBeenCalled();
    expect(lastProps(mockBar)).toEqual(expect.objectContaining({ barSize: undefined }));
    expect(new Set(mockBar.mock.calls.map(([props]) => props.dataKey))).toEqual(new Set(["maxWorkspaceUsage"]));
  });

  it("renders the dense hourly workspace configuration without changing its data points", async () => {
    const hourlyData = [
      { date: "2026-08-17T16:00:00Z", used: 1 },
      { date: "2026-08-17T17:00:00Z", used: 0 },
      { date: "2026-08-18T16:00:00Z", used: 2 },
    ];
    const dailyTicks = ["2026-08-17T17:00:00Z", "2026-08-18T16:00:00Z"];

    await render(
      <DataWorkerUsageBarChart
        data={hourlyData}
        xAxisDataKey="date"
        barDataKey="used"
        xAxisTicks={dailyTicks}
        xAxisTickFormatter={(value) => String(value)}
        xAxisPadding={{ left: 20, right: 20 }}
        chartMargin={{ top: 0, right: 20, bottom: 0, left: 0 }}
        renderTooltipContent={() => <span data-testid="workspace-tooltip">Current workspace</span>}
      />
    );

    expect(lastProps(mockBarChart)).toEqual({
      data: hourlyData,
      margin: { top: 0, right: 20, bottom: 0, left: 0 },
    });
    expect(lastProps(mockXAxis)).toEqual(
      expect.objectContaining({
        dataKey: "date",
        ticks: dailyTicks,
        padding: { left: 20, right: 20 },
      })
    );
    expect(lastProps(mockBar)).toEqual(expect.objectContaining({ dataKey: "used", barSize: undefined }));
    expect(screen.getByTestId("workspace-tooltip")).toHaveTextContent("Current workspace");
    expect(mockReferenceLine).not.toHaveBeenCalled();
    expect(new Set(mockBar.mock.calls.map(([props]) => props.dataKey))).toEqual(new Set(["used"]));
  });

  it("renders unstacked Current and Previous bars with shared comparison behavior", async () => {
    const comparisonData = [
      { formattedDate: "2026-08-01", currentUsage: 2, previousUsage: 1 },
      { formattedDate: "2026-08-02", currentUsage: 3, previousUsage: 4 },
    ];
    const renderTooltipContent = jest.fn((barColor: string, comparisonBarColor: string) => (
      <span data-testid="comparison-tooltip">{`${barColor}:${comparisonBarColor}`}</span>
    ));

    await render(
      <DataWorkerUsageBarChart
        data={comparisonData}
        xAxisDataKey="formattedDate"
        barDataKey="currentUsage"
        comparisonBarDataKey="previousUsage"
        renderTooltipContent={renderTooltipContent}
        barSize={8}
        referenceLine={{ value: 12, label: "Contracted capacity" }}
      />
    );

    expect(new Set(mockBar.mock.calls.map(([props]) => props.dataKey))).toEqual(
      new Set(["currentUsage", "previousUsage"])
    );
    const currentBarProps = mockBar.mock.calls.filter(([props]) => props.dataKey === "currentUsage").at(-1)?.[0];
    const previousBarProps = mockBar.mock.calls.filter(([props]) => props.dataKey === "previousUsage").at(-1)?.[0];
    expect(currentBarProps).toEqual(
      expect.objectContaining({
        dataKey: "currentUsage",
        fill: "#bar",
        barSize: 8,
        animationDuration: 300,
        animationEasing: "linear",
      })
    );
    expect(previousBarProps).toEqual(
      expect.objectContaining({
        dataKey: "previousUsage",
        fill: "#comparison",
        barSize: 8,
        animationDuration: 300,
        animationEasing: "linear",
      })
    );
    expect(currentBarProps).not.toHaveProperty("stackId");
    expect(previousBarProps).not.toHaveProperty("stackId");
    expect(renderTooltipContent).toHaveBeenLastCalledWith("#bar", "#comparison");
    expect(screen.getByTestId("comparison-tooltip")).toHaveTextContent("#bar:#comparison");
    expect(lastProps(mockReferenceLine)).toEqual(
      expect.objectContaining({ y: 12, stroke: "#committed", ifOverflow: "extendDomain" })
    );
  });
});
