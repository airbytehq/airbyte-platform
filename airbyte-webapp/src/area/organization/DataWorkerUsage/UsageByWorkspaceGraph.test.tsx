import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { useOrganizationWorkerUsage } from "core/api";

import { GraphTooltip } from "./GraphTooltip";
import { UsageByWorkspaceGraph } from "./UsageByWorkspaceGraph";

interface MockDataWorkerUsageBarChartProps {
  data: Array<Record<string, unknown>>;
  xAxisDataKey: string;
  xAxisTickFormatter?: (value: unknown, index: number) => string;
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
            dataWorkers: [{ date: "2026-06-01T12:00:00Z", used: 1 }],
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
      return React.createElement(
        React.Fragment,
        null,
        props.data.map((item, index) =>
          React.createElement(
            "span",
            { key: String(item[props.xAxisDataKey]), "data-testid": "x-axis-tick" },
            props.xAxisTickFormatter?.(item[props.xAxisDataKey], index)
          )
        )
      );
    },
  };
});

const lastChartProps = (): MockDataWorkerUsageBarChartProps =>
  mockDataWorkerUsageBarChart.mock.calls[mockDataWorkerUsageBarChart.mock.calls.length - 1][0];

describe(`${UsageByWorkspaceGraph.name}`, () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renders date-only x-axis categories as local calendar dates", async () => {
    expect(process.env.TZ).toBe("US/Pacific");

    await render(
      <UsageByWorkspaceGraph
        selectedRegionId="region-1"
        dateRange={["2026-06-01", "2026-06-03"]}
        committedDataWorkers={4}
      />
    );

    expect(useOrganizationWorkerUsage).toHaveBeenCalledWith({
      startDate: "2026-06-01",
      endDate: "2026-06-03",
    });
    expect(screen.getAllByTestId("x-axis-tick").map((tick) => tick.textContent)).toEqual(["Jun 1", "Jun 2", "Jun 3"]);
    expect(screen.queryByText("May 31")).not.toBeInTheDocument();

    const chartProps = lastChartProps();
    expect(chartProps).toEqual(
      expect.objectContaining({
        xAxisDataKey: "formattedDate",
        barDataKey: "maxWorkspaceUsage",
        chartKey: "region-1",
        barSize: 16,
        referenceLine: { value: 4, label: "Contracted capacity" },
      })
    );

    const tooltip = (chartProps.renderTooltipContent as (barColor: string) => React.ReactElement)("#605cff");
    expect(tooltip.type).toBe(GraphTooltip);
    expect(tooltip.props).toEqual(
      expect.objectContaining({
        regionName: "Region 1",
        hasOtherCategory: false,
        barColor: "#605cff",
      })
    );
  });

  it.each([null, 0])("omits the capacity line when committed capacity is %s", async (committedDataWorkers) => {
    await render(
      <UsageByWorkspaceGraph
        selectedRegionId="region-1"
        dateRange={["2026-06-01", "2026-06-03"]}
        committedDataWorkers={committedDataWorkers}
      />
    );

    expect(lastChartProps().referenceLine).toBeUndefined();
  });
});
