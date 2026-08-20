import type { ReactNode } from "react";

import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { useOrganizationWorkerUsage } from "core/api";

import { UsageByWorkspaceGraph } from "./UsageByWorkspaceGraph";

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

jest.mock("core/utils/useAirbyteTheme", () => ({
  useAirbyteTheme: () => ({ colorValues: mockColorValues }),
}));

const mockColorValues = {};

jest.mock("recharts", () => {
  const React = jest.requireActual<typeof import("react")>("react");
  const ChartDataContext = React.createContext<Array<Record<string, unknown>>>([]);

  return {
    Bar: () => null,
    BarChart: ({ data, children }: { data: Array<Record<string, unknown>>; children: ReactNode }) =>
      React.createElement(ChartDataContext.Provider, { value: data }, children),
    CartesianGrid: () => null,
    ReferenceLine: () => null,
    ResponsiveContainer: ({ children }: { children: ReactNode }) => React.createElement(React.Fragment, null, children),
    Tooltip: () => null,
    XAxis: ({ dataKey, tickFormatter }: { dataKey: string; tickFormatter: (value: unknown) => string }) => {
      const data = React.useContext(ChartDataContext);

      return React.createElement(
        React.Fragment,
        null,
        data.map((item) =>
          React.createElement(
            "span",
            { key: String(item[dataKey]), "data-testid": "x-axis-tick" },
            tickFormatter(item[dataKey])
          )
        )
      );
    },
    YAxis: () => null,
  };
});

describe(`${UsageByWorkspaceGraph.name}`, () => {
  it("renders date-only x-axis categories as local calendar dates", async () => {
    expect(process.env.TZ).toBe("US/Pacific");

    await render(
      <UsageByWorkspaceGraph
        selectedRegionId="region-1"
        dateRange={["2026-06-01", "2026-06-03"]}
        committedDataWorkers={null}
      />
    );

    expect(useOrganizationWorkerUsage).toHaveBeenCalledWith({
      startDate: "2026-06-01",
      endDate: "2026-06-03",
    });
    expect(screen.getAllByTestId("x-axis-tick").map((tick) => tick.textContent)).toEqual(["Jun 1", "Jun 2", "Jun 3"]);
    expect(screen.queryByText("May 31")).not.toBeInTheDocument();
  });
});
