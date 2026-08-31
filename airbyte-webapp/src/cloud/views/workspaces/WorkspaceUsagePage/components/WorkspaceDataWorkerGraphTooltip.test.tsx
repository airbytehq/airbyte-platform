import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { WorkspaceDataWorkerGraphTooltip } from "./WorkspaceDataWorkerGraphTooltip";

const workspaceName = "Analytics workspace";
const hourlyPayload = [{ value: 1.456, payload: { date: "2026-06-01T19:00:00Z", used: 1.456 } }];
const comparisonPayload = [
  {
    value: 1.235,
    payload: {
      date: "2026-06-01T19:00:00Z",
      used: 1.235,
      currentDate: "2026-06-01T19:00:00Z",
      previousDate: "2026-05-25T19:00:00Z",
      currentUsage: 1.235,
      previousUsage: 0,
    },
  },
];

const comparison = (selectedTimeRange: "1d" | "1w" | "1m" | "1q" | "1y") => ({
  barColor: "#605cff",
  comparisonBarColor: "#00aabb",
  selectedTimeRange,
});

describe(`${WorkspaceDataWorkerGraphTooltip.name}`, () => {
  it("renders nothing when inactive or missing a payload", async () => {
    await render(
      <WorkspaceDataWorkerGraphTooltip
        active={false}
        payload={hourlyPayload}
        workspaceName={workspaceName}
        granularity="hour"
      />
    );

    expect(screen.queryByText(workspaceName)).not.toBeInTheDocument();

    await render(
      <WorkspaceDataWorkerGraphTooltip active payload={[]} workspaceName={workspaceName} granularity="hour" />
    );

    expect(screen.queryByText(workspaceName)).not.toBeInTheDocument();
  });

  it("renders only the exact hourly timestamp, workspace name, and rounded usage", async () => {
    expect(process.env.TZ).toBe("US/Pacific");

    await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={hourlyPayload}
        workspaceName={workspaceName}
        granularity="hour"
      />
    );

    expect(screen.getByText("Mon, Jun 1, 12:00 PM PDT")).toBeInTheDocument();
    expect(screen.getByText(workspaceName)).toBeInTheDocument();
    expect(screen.getByText("1.46")).toBeInTheDocument();
    expect(screen.queryByText("Data workers")).not.toBeInTheDocument();
    expect(screen.queryByText("Region max")).not.toBeInTheDocument();
    expect(screen.queryByText("Per-workspace max")).not.toBeInTheDocument();
  });

  it("omits the time for a daily peak", async () => {
    await render(
      <WorkspaceDataWorkerGraphTooltip active payload={hourlyPayload} workspaceName={workspaceName} granularity="day" />
    );

    expect(screen.getByText("Mon, Jun 1")).toBeInTheDocument();
    expect(screen.queryByText("Mon, Jun 1, 12:00 PM PDT")).not.toBeInTheDocument();
  });

  it("shows only the weekly bucket start date for a weekly peak", async () => {
    await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={[{ value: 1.46, payload: { date: "2026-08-02T07:00:00Z", used: 1.46 } }]}
        workspaceName={workspaceName}
        granularity="week"
      />
    );

    expect(screen.getByText("Aug 2, 2026")).toBeInTheDocument();
    expect(screen.queryByText(/Aug 9/)).not.toBeInTheDocument();
  });

  it("renders zero usage", async () => {
    await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={[{ value: 0, payload: { date: "2026-06-01T19:00:00Z", used: 0 } }]}
        workspaceName={workspaceName}
        granularity="hour"
      />
    );

    expect(screen.getByText("0.00")).toBeInTheDocument();
  });

  it.each([
    ["1d", "Comparing vs previous day"],
    ["1w", "Comparing vs previous week"],
    ["1m", "Comparing vs previous month"],
    ["1q", "Comparing vs previous quarter"],
    ["1y", "Comparing vs previous year"],
  ] as const)("renders the %s comparison heading", async (selectedTimeRange, heading) => {
    await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={comparisonPayload}
        granularity="hour"
        comparison={comparison(selectedTimeRange)}
      />
    );

    expect(screen.getByText(heading)).toBeInTheDocument();
  });

  it("renders hourly comparison dates, two-decimal values, and themed swatches without organization copy", async () => {
    const { container } = await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={comparisonPayload}
        granularity="hour"
        comparison={comparison("1w")}
      />
    );

    expect(screen.getByText("Current")).toBeInTheDocument();
    expect(screen.getByText("Previous")).toBeInTheDocument();
    expect(screen.getByText("Mon, Jun 1, 12:00 PM PDT")).toBeInTheDocument();
    expect(screen.getByText("Mon, May 25, 12:00 PM PDT")).toBeInTheDocument();
    expect(screen.getByText("1.24 DW")).toBeInTheDocument();
    expect(screen.getByText("0.00 DW")).toBeInTheDocument();
    expect(screen.queryByText(workspaceName)).not.toBeInTheDocument();
    expect(screen.queryByText("Region max")).not.toBeInTheDocument();
    expect(screen.queryByText("Per-workspace max")).not.toBeInTheDocument();

    const swatches = container.querySelectorAll('span[aria-hidden="true"]');
    expect(swatches).toHaveLength(2);
    expect(swatches[0]).toHaveStyle({ color: "#605cff" });
    expect(swatches[1]).toHaveStyle({ color: "#00aabb" });
  });

  it("renders N/A for either missing comparison metric while preserving the present side", async () => {
    const { rerender } = await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={[
          {
            ...comparisonPayload[0],
            payload: {
              ...comparisonPayload[0].payload,
              previousDate: undefined,
              previousUsage: null,
            },
          },
        ]}
        granularity="hour"
        comparison={comparison("1w")}
      />
    );

    expect(screen.getByText("N/A")).toBeInTheDocument();
    expect(screen.getByText("Mon, Jun 1, 12:00 PM PDT")).toBeInTheDocument();
    expect(screen.getByText("1.24 DW")).toBeInTheDocument();

    rerender(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={[
          {
            ...comparisonPayload[0],
            payload: {
              ...comparisonPayload[0].payload,
              currentDate: undefined,
              currentUsage: null,
            },
          },
        ]}
        granularity="hour"
        comparison={comparison("1w")}
      />
    );

    expect(screen.getByText("N/A")).toBeInTheDocument();
    expect(screen.getByText("Mon, May 25, 12:00 PM PDT")).toBeInTheDocument();
    expect(screen.getByText("0.00 DW")).toBeInTheDocument();
  });

  it("distinguishes repeated fall-back hours with time-zone names", async () => {
    expect(process.env.TZ).toBe("US/Pacific");

    await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={[
          {
            ...comparisonPayload[0],
            payload: {
              ...comparisonPayload[0].payload,
              currentDate: "2026-11-01T08:00:00.000Z",
              previousDate: "2026-11-01T09:00:00.000Z",
            },
          },
        ]}
        granularity="hour"
        comparison={comparison("1d")}
      />
    );

    expect(screen.getByText("Sun, Nov 1, 1:00 AM PDT")).toBeInTheDocument();
    expect(screen.getByText("Sun, Nov 1, 1:00 AM PST")).toBeInTheDocument();
  });

  it("formats daily comparison dates without times", async () => {
    await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={comparisonPayload}
        granularity="day"
        comparison={comparison("1m")}
      />
    );

    expect(screen.getByText("Mon, Jun 1")).toBeInTheDocument();
    expect(screen.getByText("Mon, May 25")).toBeInTheDocument();
    expect(screen.queryByText(/12:00 PM/)).not.toBeInTheDocument();
  });

  it("formats weekly comparison dates as bucket starts", async () => {
    await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={comparisonPayload}
        granularity="week"
        comparison={comparison("1y")}
      />
    );

    expect(screen.getByText("Jun 1, 2026")).toBeInTheDocument();
    expect(screen.getByText("May 25, 2026")).toBeInTheDocument();
  });

  it("renders nothing for comparison payloads without paired values", async () => {
    await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={hourlyPayload}
        granularity="hour"
        comparison={comparison("1d")}
      />
    );

    expect(screen.queryByText("Comparing vs previous day")).not.toBeInTheDocument();
  });
});
