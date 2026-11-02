import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { DataplaneHealthHistoryBucket } from "core/api/types/AirbyteClient";

import { HealthHistoryBar, HISTORY_PERIOD_MS } from "./HealthHistoryBar";

const WINDOW_START = Date.UTC(2026, 9, 27, 12, 0, 0);

const periods = (statuses: Array<DataplaneHealthHistoryBucket["status"]>): DataplaneHealthHistoryBucket[] =>
  statuses.map((status, index) => ({ bucket_start: WINDOW_START + index * HISTORY_PERIOD_MS, status }));

describe("HealthHistoryBar", () => {
  it("renders one gray segment when there is no history", async () => {
    await render(<HealthHistoryBar history={[]} />);

    expect(screen.getByTestId("health-history-empty")).toBeInTheDocument();
    expect(screen.queryAllByTestId("health-history-segment")).toHaveLength(0);
  });

  it("renders one gray segment when the history field is missing", async () => {
    await render(<HealthHistoryBar />);

    expect(screen.getByTestId("health-history-empty")).toBeInTheDocument();
  });

  it("renders one segment per period, oldest first, with UNKNOWN periods gray", async () => {
    await render(<HealthHistoryBar history={periods(["UNKNOWN", "UNKNOWN", "HEALTHY", "DEGRADED", "UNHEALTHY"])} />);

    const segments = screen.getAllByTestId("health-history-segment");
    expect(segments.map((segment) => segment.dataset.status)).toEqual([
      "UNKNOWN",
      "UNKNOWN",
      "HEALTHY",
      "DEGRADED",
      "UNHEALTHY",
    ]);
    expect(segments[0]).toHaveClass("unknown");
    expect(segments[2]).toHaveClass("healthy");
    expect(segments[3]).toHaveClass("degraded");
    expect(segments[4]).toHaveClass("unhealthy");
  });

  it("labels a closed period with its time range and the current period with since", async () => {
    const currentPeriodStart = Math.floor(Date.now() / HISTORY_PERIOD_MS) * HISTORY_PERIOD_MS;
    const history: DataplaneHealthHistoryBucket[] = [
      { bucket_start: currentPeriodStart - HISTORY_PERIOD_MS, status: "HEALTHY" },
      { bucket_start: currentPeriodStart, status: "DEGRADED" },
    ];
    await render(<HealthHistoryBar history={history} />);
    const [closed, current] = screen.getAllByTestId("health-history-segment");

    await userEvent.hover(closed);
    await waitFor(() =>
      expect(screen.getByRole("tooltip")).toHaveTextContent(/^\d+:\d+ [AP]M.*\d+:\d+ [AP]M: Healthy$/)
    );

    await userEvent.unhover(closed);
    await userEvent.hover(current);
    await waitFor(() => expect(screen.getByRole("tooltip")).toHaveTextContent(/^Since \d+:\d+ [AP]M: Degraded$/));
  });

  it("summarizes the period counts for assistive technology", async () => {
    await render(<HealthHistoryBar history={periods(["UNKNOWN", "HEALTHY", "HEALTHY", "UNHEALTHY"])} />);

    expect(screen.getByRole("img")).toHaveAccessibleName(
      "12-hour health history: 2 healthy, 1 unhealthy, 1 without check-ins"
    );
  });

  it("renders all 48 periods of the 12-hour window", async () => {
    await render(<HealthHistoryBar history={periods(Array(48).fill("HEALTHY"))} />);

    expect(screen.getAllByTestId("health-history-segment")).toHaveLength(48);
  });
});
