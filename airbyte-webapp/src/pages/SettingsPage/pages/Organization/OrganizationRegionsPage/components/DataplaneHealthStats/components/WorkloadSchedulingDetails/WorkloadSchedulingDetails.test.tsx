import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { WorkloadSchedulingDetailsData } from "./types";
import { WorkloadSchedulingDetails } from "./WorkloadSchedulingDetails";
import { formatAge, formatDuration } from "../../utils";

const MINUTE = 60_000;

const details = (overrides: Partial<WorkloadSchedulingDetailsData> = {}): WorkloadSchedulingDetailsData => ({
  blocked: { kind: "none" },
  lastScheduled: { workloadType: "SYNC", age: formatAge(2 * MINUTE) },
  ...overrides,
});

describe("WorkloadSchedulingDetails", () => {
  describe("when nothing is blocked", () => {
    beforeEach(async () => {
      await render(<WorkloadSchedulingDetails data={details()} />);
    });

    it("has a title", () => {
      expect(screen.getByText("Workload scheduling details")).toBeInTheDocument();
    });

    it("labels the three columns", () => {
      expect(
        ["Blocked workloads", "Last pod scheduled", "Last scheduling problem"].map((label) => screen.getByText(label))
      ).toHaveLength(3);
    });

    it("says nothing is blocked", () => {
      expect(screen.getByText("None")).toBeInTheDocument();
    });

    it("says when the last pod was scheduled and what kind of workload it was", () => {
      expect(screen.getByText("2m 0s ago · Sync")).toBeInTheDocument();
    });

    it("says no problem was recorded", () => {
      expect(screen.getByText("None recorded")).toBeInTheDocument();
    });

    it("shows no error", () => {
      expect(screen.queryByText(/ACCESS_DENIED/)).not.toBeInTheDocument();
    });
  });

  describe("when a workload is blocked", () => {
    beforeEach(async () => {
      await render(
        <WorkloadSchedulingDetails
          data={details({
            blocked: {
              kind: "blocked",
              count: 3,
              longest: { workloadType: "SYNC", waiting: formatDuration(7 * MINUTE) },
            },
            lastScheduled: { workloadType: "CHECK", age: formatAge(MINUTE) },
            lastProblem: { message: "0/3 nodes are available: 3 Insufficient memory.", age: formatAge(5_000) },
          })}
        />
      );
    });

    it("names the longest blocked workload and how long it has waited", () => {
      expect(screen.getByText("Sync · waiting 7m 0s")).toBeInTheDocument();
    });

    it("says when the last pod was scheduled", () => {
      expect(screen.getByText("1m 0s ago · Check")).toBeInTheDocument();
    });

    it("says when the last problem was observed and why", () => {
      expect(screen.getByText("5 seconds ago · 0/3 nodes are available: 3 Insufficient memory.")).toBeInTheDocument();
    });
  });

  describe("when the scan could not complete", () => {
    beforeEach(async () => {
      await render(
        <WorkloadSchedulingDetails
          data={details({
            blocked: { kind: "notMeasured" },
            lastProblem: { message: "0/3 nodes are available: 3 Insufficient memory.", age: formatAge(7 * MINUTE) },
            error: {
              code: "ACCESS_DENIED",
              message: "The launcher is not allowed to list pods in its jobs namespace.",
            },
          })}
        />
      );
    });

    it("says the blocked workloads were not measured", () => {
      expect(screen.getByText("Not measured")).toBeInTheDocument();
    });

    it("keeps the earlier events", () => {
      expect(screen.getByText("7m 0s ago · 0/3 nodes are available: 3 Insufficient memory.")).toBeInTheDocument();
    });

    it("shows the error below the details", () => {
      expect(
        screen.getByText("ACCESS_DENIED · The launcher is not allowed to list pods in its jobs namespace.")
      ).toBeInTheDocument();
    });
  });

  describe("when no pod has been scheduled yet", () => {
    it("shows a dash", async () => {
      await render(<WorkloadSchedulingDetails data={details({ lastScheduled: undefined })} />);

      expect(screen.getByText("-")).toBeInTheDocument();
    });
  });

  describe("with a workload type this page does not know", () => {
    it("shows the type as the data plane reports it", async () => {
      await render(
        <WorkloadSchedulingDetails
          data={details({ lastScheduled: { workloadType: "REPLICATE", age: formatAge(MINUTE) } })}
        />
      );

      expect(screen.getByText("1m 0s ago · REPLICATE")).toBeInTheDocument();
    });
  });
});
