import { RegionDataWorkerUsage } from "core/api/types/AirbyteClient";

import { calculateGraphData } from "./calculateGraphData";

const regionUsage = (workspaces: RegionDataWorkerUsage["workspaces"]): RegionDataWorkerUsage => ({
  id: "region-1",
  name: "Region 1",
  workspaces,
});

describe(`${calculateGraphData.name}`, () => {
  describe("bucket generation", () => {
    it("generates exactly 24 end-exclusive hourly buckets for 1D", () => {
      const result = calculateGraphData(["2026-08-23T20:00:00.000Z", "2026-08-24T20:00:00.000Z"], "hour", undefined);

      expect(result).toHaveLength(24);
      expect(result[0]).toEqual({
        formattedDate: "2026-08-23T20:00:00.000Z",
        regionUsage: 0,
        maxWorkspaceUsage: 0,
        workspaceUsage: {},
      });
      expect(result.at(-1)?.formattedDate).toBe("2026-08-24T19:00:00.000Z");
    });

    it("generates exactly 168 end-exclusive hourly buckets for 1W", () => {
      const result = calculateGraphData(["2026-08-17T20:00:00.000Z", "2026-08-24T20:00:00.000Z"], "hour", undefined);

      expect(result).toHaveLength(168);
      expect(result.at(-1)?.formattedDate).toBe("2026-08-24T19:00:00.000Z");
    });

    it("generates exactly 30 local-calendar daily buckets for 1M across a DST change", () => {
      expect(process.env.TZ).toBe("US/Pacific");

      const result = calculateGraphData(["2026-10-15T07:00:00.000Z", "2026-11-14T08:00:00.000Z"], "day", undefined);

      expect(result).toHaveLength(30);
      expect(result[0].formattedDate).toBe("2026-10-15T07:00:00.000Z");
      expect(result.at(-1)?.formattedDate).toBe("2026-11-13T08:00:00.000Z");
    });
  });

  describe("hourly aggregation", () => {
    it("zero-fills the range when the region has no workspaces", () => {
      const result = calculateGraphData(
        ["2026-08-24T18:00:00.000Z", "2026-08-24T20:00:00.000Z"],
        "hour",
        regionUsage([])
      );

      expect(result).toHaveLength(2);
      expect(result.every(({ regionUsage, maxWorkspaceUsage }) => regionUsage === 0 && maxWorkspaceUsage === 0)).toBe(
        true
      );
      expect(result.every(({ workspaceUsage }) => Object.keys(workspaceUsage).length === 0)).toBe(true);
    });

    it("clips over-fetched points, normalizes points within an hour, and keeps regional concurrency", () => {
      const result = calculateGraphData(
        ["2026-08-24T18:00:00.000Z", "2026-08-24T20:00:00.000Z"],
        "hour",
        regionUsage([
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2026-08-24T17:59:00.000Z", used: 100 },
              { date: "2026-08-24T18:10:00.000Z", used: 1 },
              { date: "2026-08-24T18:45:00.000Z", used: 2 },
              { date: "2026-08-24T19:10:00.000Z", used: 4 },
              { date: "2026-08-24T20:00:00.000Z", used: 100 },
            ],
          },
          {
            id: "workspace-2",
            name: "Workspace 2",
            dataWorkers: [{ date: "2026-08-24T18:30:00.000Z", used: 3 }],
          },
        ])
      );

      expect(result).toEqual([
        {
          formattedDate: "2026-08-24T18:00:00.000Z",
          regionUsage: 5,
          maxWorkspaceUsage: 3,
          workspaceUsage: { "workspace-1": 2, "workspace-2": 3 },
        },
        {
          formattedDate: "2026-08-24T19:00:00.000Z",
          regionUsage: 4,
          maxWorkspaceUsage: 4,
          workspaceUsage: { "workspace-1": 4, "workspace-2": 0 },
        },
      ]);
    });

    it("preserves the repeated fall-back hour as two distinct absolute buckets", () => {
      expect(process.env.TZ).toBe("US/Pacific");

      const result = calculateGraphData(
        ["2026-11-01T08:00:00.000Z", "2026-11-01T11:00:00.000Z"],
        "hour",
        regionUsage([
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2026-11-01T08:30:00.000Z", used: 1 },
              { date: "2026-11-01T09:30:00.000Z", used: 2 },
            ],
          },
        ])
      );

      expect(result.map(({ formattedDate }) => formattedDate)).toEqual([
        "2026-11-01T08:00:00.000Z",
        "2026-11-01T09:00:00.000Z",
        "2026-11-01T10:00:00.000Z",
      ]);
      expect(result.map(({ regionUsage }) => regionUsage)).toEqual([1, 2, 0]);
    });

    it("retains explicit zero usage", () => {
      const result = calculateGraphData(
        ["2026-08-24T18:00:00.000Z", "2026-08-24T19:00:00.000Z"],
        "hour",
        regionUsage([
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [{ date: "2026-08-24T18:00:00.000Z", used: 0 }],
          },
        ])
      );

      expect(result[0]).toEqual(
        expect.objectContaining({ regionUsage: 0, maxWorkspaceUsage: 0, workspaceUsage: { "workspace-1": 0 } })
      );
    });
  });

  describe("daily peak aggregation", () => {
    it("uses each workspace value from the region's peak hour", () => {
      const result = calculateGraphData(
        ["2025-01-15T08:00:00.000Z", "2025-01-16T08:00:00.000Z"],
        "day",
        regionUsage([
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2025-01-15T18:00:00.000Z", used: 10 },
              { date: "2025-01-15T19:00:00.000Z", used: 5 },
            ],
          },
          {
            id: "workspace-2",
            name: "Workspace 2",
            dataWorkers: [
              { date: "2025-01-15T18:00:00.000Z", used: 2 },
              { date: "2025-01-15T19:00:00.000Z", used: 20 },
            ],
          },
        ])
      );

      expect(result[0]).toEqual({
        formattedDate: "2025-01-15T08:00:00.000Z",
        regionUsage: 25,
        maxWorkspaceUsage: 20,
        workspaceUsage: { "workspace-1": 5, "workspace-2": 20 },
      });
    });

    it("aggregates non-top-ten workspaces into Other at the peak hour", () => {
      const result = calculateGraphData(
        ["2025-01-15T08:00:00.000Z", "2025-01-16T08:00:00.000Z"],
        "day",
        regionUsage([
          {
            id: "workspace-top",
            name: "Top",
            dataWorkers: [{ date: "2025-01-15T18:00:00.000Z", used: 5 }],
          },
          {
            id: "workspace-other-1",
            name: "Other 1",
            dataWorkers: [{ date: "2025-01-15T18:00:00.000Z", used: 2 }],
          },
          {
            id: "workspace-other-2",
            name: "Other 2",
            dataWorkers: [{ date: "2025-01-15T18:00:00.000Z", used: 3 }],
          },
        ]),
        ["workspace-top"],
        ["workspace-other-1", "workspace-other-2"]
      );

      expect(result[0]).toEqual(
        expect.objectContaining({
          regionUsage: 10,
          maxWorkspaceUsage: 5,
          workspaceUsage: { "workspace-top": 5, other: 5 },
        })
      );
    });

    it("distinguishes both fall-back hours when selecting a daily peak", () => {
      const result = calculateGraphData(
        ["2026-11-01T07:00:00.000Z", "2026-11-02T08:00:00.000Z"],
        "day",
        regionUsage([
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2026-11-01T08:30:00.000Z", used: 1 },
              { date: "2026-11-01T09:30:00.000Z", used: 4 },
            ],
          },
        ])
      );

      expect(result[0]).toEqual(
        expect.objectContaining({ regionUsage: 4, maxWorkspaceUsage: 4, workspaceUsage: { "workspace-1": 4 } })
      );
    });
  });
});
