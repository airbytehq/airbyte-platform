import { RegionDataWorkerUsage } from "core/api/types/AirbyteClient";

import { calculateGraphData } from "./calculateGraphData";

describe(`${calculateGraphData.name}`, () => {
  describe("date range generation", () => {
    it("generates data points for a single day range", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const result = calculateGraphData(dateRange, undefined);

      expect(result).toHaveLength(1);
      expect(result[0].formattedDate).toBe("2025-01-15");
      expect(result[0].workspaceUsage).toEqual({});
    });

    it("generates data points for a multi-day range", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-17"];
      const result = calculateGraphData(dateRange, undefined);

      expect(result).toHaveLength(3);
      expect(result[0].formattedDate).toBe("2025-01-15");
      expect(result[1].formattedDate).toBe("2025-01-16");
      expect(result[2].formattedDate).toBe("2025-01-17");
    });

    it("generates data points for a week range", () => {
      const dateRange: [string, string] = ["2025-01-01", "2025-01-07"];
      const result = calculateGraphData(dateRange, undefined);

      expect(result).toHaveLength(7);
      expect(result[0].formattedDate).toBe("2025-01-01");
      expect(result[6].formattedDate).toBe("2025-01-07");
    });

    it("generates data points for a month range", () => {
      const dateRange: [string, string] = ["2025-01-01", "2025-01-31"];
      const result = calculateGraphData(dateRange, undefined);

      expect(result).toHaveLength(31);
      expect(result[0].formattedDate).toBe("2025-01-01");
      expect(result[30].formattedDate).toBe("2025-01-31");
    });
  });

  describe("workspace usage aggregation", () => {
    it("handles undefined region data by returning empty workspace usage", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-17"];
      const result = calculateGraphData(dateRange, undefined);

      expect(result).toHaveLength(3);
      result.forEach((day) => {
        expect(day.workspaceUsage).toEqual({});
      });
    });

    it("populates workspace usage from region data", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-17"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2025-01-15T10:00:00Z", used: 5 },
              { date: "2025-01-16T10:00:00Z", used: 10 },
            ],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      expect(result[0].workspaceUsage["workspace-1"]).toBe(5);
      expect(result[1].workspaceUsage["workspace-1"]).toBe(10);
      expect(result[2].workspaceUsage["workspace-1"]).toBeUndefined();
    });

    it("handles multiple workspaces at the same hour", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [{ date: "2025-01-15T10:00:00Z", used: 5 }],
          },
          {
            id: "workspace-2",
            name: "Workspace 2",
            dataWorkers: [{ date: "2025-01-15T10:00:00Z", used: 8 }],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      expect(result[0].workspaceUsage["workspace-1"]).toBe(5);
      expect(result[0].workspaceUsage["workspace-2"]).toBe(8);
    });

    it("uses peak hour values when multiple hours exist for the same day", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2025-01-15T10:00:00Z", used: 5 },
              { date: "2025-01-15T11:00:00Z", used: 10 },
              { date: "2025-01-15T12:00:00Z", used: 7 },
            ],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      // Peak hour is 11:00 with total 10, so workspace-1 gets its value at 11:00
      expect(result[0].workspaceUsage["workspace-1"]).toBe(10);
    });

    it("handles empty workspaces array", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [],
      };

      const result = calculateGraphData(dateRange, regionData);

      expect(result).toHaveLength(1);
      expect(result[0].workspaceUsage).toEqual({});
    });

    it("handles workspace with empty dataWorkers array", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      expect(result).toHaveLength(1);
      expect(result[0].workspaceUsage).toEqual({});
    });
  });

  describe("edge cases", () => {
    it("handles usage value of 0", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [{ date: "2025-01-15T10:00:00Z", used: 0 }],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      expect(result[0].workspaceUsage["workspace-1"]).toBe(0);
    });

    it("uses peak hour value based on total across workspaces", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2025-01-15T10:00:00Z", used: 10 },
              { date: "2025-01-15T11:00:00Z", used: 5 },
            ],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      // Peak hour is 10:00 with total 10, so workspace-1 gets its value at 10:00
      expect(result[0].workspaceUsage["workspace-1"]).toBe(10);
    });
  });

  describe("top 10 and other workspace filtering", () => {
    it("uses peak hour values and aggregates 'other' workspaces", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-11",
            name: "Workspace 11",
            dataWorkers: [
              { date: "2025-01-15T10:00:00Z", used: 5 },
              { date: "2025-01-15T11:00:00Z", used: 10 },
              { date: "2025-01-15T12:00:00Z", used: 3 },
            ],
          },
          {
            id: "workspace-12",
            name: "Workspace 12",
            dataWorkers: [
              { date: "2025-01-15T10:00:00Z", used: 7 },
              { date: "2025-01-15T11:00:00Z", used: 8 },
            ],
          },
        ],
      };

      const result = calculateGraphData(
        dateRange,
        regionData,
        [], // no top 10
        ["workspace-11", "workspace-12"] // both in other
      );

      // Peak hour is 11:00 with total 10 + 8 = 18
      // Workspace 11 at 11:00: 10
      // Workspace 12 at 11:00: 8
      // Other total: 10 + 8 = 18
      expect(result[0].workspaceUsage.other).toBe(18);
    });

    it("uses peak hour value for individual workspace in top 10", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2025-01-15T10:00:00Z", used: 10 },
              { date: "2025-01-15T11:00:00Z", used: 15 },
              { date: "2025-01-15T12:00:00Z", used: 12 },
            ],
          },
        ],
      };

      const result = calculateGraphData(
        dateRange,
        regionData,
        ["workspace-1"], // in top 10
        [] // none in other
      );

      // Peak hour is 11:00 with total 15, so workspace-1 gets its value at 11:00
      expect(result[0].workspaceUsage["workspace-1"]).toBe(15);
    });
  });

  describe("peak hour calculation across workspaces", () => {
    it("finds peak hour based on total usage across all workspaces", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2025-01-15T10:00:00Z", used: 10 }, // hour 10 total: 10 + 2 = 12
              { date: "2025-01-15T11:00:00Z", used: 5 }, // hour 11 total: 5 + 20 = 25 (peak)
            ],
          },
          {
            id: "workspace-2",
            name: "Workspace 2",
            dataWorkers: [
              { date: "2025-01-15T10:00:00Z", used: 2 },
              { date: "2025-01-15T11:00:00Z", used: 20 },
            ],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      // Peak hour is 11:00 (total 25), so each workspace gets its 11:00 value
      expect(result[0].workspaceUsage["workspace-1"]).toBe(5);
      expect(result[0].workspaceUsage["workspace-2"]).toBe(20);
    });

    it("returns 0 for workspace that has no data at peak hour", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2025-01-15T10:00:00Z", used: 5 },
              { date: "2025-01-15T11:00:00Z", used: 15 }, // peak hour
            ],
          },
          {
            id: "workspace-2",
            name: "Workspace 2",
            dataWorkers: [
              { date: "2025-01-15T10:00:00Z", used: 3 }, // only has data at 10:00
            ],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      // Peak hour is 11:00 (total 15 > 8)
      expect(result[0].workspaceUsage["workspace-1"]).toBe(15);
      expect(result[0].workspaceUsage["workspace-2"]).toBe(0); // no data at peak hour
    });
  });
});
