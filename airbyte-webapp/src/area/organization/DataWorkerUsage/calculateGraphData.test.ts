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
              { date: "2025-01-15", used: 5 },
              { date: "2025-01-16", used: 10 },
            ],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      expect(result[0].workspaceUsage["workspace-1"]).toBe(5);
      expect(result[1].workspaceUsage["workspace-1"]).toBe(10);
      expect(result[2].workspaceUsage["workspace-1"]).toBeUndefined();
    });

    it("handles multiple workspaces", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [{ date: "2025-01-15", used: 5 }],
          },
          {
            id: "workspace-2",
            name: "Workspace 2",
            dataWorkers: [{ date: "2025-01-15", used: 8 }],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      expect(result[0].workspaceUsage["workspace-1"]).toBe(5);
      expect(result[0].workspaceUsage["workspace-2"]).toBe(8);
    });

    it("sums all usage when multiple data points exist for the same day", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2025-01-15", used: 5 },
              { date: "2025-01-15", used: 10 },
              { date: "2025-01-15", used: 7 },
            ],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      expect(result[0].workspaceUsage["workspace-1"]).toBe(22); // 5 + 10 + 7 = 22
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
            dataWorkers: [{ date: "2025-01-15", used: 0 }],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      expect(result[0].workspaceUsage["workspace-1"]).toBe(0);
    });

    it("sums usage values regardless of order", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2025-01-15", used: 10 },
              { date: "2025-01-15", used: 5 },
            ],
          },
        ],
      };

      const result = calculateGraphData(dateRange, regionData);

      expect(result[0].workspaceUsage["workspace-1"]).toBe(15); // 10 + 5 = 15
    });
  });

  describe("top 10 and other workspace filtering", () => {
    it("sums usage per workspace, then aggregates 'other' workspaces", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-11",
            name: "Workspace 11",
            dataWorkers: [
              { date: "2025-01-15", used: 5 },
              { date: "2025-01-15", used: 10 },
              { date: "2025-01-15", used: 3 },
            ],
          },
          {
            id: "workspace-12",
            name: "Workspace 12",
            dataWorkers: [
              { date: "2025-01-15", used: 7 },
              { date: "2025-01-15", used: 8 },
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

      // Workspace 11: 5 + 10 + 3 = 18
      // Workspace 12: 7 + 8 = 15
      // Other total: 18 + 15 = 33
      expect(result[0].workspaceUsage.other).toBe(33);
    });

    it("sums usage for individual workspace in top 10", () => {
      const dateRange: [string, string] = ["2025-01-15", "2025-01-15"];
      const regionData: RegionDataWorkerUsage = {
        id: "region-1",
        name: "Region 1",
        workspaces: [
          {
            id: "workspace-1",
            name: "Workspace 1",
            dataWorkers: [
              { date: "2025-01-15", used: 10 },
              { date: "2025-01-15", used: 15 },
              { date: "2025-01-15", used: 12 },
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

      // Should sum: 10 + 15 + 12 = 37
      expect(result[0].workspaceUsage["workspace-1"]).toBe(37);
    });
  });
});
