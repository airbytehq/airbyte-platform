import {
  mockConsumptionSixMonths,
  mockConsumptionThirtyDay,
  mockConsumptionYear,
} from "test-utils/mock-data/mockBillingData";
import { mockWorkspaceUsage } from "test-utils/mock-data/mockWorkspaceUsage";

import { ConsumptionTimeWindow } from "core/api/types/CloudApi";

import {
  calculateFreeAndPaidUsageByConnection,
  calculateFreeAndPaidUsageByTimeChunk,
  getWorkspaceUsageByConnection,
} from "./calculateUsageDataObjects";

describe("calculateUsageDataObjects", () => {
  describe(`${calculateFreeAndPaidUsageByTimeChunk.name}`, () => {
    describe("thirty day lookback", () => {
      it("should calculate the correct usage with an empty set of filteredConsumptionData", () => {
        const result = calculateFreeAndPaidUsageByTimeChunk([], ConsumptionTimeWindow.lastMonth);
        expect(result).toHaveLength(0);
      });
      it("should calculate the correct usage with a set of filteredConsumptionData", () => {
        const result = calculateFreeAndPaidUsageByTimeChunk(mockConsumptionThirtyDay, ConsumptionTimeWindow.lastMonth);
        expect(result).toHaveLength(30);
        expect(result[0].freeUsage).toEqual(0);
        expect(result[0].billedCost).toEqual(0);
        expect(result[29].freeUsage).toEqual(316);
        expect(result[29].billedCost).toEqual(426);
      });
    });
    describe("six month lookback", () => {
      it("should calculate the correct usage with an empty set of filteredConsumptionData", () => {
        const result = calculateFreeAndPaidUsageByTimeChunk([], ConsumptionTimeWindow.lastMonth);
        expect(result).toHaveLength(0);
      });
      it("should calculate the correct usage with a set of filteredConsumptionData", () => {
        const result = calculateFreeAndPaidUsageByTimeChunk(
          mockConsumptionSixMonths,
          ConsumptionTimeWindow.lastSixMonths
        );
        expect(result).toHaveLength(26);
        expect(result[0].freeUsage).toEqual(0);
        expect(result[0].billedCost).toEqual(0);
        // three weeks ago
        expect(result[22].freeUsage).toEqual(32);
        expect(result[22].billedCost).toEqual(52);
        // last week
        expect(result[24].freeUsage).toEqual(401);
        expect(result[24].billedCost).toEqual(601);
      });
    });
    describe("year lookback", () => {
      it("should calculate the correct usage with an empty set of filteredConsumptionData", () => {
        const result = calculateFreeAndPaidUsageByTimeChunk([], ConsumptionTimeWindow.lastMonth);
        expect(result).toHaveLength(0);
      });
      it("should calculate the correct usage with a set of filteredConsumptionData", () => {
        const result = calculateFreeAndPaidUsageByTimeChunk(mockConsumptionYear, ConsumptionTimeWindow.lastYear);

        expect(result).toHaveLength(12);
        expect(result[0].freeUsage).toEqual(0);
        expect(result[0].billedCost).toEqual(0);
        // three months ago
        expect(result[7].freeUsage).toEqual(32);
        expect(result[7].billedCost).toEqual(52);
        // last month
        expect(result[11].freeUsage).toEqual(401);
        expect(result[11].billedCost).toEqual(601);
      });
    });
  });

  describe(`${calculateFreeAndPaidUsageByConnection.name}`, () => {
    it("should calculate the correct usage with an empty set of filteredConsumptionData", () => {
      const result = calculateFreeAndPaidUsageByConnection([], ConsumptionTimeWindow.lastMonth);
      expect(result).toHaveLength(0);
    });
    it("should calculate the correct usage with a set of filteredConsumptionData", () => {
      const result = calculateFreeAndPaidUsageByConnection(mockConsumptionThirtyDay, ConsumptionTimeWindow.lastMonth);
      expect(result).toHaveLength(3);

      expect(result[0].connection.connectionName).toEqual("my connection");
      expect(result[0].usage).toHaveLength(30);
      expect(result[0].totalUsage).toEqual(1002);

      expect(result[1].connection.connectionName).toEqual("my second connection");
      expect(result[1].usage).toHaveLength(30);
      expect(result[1].totalUsage).toEqual(60);

      expect(result[2].connection.connectionName).toEqual("my third connection");
      expect(result[2].usage).toHaveLength(30);
      expect(result[2].totalUsage).toEqual(24);
    });
  });
});

describe(`${getWorkspaceUsageByConnection.name}`, () => {
  it("should calculate the correct usage with an empty set of filteredConsumptionData", () => {
    const result = getWorkspaceUsageByConnection([], ConsumptionTimeWindow.lastMonth);
    expect(result).toHaveLength(0);
  });

  it("should calculate the correct usage with internal, free and regular usage", () => {
    const result = getWorkspaceUsageByConnection(mockWorkspaceUsage.data, ConsumptionTimeWindow.lastMonth);
    expect(result).toHaveLength(2);
    expect(result[0].totalFreeUsage).toEqual(10);
    expect(result[0].totalInternalUsage).toEqual(10);
    expect(result[0].totalBilledCost).toEqual(53.5);
    expect(result[0].totalUsage).toEqual(73.5);
  });
});
