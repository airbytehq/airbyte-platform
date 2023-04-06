import { mockConsumptionData } from "test-utils/mock-data/mockWorkspace";

import {
  calculateFreeAndPaidUsageByConnection,
  calculateFreeAndPaidUsageByTimeframe,
  generateArrayForTimeWindow,
} from "./calculateUsageDataObjects";

describe("calculateUsageDataObjects", () => {
  describe(`${generateArrayForTimeWindow.name}`, () => {
    it("should generate an array of the correct length", () => {
      const result = generateArrayForTimeWindow();
      expect(result).toHaveLength(30);
    });
  });
  describe("#calculateFreeAndPaidUsageByTimeframe", () => {
    it("should calculate the correct usage with an empty set of filteredConsumptionData", () => {
      const result = calculateFreeAndPaidUsageByTimeframe([]);
      expect(result).toHaveLength(0);
    });
    it("should calculate the correct usage with a set of filteredConsumptionData", () => {
      const result = calculateFreeAndPaidUsageByTimeframe(mockConsumptionData);
      expect(result).toHaveLength(30);
      expect(result[0].freeUsage).toEqual(0);
      expect(result[0].billedCost).toEqual(0);
      expect(result[29].freeUsage).toEqual(316);
      expect(result[29].billedCost).toEqual(426);
    });
  });

  describe("#calculateFreeAndPaidUsageByConnection", () => {
    it("should calculate the correct usage with an empty set of filteredConsumptionData", () => {
      const result = calculateFreeAndPaidUsageByConnection([]);
      expect(result).toHaveLength(0);
    });
    it("should calculate the correct usage with a set of filteredConsumptionData", () => {
      const result = calculateFreeAndPaidUsageByConnection(mockConsumptionData);
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
