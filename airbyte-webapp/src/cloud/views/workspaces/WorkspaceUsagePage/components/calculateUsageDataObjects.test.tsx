import { mockWorkspaceUsage } from "test-utils/mock-data/mockWorkspaceUsage";

import { ConsumptionTimeWindow } from "core/api/types/AirbyteClient";

import { getWorkspaceUsageByConnection } from "./calculateUsageDataObjects";

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
