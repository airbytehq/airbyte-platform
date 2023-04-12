import { mockConsumptionSixMonths } from "test-utils/mock-data/mockBillingData";

import { calculateAvailableSourcesAndDestinations } from "./calculateAvailableSourcesAndDestinations";

describe("calculateAvailableSourcesAndDestinations", () => {
  it("calculates a complete set of available sources and destinations", () => {
    const result = calculateAvailableSourcesAndDestinations(mockConsumptionSixMonths);
    expect(result.destinations).toHaveLength(2);
    expect(result.sources).toHaveLength(3);

    expect(result.destinations[0].connectedSources).toHaveLength(2);
    expect(result.destinations[1].connectedSources).toHaveLength(1);

    expect(result.sources[0].connectedDestinations).toHaveLength(1);
    expect(result.sources[1].connectedDestinations).toHaveLength(1);
    expect(result.sources[2].connectedDestinations).toHaveLength(1);
  });
});
