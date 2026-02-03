import { ConsumptionTimeWindow } from "core/api/types/AirbyteClient";

import { generateArrayForTimeWindow } from "./chartUtils";

describe(`generateArrayForTimeWindow`, () => {
  it("should generate an array of the correct length for past 30 days", () => {
    const result = generateArrayForTimeWindow(ConsumptionTimeWindow.lastMonth);
    expect(result).toHaveLength(30);
  });

  it("should generate an array of the correct length for past 6 months", () => {
    // because the "buckets" always start on a Sunday, there is some variation between
    // whether we end up with 26 or 27 buckets
    const result = generateArrayForTimeWindow(ConsumptionTimeWindow.lastSixMonths);
    expect(result.length).toEqual(26);
  });
  it("should generate an array of the correct length for past year", () => {
    // because the "buckets" always start on the 1st of the month, there is some variation between
    // whether we end up with 12 or 13 buckets
    const result = generateArrayForTimeWindow(ConsumptionTimeWindow.lastYear);
    expect(result.length).toEqual(12);
  });
});
