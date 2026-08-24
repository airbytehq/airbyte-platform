import { enumerateTimeBuckets } from "./enumerateTimeBuckets";

describe(`${enumerateTimeBuckets.name}`, () => {
  it("returns no buckets when either boundary is invalid", () => {
    expect(enumerateTimeBuckets(["invalid", "2026-08-24"], "day")).toEqual([]);
    expect(enumerateTimeBuckets(["2026-08-24", "invalid"], "hour")).toEqual([]);
  });

  it("includes both daily endpoints", () => {
    expect(enumerateTimeBuckets(["2026-08-22", "2026-08-24"], "day").map((day) => day.format("YYYY-MM-DD"))).toEqual([
      "2026-08-22",
      "2026-08-23",
      "2026-08-24",
    ]);
  });

  it("includes the hourly endpoint", () => {
    expect(
      enumerateTimeBuckets(["2026-08-24T17:00:00Z", "2026-08-24T19:00:00Z"], "hour").map((hour) => hour.toISOString())
    ).toEqual(["2026-08-24T17:00:00.000Z", "2026-08-24T18:00:00.000Z", "2026-08-24T19:00:00.000Z"]);
  });
});
