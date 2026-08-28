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

  it("aligns weekly buckets to Sunday and includes partial boundary weeks", () => {
    expect(
      enumerateTimeBuckets(["2025-08-25T07:00:00Z", "2025-09-08T06:59:59Z"], "week").map((week) => week.toISOString())
    ).toEqual(["2025-08-24T07:00:00.000Z", "2025-08-31T07:00:00.000Z", "2025-09-07T07:00:00.000Z"]);
  });

  it("does not add an empty weekly bucket when the end-exclusive boundary is Sunday midnight", () => {
    expect(
      enumerateTimeBuckets(["2025-08-25T07:00:00Z", "2025-09-07T07:00:00Z"], "week").map((week) => week.toISOString())
    ).toEqual(["2025-08-24T07:00:00.000Z", "2025-08-31T07:00:00.000Z"]);
  });

  it("keeps Sunday alignment across a year boundary and leap year", () => {
    expect(
      enumerateTimeBuckets(["2023-12-30T08:00:00Z", "2024-01-03T07:59:59Z"], "week").map((week) =>
        week.format("YYYY-MM-DD")
      )
    ).toEqual(["2023-12-24", "2023-12-31"]);
  });
});
