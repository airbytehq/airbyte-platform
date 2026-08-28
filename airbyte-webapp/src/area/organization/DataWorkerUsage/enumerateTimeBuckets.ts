import type { Dayjs } from "dayjs";

import dayjs from "dayjs";

type TimeBucketGranularity = "day" | "hour" | "week";

export const enumerateTimeBuckets = (dateRange: [string, string], granularity: TimeBucketGranularity): Dayjs[] => {
  const rangeEnd = dayjs(dateRange[1]);
  const buckets: Dayjs[] = [];
  const rangeStart = dayjs(dateRange[0]);
  let cursor =
    granularity === "week"
      ? rangeStart.startOf("day").subtract(rangeStart.day(), "day")
      : rangeStart.startOf(granularity);

  if (!cursor.isValid() || !rangeEnd.isValid()) {
    return buckets;
  }

  while (granularity === "week" ? cursor.isBefore(rangeEnd) : !cursor.isAfter(rangeEnd)) {
    buckets.push(cursor);
    cursor = cursor.add(1, granularity);
  }

  return buckets;
};
