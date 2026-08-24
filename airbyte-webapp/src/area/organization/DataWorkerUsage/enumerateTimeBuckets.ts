import type { Dayjs } from "dayjs";

import dayjs from "dayjs";

type TimeBucketGranularity = "day" | "hour";

export const enumerateTimeBuckets = (dateRange: [string, string], granularity: TimeBucketGranularity): Dayjs[] => {
  const rangeEnd = dayjs(dateRange[1]);
  const buckets: Dayjs[] = [];
  let cursor = dayjs(dateRange[0]).startOf(granularity);

  if (!cursor.isValid() || !rangeEnd.isValid()) {
    return buckets;
  }

  while (!cursor.isAfter(rangeEnd)) {
    buckets.push(cursor);
    cursor = cursor.add(1, granularity);
  }

  return buckets;
};
