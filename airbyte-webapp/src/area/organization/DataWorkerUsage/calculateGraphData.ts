import dayjs from "dayjs";

import { RegionDataWorkerUsage } from "core/api/types/AirbyteClient";

export type UsageGraphGranularity = "hour" | "day" | "week";

export interface RegionDataBar {
  formattedDate: string;
  regionUsage: number;
  maxWorkspaceUsage: number;
  workspaceUsage: Record<string, number>;
}

interface TimeBucket extends RegionDataBar {
  startTimestamp: number;
}

const DATE_FORMAT = "YYYY-MM-DD";
const HOUR_IN_MS = 60 * 60 * 1000;
const startOfCalendarWeek = (date: dayjs.Dayjs) => date.startOf("day").subtract(date.day(), "day");

const createDisplayBuckets = (displayRange: [string, string], granularity: UsageGraphGranularity): TimeBucket[] => {
  const rangeEnd = dayjs(displayRange[1]);
  const buckets: TimeBucket[] = [];
  let cursor = dayjs(displayRange[0]);

  if (granularity === "week") {
    cursor = startOfCalendarWeek(cursor);
  }

  while (cursor.isBefore(rangeEnd)) {
    buckets.push({
      formattedDate: cursor.toISOString(),
      startTimestamp: cursor.valueOf(),
      regionUsage: 0,
      maxWorkspaceUsage: 0,
      workspaceUsage: {},
    });
    cursor = cursor.add(1, granularity);
  }

  return buckets;
};

/**
 * Creates a bar for each bucket in an end-exclusive display range and populates it with concurrent usage data.
 * Hourly ranges retain every absolute hour. Daily and weekly ranges use the workspace values from the hour when the
 * region's total usage peaked. API data outside the exact display range is ignored because date-only requests can
 * over-fetch.
 */
export const calculateGraphData = (
  displayRange: [string, string],
  granularity: UsageGraphGranularity,
  regionDataWorkerUsage: RegionDataWorkerUsage | undefined,
  top10WorkspaceIds?: string[],
  otherWorkspaceIds?: string[]
): RegionDataBar[] => {
  const buckets = createDisplayBuckets(displayRange, granularity);
  const rangeStartTimestamp = dayjs(displayRange[0]).valueOf();
  const rangeEndTimestamp = dayjs(displayRange[1]).valueOf();
  const bucketsByKey = new Map(
    buckets.map((bucket) => [
      granularity === "hour" ? String(bucket.startTimestamp) : dayjs(bucket.formattedDate).format(DATE_FORMAT),
      bucket,
    ])
  );

  if (!regionDataWorkerUsage) {
    return buckets.map(({ startTimestamp: _startTimestamp, ...bucket }) => bucket);
  }

  const workspaceHourlyUsage = new Map<string, Map<number, number>>();
  const hoursByBucket = new Map<string, Set<number>>();

  regionDataWorkerUsage.workspaces.forEach((workspace) => {
    const hourlyUsage = new Map<number, number>();

    workspace.dataWorkers.forEach(({ date, used }) => {
      const timestamp = dayjs(date);
      const timestampValue = timestamp.valueOf();

      if (timestampValue < rangeStartTimestamp || timestampValue >= rangeEndTimestamp) {
        return;
      }

      const bucketKey =
        granularity === "hour"
          ? String(rangeStartTimestamp + Math.floor((timestampValue - rangeStartTimestamp) / HOUR_IN_MS) * HOUR_IN_MS)
          : granularity === "week"
          ? startOfCalendarWeek(timestamp).format(DATE_FORMAT)
          : timestamp.format(DATE_FORMAT);
      const bucket = bucketsByKey.get(bucketKey);

      if (!bucket) {
        return;
      }

      const hourTimestamp =
        bucket.startTimestamp + Math.floor((timestampValue - bucket.startTimestamp) / HOUR_IN_MS) * HOUR_IN_MS;
      hourlyUsage.set(hourTimestamp, Math.max(hourlyUsage.get(hourTimestamp) ?? 0, used));

      const bucketHours = hoursByBucket.get(bucketKey) ?? new Set<number>();
      bucketHours.add(hourTimestamp);
      hoursByBucket.set(bucketKey, bucketHours);
    });

    workspaceHourlyUsage.set(workspace.id, hourlyUsage);
  });

  const peakHourByBucket = new Map<string, number>();

  hoursByBucket.forEach((hours, bucketKey) => {
    let peakHour: number | undefined;
    let peakRegionUsage = -1;
    let maxWorkspaceUsage = 0;

    hours.forEach((hourTimestamp) => {
      let regionUsage = 0;
      let largestWorkspaceUsage = 0;

      workspaceHourlyUsage.forEach((hourlyUsage) => {
        const workspaceUsage = hourlyUsage.get(hourTimestamp) ?? 0;
        regionUsage += workspaceUsage;
        largestWorkspaceUsage = Math.max(largestWorkspaceUsage, workspaceUsage);
      });

      if (regionUsage > peakRegionUsage) {
        peakHour = hourTimestamp;
        peakRegionUsage = regionUsage;
        maxWorkspaceUsage = largestWorkspaceUsage;
      }
    });

    const bucket = bucketsByKey.get(bucketKey);
    if (bucket && peakHour !== undefined) {
      peakHourByBucket.set(bucketKey, peakHour);
      bucket.regionUsage = peakRegionUsage;
      bucket.maxWorkspaceUsage = maxWorkspaceUsage;
    }
  });

  regionDataWorkerUsage.workspaces.forEach((workspace) => {
    const isInTop10 = !top10WorkspaceIds || top10WorkspaceIds.includes(workspace.id);
    const isInOther = otherWorkspaceIds?.includes(workspace.id);
    const hourlyUsage = workspaceHourlyUsage.get(workspace.id)!;

    peakHourByBucket.forEach((peakHour, bucketKey) => {
      const bucket = bucketsByKey.get(bucketKey);
      if (!bucket) {
        return;
      }

      const usageAtPeakHour = hourlyUsage.get(peakHour) ?? 0;
      if (isInTop10 && !isInOther) {
        bucket.workspaceUsage[workspace.id] = usageAtPeakHour;
      } else if (isInOther) {
        bucket.workspaceUsage.other = (bucket.workspaceUsage.other ?? 0) + usageAtPeakHour;
      }
    });
  });

  return buckets.map(({ startTimestamp: _startTimestamp, ...bucket }) => bucket);
};
