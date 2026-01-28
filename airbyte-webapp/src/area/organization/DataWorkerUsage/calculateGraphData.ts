import dayjs from "dayjs";

import { RegionDataWorkerUsage } from "core/api/types/AirbyteClient";

export interface RegionDataBar {
  formattedDate: string;
  workspaceUsage: Record<string, number>;
}

const DATE_FORMAT = "YYYY-MM-DD";
const DATETIME_FORMAT = "YYYY-MM-DD HH:mm";

/**
 * Calculates graph data for a given date range and region usage.
 * Creates a data point for each day in the range, populating workspace usage values.
 *
 * For each day, finds the "peak hour" - the hour with the highest total usage across all workspaces.
 * Then uses each workspace's usage value from that peak hour as the daily value.
 * This represents the actual concurrent usage at the moment of peak demand.
 *
 * @param dateRange - A tuple of [startDate, endDate] in YYYY-MM-DD format
 * @param regionDataWorkerUsage - Optional region usage data containing workspace usage information
 * @param top10WorkspaceIds - Optional array of workspace IDs to include in the graph (top 10)
 * @param otherWorkspaceIds - Optional array of workspace IDs to aggregate into "Other" category
 * @returns An array of RegionDataBar objects, one for each day in the range
 */
export const calculateGraphData = (
  dateRange: [string, string],
  regionDataWorkerUsage: RegionDataWorkerUsage | undefined,
  top10WorkspaceIds?: string[],
  otherWorkspaceIds?: string[]
): RegionDataBar[] => {
  const firstDay = dayjs(dateRange[0]).startOf("day");
  const lastDay = dayjs(dateRange[1]).endOf("day");
  let cursor = firstDay;
  const days: Map<string, RegionDataBar> = new Map();

  // Create a data point for each day in the range
  while (cursor.isBefore(lastDay)) {
    const formattedDate = cursor.format(DATE_FORMAT);
    days.set(formattedDate, {
      formattedDate,
      workspaceUsage: {},
    });
    cursor = cursor.add(1, "day");
  }

  // Populate workspace usage data
  if (regionDataWorkerUsage) {
    // First pass: collect all hourly data points per workspace
    // Map: workspaceId -> Map<hourTimestamp, usage>
    const workspaceHourlyUsage = new Map<string, Map<string, number>>();

    regionDataWorkerUsage.workspaces.forEach((workspace) => {
      const hourlyUsage = new Map<string, number>();

      workspace.dataWorkers.forEach(({ date, used }) => {
        const hourKey = dayjs(date).format(DATETIME_FORMAT);
        // If multiple entries for the same hour, take the max
        const existing = hourlyUsage.get(hourKey) ?? 0;
        hourlyUsage.set(hourKey, Math.max(existing, used));
      });

      workspaceHourlyUsage.set(workspace.id, hourlyUsage);
    });

    // Second pass: for each day, find the peak hour (hour with highest total across all workspaces)
    // Map: day -> peakHourKey
    const peakHourByDay = new Map<string, string>();

    // Collect all unique hours and group by day
    const hoursByDay = new Map<string, Set<string>>();
    workspaceHourlyUsage.forEach((hourlyUsage) => {
      hourlyUsage.forEach((_, hourKey) => {
        const dayKey = dayjs(hourKey, DATETIME_FORMAT).format(DATE_FORMAT);
        if (days.has(dayKey)) {
          if (!hoursByDay.has(dayKey)) {
            hoursByDay.set(dayKey, new Set());
          }
          hoursByDay.get(dayKey)!.add(hourKey);
        }
      });
    });

    // For each day, find the hour with the highest total usage
    hoursByDay.forEach((hours, dayKey) => {
      let maxTotal = -1;
      let peakHour = "";

      hours.forEach((hourKey) => {
        let totalForHour = 0;
        workspaceHourlyUsage.forEach((hourlyUsage) => {
          totalForHour += hourlyUsage.get(hourKey) ?? 0;
        });

        if (totalForHour > maxTotal) {
          maxTotal = totalForHour;
          peakHour = hourKey;
        }
      });

      if (peakHour) {
        peakHourByDay.set(dayKey, peakHour);
      }
    });

    // Third pass: for each workspace, use the value from the peak hour of each day
    regionDataWorkerUsage.workspaces.forEach((workspace) => {
      const isInTop10 = !top10WorkspaceIds || top10WorkspaceIds.includes(workspace.id);
      const isInOther = otherWorkspaceIds?.includes(workspace.id);
      const hourlyUsage = workspaceHourlyUsage.get(workspace.id)!;

      peakHourByDay.forEach((peakHourKey, dayKey) => {
        const day = days.get(dayKey);
        if (day) {
          const usageAtPeakHour = hourlyUsage.get(peakHourKey) ?? 0;

          if (isInTop10 && !isInOther) {
            // Add to top 10 workspace
            day.workspaceUsage[workspace.id] = usageAtPeakHour;
          } else if (isInOther) {
            // Sum usage values into "Other" category
            const existingOtherUsage = day.workspaceUsage.other ?? 0;
            day.workspaceUsage.other = existingOtherUsage + usageAtPeakHour;
          }
        }
      });
    });
  }

  return Array.from(days.values());
};
