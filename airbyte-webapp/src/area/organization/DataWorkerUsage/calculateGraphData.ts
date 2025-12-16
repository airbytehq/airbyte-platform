import dayjs from "dayjs";

import { RegionDataWorkerUsage } from "core/api/types/AirbyteClient";

export interface RegionDataBar {
  formattedDate: string;
  workspaceUsage: Record<string, number>;
}

const DATE_FORMAT = "YYYY-MM-DD";

/**
 * Calculates graph data for a given date range and region usage.
 * Creates a data point for each day in the range, populating workspace usage values.
 * When multiple data points exist for the same workspace on the same day, keeps the maximum value.
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
    // First pass: calculate max usage per workspace per day
    const workspaceMaxUsage = new Map<string, Map<string, number>>();

    regionDataWorkerUsage.workspaces.forEach((workspace) => {
      const dailyMaxUsage = new Map<string, number>();

      workspace.dataWorkers.forEach(({ date, used }) => {
        const formattedDate = dayjs(date).format(DATE_FORMAT);
        if (days.has(formattedDate)) {
          const existingMax = dailyMaxUsage.get(formattedDate) ?? 0;
          dailyMaxUsage.set(formattedDate, Math.max(existingMax, used));
        }
      });

      workspaceMaxUsage.set(workspace.id, dailyMaxUsage);
    });

    // Second pass: assign to top 10 or sum into "Other"
    regionDataWorkerUsage.workspaces.forEach((workspace) => {
      const isInTop10 = !top10WorkspaceIds || top10WorkspaceIds.includes(workspace.id);
      const isInOther = otherWorkspaceIds?.includes(workspace.id);
      const dailyMaxUsage = workspaceMaxUsage.get(workspace.id)!;

      dailyMaxUsage.forEach((maxUsage, formattedDate) => {
        const day = days.get(formattedDate);
        if (day) {
          if (isInTop10 && !isInOther) {
            // Add to top 10 workspace
            day.workspaceUsage[workspace.id] = maxUsage;
          } else if (isInOther) {
            // Sum max usage values into "Other" category
            const existingOtherUsage = day.workspaceUsage.other ?? 0;
            day.workspaceUsage.other = existingOtherUsage + maxUsage;
          }
        }
      });
    });
  }

  return Array.from(days.values());
};
