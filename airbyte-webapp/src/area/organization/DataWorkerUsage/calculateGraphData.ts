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
 * @returns An array of RegionDataBar objects, one for each day in the range
 */
export const calculateGraphData = (
  dateRange: [string, string],
  regionDataWorkerUsage: RegionDataWorkerUsage | undefined
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
    regionDataWorkerUsage.workspaces.forEach((workspace) => {
      workspace.dataWorkers.forEach(({ date, used }) => {
        const day = days.get(dayjs(date).format(DATE_FORMAT));
        if (day) {
          const existingUsage = day.workspaceUsage[workspace.id] ?? 0;
          // Keep the maximum usage value if multiple data points exist
          day.workspaceUsage[workspace.id] = used > existingUsage ? used : existingUsage;
        }
      });
    });
  }

  return Array.from(days.values());
};
