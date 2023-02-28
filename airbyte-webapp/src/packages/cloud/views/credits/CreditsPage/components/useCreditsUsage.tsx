import { mockWorkspaceCreditsUsage } from "test-utils/mock-data/mockWorkspaceCreditsUsage";

import { ConsumptionPerConnectionPerTimeframe } from "packages/cloud/lib/domain/cloudWorkspaces/types";

export const useCreditsUsage = () => {
  // const { workspaceId } = useCurrentWorkspace();
  // const data = useGetCloudWorkspaceUsage(workspaceId);
  const data = mockWorkspaceCreditsUsage;
  const { consumptionPerConnectionPerTimeframe } = data;

  /**
   * for phase 1, we assume a time window of the past 30 days and an aggregation of per day.
   * for phase 2, we will be pulling time window and aggregation from the response instead as we
   * will be supporting more time windows and aggregations.
   *
   * in phase 2, we can, and will need to, readjust this a bit
   *  */

  const windowEnd = new Date();
  const windowStart = new Date(new Date().setDate(windowEnd.getDate() - 30));

  const timeWindow = [windowStart, windowEnd];

  const freeAndPaidUsagePerTimeframe: Array<Omit<ConsumptionPerConnectionPerTimeframe, "connection">> = [];

  const date = new Date(timeWindow[0]);
  while (date <= timeWindow[1]) {
    const localizedDateTimeframe = date.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    });
    freeAndPaidUsagePerTimeframe.push({
      timeframe: localizedDateTimeframe,
      billedCost: 0,
      freeUsage: 0,
    });
    date.setDate(date.getDate() + 1);
  }

  consumptionPerConnectionPerTimeframe
    ?.sort((a, b) => {
      return a.timeframe.localeCompare(b.timeframe);
    })
    .forEach((consumption) => {
      const consumptionTimeframeToDay = new Date(consumption.timeframe);

      const localizedDateTimeframe = consumptionTimeframeToDay.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
      });

      const timeframeItemIndex = freeAndPaidUsagePerTimeframe.findIndex(
        (item) => item.timeframe === localizedDateTimeframe
      );

      if (timeframeItemIndex !== -1) {
        freeAndPaidUsagePerTimeframe[timeframeItemIndex].billedCost =
          (freeAndPaidUsagePerTimeframe[timeframeItemIndex].billedCost ?? 0) + consumption.billedCost;
        freeAndPaidUsagePerTimeframe[timeframeItemIndex].freeUsage =
          (freeAndPaidUsagePerTimeframe[timeframeItemIndex].freeUsage ?? 0) + consumption.freeUsage;
      }
    });

  return { data, freeAndPaidUsagePerTimeframe };
};
