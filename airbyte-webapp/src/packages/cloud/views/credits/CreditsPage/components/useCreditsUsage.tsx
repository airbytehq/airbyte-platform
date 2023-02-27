import { mockWorkspaceCreditsUsage } from "test-utils/mock-data/mockWorkspaceCreditsUsage";

import { ConsumptionPerConnectionPerTimeframe } from "packages/cloud/lib/domain/cloudWorkspaces/types";

export const useCreditsUsage = () => {
  // const { workspaceId } = useCurrentWorkspace();
  // const data = useGetCloudWorkspaceUsage(workspaceId);
  const data = mockWorkspaceCreditsUsage;

  const { consumptionPerConnectionPerTimeframe } = data;
  const freeAndPaidUsagePerDay: Array<Omit<ConsumptionPerConnectionPerTimeframe, "connection">> =
    consumptionPerConnectionPerTimeframe
      ?.sort((a, b) => {
        return a.timeframe.localeCompare(b.timeframe);
      })
      ?.reduce((allConsumption: Array<Omit<ConsumptionPerConnectionPerTimeframe, "connection">>, consumption) => {
        const consumptionTimeframeToDay = new Date(consumption.timeframe);

        const localizedDateTimeframe = consumptionTimeframeToDay.toLocaleDateString("en-US", {
          month: "short",
          day: "numeric",
        });

        if (allConsumption.some((item) => item.timeframe === localizedDateTimeframe)) {
          const timeframeItem = allConsumption.filter((item) => {
            return item.timeframe === localizedDateTimeframe;
          })[0];
          timeframeItem.billedCost = (timeframeItem.billedCost ?? 0) + consumption.billedCost;
          timeframeItem.freeUsage = (timeframeItem.freeUsage ?? 0) + consumption.freeUsage;
        } else {
          allConsumption.push({
            timeframe: localizedDateTimeframe,
            billedCost: consumption.billedCost,
            freeUsage: consumption.freeUsage,
          });
        }

        return allConsumption;
      }, []);

  return { data, freeAndPaidUsagePerDay };
};
