import { mockWorkspaceCreditsUsage } from "test-utils/mock-data/mockWorkspaceCreditsUsage";

import { ConsumptionPerConnectionPerTimeframe } from "packages/cloud/lib/domain/cloudWorkspaces/types";

export const useCreditsUsage = () => {
  // const { workspaceId } = useCurrentWorkspace();
  // const data = useGetCloudWorkspaceUsage(workspaceId);
  const data = mockWorkspaceCreditsUsage;

  const { consumptionPerConnectionPerTimeframe } = data;
  const freeAndPaidUsagePerDay: Array<Omit<ConsumptionPerConnectionPerTimeframe, "connection">> =
    consumptionPerConnectionPerTimeframe
      ?.reduce((allConsumption: Array<Omit<ConsumptionPerConnectionPerTimeframe, "connection">>, consumption) => {
        if (allConsumption.some((item) => item.timeframe === consumption.timeframe.split("T")[0])) {
          const timeframeItem = allConsumption.filter((item) => {
            return item.timeframe === consumption.timeframe.split("T")[0];
          })[0];
          timeframeItem.billedCost = (timeframeItem.billedCost ?? 0) + consumption.billedCost;
          timeframeItem.freeUsage = (timeframeItem.freeUsage ?? 0) + consumption.freeUsage;
        } else {
          allConsumption.push({
            timeframe: consumption.timeframe.split("T")[0],
            billedCost: consumption.billedCost,
            freeUsage: consumption.freeUsage,
          });
        }

        return allConsumption;
      }, [])
      .sort((a, b) => {
        return a.timeframe.localeCompare(b.timeframe);
      });

  return { data, freeAndPaidUsagePerDay };
};
