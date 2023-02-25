import { mockWorkspaceCreditsUsage } from "test-utils/mock-data/mockWorkspaceCreditsUsage";

import { ConsumptionPerConnectionPerTimeframe } from "packages/cloud/lib/domain/cloudWorkspaces/types";

export const useCreditsUsage = () => {
  // const { workspaceId } = useCurrentWorkspace();
  // const data = useGetCloudWorkspaceUsage(workspaceId);
  const data = mockWorkspaceCreditsUsage;

  const { consumptionPerConnectionPerTimeframe } = data;
  const freeAndPaidUsagePerDay: Array<Partial<ConsumptionPerConnectionPerTimeframe>> =
    consumptionPerConnectionPerTimeframe?.reduce(
      (allConsumption: Array<Partial<ConsumptionPerConnectionPerTimeframe>>, consumption) => {
        if (allConsumption.some((item) => item.timeframe === consumption.timeframe)) {
          const timeframeItem = allConsumption.filter((item) => {
            return item.timeframe === consumption.timeframe;
          })[0];
          timeframeItem.billedCost = (timeframeItem.billedCost ?? 0) + consumption.billedCost;
          timeframeItem.freeUsage = (timeframeItem.freeUsage ?? 0) + consumption.freeUsage;
        } else {
          allConsumption.push({
            timeframe: consumption.timeframe,
            billedCost: consumption.billedCost,
            freeUsage: consumption.freeUsage,
          });
        }

        return allConsumption;
      },
      []
    );

  console.log({ freeAndPaidUsagePerDay });

  return { data, freeAndPaidUsagePerDay };
};
