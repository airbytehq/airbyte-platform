import dayjs from "dayjs";
import { useMemo } from "react";
import { FormattedNumber } from "react-intl";

import { ConsumptionPerConnectionPerTimeframe } from "packages/cloud/lib/domain/cloudWorkspaces/types";
import { useGetCloudWorkspaceUsage } from "packages/cloud/services/workspaces/CloudWorkspacesService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

export interface ConnectionFreeAndPaidUsage extends Pick<ConsumptionPerConnectionPerTimeframe, "connection"> {
  usage: Array<Omit<ConsumptionPerConnectionPerTimeframe, "connection">>;
  totalFreeUsage: number;
  totalBilledCost: number;
  totalUsage: number;
}

export const useCreditsUsage = () => {
  const { workspaceId } = useCurrentWorkspace();
  const data = useGetCloudWorkspaceUsage(workspaceId);
  const { consumptionPerConnectionPerTimeframe } = data;

  const formatCredits = (credits: number) => {
    return credits < 0.005 && credits > 0 ? (
      "<0.01"
    ) : (
      <FormattedNumber value={credits} maximumFractionDigits={2} minimumFractionDigits={2} />
    );
  };

  const generateArrayForTimeWindow = () => {
    const usagePerTimeframe: Array<Omit<ConsumptionPerConnectionPerTimeframe, "connection">> = [];

    for (
      let currentDay = dayjs().subtract(30, "day");
      currentDay.isSame(dayjs()) || currentDay.isBefore(dayjs());
      currentDay = currentDay.add(1, "day")
    ) {
      usagePerTimeframe.push({
        timeframe: currentDay.format("YYYY-MM-DD"),
        billedCost: 0,
        freeUsage: 0.0,
      });
    }

    return usagePerTimeframe;
  };

  const mergeUsageData = (
    usageArray: Array<Omit<ConsumptionPerConnectionPerTimeframe, "connection">>,
    consumption: ConsumptionPerConnectionPerTimeframe
  ) => {
    const timeframeItemIndex = usageArray.findIndex((item) => item.timeframe === consumption.timeframe);

    if (timeframeItemIndex !== -1) {
      const usage = usageArray[timeframeItemIndex];
      usage.billedCost += consumption.billedCost;
      usage.freeUsage += consumption.freeUsage;
    }
  };

  /**
   * Returns free and paid usage per timeframe for a given window.
   *
   * for phase 1, we assume a time window of the past 30 days and an aggregation of per day.
   * this is subject to change in phase 2, when we introduce  more static windows
   * and custom time windows, and add additional aggregation periods based on them
   *  */

  const freeAndPaidUsageByTimeframe: Array<Omit<ConsumptionPerConnectionPerTimeframe, "connection">> = useMemo(() => {
    if (consumptionPerConnectionPerTimeframe.length === 0) {
      return [];
    }

    /**
     * if there is no consumption for a given timeframe (in this case, day) we will not receive a data point
     * however, we still want to include that day on our graph, so we create an array with an entry for each time window
     * then backfill it with the data from the API.
     */

    const usagePerTimeframe = generateArrayForTimeWindow();

    consumptionPerConnectionPerTimeframe
      ?.sort((a, b) => {
        return a.timeframe.localeCompare(b.timeframe);
      })
      .forEach((consumption) => {
        mergeUsageData(usagePerTimeframe, consumption);
      });

    return usagePerTimeframe;
  }, [consumptionPerConnectionPerTimeframe]);

  const freeAndPaidUsageByConnection: ConnectionFreeAndPaidUsage[] = useMemo(() => {
    if (consumptionPerConnectionPerTimeframe.length === 0) {
      return [];
    }
    const usagePerConnection = consumptionPerConnectionPerTimeframe.reduce((allConsumption, consumption) => {
      const { connection } = consumption;

      // if this connection isn't in our list yet, add it
      // also, generate an array for the usage array
      if (!allConsumption[connection.connectionId]) {
        allConsumption[connection.connectionId] = {
          connection,
          totalFreeUsage: consumption.freeUsage,
          totalBilledCost: consumption.billedCost,
          totalUsage: consumption.freeUsage + consumption.billedCost,
          usage: generateArrayForTimeWindow(),
        };
      }

      mergeUsageData(allConsumption[connection.connectionId].usage, consumption);

      return allConsumption;
    }, {} as Record<string, ConnectionFreeAndPaidUsage>);

    const array = Object.values(usagePerConnection);
    return array;
  }, [consumptionPerConnectionPerTimeframe]);

  return { freeAndPaidUsageByTimeframe, freeAndPaidUsageByConnection, formatCredits };
};
