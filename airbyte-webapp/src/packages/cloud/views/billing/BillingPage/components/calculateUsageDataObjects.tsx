import dayjs from "dayjs";

import { ConsumptionPerConnectionPerTimeframe } from "packages/cloud/lib/domain/cloudWorkspaces/types";

export type UsagePerTimeframe = Array<Omit<ConsumptionPerConnectionPerTimeframe, "connection">>;

export interface ConnectionFreeAndPaidUsage extends Pick<ConsumptionPerConnectionPerTimeframe, "connection"> {
  usage: UsagePerTimeframe;
  totalFreeUsage: number;
  totalBilledCost: number;
  totalUsage: number;
}

export const generateArrayForTimeWindow = () => {
  const usagePerTimeframe: UsagePerTimeframe = [];

  for (
    let currentDay = dayjs().subtract(29, "day");
    !currentDay.isAfter(dayjs());
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

/**
 * if there is no consumption for a given timeframe (in this case, day) we will not receive a data point
 * however, we still want to include that day on our graph, so we create an array with an entry for each time window
 * then backfill it with the data from the API.
 */

const mergeUsageData = (usageArray: UsagePerTimeframe, consumption: ConsumptionPerConnectionPerTimeframe) => {
  const timeframeItemIndex = usageArray.findIndex((item) => item.timeframe === consumption.timeframe);

  if (timeframeItemIndex !== -1) {
    const usage = usageArray[timeframeItemIndex];
    usage.billedCost += consumption.billedCost;
    usage.freeUsage += consumption.freeUsage;
  }
};
export const calculateFreeAndPaidUsageByConnection = (
  filteredConsumptionData: ConsumptionPerConnectionPerTimeframe[]
) => {
  if (filteredConsumptionData.length === 0) {
    return [];
  }
  const usagePerConnection = filteredConsumptionData.reduce((allConsumption, consumption) => {
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
    } else {
      allConsumption[connection.connectionId].totalFreeUsage += consumption.freeUsage;
      allConsumption[connection.connectionId].totalBilledCost += consumption.billedCost;
      allConsumption[connection.connectionId].totalUsage += consumption.freeUsage + consumption.billedCost;
    }

    mergeUsageData(allConsumption[connection.connectionId].usage, consumption);

    return allConsumption;
  }, {} as Record<string, ConnectionFreeAndPaidUsage>);

  const array = Object.values(usagePerConnection);
  return array;
};

// currently assumes a default timeframe of 30 days and no other conditions (yet)
export const calculateFreeAndPaidUsageByTimeframe = (
  filteredConsumptionData: ConsumptionPerConnectionPerTimeframe[]
) => {
  if (filteredConsumptionData.length === 0) {
    return [];
  }

  const usagePerTimeframe = generateArrayForTimeWindow();

  filteredConsumptionData
    ?.sort((a, b) => {
      return a.timeframe.localeCompare(b.timeframe);
    })
    .forEach((consumption) => {
      mergeUsageData(usagePerTimeframe, consumption);
    });

  return usagePerTimeframe;
};
