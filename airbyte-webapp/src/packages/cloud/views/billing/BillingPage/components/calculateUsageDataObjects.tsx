import dayjs, { ConfigType, ManipulateType } from "dayjs";

import { ConsumptionRead, ConsumptionTimeWindow } from "core/api/types/CloudApi";

export type UsagePerTimeChunk = Array<{
  timeChunkLabel: string;
  billedCost: number;
  freeUsage: number;
  startTime: string;
  endTime: string;
}>;

export interface ConnectionFreeAndPaidUsage extends Pick<ConsumptionRead, "connection"> {
  usage: UsagePerTimeChunk;
  totalFreeUsage: number;
  totalBilledCost: number;
  totalUsage: number;
}

export const generateArrayForTimeWindow = (timeWindow?: ConsumptionTimeWindow) => {
  const usagePerTimeChunk: UsagePerTimeChunk = [];

  // base case: lastMonth, which returns past 30 days of usage
  let start = dayjs().subtract(29, "day");
  let aggregation: ManipulateType = "day";
  let formatterString = "MMM DD";

  if (timeWindow === "lastSixMonths") {
    aggregation = "week";
    formatterString = "MMM DD";
    start = dayjs().subtract(6, "month").startOf(aggregation);
  } else if (timeWindow === "lastYear") {
    aggregation = "month";
    formatterString = "MMM 'YY";
    start = dayjs().subtract(1, "year").startOf(aggregation);
  }

  const end = dayjs();

  for (let current = start; !current.isAfter(end); current = current.add(1, aggregation)) {
    usagePerTimeChunk.push({
      timeChunkLabel: current.format(formatterString),
      billedCost: 0,
      freeUsage: 0,
      startTime: current.format("YYYY-MM-DD"),
      endTime:
        aggregation === "day"
          ? current.add(1, "day").format("YYYY-MM-DD")
          : current.endOf(aggregation).format("YYYY-MM-DD"),
    });
  }

  // depending on the day of the week/month, we are likely
  // to get a partial first entry of data... we'll just toss it
  // since saying I am showing you March's data, but only providing 14 days worth of data
  // seems like a bad idea
  if (aggregation === "week") {
    return usagePerTimeChunk.slice(-26);
  }
  if (aggregation === "month") {
    return usagePerTimeChunk.slice(-12);
  }

  return usagePerTimeChunk;
};

/**
 * if there is no consumption for a given time chunk (in this case, day) we will not receive a data point
 * however, we still want to include that day on our graph, so we create an array with an entry for each time window
 * then backfill it with the data from the API.
 */

const mergeUsageData = (usageArray: UsagePerTimeChunk, consumption: ConsumptionRead) => {
  const timeframeItemIndex = usageArray.findIndex((item) => {
    // first two params are the start and end of the timeframe
    // final param makes the compare inclusive
    const isBetween = dayjs(consumption.startTime as ConfigType).isBetween(
      dayjs(item.startTime),
      dayjs(item.endTime),
      "day",
      "[)"
    );

    return isBetween;
  });

  if (timeframeItemIndex !== -1) {
    const usage = usageArray[timeframeItemIndex];
    usage.billedCost += consumption.billedCost;
    usage.freeUsage += consumption.freeUsage;
  }
};
export const calculateFreeAndPaidUsageByConnection = (
  filteredConsumptionData: ConsumptionRead[],
  timeWindow: ConsumptionTimeWindow
) => {
  if (filteredConsumptionData.length === 0) {
    return [];
  }
  const usagePerConnection = filteredConsumptionData.reduce(
    (allConsumption, consumption) => {
      const { connection } = consumption;

      // if this connection isn't in our list yet, add it
      // also, generate an array for the usage array
      if (!allConsumption[connection.connectionId]) {
        allConsumption[connection.connectionId] = {
          connection,
          totalFreeUsage: consumption.freeUsage,
          totalBilledCost: consumption.billedCost,
          totalUsage: consumption.freeUsage + consumption.billedCost,
          usage: generateArrayForTimeWindow(timeWindow),
        };
      } else {
        allConsumption[connection.connectionId].totalFreeUsage += consumption.freeUsage;
        allConsumption[connection.connectionId].totalBilledCost += consumption.billedCost;
        allConsumption[connection.connectionId].totalUsage += consumption.freeUsage + consumption.billedCost;
      }

      mergeUsageData(allConsumption[connection.connectionId].usage, consumption);

      return allConsumption;
    },
    {} as Record<string, ConnectionFreeAndPaidUsage>
  );

  const array = Object.values(usagePerConnection);
  return array;
};

// currently assumes a default time window of 30 days and no other conditions (yet)
export const calculateFreeAndPaidUsageByTimeChunk = (
  filteredConsumptionData: ConsumptionRead[],
  timeWindow: ConsumptionTimeWindow
) => {
  if (filteredConsumptionData.length === 0) {
    return [];
  }

  const usagePerTimeChunk = generateArrayForTimeWindow(timeWindow);

  filteredConsumptionData.forEach((consumption) => {
    mergeUsageData(usagePerTimeChunk, consumption);
  });

  return usagePerTimeChunk;
};
