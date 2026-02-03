import dayjs, { ManipulateType } from "dayjs";

import { ConsumptionTimeWindow } from "core/api/types/AirbyteClient";

export type UsagePerTimeChunk = Array<{
  timeChunkLabel: string;
  billedCost: number;
  freeUsage: number;
  internalUsage: number;
  startTime: string;
  endTime: string;
}>;

export const generateArrayForTimeWindow = (timeWindow?: ConsumptionTimeWindow) => {
  const usagePerTimeChunk: UsagePerTimeChunk = [];

  // base case: lastMonth, which returns past 30 days of usage
  const end = dayjs();
  let start = end.subtract(29, "day");
  let aggregation: ManipulateType = "day";
  let formatterString = "MMM DD";

  if (timeWindow === "lastSixMonths") {
    aggregation = "week";
    formatterString = "MMM DD";
    start = end.subtract(6, "month").startOf(aggregation);
  } else if (timeWindow === "lastYear") {
    aggregation = "month";
    formatterString = "MMM 'YY";
    start = end.subtract(1, "year").startOf(aggregation);
  }

  for (let current = start; !current.isAfter(end.endOf(aggregation)); current = current.add(1, aggregation)) {
    usagePerTimeChunk.push({
      timeChunkLabel: current.format(formatterString),
      billedCost: 0,
      freeUsage: 0,
      internalUsage: 0,
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
