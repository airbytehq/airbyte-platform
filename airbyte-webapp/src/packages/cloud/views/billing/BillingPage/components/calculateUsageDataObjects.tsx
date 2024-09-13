import dayjs, { ConfigType, ManipulateType } from "dayjs";

import { ConnectionUsageRead, TimeframeUsage } from "core/api/types/AirbyteClient";
import { ConnectionProto, ConsumptionRead, ConsumptionTimeWindow } from "core/api/types/CloudApi";

export type UsagePerTimeChunk = Array<{
  timeChunkLabel: string;
  billedCost: number;
  freeUsage: number;
  internalUsage: number;
  startTime: string;
  endTime: string;
}>;

export interface ConnectionFreeAndPaidUsage {
  connection: ConnectionProto;
  usage: UsagePerTimeChunk;
  totalFreeUsage: number;
  totalBilledCost: number;
  totalInternalUsage: number;
  totalUsage: number;
}

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
          // Hard-coded to 0 because there is no internal usage in the old billing page, only in the new workspace usage page
          totalInternalUsage: 0,
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

// Used for the workspace usage page
export function getWorkspaceUsageByConnection(
  filteredWorkspaceUsage: ConnectionUsageRead[],
  timeWindow: ConsumptionTimeWindow
): ConnectionFreeAndPaidUsage[] {
  const usageByConnection: ConnectionFreeAndPaidUsage[] = [];

  filteredWorkspaceUsage.forEach((usage) => {
    const connectionUsage: ConnectionFreeAndPaidUsage = {
      connection: {
        connectionId: usage.connection.connectionId,
        connectionName: usage.connection.name,
        status: usage.connection.status,
        sourceId: usage.source.sourceId,
        sourceConnectionName: usage.source.name,
        sourceCustom: !!usage.sourceDefinition.custom,
        sourceIcon: usage.source.icon ?? "",
        sourceDefinitionId: usage.source.sourceDefinitionId,
        sourceDefinitionName: usage.sourceDefinition.name,
        sourceReleaseStage: usage.sourceDefinition.releaseStage ?? "custom",
        sourceSupportLevel: usage.sourceDefinition.supportLevel ?? "none",
        destinationId: usage.destination.destinationId,
        destinationConnectionName: usage.destination.name,
        destinationCustom: !!usage.destinationDefinition.custom,
        destinationIcon: usage.destination.icon ?? "",
        destinationDefinitionId: usage.destination.destinationDefinitionId,
        destinationDefinitionName: usage.destinationDefinition.name,
        destinationReleaseStage: usage.destinationDefinition.releaseStage ?? "custom",
        destinationSupportLevel: usage.destinationDefinition.supportLevel ?? "none",
      },
      usage: generateArrayForTimeWindow(timeWindow),
      totalFreeUsage: 0,
      totalBilledCost: 0,
      totalInternalUsage: 0,
      totalUsage: 0,
    };

    usage.usage.free.forEach((freeUsage) => {
      appendTimeframeUsage(connectionUsage.usage, freeUsage, "freeUsage", timeWindow);
    });
    usage.usage.regular.forEach((regularUsage) => {
      appendTimeframeUsage(connectionUsage.usage, regularUsage, "billedCost", timeWindow);
    });
    usage.usage.internal.forEach((internalUsage) => {
      appendTimeframeUsage(connectionUsage.usage, internalUsage, "internalUsage", timeWindow);
    });

    connectionUsage.usage.forEach((timeframe) => {
      connectionUsage.totalFreeUsage += timeframe.freeUsage;
      connectionUsage.totalBilledCost += timeframe.billedCost;
      connectionUsage.totalUsage += timeframe.freeUsage + timeframe.billedCost + timeframe.internalUsage;
      connectionUsage.totalInternalUsage += timeframe.internalUsage;
    });

    usageByConnection.push(connectionUsage);
  });

  return usageByConnection;
}

function appendTimeframeUsage(
  usagePerTimeChunk: UsagePerTimeChunk,
  timeframeUsage: TimeframeUsage,
  usageType: "freeUsage" | "billedCost" | "internalUsage",
  timeWindow: ConsumptionTimeWindow
) {
  const dateFormat = timeWindow === "lastYear" ? "MMM 'YY" : "MMM DD";
  const timeChunkLabel = dayjs(timeframeUsage.timeframeStart).format(dateFormat);
  const existingTimeChunk = usagePerTimeChunk.find((timeChunk) => timeChunk.timeChunkLabel === timeChunkLabel);

  if (existingTimeChunk) {
    existingTimeChunk[usageType] += timeframeUsage.quantity;
  } else {
    usagePerTimeChunk.push({
      timeChunkLabel,
      freeUsage: usageType === "freeUsage" ? timeframeUsage.quantity : 0,
      billedCost: usageType === "billedCost" ? timeframeUsage.quantity : 0,
      internalUsage: usageType === "internalUsage" ? timeframeUsage.quantity : 0,
      startTime: timeframeUsage.timeframeStart,
      endTime: timeframeUsage.timeframeEnd,
    });
  }
}

// Used for the workspace usage page
export function getWorkspaceUsageByTimeChunk(
  filteredWorkspaceUsage: ConnectionUsageRead[],
  timeWindow: ConsumptionTimeWindow
): UsagePerTimeChunk {
  const usagePerTimeChunk: UsagePerTimeChunk = generateArrayForTimeWindow(timeWindow);

  filteredWorkspaceUsage.forEach((usage) => {
    usage.usage.free.forEach((freeUsage) => {
      appendTimeframeUsage(usagePerTimeChunk, freeUsage, "freeUsage", timeWindow);
    });
    usage.usage.regular.forEach((regularUsage) => {
      appendTimeframeUsage(usagePerTimeChunk, regularUsage, "billedCost", timeWindow);
    });
    usage.usage.internal.forEach((internalUsage) => {
      appendTimeframeUsage(usagePerTimeChunk, internalUsage, "internalUsage", timeWindow);
    });
  });

  return usagePerTimeChunk;
}
