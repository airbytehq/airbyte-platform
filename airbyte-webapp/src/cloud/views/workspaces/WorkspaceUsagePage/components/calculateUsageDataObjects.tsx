import dayjs from "dayjs";

import { generateArrayForTimeWindow, UsagePerTimeChunk } from "cloud/area/billing/utils/chartUtils";
import {
  ConnectionSchedule,
  ConnectionScheduleType,
  ConnectionStatus,
  ConnectionUsageRead,
  ConsumptionTimeWindow,
  ReleaseStage,
  SupportLevel,
  TimeframeUsage,
} from "core/api/types/AirbyteClient";

interface ConnectionSummary {
  connectionId: string;
  connectionName: string;
  connectionScheduleType?: ConnectionScheduleType;
  connectionSchedule?: ConnectionSchedule;
  status: ConnectionStatus;
  sourceId: string;
  sourceConnectionName: string;
  sourceCustom: boolean;
  sourceIcon: string;
  sourceDefinitionId: string;
  sourceDefinitionName: string;
  sourceReleaseStage: ReleaseStage;
  sourceSupportLevel: SupportLevel;
  destinationId: string;
  destinationConnectionName: string;
  destinationCustom: boolean;
  destinationIcon: string;
  destinationDefinitionId: string;
  destinationDefinitionName: string;
  destinationReleaseStage: ReleaseStage;
  destinationSupportLevel: SupportLevel;
}

export interface ConnectionFreeAndPaidUsage {
  connection: ConnectionSummary;
  usage: UsagePerTimeChunk;
  totalFreeUsage: number;
  totalBilledCost: number;
  totalInternalUsage: number;
  totalUsage: number;
}

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
        connectionScheduleType: usage.connection.scheduleType,
        connectionSchedule: usage.connection.schedule,
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
