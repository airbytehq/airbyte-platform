import dayjs from "dayjs";
import { createContext, useContext } from "react";

import { useFilters, useOrganizationUsage } from "core/api";
import { ConsumptionTimeWindow, IndividualWorkspaceUsageRead, TimeframeUsage } from "core/api/types/AirbyteClient";
import { generateArrayForTimeWindow, UsagePerTimeChunk } from "packages/cloud/area/billing/utils/chartUtils";

export interface WorkspaceUsage {
  workspace: IndividualWorkspaceUsageRead["workspace"];
  usage: UsagePerTimeChunk;
  totalFreeUsage: number;
  totalBilledCost: number;
  totalInternalUsage: number;
  totalUsage: number;
}

interface CreditsUsageContext {
  freeAndPaidUsageByTimeChunk: UsagePerTimeChunk;
  freeAndPaidUsageByWorkspace: WorkspaceUsage[];
  selectedTimeWindow: ConsumptionTimeWindow;
  setSelectedTimeWindow: (timeWindow: ConsumptionTimeWindow) => void;
  hasFreeUsage: boolean;
  hasInternalUsage: boolean;
}

export const creditsUsageContext = createContext<CreditsUsageContext | null>(null);

export const useOrganizationCreditsContext = (): CreditsUsageContext => {
  const creditsUsageHelpers = useContext(creditsUsageContext);
  if (!creditsUsageHelpers) {
    throw new Error("useOrganizationCreditsContext must be used within OrganizationCreditUsageContextProvider");
  }
  return creditsUsageHelpers;
};

export function getWorkspaceUsageByConnection(
  organizationUsage: IndividualWorkspaceUsageRead[],
  timeWindow: ConsumptionTimeWindow
): WorkspaceUsage[] {
  return organizationUsage.map((usage) => {
    const workspaceUsage: WorkspaceUsage = {
      workspace: usage.workspace,
      usage: generateArrayForTimeWindow(timeWindow),
      totalFreeUsage: 0,
      totalBilledCost: 0,
      totalInternalUsage: 0,
      totalUsage: 0,
    };

    usage.usage.free.forEach((freeUsage) => {
      appendTimeframeUsage(workspaceUsage.usage, freeUsage, "freeUsage", timeWindow);
    });
    usage.usage.regular.forEach((regularUsage) => {
      appendTimeframeUsage(workspaceUsage.usage, regularUsage, "billedCost", timeWindow);
    });
    usage.usage.internal.forEach((internalUsage) => {
      appendTimeframeUsage(workspaceUsage.usage, internalUsage, "internalUsage", timeWindow);
    });

    workspaceUsage.usage.forEach((timeframe) => {
      workspaceUsage.totalFreeUsage += timeframe.freeUsage;
      workspaceUsage.totalInternalUsage += timeframe.internalUsage;
      workspaceUsage.totalBilledCost += timeframe.billedCost;
      workspaceUsage.totalUsage += timeframe.freeUsage + timeframe.billedCost + timeframe.internalUsage;
    });

    return workspaceUsage;
  });
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

function getOrganizationUsageByTimeChunk(
  filteredWorkspaceUsage: IndividualWorkspaceUsageRead[],
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

export const OrganizationCreditUsageContextProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [filters, setFilterValue] = useFilters<{ selectedTimeWindow: ConsumptionTimeWindow }>({
    selectedTimeWindow: ConsumptionTimeWindow.lastMonth,
  });
  const { selectedTimeWindow } = filters;
  const organizationUsage = useOrganizationUsage({ timeWindow: selectedTimeWindow });

  const freeAndPaidUsageByTimeChunk = getOrganizationUsageByTimeChunk(organizationUsage.data, selectedTimeWindow);
  const freeAndPaidUsageByWorkspace = getWorkspaceUsageByConnection(organizationUsage.data, selectedTimeWindow);

  const contextValue = {
    freeAndPaidUsageByTimeChunk,
    freeAndPaidUsageByWorkspace,
    selectedTimeWindow,
    setSelectedTimeWindow: (selectedTimeWindow: ConsumptionTimeWindow) =>
      setFilterValue("selectedTimeWindow", selectedTimeWindow),
    hasFreeUsage: freeAndPaidUsageByTimeChunk.some((usage) => usage.freeUsage > 0),
    hasInternalUsage: freeAndPaidUsageByTimeChunk.some((usage) => usage.internalUsage > 0),
  };

  return <creditsUsageContext.Provider value={contextValue}>{children}</creditsUsageContext.Provider>;
};
