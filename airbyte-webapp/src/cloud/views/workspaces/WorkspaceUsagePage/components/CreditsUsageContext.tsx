import { createContext, useContext, useMemo } from "react";

import { Option } from "components/ui/ListBox";

import { UsagePerTimeChunk } from "cloud/area/billing/utils/chartUtils";
import { useFilters } from "core/api";
import { useGetWorkspaceUsage } from "core/api/cloud";
import { ConsumptionTimeWindow, DestinationId, SourceId, SupportLevel } from "core/api/types/AirbyteClient";

import {
  ConnectionFreeAndPaidUsage,
  getWorkspaceUsageByTimeChunk,
  getWorkspaceUsageByConnection,
} from "./calculateUsageDataObjects";
import { ConnectorOptionLabel } from "./ConnectorOptionLabel";

export interface AvailableSource {
  id: string;
  icon: string;
  name: string;
  supportLevel: SupportLevel;
  custom: boolean;
  connectedDestinations: string[];
}

export interface AvailableDestination {
  id: string;
  icon: string;
  name: string;
  supportLevel: SupportLevel;
  custom: boolean;
  connectedSources: string[];
}

interface CreditsUsageContext {
  freeAndPaidUsageByTimeChunk: UsagePerTimeChunk;
  freeAndPaidUsageByConnection: ConnectionFreeAndPaidUsage[];
  sourceOptions: Array<Option<string>>;
  destinationOptions: Array<Option<string>>;
  selectedSource: SourceId | null;
  selectedDestination: DestinationId | null;
  setSelectedSource: (sourceId: SourceId | null) => void;
  setSelectedDestination: (destinationId: DestinationId | null) => void;
  selectedTimeWindow: ConsumptionTimeWindow;
  setSelectedTimeWindow: (timeWindow: ConsumptionTimeWindow) => void;
  hasFreeUsage: boolean;
  hasInternalUsage: boolean;
}

export const creditsUsageContext = createContext<CreditsUsageContext | null>(null);

export const useCreditsContext = (): CreditsUsageContext => {
  const creditsUsageHelpers = useContext(creditsUsageContext);
  if (!creditsUsageHelpers) {
    throw new Error("useCreditsContext should be used within CreditsUsageContextProvider");
  }
  return creditsUsageHelpers;
};

interface FilterValues {
  selectedTimeWindow: ConsumptionTimeWindow;
  selectedSource: SourceId | null;
  selectedDestination: DestinationId | null;
}

export const WorkspaceCreditUsageContextProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [filters, setFilterValue] = useFilters<FilterValues>({
    selectedTimeWindow: ConsumptionTimeWindow.lastMonth,
    selectedSource: null,
    selectedDestination: null,
  });
  const { selectedTimeWindow, selectedSource, selectedDestination } = filters;
  const workspaceUsage = useGetWorkspaceUsage({ timeWindow: selectedTimeWindow });

  const sourceOptions = useMemo(() => {
    const optionsMap = new Map<string, Option<string>>();
    workspaceUsage.data.forEach((usage) => {
      if (selectedDestination) {
        // Only show sources that have a connection to the selected destination
        if (selectedDestination === usage.destination.destinationId) {
          optionsMap.set(usage.source.sourceId, {
            label: <ConnectorOptionLabel connector={{ name: usage.source.name, icon: usage.source.icon ?? "" }} />,
            value: usage.source.sourceId,
          });
        }
      } else {
        optionsMap.set(usage.source.sourceId, {
          label: <ConnectorOptionLabel connector={{ name: usage.source.name, icon: usage.source.icon ?? "" }} />,
          value: usage.source.sourceId,
        });
      }
    });
    return Array.from(optionsMap.values());
  }, [workspaceUsage, selectedDestination]);

  const destinationOptions = useMemo(() => {
    const optionsMap = new Map<string, Option<string>>();
    workspaceUsage.data.forEach((usage) => {
      if (selectedSource) {
        // Only show destinations that have a connection to the selected source
        if (selectedSource === usage.source.sourceId) {
          optionsMap.set(usage.destination.destinationId, {
            label: (
              <ConnectorOptionLabel connector={{ name: usage.destination.name, icon: usage.destination.icon ?? "" }} />
            ),
            value: usage.destination.destinationId,
          });
        }
      } else {
        optionsMap.set(usage.destination.destinationId, {
          label: (
            <ConnectorOptionLabel connector={{ name: usage.destination.name, icon: usage.destination.icon ?? "" }} />
          ),
          value: usage.destination.destinationId,
        });
      }
    });
    return Array.from(optionsMap.values());
  }, [workspaceUsage, selectedSource]);

  const filteredWorkspaceUsageData = useMemo(() => {
    if (selectedSource && selectedDestination) {
      return workspaceUsage.data.filter(
        (consumption) =>
          consumption.source.sourceId === selectedSource &&
          consumption.destination.destinationId === selectedDestination
      );
    } else if (selectedSource) {
      return workspaceUsage.data.filter((consumption) => consumption.source.sourceId === selectedSource);
    } else if (selectedDestination) {
      return workspaceUsage.data.filter((consumption) => consumption.destination.destinationId === selectedDestination);
    }

    return workspaceUsage.data;
  }, [workspaceUsage, selectedDestination, selectedSource]);

  const freeAndPaidUsageByTimeChunk = getWorkspaceUsageByTimeChunk(filteredWorkspaceUsageData, selectedTimeWindow);
  const freeAndPaidUsageByConnection = getWorkspaceUsageByConnection(filteredWorkspaceUsageData, selectedTimeWindow);

  const contextValue = {
    freeAndPaidUsageByTimeChunk,
    freeAndPaidUsageByConnection,
    sourceOptions,
    destinationOptions,
    selectedSource,
    setSelectedSource: (selectedSource: SourceId | null) => setFilterValue("selectedSource", selectedSource),
    selectedDestination,
    setSelectedDestination: (selectedDestination: SourceId | null) =>
      setFilterValue("selectedDestination", selectedDestination),
    selectedTimeWindow,
    setSelectedTimeWindow: (selectedTimeWindow: ConsumptionTimeWindow) =>
      setFilterValue("selectedTimeWindow", selectedTimeWindow),
    hasFreeUsage: freeAndPaidUsageByTimeChunk.some((usage) => usage.freeUsage > 0),
    hasInternalUsage: freeAndPaidUsageByTimeChunk.some((usage) => usage.internalUsage && usage.internalUsage > 0),
  };

  return <creditsUsageContext.Provider value={contextValue}>{children}</creditsUsageContext.Provider>;
};
