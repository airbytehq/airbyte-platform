import dayjs from "dayjs";
import { Dispatch, SetStateAction, createContext, useContext, useMemo, useState } from "react";

import { Option } from "components/ui/ListBox";

import { useCurrentWorkspace } from "core/api";
import { useGetCloudWorkspaceUsage } from "core/api/cloud";
import { DestinationId, SourceId, SupportLevel } from "core/api/types/AirbyteClient";
import { ConsumptionTimeWindow } from "core/api/types/CloudApi";

import { calculateAvailableSourcesAndDestinations } from "./calculateAvailableSourcesAndDestinations";
import {
  ConnectionFreeAndPaidUsage,
  UsagePerTimeChunk,
  calculateFreeAndPaidUsageByTimeChunk,
  calculateFreeAndPaidUsageByConnection,
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
  setSelectedSource: Dispatch<SetStateAction<string | null>>;
  setSelectedDestination: Dispatch<SetStateAction<string | null>>;
  selectedTimeWindow: ConsumptionTimeWindow;
  setSelectedTimeWindow: Dispatch<SetStateAction<ConsumptionTimeWindow>>;
  hasFreeUsage: boolean;
}

export const creditsUsageContext = createContext<CreditsUsageContext | null>(null);

export const useCreditsContext = (): CreditsUsageContext => {
  const creditsUsageHelpers = useContext(creditsUsageContext);
  if (!creditsUsageHelpers) {
    throw new Error("useCreditsContext should be used within CreditsUsageContextProvider");
  }
  return creditsUsageHelpers;
};

export const CreditsUsageContextProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const [selectedTimeWindow, setSelectedTimeWindow] = useState<ConsumptionTimeWindow>(ConsumptionTimeWindow.lastMonth);
  const [hasFreeUsage, setHasFreeUsage] = useState<boolean>(false);

  const { workspaceId } = useCurrentWorkspace();
  const data = useGetCloudWorkspaceUsage(workspaceId, selectedTimeWindow);

  const { consumptionPerConnectionPerTimeframe, timeWindow } = data;

  const rawConsumptionData = useMemo(() => {
    return consumptionPerConnectionPerTimeframe.map((consumption) => {
      if (consumption.freeUsage > 0) {
        setHasFreeUsage(true);
      }

      return {
        ...consumption,
        startTime: dayjs(consumption.startTime).format("YYYY-MM-DD"),
        endTime: dayjs(consumption.endTime).format("YYYY-MM-DD"),
      };
    });
  }, [consumptionPerConnectionPerTimeframe]);

  const [selectedSource, setSelectedSource] = useState<SourceId | null>(null);
  const [selectedDestination, setSelectedDestination] = useState<DestinationId | null>(null);
  const availableSourcesAndDestinations = useMemo(
    () => calculateAvailableSourcesAndDestinations(rawConsumptionData),
    [rawConsumptionData]
  );

  const filteredConsumptionData = useMemo(() => {
    if (selectedSource && selectedDestination) {
      return rawConsumptionData.filter(
        (consumption) =>
          consumption.connection.sourceId === selectedSource &&
          consumption.connection.destinationId === selectedDestination
      );
    } else if (selectedSource) {
      return rawConsumptionData.filter((consumption) => consumption.connection.sourceId === selectedSource);
    } else if (selectedDestination) {
      return rawConsumptionData.filter((consumption) => consumption.connection.destinationId === selectedDestination);
    }

    return rawConsumptionData;
  }, [rawConsumptionData, selectedDestination, selectedSource]);

  const sourceOptions = useMemo(() => {
    return availableSourcesAndDestinations.sources.map((source) => {
      const disabled = !selectedDestination ? false : !source.connectedDestinations.includes(selectedDestination);
      return {
        label: <ConnectorOptionLabel connector={source} disabled={disabled} />,
        value: source.id,
        disabled,
      };
    });
  }, [availableSourcesAndDestinations.sources, selectedDestination]);

  const destinationOptions = useMemo(() => {
    return availableSourcesAndDestinations.destinations.map((destination) => {
      const disabled = !selectedSource ? false : !destination.connectedSources.includes(selectedSource);

      return {
        label: <ConnectorOptionLabel connector={destination} disabled={disabled} />,
        value: destination.id,
        disabled,
      };
    });
  }, [availableSourcesAndDestinations.destinations, selectedSource]);

  return (
    <creditsUsageContext.Provider
      value={{
        freeAndPaidUsageByTimeChunk: calculateFreeAndPaidUsageByTimeChunk(filteredConsumptionData, timeWindow),
        freeAndPaidUsageByConnection: calculateFreeAndPaidUsageByConnection(filteredConsumptionData, timeWindow),
        sourceOptions,
        destinationOptions,
        selectedSource,
        setSelectedSource,
        selectedDestination,
        setSelectedDestination,
        selectedTimeWindow,
        setSelectedTimeWindow,
        hasFreeUsage,
      }}
    >
      {children}
    </creditsUsageContext.Provider>
  );
};
