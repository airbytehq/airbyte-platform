import { Dispatch, SetStateAction, createContext, useContext, useMemo, useState } from "react";

import { Option } from "components/ui/ListBox";

import { DestinationId, SourceId } from "core/request/AirbyteClient";
import { useGetCloudWorkspaceUsage } from "packages/cloud/services/workspaces/CloudWorkspacesService";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";
import { getIcon } from "utils/imageUtils";

import { calculateAvailableSourcesAndDestinations } from "./calculateAvailableSourcesAndDestinations";
import {
  ConnectionFreeAndPaidUsage,
  UsagePerTimeframe,
  calculateFreeAndPaidUsageByConnection,
  calculateFreeAndPaidUsageByTimeframe,
} from "./calculateUsageDataObjects";

export interface AvailableSource {
  id: string;
  icon: string;
  name: string;
  connectedDestinations: string[];
}

export interface AvailableDestination {
  id: string;
  icon: string;
  name: string;
  connectedSources: string[];
}

interface CreditsUsageContext {
  freeAndPaidUsageByTimeframe: UsagePerTimeframe;
  freeAndPaidUsageByConnection: ConnectionFreeAndPaidUsage[];
  sourceOptions: Array<Option<string>>;
  destinationOptions: Array<Option<string>>;
  selectedSource: SourceId | null;
  selectedDestination: DestinationId | null;
  setSelectedSource: Dispatch<SetStateAction<string | null>>;
  setSelectedDestination: Dispatch<SetStateAction<string | null>>;
}

export const creditsUsageContext = createContext<CreditsUsageContext | null>(null);

export const useCreditsContext = (): CreditsUsageContext => {
  const creditsUsageHelpers = useContext(creditsUsageContext);
  if (!creditsUsageHelpers) {
    throw new Error("useConnectorForm should be used within ConnectorFormContextProvider");
  }
  return creditsUsageHelpers;
};

export const CreditsUsageContextProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { workspaceId } = useCurrentWorkspace();
  const data = useGetCloudWorkspaceUsage(workspaceId);
  const { consumptionPerConnectionPerTimeframe: rawConsumptionData } = data;

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
      return {
        label: source.name,
        value: source.id,
        icon: getIcon(source.icon),
        disabled: !selectedDestination ? false : !source.connectedDestinations.includes(selectedDestination),
      };
    });
  }, [availableSourcesAndDestinations.sources, selectedDestination]);

  const destinationOptions = useMemo(() => {
    return availableSourcesAndDestinations.destinations.map((destination) => {
      return {
        label: destination.name,
        value: destination.id,
        icon: getIcon(destination.icon),
        disabled: !selectedSource ? false : !destination.connectedSources.includes(selectedSource),
      };
    });
  }, [availableSourcesAndDestinations.destinations, selectedSource]);
  return (
    <creditsUsageContext.Provider
      value={{
        freeAndPaidUsageByTimeframe: calculateFreeAndPaidUsageByTimeframe(filteredConsumptionData),
        freeAndPaidUsageByConnection: calculateFreeAndPaidUsageByConnection(filteredConsumptionData),
        sourceOptions,
        destinationOptions,
        selectedSource,
        setSelectedSource,
        selectedDestination,
        setSelectedDestination,
      }}
    >
      {children}
    </creditsUsageContext.Provider>
  );
};
