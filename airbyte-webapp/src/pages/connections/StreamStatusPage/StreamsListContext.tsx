import { createContext, useContext, useMemo, useState } from "react";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { sortStreamsAlphabetically, sortStreamsByStatus } from "components/connection/StreamStatus/streamStatusUtils";

import { useStreamsStatuses } from "area/connection/utils";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

const useStreamsContextInit = (connectionId: string) => {
  const [searchTerm, setSearchTerm] = useState("");

  const { enabledStreams, streamStatuses } = useStreamsStatuses(connectionId);
  const streamsByStatus = sortStreamsByStatus(enabledStreams, streamStatuses);
  const streamsByName = sortStreamsAlphabetically(enabledStreams, streamStatuses);

  const enabledStreamsByStatus = Object.entries(streamsByStatus)
    .filter(([status]) => status !== ConnectionStatusIndicatorStatus.Paused)
    .flatMap(([_, stream]) => stream);

  const enabledStreamsByName = Object.entries(streamsByName)
    .filter(([status]) => status !== ConnectionStatusIndicatorStatus.Paused)
    .flatMap(([_, stream]) => stream);

  /** deprecated... will remove with sync progress project */
  const filteredStreamsByStatus = useMemo(
    () => enabledStreamsByStatus.filter((stream) => stream.streamName.includes(searchTerm)),
    [searchTerm, enabledStreamsByStatus]
  );

  const filteredStreamsByName = useMemo(
    () => enabledStreamsByName.filter((stream) => stream.streamName.includes(searchTerm)),
    [enabledStreamsByName, searchTerm]
  );
  return {
    setSearchTerm,
    filteredStreamsByStatus,
    filteredStreamsByName,
  };
};

const StreamsContext = createContext<ReturnType<typeof useStreamsContextInit> | null>(null);

export const useStreamsListContext = () => {
  const context = useContext(StreamsContext);
  if (context === null) {
    throw new Error("useStreamsContext must be used within a StreamsContextProvider");
  }
  return context;
};

export const StreamsListContextProvider: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { connection } = useConnectionEditService();
  const streamsContext = useStreamsContextInit(connection.connectionId);

  return <StreamsContext.Provider value={streamsContext}>{children}</StreamsContext.Provider>;
};
