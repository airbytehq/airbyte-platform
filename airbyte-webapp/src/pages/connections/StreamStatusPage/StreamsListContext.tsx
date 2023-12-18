import { createContext, useContext, useMemo, useState } from "react";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { sortStreams } from "components/connection/StreamStatus/streamStatusUtils";

import { useStreamsStatuses } from "area/connection/utils";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

const useStreamsContextInit = (connectionId: string) => {
  const [searchTerm, setSearchTerm] = useState("");

  const { enabledStreams, streamStatuses } = useStreamsStatuses(connectionId);
  const sortedStreams = sortStreams(enabledStreams, streamStatuses);

  const streams = Object.entries(sortedStreams)
    .filter(([status]) => status !== ConnectionStatusIndicatorStatus.Disabled)
    .flatMap(([_, stream]) => stream);

  const filteredStreams = useMemo(
    () => streams.filter((stream) => stream.streamName.includes(searchTerm)),
    [searchTerm, streams]
  );

  return {
    setSearchTerm,
    filteredStreams,
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
