import { createContext, useContext } from "react";

import { sortStreamsAlphabetically } from "components/connection/StreamStatus/streamStatusUtils";
import { StreamStatusType } from "components/connection/StreamStatusIndicator";

import { useStreamsStatuses } from "area/connection/utils";
import { useCurrentConnection } from "core/api";

const useStreamsContextInit = (connectionId: string) => {
  const { enabledStreams, streamStatuses } = useStreamsStatuses(connectionId);
  const streamsByName = sortStreamsAlphabetically(enabledStreams, streamStatuses);

  const enabledStreamsByName = Object.entries(streamsByName)
    .filter(([status]) => status !== StreamStatusType.Paused)
    .flatMap(([_, stream]) => stream);

  return {
    enabledStreamsByName,
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
  const connection = useCurrentConnection();
  const streamsContext = useStreamsContextInit(connection.connectionId);

  return <StreamsContext.Provider value={streamsContext}>{children}</StreamsContext.Provider>;
};
