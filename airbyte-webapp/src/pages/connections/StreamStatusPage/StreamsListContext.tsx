import { createContext, useContext, useMemo, useState } from "react";

import { useStreamsWithStatus } from "components/connection/StreamStatus/getStreamsWithStatus";
import {
  filterEmptyStreamStatuses,
  useSortStreams,
  StreamStatusType,
} from "components/connection/StreamStatus/streamStatusUtils";

import { useListJobsForConnectionStatus } from "core/api";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

const useStreamsContextInit = (connectionId: string) => {
  const {
    data: { jobs },
  } = useListJobsForConnectionStatus(connectionId);
  const { connection } = useConnectionEditService();
  const [searchTerm, setSearchTerm] = useState("");

  const streamsWithStatus = useStreamsWithStatus(connection, jobs);
  const sortedStreams = useSortStreams(streamsWithStatus);

  const streams = useMemo(
    () =>
      filterEmptyStreamStatuses(sortedStreams)
        .filter(([status]) => status !== StreamStatusType.Disabled)
        .flatMap(([_, stream]) => stream),
    [sortedStreams]
  );

  const filteredStreams = useMemo(
    () => streams.filter((stream) => stream.stream?.name.includes(searchTerm)),
    [searchTerm, streams]
  );

  return {
    setSearchTerm,
    streams,
    filteredStreams,
    jobs,
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

export const StreamsListContextProvider: React.FC = ({ children }) => {
  const { connection } = useConnectionEditService();
  const streamsContext = useStreamsContextInit(connection.connectionId);

  return <StreamsContext.Provider value={streamsContext}>{children}</StreamsContext.Provider>;
};
