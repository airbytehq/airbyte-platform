import { createContext, useContext, useMemo, useState } from "react";

import { useStreamsWithStatus } from "components/connection/StreamStatus/getStreamsWithStatus";
import {
  filterEmptyStreamStatuses,
  useSortStreams,
  StreamStatusType,
} from "components/connection/StreamStatus/streamStatusUtils";

import { JobWithAttemptsRead } from "core/request/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

const useStreamsContextInit = (jobs: JobWithAttemptsRead[]) => {
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

export const StreamsListContextProvider: React.FC<{ jobs: JobWithAttemptsRead[] }> = ({ children, jobs }) => {
  const streamsContext = useStreamsContextInit(jobs);

  return <StreamsContext.Provider value={streamsContext}>{children}</StreamsContext.Provider>;
};
