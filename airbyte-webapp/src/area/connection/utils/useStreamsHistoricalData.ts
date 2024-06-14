import { useMemo } from "react";

import { useGetLastJobPerStream } from "core/api";
import { ConnectionLastJobPerStreamReadItem, ConnectionStream } from "core/api/types/AirbyteClient";

import { getStreamKey } from "./computeStreamStatus";

interface HistoricalStreamData {
  historicalStreamsData: Map<string, ConnectionLastJobPerStreamReadItem>;
  isFetching: boolean;
}

export const useHistoricalStreamData = (connectionId: string, streams: ConnectionStream[]): HistoricalStreamData => {
  const { data: lastJobPerStreamData, isFetching } = useGetLastJobPerStream(connectionId, streams);
  const historicalStreamsData = useMemo(() => {
    return new Map(
      lastJobPerStreamData?.map((stream) => [
        getStreamKey({
          name: stream.streamName,
          namespace: stream.streamNamespace ?? "",
        }),
        stream,
      ])
    );
  }, [lastJobPerStreamData]);

  return { historicalStreamsData, isFetching };
};
