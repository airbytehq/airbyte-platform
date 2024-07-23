import { useMemo } from "react";

import { useGetLastJobPerStream } from "core/api";
import { ConnectionLastJobPerStreamReadItem } from "core/api/types/AirbyteClient";

import { getStreamKey } from "./computeStreamStatus";

interface HistoricalStreamData {
  historicalStreamsData: Map<string, ConnectionLastJobPerStreamReadItem>;
  isFetching: boolean;
}

export const useHistoricalStreamData = (connectionId: string): HistoricalStreamData => {
  const { data: lastJobPerStreamData, isFetching } = useGetLastJobPerStream(connectionId);
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
