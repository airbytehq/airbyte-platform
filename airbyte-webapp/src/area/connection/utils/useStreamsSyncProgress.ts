import { useMemo } from "react";

import { useGetConnectionSyncProgress } from "core/api";
import { StreamSyncProgressReadItem } from "core/api/types/AirbyteClient";

import { getStreamKey } from "./computeStreamStatus";

export const useStreamsSyncProgress = (
  connectionId: string,
  isRunning: boolean
): Map<string, StreamSyncProgressReadItem> => {
  const { data: connectionSyncProgress } = useGetConnectionSyncProgress(connectionId, isRunning);

  const syncProgressMap = useMemo(() => {
    if (isRunning !== true) {
      return new Map();
    }

    return new Map(
      connectionSyncProgress?.streams.map((stream) => [
        getStreamKey({
          name: stream.streamName,
          namespace: stream.streamNamespace ?? "",
        }),
        stream,
      ])
    );
  }, [connectionSyncProgress?.streams, isRunning]);

  return syncProgressMap;
};
