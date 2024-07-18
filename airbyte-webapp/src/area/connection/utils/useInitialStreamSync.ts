import isEqual from "lodash/isEqual";

import { StreamStatusJobType, StreamStatusRunState } from "core/api/types/AirbyteClient";

import { useStreamsStatuses } from "./useStreamsStatuses";
export const useInitialStreamSync = (connectionId: string) => {
  const { streamStatuses, enabledStreams } = useStreamsStatuses(connectionId);

  const streamsSyncingForFirstTime: string[] = [];

  streamStatuses.forEach((stream) => {
    const lastSuccessfulClear = stream.relevantHistory?.find(
      (status) => status.jobType === StreamStatusJobType.RESET && status.runState === StreamStatusRunState.COMPLETE
    );
    const lastSuccessfulSync = stream.relevantHistory?.find(
      (status) => status.jobType === StreamStatusJobType.SYNC && status.runState === StreamStatusRunState.COMPLETE
    );
    // if the stream has never synced before
    if (!lastSuccessfulSync) {
      streamsSyncingForFirstTime.push(stream.streamName);
    }
    // if most clear and was more recent than the most recent sync
    if ((lastSuccessfulClear?.transitionedAt ?? 0) > (lastSuccessfulSync?.transitionedAt ?? 0)) {
      streamsSyncingForFirstTime.push(stream.streamName);
    }
  });

  return {
    streamsSyncingForFirstTime,
    isConnectionInitialSync: isEqual(streamsSyncingForFirstTime.sort(), enabledStreams.sort()),
  };
};
