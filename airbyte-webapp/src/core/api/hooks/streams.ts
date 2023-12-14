import { getStreamKey, sortStreamStatuses } from "area/connection/utils";

import { getStreamStatusesByRunState } from "../generated/AirbyteClient";
import { ConnectionIdRequestBody, StreamStatusRead, StreamStatusRunState } from "../generated/AirbyteClient.schemas";
import { SCOPE_WORKSPACE } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const useListStreamsStatuses = (listParams: ConnectionIdRequestBody, keepPreviousData = true) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(
    [SCOPE_WORKSPACE, "stream_statuses", "by_run_state", listParams.connectionId],
    () => getStreamStatusesByRunState(listParams, requestOptions),
    {
      refetchInterval: (data) => {
        if (data?.streamStatuses != null) {
          // first bucket stream statuses by stream
          const histories = new Map<string, StreamStatusRead[]>();
          for (let i = 0; i < data.streamStatuses.length; i++) {
            const streamStatus = data.streamStatuses[i];
            const streamKey = getStreamKey(streamStatus);
            const history = histories.get(streamKey) ?? [];
            history.push(streamStatus);
            histories.set(streamKey, history);
          }

          // then look at the most recent stream status for each stream
          // to see if it is running
          const historiesEntries = Array.from(histories);
          for (let i = 0; i < historiesEntries.length; i++) {
            const [, history] = historiesEntries[i];
            history.sort(sortStreamStatuses);
            if (history?.[0].runState === StreamStatusRunState.RUNNING) {
              return 2500; // 2.5s when running
            }
          }
        }

        return 10000; // 10s when no data, or no running streams
      },
      keepPreviousData,
    }
  );
};
