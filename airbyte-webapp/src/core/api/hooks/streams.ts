import { SCOPE_WORKSPACE } from "services/Scope";

import { getStreamStatusesByRunState } from "../generated/AirbyteClient";
import { ConnectionIdRequestBody } from "../generated/AirbyteClient.schemas";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const useListStreamsStatuses = (listParams: ConnectionIdRequestBody, keepPreviousData = true) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(
    [SCOPE_WORKSPACE, "stream_statuses", "by_run_state", listParams.connectionId],
    () => getStreamStatusesByRunState(listParams, requestOptions),
    {
      // 2.5 second refresh
      refetchInterval: 2500,
      keepPreviousData,
    }
  );
};
