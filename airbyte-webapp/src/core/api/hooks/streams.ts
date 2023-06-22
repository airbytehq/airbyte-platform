import { SCOPE_WORKSPACE } from "services/Scope";

import { getStreamStatuses } from "../generated/AirbyteClient";
import { StreamStatusListRequestBody } from "../generated/AirbyteClient.schemas";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const useListStreamsStatuses = (listParams: StreamStatusListRequestBody, keepPreviousData = true) => {
  const requestOptions = useRequestOptions();
  const { workspaceId, connectionId, pagination, ...queryKey } = listParams;
  return useSuspenseQuery(
    [SCOPE_WORKSPACE, "stream_statuses", "list", workspaceId, connectionId, pagination, queryKey],
    () => getStreamStatuses(listParams, requestOptions),
    {
      // 2.5 second refresh
      refetchInterval: 2500,
      keepPreviousData,
    }
  );
};
