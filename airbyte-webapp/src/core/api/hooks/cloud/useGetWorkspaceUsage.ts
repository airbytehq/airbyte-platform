import { useCurrentWorkspaceId } from "area/workspace/utils";
import { getWorkspaceUsage } from "core/api/generated/AirbyteClient";
import { ConsumptionTimeWindow } from "core/api/generated/AirbyteClient.schemas";
import { useRequestOptions } from "core/api/useRequestOptions";
import { useSuspenseQuery } from "core/api/useSuspenseQuery";

import { workspaceKeys } from "../workspaces";

export const useGetWorkspaceUsage = ({ timeWindow }: { timeWindow: ConsumptionTimeWindow }) => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(workspaceKeys.usage(workspaceId, timeWindow), () =>
    getWorkspaceUsage({ workspaceId, timeWindow }, requestOptions)
  );
};
