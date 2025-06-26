import { useQueries } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { getListPermissionsQueryKey, useListPermissionsQuery } from "core/api";
import { useCurrentUser } from "core/services/auth";

import {
  getCurrentWorkspaceStateQueryKey,
  getWorkspaceQueryKey,
  useGetCurrentWorkspaceStateQuery,
  useGetWorkspaceQuery,
} from "../workspaces";

/**
 * Warms the react-query cache with data that is most likely needed for the given workspace
 */
export const usePrefetchWorkspaceData = () => {
  const workspaceId = useCurrentWorkspaceId();
  const user = useCurrentUser();

  const queries = useQueries({
    queries: [
      {
        queryKey: getWorkspaceQueryKey(workspaceId),
        queryFn: useGetWorkspaceQuery(workspaceId),
        suspense: true,
        staleTime: 10000,
        enabled: Boolean(workspaceId),
      },
      {
        queryKey: getCurrentWorkspaceStateQueryKey(workspaceId),
        queryFn: useGetCurrentWorkspaceStateQuery(workspaceId),
        suspense: true,
        staleTime: 10000,
        enabled: Boolean(workspaceId),
      },
      {
        queryKey: getListPermissionsQueryKey(user.userId),
        queryFn: useListPermissionsQuery(user.userId),
        suspense: true,
        staleTime: 10000,
        enabled: Boolean(workspaceId),
      },
    ],
  });

  return queries;
};
