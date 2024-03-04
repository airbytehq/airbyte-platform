import { useQueries } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  getConnectionListQueryKey,
  getListPermissionsQueryKey,
  useConnectionListQuery,
  useListPermissionsQuery,
} from "core/api";
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
export const usePrefetchCloudWorkspaceData = () => {
  const workspaceId = useCurrentWorkspaceId();
  const user = useCurrentUser();

  const queries = useQueries({
    queries: [
      {
        queryKey: getWorkspaceQueryKey(workspaceId),
        queryFn: useGetWorkspaceQuery(workspaceId),
        suspense: true,
        staleTime: 10000,
      },
      {
        queryKey: getConnectionListQueryKey(),
        queryFn: useConnectionListQuery(workspaceId),
        suspense: true,
        staleTime: 10000,
      },
      {
        queryKey: getCurrentWorkspaceStateQueryKey(workspaceId),
        queryFn: useGetCurrentWorkspaceStateQuery(workspaceId),
        suspense: true,
        staleTime: 10000,
      },
      {
        queryKey: getListPermissionsQueryKey(user.userId),
        queryFn: useListPermissionsQuery(user.userId),
        suspense: true,
        staleTime: 10000,
      },
    ],
  });

  return queries;
};
