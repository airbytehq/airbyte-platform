import { useQueries } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { getConnectionListQueryKey, useConnectionListQuery } from "core/api";
import { useCurrentUser } from "core/services/auth";

import { getListCloudWorkspacesAsyncQueryKey, useListCloudWorkspacesAsyncQuery } from "./cloudWorkspaces";
import { getListUsersQueryKey, useListUsersQuery } from "./users";
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
  const user = useCurrentUser();
  const workspaceId = useCurrentWorkspaceId();

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
        queryKey: getListUsersQueryKey(workspaceId),
        queryFn: useListUsersQuery(workspaceId),
        suspense: true,
        staleTime: 10000,
      },
      {
        queryKey: getListCloudWorkspacesAsyncQueryKey(),
        queryFn: useListCloudWorkspacesAsyncQuery(user.userId),
        suspense: true,
        staleTime: 10000,
      },
    ],
  });

  return queries;
};
