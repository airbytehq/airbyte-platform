import { useInfiniteQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useCallback, useLayoutEffect } from "react";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentUser } from "core/services/auth";

import {
  createWorkspace,
  deleteWorkspace,
  getWorkspace,
  listUsersInWorkspace,
  listWorkspaces,
  listWorkspacesByUser,
  updateWorkspace,
  updateWorkspaceName,
  webBackendGetWorkspaceState,
} from "../generated/AirbyteClient";
import { SCOPE_USER, SCOPE_WORKSPACE } from "../scopes";
import {
  WorkspaceCreate,
  WorkspaceRead,
  WorkspaceReadList,
  WorkspaceUpdate,
  WorkspaceUpdateName,
} from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const workspaceKeys = {
  all: [SCOPE_USER, "workspaces"] as const,
  lists: () => [...workspaceKeys.all, "list"] as const,
  list: (filters: string | Record<string, string>) => [...workspaceKeys.lists(), { filters }] as const,
  allListUsers: [SCOPE_WORKSPACE, "users", "list"] as const,
  listUsers: (workspaceId: string) => [SCOPE_WORKSPACE, "users", "list", workspaceId] as const,
  detail: (workspaceId: string) => [...workspaceKeys.all, "details", workspaceId] as const,
  state: (workspaceId: string) => [...workspaceKeys.all, "state", workspaceId] as const,
};

export const useCurrentWorkspace = () => {
  const workspaceId = useCurrentWorkspaceId();

  return useGetWorkspace(workspaceId, {
    staleTime: Infinity,
  });
};

export const getCurrentWorkspaceStateQueryKey = (workspaceId: string) => {
  return workspaceKeys.state(workspaceId);
};

export const useCreateWorkspace = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(async (workspaceCreate: WorkspaceCreate) => createWorkspace(workspaceCreate, requestOptions), {
    onSuccess: (result) => {
      queryClient.setQueryData<WorkspaceReadList>(workspaceKeys.lists(), (old) => ({
        workspaces: [result, ...(old?.workspaces ?? [])],
      }));
    },
  });
};

export const useDeleteWorkspace = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(async (workspaceId: string) => deleteWorkspace({ workspaceId }, requestOptions), {
    onSuccess: (_, workspaceId) => {
      queryClient.setQueryData<WorkspaceReadList>(workspaceKeys.lists(), (old) =>
        old ? { workspaces: old.workspaces.filter((workspace) => workspace.workspaceId !== workspaceId) } : undefined
      );
    },
  });
};

export const useGetCurrentWorkspaceStateQuery = (workspaceId: string) => {
  const requestOptions = useRequestOptions();
  return () => webBackendGetWorkspaceState({ workspaceId }, requestOptions);
};

export const useCurrentWorkspaceState = () => {
  const workspaceId = useCurrentWorkspaceId();
  const queryKey = getCurrentWorkspaceStateQueryKey(workspaceId);
  const queryFn = useGetCurrentWorkspaceStateQuery(workspaceId);

  return useSuspenseQuery(queryKey, queryFn, {
    // We want to keep this query only shortly in cache, so we refetch
    // the data whenever the user might have changed sources/destinations/connections
    // without requiring to manually invalidate that query on each change.
    cacheTime: 5 * 1000,
  });
};

export const useInvalidateWorkspaceStateQuery = () => {
  const queryClient = useQueryClient();
  const workspaceId = useCurrentWorkspaceId();
  return useCallback(() => {
    queryClient.invalidateQueries(workspaceKeys.state(workspaceId));
  }, [queryClient, workspaceId]);
};

// todo: after merging https://github.com/airbytehq/airbyte-platform-internal/pull/7779 this should get workspace by user id
export const useListWorkspaces = () => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(workspaceKeys.lists(), () => listWorkspaces(requestOptions));
};

export const getListWorkspacesAsyncQueryKey = () => {
  return workspaceKeys.lists();
};

export const useListWorkspacesAsyncQuery = () => {
  const requestOptions = useRequestOptions();

  return () => listWorkspaces(requestOptions);
};

export const getWorkspaceQueryKey = (workspaceId: string) => {
  return workspaceKeys.detail(workspaceId);
};

export const useGetWorkspaceQuery = (workspaceId: string) => {
  const requestOptions = useRequestOptions();
  return () => getWorkspace({ workspaceId }, requestOptions);
};

export const useGetWorkspace = (
  workspaceId: string,
  options?: Parameters<typeof useSuspenseQuery<WorkspaceRead>>[2]
) => {
  const queryKey = getWorkspaceQueryKey(workspaceId);
  const queryFn = useGetWorkspaceQuery(workspaceId);

  return useSuspenseQuery(queryKey, queryFn, options);
};

export const useListUsersInWorkspace = (workspaceId: string) => {
  const requestOptions = useRequestOptions();
  const queryKey = workspaceKeys.listUsers(workspaceId);

  return useSuspenseQuery(queryKey, () => listUsersInWorkspace({ workspaceId }, requestOptions));
};

export const useListWorkspacesInfinite = (pageSize: number, nameContains: string, suspense?: boolean) => {
  const { userId } = useCurrentUser();
  const requestOptions = useRequestOptions();

  return useInfiniteQuery(
    workspaceKeys.list({ pageSize: pageSize.toString(), nameContains }),
    async ({ pageParam = 0 }) => {
      const rowOffset = pageParam * pageSize;
      return {
        data: await listWorkspacesByUser({ userId, pagination: { pageSize, rowOffset }, nameContains }, requestOptions),
        pageParam,
      };
    },
    {
      suspense: suspense ?? false,
      getPreviousPageParam: (firstPage) => (firstPage.pageParam > 0 ? firstPage.pageParam - 1 : undefined),
      getNextPageParam: (lastPage) => (lastPage.data.workspaces.length < pageSize ? undefined : lastPage.pageParam + 1),
      cacheTime: 10000,
    }
  );
};

export const useUpdateWorkspace = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation((workspace: WorkspaceUpdate) => updateWorkspace(workspace, requestOptions), {
    onSuccess: (data) => {
      queryClient.setQueryData<WorkspaceRead>(workspaceKeys.detail(data.workspaceId), data);
      queryClient.setQueryData<WorkspaceReadList>(workspaceKeys.lists(), (old) => {
        return {
          workspaces: old?.workspaces.map((workspace) => {
            return workspace.workspaceId === data.workspaceId ? data : workspace;
          }) ?? [data],
        };
      });
    },
  });
};

export const useUpdateWorkspaceName = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    (workspaceUpdateNameBody: WorkspaceUpdateName) => updateWorkspaceName(workspaceUpdateNameBody, requestOptions),
    {
      onSuccess: (data) => {
        queryClient.setQueryData<WorkspaceRead>(workspaceKeys.detail(data.workspaceId), data);
        queryClient.setQueryData<WorkspaceReadList>(workspaceKeys.lists(), (old) => {
          return {
            workspaces: old?.workspaces.map((workspace) => {
              return workspace.workspaceId === data.workspaceId ? data : workspace;
            }) ?? [data],
          };
        });
      },
    }
  );
};

export const useInvalidateWorkspace = (workspaceId: string) => {
  const queryClient = useQueryClient();

  return useCallback(
    () => queryClient.invalidateQueries(workspaceKeys.detail(workspaceId)),
    [queryClient, workspaceId]
  );
};

export const useInvalidateAllWorkspaceScope = () => {
  const queryClient = useQueryClient();

  return useCallback(() => {
    queryClient.removeQueries([SCOPE_WORKSPACE]);
  }, [queryClient]);
};

export const useInvalidateAllWorkspaceScopeOnChange = (workspaceId: string) => {
  const invalidateWorkspaceScope = useInvalidateAllWorkspaceScope();
  // useLayoutEffect so the query removal happens before the new workspace's queries are triggered
  useLayoutEffect(() => {
    invalidateWorkspaceScope();
  }, [invalidateWorkspaceScope, workspaceId]);
};
