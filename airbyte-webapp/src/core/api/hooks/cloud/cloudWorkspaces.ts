import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import { useCurrentUser } from "core/services/auth";

import {
  deleteCloudWorkspace,
  getCloudWorkspace,
  getCloudWorkspaceUsage,
  updateCloudWorkspace,
  webBackendCreatePermissionedCloudWorkspace,
  webBackendListWorkspacesByUser,
  webBackendListWorkspacesByUserPaginated,
} from "../../generated/CloudApi";
import { SCOPE_USER } from "../../scopes";
import {
  CloudWorkspaceRead,
  CloudWorkspaceReadList,
  ConsumptionTimeWindow,
  PermissionedCloudWorkspaceCreate,
} from "../../types/CloudApi";
import { useRequestOptions } from "../../useRequestOptions";
import { useSuspenseQuery } from "../../useSuspenseQuery";
import { useListPermissions } from "../permissions";
import { useCurrentWorkspace } from "../workspaces";

export const workspaceKeys = {
  all: [SCOPE_USER, "cloud_workspaces"] as const,
  lists: () => [...workspaceKeys.all, "list"] as const,
  list: (filters: string | Record<string, string>) => [...workspaceKeys.lists(), { filters }] as const,
  detail: (id: number | string) => [...workspaceKeys.all, "detail", id] as const,
  usage: (id: number | string, timeWindow: string) => [...workspaceKeys.all, id, timeWindow, "usage"] as const,
};

export function useListCloudWorkspaces() {
  const requestOptions = useRequestOptions();
  const user = useCurrentUser();

  return useSuspenseQuery(workspaceKeys.lists(), () =>
    webBackendListWorkspacesByUser({ userId: user.userId }, requestOptions)
  );
}

export function useCreateCloudWorkspace() {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const user = useCurrentUser();

  return useMutation(
    async (values: Omit<PermissionedCloudWorkspaceCreate, "userId">) =>
      webBackendCreatePermissionedCloudWorkspace({ ...values, userId: user.userId }, requestOptions),
    {
      onSuccess: (result) => {
        queryClient.setQueryData<CloudWorkspaceReadList>(workspaceKeys.lists(), (old) => ({
          workspaces: [result, ...(old?.workspaces ?? [])],
        }));
      },
    }
  );
}

export const useListCloudWorkspacesInfinite = (pageSize: number, nameContains: string) => {
  const { userId } = useCurrentUser();
  const requestOptions = useRequestOptions();

  return useInfiniteQuery(
    workspaceKeys.list({ pageSize: pageSize.toString(), nameContains }),
    async ({ pageParam = 0 }: { pageParam?: number }) => {
      return {
        data: await webBackendListWorkspacesByUserPaginated(
          { userId, pagination: { pageSize, rowOffset: pageParam * pageSize }, nameContains },
          requestOptions
        ),
        pageParam,
      };
    },
    {
      suspense: false,
      getPreviousPageParam: (firstPage) => (firstPage.pageParam > 0 ? firstPage.pageParam - 1 : undefined),
      getNextPageParam: (lastPage) => (lastPage.data.workspaces.length < pageSize ? undefined : lastPage.pageParam + 1),
      cacheTime: 10000,
    }
  );
};

export function useUpdateCloudWorkspace() {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    async (payload: { workspaceId: string; name: string }) =>
      updateCloudWorkspace({ workspaceId: payload.workspaceId, name: payload.name }, requestOptions),
    {
      onSuccess: (result) => {
        queryClient.setQueryData<CloudWorkspaceReadList>(workspaceKeys.lists(), (old) => {
          const list = old?.workspaces ?? [];
          if (list.length === 0) {
            return { workspaces: [result] };
          }

          const index = list.findIndex((item) => item.workspaceId === result.workspaceId);

          if (index === -1) {
            return { workspaces: list };
          }

          return { workspaces: [...list.slice(0, index), result, ...list.slice(index + 1)] };
        });

        queryClient.setQueryData<CloudWorkspaceRead>(workspaceKeys.detail(result.workspaceId), (old) => {
          return {
            ...old,
            ...result,
          };
        });
      },
    }
  );
}

export function useRemoveCloudWorkspace() {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(async (workspaceId: string) => deleteCloudWorkspace({ workspaceId }, requestOptions), {
    onSuccess: (_, workspaceId) => {
      queryClient.setQueryData<CloudWorkspaceReadList>(workspaceKeys.lists(), (old) =>
        old ? { workspaces: old.workspaces.filter((workspace) => workspace.workspaceId !== workspaceId) } : undefined
      );
    },
  });
}

export function getCloudWorkspaceQueryKey(workspaceId: string) {
  return workspaceKeys.detail(workspaceId);
}

export function useGetCloudWorkspaceQuery(workspaceId: string) {
  const requestOptions = useRequestOptions();

  return () => getCloudWorkspace({ workspaceId }, requestOptions);
}

export function useGetCloudWorkspace(workspaceId: string) {
  const queryKey = getCloudWorkspaceQueryKey(workspaceId);
  const queryFn = useGetCloudWorkspaceQuery(workspaceId);

  return useSuspenseQuery(queryKey, queryFn);
}

export function useGetCloudWorkspaceAsync(workspaceId: string) {
  const queryKey = getCloudWorkspaceQueryKey(workspaceId);
  const queryFn = useGetCloudWorkspaceQuery(workspaceId);

  return useQuery(queryKey, queryFn).data;
}

export function useInvalidateCloudWorkspace(workspaceId: string): () => Promise<void> {
  const queryClient = useQueryClient();

  return useCallback(
    () => queryClient.invalidateQueries(workspaceKeys.detail(workspaceId)),
    [queryClient, workspaceId]
  );
}

export function useGetCloudWorkspaceUsage(workspaceId: string, timeWindow: ConsumptionTimeWindow) {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(workspaceKeys.usage(workspaceId, timeWindow), () =>
    getCloudWorkspaceUsage({ workspaceId, timeWindow }, requestOptions)
  );
}

/**
 * Checks whether a user is in a foreign workspace. A foreign workspace is any workspace the user doesn't
 * have explicit permissions to via workspace permissions or being part of the organization the workspace is in.
 */
export const useIsForeignWorkspace = () => {
  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);
  const { workspaceId, organizationId } = useCurrentWorkspace();

  return !permissions.some(
    (permission) =>
      permission.workspaceId === workspaceId || (organizationId && permission.organizationId === organizationId)
  );
};
