import { useInfiniteQuery, useMutation, useQueryClient } from "@tanstack/react-query";

import { useCurrentUser } from "core/services/auth";

import {
  deleteCloudWorkspace,
  updateCloudWorkspace,
  webBackendCreatePermissionedCloudWorkspace,
  webBackendListWorkspacesByUserPaginated,
} from "../../generated/CloudApi";
import { SCOPE_USER } from "../../scopes";
import { CloudWorkspaceRead, CloudWorkspaceReadList, PermissionedCloudWorkspaceCreate } from "../../types/CloudApi";
import { useRequestOptions } from "../../useRequestOptions";
import { useSuspenseQuery } from "../../useSuspenseQuery";
import { useListPermissions } from "../permissions";
import { useCurrentWorkspace } from "../workspaces";

export const workspaceKeys = {
  all: [SCOPE_USER, "cloud_workspaces"] as const,
  lists: () => [...workspaceKeys.all, "list"] as const,
  list: (filters: string | Record<string, string>) => [...workspaceKeys.lists(), { filters }] as const,
};

type CloudWorkspaceCount = { count: "zero" } | { count: "one"; workspace: CloudWorkspaceRead } | { count: "multiple" };

export const useCloudWorkspaceCount = () => {
  const { userId } = useCurrentUser();
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(
    workspaceKeys.list({ pageSize: "2", nameContains: "", rowOffset: "0" }),
    async (): Promise<CloudWorkspaceCount> =>
      webBackendListWorkspacesByUserPaginated(
        { userId, nameContains: "", pagination: { pageSize: 2, rowOffset: 0 } },
        requestOptions
      ).then((workspaces) => {
        if (workspaces.workspaces.length === 0) {
          return { count: "zero" };
        } else if (workspaces.workspaces.length === 1) {
          return { count: "one", workspace: workspaces.workspaces[0] };
        }
        return { count: "multiple" };
      })
  );
};

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

/**
 * Checks whether a user is in a foreign workspace. A foreign workspace is any workspace the user doesn't
 * have explicit permissions to via workspace permissions or being part of the organization the workspace is in.
 */
export const useIsForeignWorkspace = () => {
  const { userId } = useCurrentUser();
  const { permissions } = useListPermissions(userId);
  const { workspaceId, organizationId } = useCurrentWorkspace();

  return !permissions.some(
    (permission) => permission.workspaceId === workspaceId || permission.organizationId === organizationId
  );
};
