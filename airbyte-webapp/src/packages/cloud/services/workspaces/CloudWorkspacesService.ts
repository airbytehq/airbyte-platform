import { QueryObserverResult, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback } from "react";

import { MissingConfigError, useConfig } from "config";
import { CloudWorkspacesService } from "packages/cloud/lib/domain/cloudWorkspaces/CloudWorkspacesService";
import {
  CloudWorkspace,
  CloudWorkspaceUsage,
  ConsumptionTimeWindow,
} from "packages/cloud/lib/domain/cloudWorkspaces/types";
import { useCurrentUser } from "packages/cloud/services/auth/AuthService";
import { useSuspenseQuery } from "services/connector/useSuspenseQuery";
import { SCOPE_USER } from "services/Scope";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";
import { useInitService } from "services/useInitService";

export const workspaceKeys = {
  all: [SCOPE_USER, "cloud_workspaces"] as const,
  lists: () => [...workspaceKeys.all, "list"] as const,
  list: (filters: string) => [...workspaceKeys.lists(), { filters }] as const,
  details: () => [...workspaceKeys.all, "detail"] as const,
  detail: (id: number | string) => [...workspaceKeys.details(), id] as const,
  usage: (id: number | string, timeWindow: string) => [...workspaceKeys.all, id, timeWindow, "usage"] as const,
};

export function useGetWorkspaceService(): CloudWorkspacesService {
  const { cloudApiUrl } = useConfig();

  if (!cloudApiUrl) {
    throw new MissingConfigError("Missing required configuration cloudApiUrl");
  }

  const requestAuthMiddleware = useDefaultRequestMiddlewares();

  return useInitService(
    () => new CloudWorkspacesService(cloudApiUrl, requestAuthMiddleware),
    [cloudApiUrl, requestAuthMiddleware]
  );
}

export function useListCloudWorkspaces(): CloudWorkspace[] {
  const service = useGetWorkspaceService();
  const user = useCurrentUser();

  return useSuspenseQuery<CloudWorkspace[]>(workspaceKeys.lists(), () => service.listByUser(user.userId));
}

export const getListCloudWorkspacesAsyncQueryKey = () => {
  return workspaceKeys.lists();
};

export const useListCloudWorkspacesAsyncQuery = (userId: string) => {
  const service = useGetWorkspaceService();

  return () => service.listByUser(userId);
};

export function useListCloudWorkspacesAsync(): QueryObserverResult<CloudWorkspace[]> {
  const user = useCurrentUser();
  const queryKey = getListCloudWorkspacesAsyncQueryKey();
  const queryFn = useListCloudWorkspacesAsyncQuery(user.userId);

  return useQuery<CloudWorkspace[]>(queryKey, queryFn);
}

export function useCreateCloudWorkspace() {
  const service = useGetWorkspaceService();
  const queryClient = useQueryClient();
  const user = useCurrentUser();

  return useMutation(async (name: string) => service.create({ name, userId: user.userId }), {
    onSuccess: (result) => {
      queryClient.setQueryData<CloudWorkspace[]>(workspaceKeys.lists(), (old) => [...(old ?? []), result]);
    },
  });
}

export function useUpdateCloudWorkspace() {
  const service = useGetWorkspaceService();
  const queryClient = useQueryClient();

  return useMutation(
    async (payload: { workspaceId: string; name: string }) =>
      service.update(payload.workspaceId, { name: payload.name }),
    {
      onSuccess: (result) => {
        queryClient.setQueryData<CloudWorkspace[]>(workspaceKeys.lists(), (old) => {
          const list = old ?? [];
          if (list.length === 0) {
            return [result];
          }

          const index = list.findIndex((item) => item.workspaceId === result.workspaceId);

          if (index === -1) {
            return list;
          }

          return [...list.slice(0, index), result, ...list.slice(index + 1)];
        });

        queryClient.setQueryData<CloudWorkspace>(workspaceKeys.detail(result.workspaceId), (old) => {
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
  const service = useGetWorkspaceService();
  const queryClient = useQueryClient();

  return useMutation(async (workspaceId: string) => service.remove(workspaceId), {
    onSuccess: (_, workspaceId) => {
      queryClient.setQueryData<CloudWorkspace[] | undefined>(workspaceKeys.lists(), (old) =>
        old?.filter((workspace) => workspace.workspaceId !== workspaceId)
      );
    },
  });
}

export function getCloudWorkspaceQueryKey(workspaceId: string) {
  return workspaceKeys.detail(workspaceId);
}

export function useGetCloudWorkspaceQuery(workspaceId: string) {
  const service = useGetWorkspaceService();

  return () => service.get(workspaceId);
}

export function useGetCloudWorkspace(workspaceId: string): CloudWorkspace {
  const queryKey = getCloudWorkspaceQueryKey(workspaceId);
  const queryFn = useGetCloudWorkspaceQuery(workspaceId);

  return useSuspenseQuery<CloudWorkspace>(queryKey, queryFn);
}

export function useInvalidateCloudWorkspace(workspaceId: string): () => Promise<void> {
  const queryClient = useQueryClient();

  return useCallback(
    () => queryClient.invalidateQueries(workspaceKeys.detail(workspaceId)),
    [queryClient, workspaceId]
  );
}

export function useGetCloudWorkspaceUsage(workspaceId: string, timeWindow: ConsumptionTimeWindow): CloudWorkspaceUsage {
  const service = useGetWorkspaceService();

  return useSuspenseQuery<CloudWorkspaceUsage>(workspaceKeys.usage(workspaceId, timeWindow), () =>
    service.getUsage(workspaceId, timeWindow)
  );
}
