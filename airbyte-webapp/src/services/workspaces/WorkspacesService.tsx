import { useMutation, useQueryClient, useQueries } from "@tanstack/react-query";
import React, { useCallback, useContext, useLayoutEffect, useMemo } from "react";
import { useNavigate, useMatch } from "react-router-dom";

import {
  getCloudWorkspaceQueryKey,
  getListCloudWorkspacesAsyncQueryKey,
  useGetCloudWorkspaceQuery,
  useListCloudWorkspacesAsyncQuery,
} from "core/api/cloud";
import { Workspace, WorkspaceService } from "core/domain/workspace";
import { getConnectionListQueryKey, useConnectionListQuery } from "hooks/services/useConnectionHook";
import { useCurrentUser } from "packages/cloud/services/auth/AuthService";
import { getListUsersQueryKey, useListUsersQuery } from "packages/cloud/services/users/UseUserHook";
import { RoutePaths } from "pages/routePaths";

import { useConfig } from "../../config";
import { WorkspaceUpdate } from "../../core/request/AirbyteClient";
import { useSuspenseQuery } from "../connector/useSuspenseQuery";
import { SCOPE_USER, SCOPE_WORKSPACE } from "../Scope";
import { useDefaultRequestMiddlewares } from "../useDefaultRequestMiddlewares";
import { useInitService } from "../useInitService";

export const workspaceKeys = {
  all: [SCOPE_USER, "workspaces"] as const,
  lists: () => [...workspaceKeys.all, "list"] as const,
  list: (filters: string) => [...workspaceKeys.lists(), { filters }] as const,
  detail: (workspaceId: string) => [...workspaceKeys.all, "details", workspaceId] as const,
  state: (workspaceId: string) => [...workspaceKeys.all, "state", workspaceId] as const,
};

interface Context {
  selectWorkspace: (workspaceId?: string | null | Workspace) => void;
  exitWorkspace: () => void;
}

export const WorkspaceServiceContext = React.createContext<Context | null>(null);

const useSelectWorkspace = (): ((workspace?: string | null | Workspace) => void) => {
  const navigate = useNavigate();

  return useCallback(
    async (workspace) => {
      if (typeof workspace === "object") {
        navigate(`/${RoutePaths.Workspaces}/${workspace?.workspaceId}`);
      } else {
        navigate(`/${RoutePaths.Workspaces}/${workspace}`);
      }
    },
    [navigate]
  );
};

export const WorkspaceServiceProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const selectWorkspace = useSelectWorkspace();

  const ctx = useMemo<Context>(
    () => ({
      selectWorkspace,
      exitWorkspace: () => {
        selectWorkspace("");
      },
    }),
    [selectWorkspace]
  );

  return <WorkspaceServiceContext.Provider value={ctx}>{children}</WorkspaceServiceContext.Provider>;
};

export const useWorkspaceService = (): Context => {
  const workspaceService = useContext(WorkspaceServiceContext);
  if (!workspaceService) {
    throw new Error("useWorkspaceService must be used within a WorkspaceServiceProvider.");
  }

  return workspaceService;
};

function useWorkspaceApiService() {
  const config = useConfig();
  const middlewares = useDefaultRequestMiddlewares();
  return useInitService(() => new WorkspaceService(config.apiUrl, middlewares), [config.apiUrl, middlewares]);
}

export const useCurrentWorkspaceId = () => {
  const match = useMatch(`/${RoutePaths.Workspaces}/:workspaceId/*`);
  return match?.params.workspaceId || "";
};

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
        queryKey: getCloudWorkspaceQueryKey(workspaceId),
        queryFn: useGetCloudWorkspaceQuery(workspaceId),
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

export const useCurrentWorkspace = () => {
  const workspaceId = useCurrentWorkspaceId();

  return useGetWorkspace(workspaceId, {
    staleTime: Infinity,
  });
};

const getCurrentWorkspaceStateQueryKey = (workspaceId: string) => {
  return workspaceKeys.state(workspaceId);
};

const useGetCurrentWorkspaceStateQuery = (workspaceId: string) => {
  const service = useWorkspaceApiService();
  return () => service.getState({ workspaceId });
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

export const useListWorkspaces = () => {
  const service = useWorkspaceApiService();

  return useSuspenseQuery(workspaceKeys.lists(), () => service.list()).workspaces;
};

const getWorkspaceQueryKey = (workspaceId: string) => {
  return workspaceKeys.detail(workspaceId);
};

const useGetWorkspaceQuery = (workspaceId: string) => {
  const service = useWorkspaceApiService();

  return () => service.get({ workspaceId });
};

export const useGetWorkspace = (
  workspaceId: string,
  options?: {
    staleTime: number;
  }
) => {
  const queryKey = getWorkspaceQueryKey(workspaceId);
  const queryFn = useGetWorkspaceQuery(workspaceId);

  return useSuspenseQuery(queryKey, queryFn, options);
};

export const useUpdateWorkspace = () => {
  const service = useWorkspaceApiService();
  const queryClient = useQueryClient();

  return useMutation((workspace: WorkspaceUpdate) => service.update(workspace), {
    onSuccess: (data) => {
      queryClient.setQueryData(workspaceKeys.detail(data.workspaceId), data);
    },
  });
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
