import { useMutation, useQueryClient } from "@tanstack/react-query";

import { SCOPE_WORKSPACE } from "services/Scope";
import { useCurrentWorkspaceId } from "services/workspaces/WorkspacesService";

import { useGetUserService } from "./UserService";
import { useSuspenseQuery } from "../../../../services/connector/useSuspenseQuery";

export const userKeys = {
  all: [SCOPE_WORKSPACE, "users"] as const,
  lists: () => [...userKeys.all, "list"] as const,
  list: (filters: string) => [...userKeys.lists(), { filters }] as const,
  details: () => [...userKeys.all, "detail"] as const,
  detail: (id: number) => [...userKeys.details(), id] as const,
};

export const getListUsersQueryKey = (workspaceId: string) => {
  return userKeys.list(workspaceId);
};

export const useListUsersQuery = (workspaceId: string) => {
  const userService = useGetUserService();

  return () => userService.listByWorkspaceId(workspaceId);
};

export const useListUsers = () => {
  const workspaceId = useCurrentWorkspaceId();
  const queryKey = getListUsersQueryKey(workspaceId);
  const queryFn = useListUsersQuery(workspaceId);

  return useSuspenseQuery(queryKey, queryFn);
};

export const useUserHook = () => {
  const service = useGetUserService();
  const queryClient = useQueryClient();

  return {
    removeUserLogic: useMutation(
      async (payload: { email: string; workspaceId: string }) => service.remove(payload.workspaceId, payload.email),
      {
        onSuccess: async () => {
          await queryClient.invalidateQueries(userKeys.lists());
        },
      }
    ),
    inviteUserLogic: useMutation(
      async (payload: {
        users: Array<{
          email: string;
        }>;
        workspaceId: string;
      }) => service.invite(payload.users, payload.workspaceId),
      {
        onSuccess: async () => {
          await queryClient.invalidateQueries(userKeys.lists());
        },
      }
    ),
  };
};
