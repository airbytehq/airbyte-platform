import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useCallback, useMemo } from "react";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { webBackendRevokeUserFromWorkspace } from "core/api/generated/CloudApi";
import { webBackendResendWithSigninLink } from "core/api/generated/CloudApi";
import { webBackendInviteUserToWorkspaceWithSignInLink } from "core/api/generated/CloudApi";
import { webBackendListUsersByWorkspace } from "core/api/generated/CloudApi";
import { SCOPE_WORKSPACE } from "services/Scope";

import {
  createUser,
  getUserByAuthId,
  getUserByEmail,
  updateUser,
  webBackendRevokeUserSession,
} from "../../generated/CloudApi";
import { UserUpdate, UserCreate, AuthProvider } from "../../types/CloudApi";
import { useRequestOptions } from "../../useRequestOptions";
import { useSuspenseQuery } from "../../useSuspenseQuery";

export const useGetUserService = () => {
  const requestOptions = useRequestOptions();

  const getByEmail = useCallback((email: string) => getUserByEmail({ email }, requestOptions), [requestOptions]);

  const getByAuthId = useCallback(
    (authUserId: string, authProvider: AuthProvider) => getUserByAuthId({ authProvider, authUserId }, requestOptions),
    [requestOptions]
  );

  const update = useCallback((params: UserUpdate) => updateUser(params, requestOptions), [requestOptions]);

  const changeName = useCallback(
    (authUserId: string, userId: string, name: string) => updateUser({ authUserId, userId, name }, requestOptions),
    [requestOptions]
  );

  const create = useCallback((user: UserCreate) => createUser(user, requestOptions), [requestOptions]);

  const revokeUserSession = useCallback(() => webBackendRevokeUserSession(requestOptions), [requestOptions]);

  const remove = useCallback(
    (workspaceId: string, email: string) => webBackendRevokeUserFromWorkspace({ email, workspaceId }, requestOptions),
    [requestOptions]
  );

  const resendWithSignInLink = useCallback(
    ({ email }: { email: string }) =>
      webBackendResendWithSigninLink({ email, continueUrl: window.location.href }, requestOptions),
    [requestOptions]
  );

  const invite = useCallback(
    (
      users: Array<{
        email: string;
      }>,
      workspaceId: string
    ) =>
      Promise.all(
        users.map(async (user) =>
          webBackendInviteUserToWorkspaceWithSignInLink(
            {
              email: user.email,
              workspaceId,
              continueUrl: window.location.href,
            },
            requestOptions
          )
        )
      ),
    [requestOptions]
  );

  const listByWorkspaceId = useCallback(
    (workspaceId: string) => webBackendListUsersByWorkspace({ workspaceId }, requestOptions),
    [requestOptions]
  );

  return useMemo(
    () => ({
      getByEmail,
      getByAuthId,
      update,
      changeName,
      create,
      revokeUserSession,
      remove,
      resendWithSignInLink,
      invite,
      listByWorkspaceId,
    }),
    [
      getByEmail,
      getByAuthId,
      update,
      changeName,
      create,
      revokeUserSession,
      remove,
      resendWithSignInLink,
      invite,
      listByWorkspaceId,
    ]
  );
};

const userKeys = {
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
