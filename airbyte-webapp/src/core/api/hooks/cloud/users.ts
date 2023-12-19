import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useCallback, useMemo } from "react";

import { useCurrentWorkspaceId } from "area/workspace/utils";

import {
  webBackendRevokeUserFromWorkspace,
  webBackendResendWithSigninLink,
  webBackendInviteUserToWorkspaceWithSignInLink,
  webBackendListUsersByWorkspace,
  updateUser,
  webBackendRevokeUserSession,
} from "../../generated/CloudApi";
import { SCOPE_WORKSPACE } from "../../scopes";
import { UserUpdate } from "../../types/CloudApi";
import { useRequestOptions } from "../../useRequestOptions";
import { useSuspenseQuery } from "../../useSuspenseQuery";

export const useGetUserService = () => {
  const requestOptions = useRequestOptions();
  const update = useCallback((params: UserUpdate) => updateUser(params, requestOptions), [requestOptions]);

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
      update,
      revokeUserSession,
      remove,
      resendWithSignInLink,
      invite,
      listByWorkspaceId,
    }),
    [update, revokeUserSession, remove, resendWithSignInLink, invite, listByWorkspaceId]
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

/**
 * This mutation is called during the authentication process. It's used after the user has authenticated
 * and we have a valid JWT, but before the auth context is updated to include that JWT. For that reason,
 * we pass getAccessToken explicitly here to attach to the authenticated request. This allows us to make
 * a single update to the auth context containing both the JWT and the airbyte user.
 */
export const useUpdateUser = () => {
  return useMutation(
    ({ userUpdate, getAccessToken }: { userUpdate: UserUpdate; getAccessToken: () => Promise<string> }) =>
      updateUser({ ...userUpdate }, { getAccessToken })
  );
};

export const useRevokeUserSession = () => {
  return useMutation(({ getAccessToken }: { getAccessToken: () => Promise<string> }) =>
    webBackendRevokeUserSession({ getAccessToken })
  );
};

export const useResendSigninLink = () => {
  return useMutation((email: string) =>
    webBackendResendWithSigninLink(
      { email, continueUrl: window.location.href },
      // This is an unsecured endpoint, so we do not need to pass an access token
      { getAccessToken: () => Promise.resolve(null) }
    )
  );
};
