import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { AuthGetAccessToken } from "core/services/auth";
import { isCorporateEmail } from "core/utils/freeEmailProviders";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { getUtmFromStorage } from "core/utils/utmStorage";

import { useGetInstanceConfiguration } from "./instanceConfiguration";
import { getOrCreateUserByAuthId, getUser, updateUser } from "../generated/AirbyteClient";
import { UserUpdate } from "../types/AirbyteClient";
import { emptyGetAccessToken, useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const userKeys = {
  all: ["users"] as const,
  detail: (id: string) => [...userKeys.all, id] as const,
};

// In Community, we do not need to pass an access token to get the current user. This function can be passed in place of an actual getAccessToken callback.
export const useGetDefaultUser = () => {
  const { defaultUserId: userId } = useGetInstanceConfiguration();

  return useSuspenseQuery(userKeys.detail(userId), () => getUser({ userId }, { getAccessToken: emptyGetAccessToken }));
};

export const useGetDefaultUserAsync = () => {
  const { defaultUserId: userId } = useGetInstanceConfiguration();
  return useMutation(
    () =>
      getUser(
        { userId },
        {
          getAccessToken: emptyGetAccessToken,
          // Currently this is only used in the Simple Auth flow, so we need to include cookies
          includeCredentials: true,
        }
      ),
    {
      retry: false,
    }
  );
};

/**
 * This mutation is called during the authentication process. It's used after the user has authenticated
 * and we have a valid JWT, but before the auth context is updated to include that JWT. For that reason,
 * we pass getAccessToken explicitly here to attach to the authenticated request. This allows us to make
 * a single update to the auth context containing both the JWT and the airbyte user.
 */
export const useGetOrCreateUser = () => {
  const analytics = useAnalyticsService();
  const [, setIsNewSignup] = useLocalStorage("airbyte_new-signup", false);

  return useMutation(({ authUserId, getAccessToken }: { authUserId: string; getAccessToken: () => Promise<string> }) =>
    getOrCreateUserByAuthId({ authUserId }, { getAccessToken }).then(({ newUserCreated, userRead }) => {
      if (newUserCreated) {
        setIsNewSignup(true);
        analytics.track(Namespace.USER, Action.CREATE, {
          actionDescription: "New user registered",
          user_id: authUserId,
          name: userRead.name,
          email: userRead.email,
          isCorporate: isCorporateEmail(userRead.email),
          ...getUtmFromStorage(),
        });
      }
      return userRead;
    })
  );
};

export const useGetUser = (userId: string) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(userKeys.detail(userId), () => getUser({ userId }, requestOptions));
};

export const useGetUserAsync = (userId: string) => {
  const requestOptions = useRequestOptions();
  return useQuery(userKeys.detail(userId), () => getUser({ userId }, requestOptions));
};

export const useUpdateUser = () => {
  const queryClient = useQueryClient();

  return useMutation(
    ({ userUpdate, getAccessToken }: { userUpdate: UserUpdate; getAccessToken: AuthGetAccessToken }) =>
      updateUser(userUpdate, { getAccessToken }),
    {
      onSuccess(user) {
        queryClient.setQueryData(userKeys.detail(user.userId), user);
      },
    }
  );
};
