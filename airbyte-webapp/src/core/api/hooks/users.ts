import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import get from "lodash/get";
import set from "lodash/set";
import { useCallback } from "react";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { AuthGetAccessToken } from "core/services/auth";
import { isCorporateEmail } from "core/utils/freeEmailProviders";
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

  return useMutation(({ authUserId, getAccessToken }: { authUserId: string; getAccessToken: () => Promise<string> }) =>
    getOrCreateUserByAuthId({ authUserId }, { getAccessToken }).then(({ newUserCreated, userRead }) => {
      if (newUserCreated) {
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

// https://stackoverflow.com/questions/71096136/how-to-implement-a-gettypebypath-type-in-typescript
type Idx<T, K extends string | undefined> = K extends keyof T ? T[K] : never;
export type DeepIndex<T, K extends string | undefined> = T extends object
  ? K extends `${infer F}.${infer R}`
    ? DeepIndex<Idx<T, F>, R>
    : Idx<T, K>
  : never;

// patches over DeepIndex needing a string path but lodash's get doesn't
type OptionallyDeepIndex<T, K extends string | undefined> = K extends ""
  ? T
  : K extends undefined
  ? T
  : DeepIndex<T, K>;

// any aspect of the metadata object, including itself, can be undefined
type WithUndefined<T> = T extends never ? never : T | undefined;

export interface UserMetadata {}

export const useUserMetadata = <
  T extends string | undefined,
  MetadataType = WithUndefined<OptionallyDeepIndex<UserMetadata, T>>,
>(
  userId: string,
  path?: T
): [MetadataType, (nextValue: MetadataType) => void] => {
  const user = useGetUser(userId);
  const setMetaData = useSetUserMetadata(userId);

  const setter = useCallback(
    (nextValue: MetadataType) => {
      // @ts-expect-error TS doesn't like the fake currying here
      setMetaData(path, nextValue);
    },
    [path, setMetaData]
  );

  return [path ? get(user.metadata, path) : user.metadata, setter];
};

export const useSetUserMetadata = <T extends string | undefined>(userId: string) => {
  const { refetch: getUser } = useGetUserAsync(userId);
  const { mutateAsync: updateUser } = useUpdateUser();
  const { getAccessToken } = useRequestOptions();

  return useCallback(
    async (path: T, value: DeepIndex<UserMetadata, T>) => {
      const { data: user } = await getUser();
      if (user) {
        const metadata = path ? set(structuredClone(user.metadata ?? {}), path, value) : value;
        await updateUser({
          userUpdate: {
            userId,
            metadata,
          },
          getAccessToken,
        });
      }
    },
    [getAccessToken, getUser, updateUser, userId]
  );
};
