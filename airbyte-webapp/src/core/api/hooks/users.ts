import { useMutation } from "@tanstack/react-query";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { isCorporateEmail } from "core/utils/freeEmailProviders";
import { getUtmFromStorage } from "core/utils/utmStorage";

import { useGetInstanceConfiguration } from "./instanceConfiguration";
import { getOrCreateUserByAuthId, getUser } from "../generated/AirbyteClient";
import { useSuspenseQuery } from "../useSuspenseQuery";

const userKeys = {
  all: ["users"] as const,
  detail: (id: string) => [...userKeys.all, id] as const,
};

// In Community, we do not need to pass an access token to get the current user. This function can be passed in place of an actual getAccessToken callback.
const emptyGetAccesToken = () => Promise.resolve(null);

export const useGetDefaultUser = ({ getAccessToken }: { getAccessToken?: () => Promise<string | null> }) => {
  const { defaultUserId: userId } = useGetInstanceConfiguration();

  return useSuspenseQuery(userKeys.detail(userId), () =>
    getUser({ userId }, { getAccessToken: getAccessToken ?? emptyGetAccesToken })
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
          user_id: userRead.authUserId,
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
