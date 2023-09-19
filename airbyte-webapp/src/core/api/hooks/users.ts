import { getUser } from "../generated/AirbyteClient";
import { useGetInstanceConfiguration, useSuspenseQuery } from "../index";
import { UserRead } from "../types/AirbyteClient";
import { UserRead as CloudUserRead } from "../types/CloudApi";
import { useRequestOptions } from "../useRequestOptions";

const userKeys = {
  all: ["users"] as const,
  detail: (id: string) => [...userKeys.all, id] as const,
};

export const useGetDefaultUser = (): UserRead & CloudUserRead => {
  const { defaultUserId: userId } = useGetInstanceConfiguration();
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(userKeys.detail(userId), () =>
    getUser({ userId }, requestOptions).then((user) => ({ ...user, defaultWorkspaceId: user.defaultWorkspaceId || "" }))
  );
};
