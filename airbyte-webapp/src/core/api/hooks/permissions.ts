import { SCOPE_USER } from "services/Scope";

import { listPermissionsByUser } from "../generated/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const permissionKeys = {
  all: [SCOPE_USER, "permissions"] as const,
  listByUser: (userId: string) => [...permissionKeys.all, "listByUser", userId] as const,
};

export const useListPermissions = (userId: string) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(permissionKeys.listByUser(userId), () => listPermissionsByUser({ userId }, requestOptions));
};
