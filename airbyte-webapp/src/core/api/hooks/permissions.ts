import { useMutation, useQueryClient } from "@tanstack/react-query";

import { SCOPE_USER } from "services/Scope";

import { organizationKeys } from "./organizations";
import { workspaceKeys } from "./workspaces";
import { listPermissionsByUser } from "../generated/AirbyteClient";
import { deletePermission, updatePermission } from "../generated/AirbyteClient";
import { PermissionRead, PermissionUpdate } from "../generated/AirbyteClient.schemas";
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

export const useUpdatePermissions = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(
    (permission: PermissionUpdate): Promise<PermissionRead> => {
      return updatePermission(permission, requestOptions);
    },
    {
      onSuccess: (data: PermissionRead) => {
        if (data.organizationId) {
          queryClient.invalidateQueries(organizationKeys.listUsers(data.organizationId));
        }
        if (data.workspaceId) {
          queryClient.invalidateQueries(workspaceKeys.listUsers(data.workspaceId));
        }
      },
    }
  );
};

export const useDeletePermissions = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation(async (permissionId: string) => deletePermission({ permissionId }, requestOptions), {
    onSuccess: () => {
      queryClient.invalidateQueries(organizationKeys.allListUsers);
      queryClient.invalidateQueries(workspaceKeys.allListUsers);
    },
  });
};
