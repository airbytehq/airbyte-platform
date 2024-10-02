import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useIntl } from "react-intl";

import { useNotificationService } from "hooks/services/Notification";

import { organizationKeys } from "./organizations";
import { workspaceKeys } from "./workspaces";
import { HttpError } from "../errors";
import {
  createPermission,
  listPermissionsByUser,
  deletePermission,
  updatePermission,
} from "../generated/AirbyteClient";
import { PermissionCreate, PermissionRead, PermissionUpdate } from "../generated/AirbyteClient.schemas";
import { SCOPE_USER } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const permissionKeys = {
  all: [SCOPE_USER, "permissions"] as const,
  listByUser: (userId?: string) => [...permissionKeys.all, "listByUser", ...(userId ? [userId] : [])] as const,
};

export const getListPermissionsQueryKey = (userId?: string) => {
  return permissionKeys.listByUser(userId);
};

export const useListPermissionsQuery = (userId: string) => {
  const requestOptions = useRequestOptions();
  return () => listPermissionsByUser({ userId }, requestOptions);
};

export const useListPermissions = (userId: string) => {
  return useSuspenseQuery(getListPermissionsQueryKey(userId), useListPermissionsQuery(userId), {
    staleTime: 60_000,
  });
};

export const useUpdatePermissions = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  return useMutation(
    (permission: PermissionUpdate) => {
      return updatePermission(permission, requestOptions);
    },
    {
      onSuccess: () => {
        registerNotification({
          id: "settings.accessManagement.permissionUpdate.success",
          text: formatMessage({ id: "settings.accessManagement.permissionUpdate.success" }),
          type: "success",
        });
        queryClient.invalidateQueries(organizationKeys.allListUsers);
        queryClient.invalidateQueries(workspaceKeys.allListAccessUsers);
      },
      onError: (e) => {
        registerNotification({
          id: "settings.accessManagement.permissionUpdate.error",
          text:
            e instanceof HttpError && e.status === 409
              ? e.response.message
              : formatMessage({ id: "settings.accessManagement.permissionUpdate.error" }),
          type: "error",
        });
      },
    }
  );
};

export const useCreatePermission = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();

  const { registerNotification } = useNotificationService();

  return useMutation(
    (permission: PermissionCreate): Promise<PermissionRead> => {
      return createPermission(permission, requestOptions);
    },
    {
      onSuccess: (data: PermissionRead) => {
        registerNotification({
          id: "settings.accessManagement.permissionCreate.success",
          text: formatMessage({ id: "userInvitations.create.success.directlyAdded" }),
          type: "success",
        });
        if (data.organizationId) {
          queryClient.invalidateQueries(organizationKeys.listUsers(data.organizationId));
        }
        queryClient.invalidateQueries(workspaceKeys.allListAccessUsers);
      },
      onError: () => {
        registerNotification({
          id: "settings.accessManagement.permissionCreate.error",
          text: formatMessage({ id: "userInvitations.create.error" }),
          type: "error",
        });
      },
    }
  );
};

export const useDeletePermissions = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  const { formatMessage } = useIntl();

  const { registerNotification } = useNotificationService();

  return useMutation(async (permissionId: string) => deletePermission({ permissionId }, requestOptions), {
    onSuccess: () => {
      registerNotification({
        id: "settings.accessManagement.permissionDelete.success",
        text: formatMessage({ id: "settings.accessManagement.permissionDelete.success" }),
        type: "success",
      });
      queryClient.invalidateQueries(organizationKeys.allListUsers);
      queryClient.invalidateQueries(workspaceKeys.allListAccessUsers);
    },
    onError: () => {
      registerNotification({
        id: "settings.accessManagement.permissionDelete.error",
        text: formatMessage({ id: "settings.accessManagement.permissionDelete.error" }),
        type: "error",
      });
    },
  });
};
