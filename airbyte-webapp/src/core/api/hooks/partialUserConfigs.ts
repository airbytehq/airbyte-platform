import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useNotificationService } from "hooks/services/Notification";
import { ALLOWED_ORIGIN_PARAM } from "pages/embedded/EmbeddedSourceCreatePage/hooks/useEmbeddedSourceParams";

import {
  createPartialUserConfig,
  listPartialUserConfigs,
  getPartialUserConfig,
  updatePartialUserConfig,
  deletePartialUserConfig,
} from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { PartialUserConfigCreate, PartialUserConfigReadList, PartialUserConfigUpdate } from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const partialUserConfigs = {
  all: [SCOPE_ORGANIZATION, "partialUserConfigs"] as const,
  lists: () => [...partialUserConfigs.all, "list"],
  detail: (pasrtialUserConfigId: string) => [...partialUserConfigs.all, "details", pasrtialUserConfigId] as const,
};

export const useListPartialUserConfigs = (workspaceId: string): PartialUserConfigReadList => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(partialUserConfigs.lists(), () => {
    return listPartialUserConfigs({ workspaceId }, requestOptions);
  });
};

export const useGetPartialUserConfig = (partialUserConfigId: string) => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(partialUserConfigs.detail(partialUserConfigId), () => {
    return getPartialUserConfig({ partialUserConfigId, workspaceId }, requestOptions);
  });
};

export const useCreatePartialUserConfig = () => {
  const requestOptions = useRequestOptions();
  const { registerNotification } = useNotificationService();
  const [searchParams] = useSearchParams();
  const queryClient = useQueryClient();

  return useMutation(
    (partialUserConfigCreate: PartialUserConfigCreate) => {
      return createPartialUserConfig(partialUserConfigCreate, requestOptions);
    },
    {
      onSuccess: (data) => {
        queryClient.invalidateQueries({ queryKey: partialUserConfigs.lists() });

        const allowedOriginParam = searchParams.get(ALLOWED_ORIGIN_PARAM);
        const allowedOrigin = allowedOriginParam ? decodeURIComponent(allowedOriginParam) : "";

        if (allowedOrigin.length > 0) {
          const successMessage = {
            type: "end_user_action_result",
            message: "partial_user_config_created",
            data,
          };

          window.parent.postMessage(successMessage, allowedOrigin);
        }
      },
      onError: (error: Error) => {
        const allowedOriginParam = searchParams.get(ALLOWED_ORIGIN_PARAM);
        const allowedOrigin = allowedOriginParam ? decodeURIComponent(allowedOriginParam) : "";

        if (allowedOrigin.length > 0) {
          const errorMessage = {
            type: "end_user_action_result",
            message: "partial_user_config_create_error",
            error,
          };
          window.parent.postMessage(errorMessage, allowedOrigin);
        }

        registerNotification({
          id: "partial-user-config-create-error",
          type: "error",
          text: error.message,
        });
      },
    }
  );
};

export const useUpdatePartialUserConfig = () => {
  const requestOptions = useRequestOptions();
  const { registerNotification } = useNotificationService();
  const [searchParams] = useSearchParams();
  const queryClient = useQueryClient();
  return useMutation(
    (partialUserConfigUpdate: PartialUserConfigUpdate) => {
      return updatePartialUserConfig(partialUserConfigUpdate, requestOptions);
    },
    {
      onSuccess: (data, variables) => {
        queryClient.invalidateQueries({ queryKey: partialUserConfigs.lists() });
        queryClient.invalidateQueries({
          queryKey: partialUserConfigs.detail(variables.partialUserConfigId),
        });

        const allowedOriginParam = searchParams.get(ALLOWED_ORIGIN_PARAM);
        const allowedOrigin = allowedOriginParam ? decodeURIComponent(allowedOriginParam) : "";
        if (allowedOrigin.length > 0) {
          const successMessage = {
            type: "end_user_action_result",
            message: "partial_user_config_updated",
            data,
          };

          window.parent.postMessage(successMessage, allowedOrigin);
        }
      },
      onError: (error: Error) => {
        const allowedOriginParam = searchParams.get(ALLOWED_ORIGIN_PARAM);
        const allowedOrigin = allowedOriginParam ? decodeURIComponent(allowedOriginParam) : "";
        if (allowedOrigin.length > 0) {
          const errorMessage = {
            type: "end_user_action_result",
            message: "partial_user_config_update_error",
            error,
          };

          window.parent.postMessage(errorMessage, allowedOrigin);
        }
        registerNotification({
          id: "partial-user-config-update-error",
          type: "error",
          text: error.message,
        });
      },
    }
  );
};

export const useDeletePartialUserConfig = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  const workspaceId = useCurrentWorkspaceId();
  return useMutation(
    (partialUserConfigId: string) => {
      return deletePartialUserConfig({ partialUserConfigId, workspaceId }, requestOptions);
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: partialUserConfigs.lists() });
      },
    }
  );
};
