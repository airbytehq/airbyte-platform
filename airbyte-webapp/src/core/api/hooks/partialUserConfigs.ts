import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";

import { useNotificationService } from "hooks/services/Notification";
import { ALLOWED_ORIGIN_PARAM } from "pages/embedded/EmbeddedSourceCreatePage/hooks/useEmbeddedSourceParams";

import {
  createEmbeddedSources,
  listEmbeddedSources,
  getEmbeddedSourcesId,
  updateEmbeddedSourcesId,
  deleteEmbeddedSourcesId,
} from "../generated/SonarClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { SourceCreateRequest, SourceGetResponse, SourceListResponse, SourceUpdateRequest } from "../types/SonarClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const partialUserConfigs = {
  all: [SCOPE_ORGANIZATION, "partialUserConfigs"] as const,
  lists: () => [...partialUserConfigs.all, "list"],
  detail: (partialUserConfigId: string) => [...partialUserConfigs.all, "details", partialUserConfigId] as const,
};

export const useListPartialUserConfigs = (workspace_id: string): SourceListResponse => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(partialUserConfigs.lists(), () => {
    return listEmbeddedSources({ workspace_id }, requestOptions);
  });
};

export const useGetPartialUserConfig = (partialUserConfigId: string): SourceGetResponse => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(partialUserConfigs.detail(partialUserConfigId), () => {
    return getEmbeddedSourcesId(partialUserConfigId, requestOptions);
  });
};

export const useCreatePartialUserConfig = () => {
  const requestOptions = useRequestOptions();
  const { registerNotification } = useNotificationService();
  const [searchParams] = useSearchParams();
  const queryClient = useQueryClient();

  return useMutation(
    (partialUserConfigCreate: SourceCreateRequest) => {
      return createEmbeddedSources(partialUserConfigCreate, requestOptions);
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
    async ({ id, partialUserConfigUpdate }: { id: string; partialUserConfigUpdate: SourceUpdateRequest }) => {
      return updateEmbeddedSourcesId(id, partialUserConfigUpdate, requestOptions);
    },
    {
      onSuccess: (data, variables) => {
        queryClient.invalidateQueries({ queryKey: partialUserConfigs.lists() });
        queryClient.invalidateQueries({
          queryKey: partialUserConfigs.detail(variables.id),
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

  return useMutation(
    (partialUserConfigId: string) => {
      return deleteEmbeddedSourcesId(partialUserConfigId, requestOptions);
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: partialUserConfigs.lists() });
      },
    }
  );
};
