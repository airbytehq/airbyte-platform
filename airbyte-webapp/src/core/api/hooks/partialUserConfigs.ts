import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { ALLOWED_ORIGIN_SEARCH_PARAM } from "core/services/auth/EmbeddedAuthService";
import { useNotificationService } from "hooks/services/Notification";
import {
  CREATE_PARTIAL_USER_CONFIG_PARAM,
  SELECTED_PARTIAL_CONFIG_ID_PARAM,
  SELECTED_TEMPLATE_ID_PARAM,
} from "pages/embedded/EmbeddedSourceCreatePage/hooks/useEmbeddedSourceParams";

import {
  createPartialUserConfig,
  listPartialUserConfigs,
  getPartialUserConfig,
  updateUserConfig,
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
  const [searchParams, setSearchParams] = useSearchParams();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();

  return useMutation(
    async (partialUserConfigCreate: PartialUserConfigCreate) => {
      return await createPartialUserConfig(partialUserConfigCreate, requestOptions);
    },
    {
      onSuccess: (data) => {
        queryClient.invalidateQueries({ queryKey: partialUserConfigs.lists() });

        setSearchParams((params: URLSearchParams) => {
          params.delete(CREATE_PARTIAL_USER_CONFIG_PARAM);
          params.delete(SELECTED_TEMPLATE_ID_PARAM);
          return params;
        });

        const allowedOriginParam = searchParams.get(ALLOWED_ORIGIN_SEARCH_PARAM);
        const allowedOrigin = allowedOriginParam ? decodeURIComponent(allowedOriginParam) : "";

        if (allowedOrigin.length > 0) {
          const successMessage = {
            type: "end_user_action_result",
            message: "partial_user_config_created",
            data,
          };

          window.parent.postMessage(successMessage, allowedOrigin);
        }

        registerNotification({
          id: "partial-user-config-create-success",
          type: "success",
          text: formatMessage({ id: "partialUserConfig.create.success" }),
        });
      },
      onError: (error: Error) => {
        const allowedOriginParam = searchParams.get(ALLOWED_ORIGIN_SEARCH_PARAM);
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
  const [searchParams, setSearchParams] = useSearchParams();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();
  return useMutation(
    async (partialUserConfigUpdate: PartialUserConfigUpdate) => {
      return await updateUserConfig(partialUserConfigUpdate, requestOptions);
    },
    {
      onSuccess: (data, variables) => {
        queryClient.invalidateQueries({ queryKey: partialUserConfigs.lists() });
        queryClient.invalidateQueries({
          queryKey: partialUserConfigs.detail(variables.partialUserConfigId),
        });

        const allowedOriginParam = searchParams.get(ALLOWED_ORIGIN_SEARCH_PARAM);
        const allowedOrigin = allowedOriginParam ? decodeURIComponent(allowedOriginParam) : "";
        if (allowedOrigin.length > 0) {
          const successMessage = {
            type: "end_user_action_result",
            message: "partial_user_config_updated",
            data,
          };

          window.parent.postMessage(successMessage, allowedOrigin);
        }

        setSearchParams((params: URLSearchParams) => {
          params.delete(SELECTED_PARTIAL_CONFIG_ID_PARAM);
          return params;
        });
        registerNotification({
          id: "partial-user-config-update-success",
          type: "success",
          text: formatMessage({ id: "partialUserConfig.create.success" }),
        });
      },
      onError: (error: Error) => {
        const allowedOriginParam = searchParams.get(ALLOWED_ORIGIN_SEARCH_PARAM);
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
