import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";

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

  return useSuspenseQuery(partialUserConfigs.detail(partialUserConfigId), () => {
    return getPartialUserConfig({ partialUserConfigId }, requestOptions);
  });
};

export const useCreatePartialUserConfig = () => {
  const requestOptions = useRequestOptions();
  const { registerNotification } = useNotificationService();
  const [, setSearchParams] = useSearchParams();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();

  return useMutation(
    async (partialUserConfigCreate: PartialUserConfigCreate) => {
      return await createPartialUserConfig(partialUserConfigCreate, requestOptions);
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: partialUserConfigs.lists() });

        setSearchParams((params: URLSearchParams) => {
          params.delete(CREATE_PARTIAL_USER_CONFIG_PARAM);
          params.delete(SELECTED_TEMPLATE_ID_PARAM);
          return params;
        });

        registerNotification({
          id: "partial-user-config-create-success",
          type: "success",
          text: formatMessage({ id: "partialUserConfig.create.success" }),
        });
      },
      onError: (error: Error) => {
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
  const [, setSearchParams] = useSearchParams();
  const queryClient = useQueryClient();
  const { formatMessage } = useIntl();
  return useMutation(
    async (partialUserConfigUpdate: PartialUserConfigUpdate) => {
      return await updateUserConfig(partialUserConfigUpdate, requestOptions);
    },
    {
      onSuccess: (_data, variables) => {
        queryClient.invalidateQueries({ queryKey: partialUserConfigs.lists() });
        queryClient.invalidateQueries({
          queryKey: partialUserConfigs.detail(variables.partialUserConfigId),
        });

        setSearchParams((params: URLSearchParams) => {
          params.delete(SELECTED_PARTIAL_CONFIG_ID_PARAM);
          return params;
        });
        registerNotification({
          id: "partial-user-config-edit-success",
          type: "success",
          text: formatMessage({ id: "partialUserConfig.create.success" }),
        });
      },
      onError: (error: Error) => {
        registerNotification({
          id: "partial-user-config-edit-error",
          type: "error",
          text: error.message,
        });
      },
    }
  );
};
