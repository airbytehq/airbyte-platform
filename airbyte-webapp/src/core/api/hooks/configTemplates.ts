import { useMutation, useQueryClient } from "@tanstack/react-query";

import { useNotificationService } from "hooks/services/Notification";

import {
  listConfigTemplates,
  getConfigTemplate,
  publicCreateConfigTemplate,
  publicCreateConnectionTemplate,
} from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import {
  ConfigTemplateCreateRequestBody,
  ConfigTemplateList,
  ConnectionTemplateCreateRequestBody,
} from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const configTemplates = {
  all: [SCOPE_ORGANIZATION, "configTemplates"] as const,
  lists: () => [...configTemplates.all, "list"],
  detail: (configTemplateId: string) => [...configTemplates.all, "details", configTemplateId] as const,
};

export const useListConfigTemplates = (workspaceId: string): ConfigTemplateList => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(configTemplates.lists(), () => {
    return listConfigTemplates({ workspaceId }, requestOptions);
  });
};

export const useGetConfigTemplate = (configTemplateId: string, workspaceId: string) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(configTemplates.detail(configTemplateId), () => {
    return getConfigTemplate({ configTemplateId, workspaceId }, requestOptions);
  });
};

export const useCreateConfigTemplate = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();

  return useMutation(
    (configTemplate: ConfigTemplateCreateRequestBody) => {
      return publicCreateConfigTemplate(configTemplate, requestOptions);
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries(configTemplates.lists());
        registerNotification({
          id: "config-template-created",
          text: "Config template created",
          type: "success",
        });
      },
      onError: () => {
        registerNotification({
          id: "config-template-created",
          text: "Failed to create config template",
          type: "error",
        });
      },
    }
  );
};

export const useCreateConnectionTemplate = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();

  return useMutation(
    (connectionTemplate: ConnectionTemplateCreateRequestBody) => {
      return publicCreateConnectionTemplate(connectionTemplate, requestOptions);
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries(configTemplates.lists());
        registerNotification({
          id: "config-template-created",
          text: "Connection template created",
          type: "success",
        });
      },
      onError: () => {
        registerNotification({
          id: "config-template-created",
          text: "Failed to create connection template",
          type: "error",
        });
      },
    }
  );
};
