import { useMutation, useQueryClient } from "@tanstack/react-query";

import { useNotificationService } from "hooks/services/Notification";

import {
  embeddedConfigTemplatesSourcesIdGetSourceConfigTemplate,
  embeddedConfigTemplatesSourcesListSourceConfigTemplates,
  embeddedConfigTemplatesSourcesCreateSourceConfigTemplate,
} from "../generated/SonarClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { CreateSourceConfigTemplateRequest, SourceConfigTemplateListResponse } from "../types/SonarClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const configTemplates = {
  all: [SCOPE_ORGANIZATION, "configTemplates"] as const,
  lists: () => [...configTemplates.all, "list"],
  detail: (configTemplateId: string) => [...configTemplates.all, "details", configTemplateId] as const,
};

export const useListConfigTemplates = (workspaceId: string): SourceConfigTemplateListResponse => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(configTemplates.lists(), () => {
    return embeddedConfigTemplatesSourcesListSourceConfigTemplates({ workspace_id: workspaceId }, requestOptions);
  });
};

export const useGetConfigTemplate = (configTemplateId: string) => {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery(configTemplates.detail(configTemplateId), () => {
    return embeddedConfigTemplatesSourcesIdGetSourceConfigTemplate(configTemplateId, requestOptions);
  });
};

export const useCreateConfigTemplate = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();

  return useMutation(
    (configTemplate: CreateSourceConfigTemplateRequest) => {
      return embeddedConfigTemplatesSourcesCreateSourceConfigTemplate(configTemplate, requestOptions);
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
