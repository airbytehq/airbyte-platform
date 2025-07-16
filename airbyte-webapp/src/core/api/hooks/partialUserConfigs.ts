import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useExperiment } from "hooks/services/Experiment";
import { useNotificationService } from "hooks/services/Notification";
import { ALLOWED_ORIGIN_PARAM } from "pages/embedded/EmbeddedSourceCreatePage/hooks/useEmbeddedSourceParams";

import { ApiCallOptions } from "../apiCall";
import {
  createPartialUserConfig,
  listPartialUserConfigs,
  getPartialUserConfig,
  updatePartialUserConfig,
  deletePartialUserConfig,
} from "../generated/AirbyteClient";
import {
  embeddedPartialUserConfigsListPartialUserConfigs,
  embeddedPartialUserConfigsCreatePartialUserConfig,
  embeddedPartialUserConfigsIdGetPartialUserConfig,
  embeddedPartialUserConfigsIdUpdatePartialUserConfig,
  embeddedPartialUserConfigsIdDeletePartialUserConfig,
} from "../generated/SonarClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import {
  PartialUserConfigCreate,
  PartialUserConfigRead,
  PartialUserConfigReadList,
  PartialUserConfigUpdate,
  SourceDefinitionSpecification,
  SourceRead,
} from "../types/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

const partialUserConfigs = {
  all: [SCOPE_ORGANIZATION, "partialUserConfigs"] as const,
  lists: () => [...partialUserConfigs.all, "list"],
  detail: (pasrtialUserConfigId: string) => [...partialUserConfigs.all, "details", pasrtialUserConfigId] as const,
};

const convertedEmbeddedListPartialUserConfigs = async (
  workspace_id: string,
  options: ApiCallOptions
): Promise<PartialUserConfigReadList> => {
  const response = await embeddedPartialUserConfigsListPartialUserConfigs({ workspace_id }, options);
  return {
    partialUserConfigs: response.data.map((item) => ({
      configTemplateId: item.source_config_template_id,
      partialUserConfigId: item.id,
      configTemplateName: item.config_template_name,
      configTemplateIcon: item.config_template_icon === null ? "" : item.config_template_icon,
    })),
  };
};

const convertedEmbeddedGetPartialUserConfig = async (
  id: string,
  options: ApiCallOptions
): Promise<PartialUserConfigRead> => {
  const response = await embeddedPartialUserConfigsIdGetPartialUserConfig(id, options);
  const objectToReturn = {
    id: response.id,
    configTemplate: {
      id: response.source_config_template.id,
      name: response.source_config_template.name,
      icon: response.source_config_template.icon as string, // Icon should be optional. It's not in the legacy API, but I'm not worried about it because it's being deprecated.
      sourceDefinitionId: response.source_config_template.actor_definition_id,
      configTemplateSpec: response.source_config_template.user_config_spec as unknown as SourceDefinitionSpecification,
    },
    actorId: response.actor_id,
    connectionConfiguration: response.connection_configuration,
  };
  return objectToReturn;
};

const convertedEmbeddedCreatePartialUserConfig = async (
  partial_user_config_create: PartialUserConfigCreate,
  options: ApiCallOptions
): Promise<SourceRead> => {
  const body = {
    workspace_id: partial_user_config_create.workspaceId,
    source_config_template_id: partial_user_config_create.configTemplateId,
    connection_configuration: partial_user_config_create.connectionConfiguration,
  };
  const response = await embeddedPartialUserConfigsCreatePartialUserConfig(body, options);
  const objectToReturn = {
    id: response.id,
    name: "", // Not provided by Sonar
    sourceId: response.source_id,
    sourceDefintionId: response.source_definition_id,
    sourceName: "", // Not provided by Sonar
    createdAt: 0, // Not provided by Sonar
    sourceDefinitionId: response.source_definition_id,
    workspaceId: "", // Not provided by Sonar
    connectionConfiguration: {}, // Not provided by Sonar
    configured: null, // Not provided by Sonar
    icon: "", // Not provided by Sonar
    sourceDefinition: {
      id: response.source_definition_id,
      name: response.source_config_template.name,
      dockerRepository: "", // Not provided by Sonar
      dockerImageTag: "", // Not provided by Sonar
      documentationUrl: "", // Not provided by Sonar
      icon: "", // Not provided by Sonar // FIXME: should it be?
      releaseStage: "", // Not provided by Sonar
      sourceType: "", // Not provided by Sonar
      spec: null,
    },
  };
  return objectToReturn;
};

const convertedEmbeddedUpdatePartialUserConfig = async (
  partial_user_config_update: PartialUserConfigUpdate,
  options: ApiCallOptions
): Promise<SourceRead> => {
  const body = {
    connection_configuration: partial_user_config_update.connectionConfiguration,
  };
  const response = await embeddedPartialUserConfigsIdUpdatePartialUserConfig(
    partial_user_config_update.partialUserConfigId,
    body,
    options
  );
  const objectToReturn = {
    id: response.id,
    name: "", // Not provided by Sonar
    sourceId: response.source_id,
    sourceName: "", // Not provided by Sonar
    createdAt: 0, // Not provided by Sonar
    sourceDefinitionId: response.source_definition_id,
    workspaceId: "", // Not provided by Sonar
    connectionConfiguration: {}, // Not provided by Sonar
    configured: null, // Not provided by Sonar
    icon: "", // Not provided by Sonar
    sourceDefinition: {
      id: response.source_definition_id,
      name: response.source_config_template.name,
      dockerRepository: "", // Not provided by Sonar
      dockerImageTag: "", // Not provided by Sonar
      documentationUrl: "", // Not provided by Sonar
      icon: "", // Not provided by Sonar // FIXME: should it be?
      releaseStage: "", // Not provided by Sonar
      sourceType: "", // Not provided by Sonar
      spec: null,
    },
  };
  return objectToReturn;
};

const convertedEmbeddedDeletePartialUserConfig = async (
  partialUserConfigId: string,
  options: ApiCallOptions
): Promise<void> => {
  await embeddedPartialUserConfigsIdDeletePartialUserConfig(partialUserConfigId, options);
};

export const useListPartialUserConfigs = (workspaceId: string): PartialUserConfigReadList => {
  const requestOptions = useRequestOptions();
  const isSonarServerEnabled = useExperiment("embedded.useSonarServer");

  return useSuspenseQuery(partialUserConfigs.lists(), () => {
    if (isSonarServerEnabled) {
      return convertedEmbeddedListPartialUserConfigs(workspaceId, requestOptions);
    }

    return listPartialUserConfigs({ workspaceId }, requestOptions);
  });
};

export const useGetPartialUserConfig = (partialUserConfigId: string) => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  const isSonarServerEnabled = useExperiment("embedded.useSonarServer");

  return useSuspenseQuery(partialUserConfigs.detail(partialUserConfigId), () => {
    if (isSonarServerEnabled) {
      return convertedEmbeddedGetPartialUserConfig(partialUserConfigId, requestOptions);
    }

    return getPartialUserConfig({ partialUserConfigId, workspaceId }, requestOptions);
  });
};

export const useCreatePartialUserConfig = () => {
  const requestOptions = useRequestOptions();
  const { registerNotification } = useNotificationService();
  const [searchParams] = useSearchParams();
  const queryClient = useQueryClient();
  const isSonarServerEnabled = useExperiment("embedded.useSonarServer");

  return useMutation(
    (partialUserConfigCreate: PartialUserConfigCreate) => {
      if (isSonarServerEnabled) {
        return convertedEmbeddedCreatePartialUserConfig(partialUserConfigCreate, requestOptions);
      }
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
  const useSonarServerEnabled = useExperiment("embedded.useSonarServer");
  return useMutation(
    (partialUserConfigUpdate: PartialUserConfigUpdate) => {
      if (useSonarServerEnabled) {
        return convertedEmbeddedUpdatePartialUserConfig(partialUserConfigUpdate, requestOptions);
      }
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
  const isSonarServerEnabled = useExperiment("embedded.useSonarServer");

  const workspaceId = useCurrentWorkspaceId();
  return useMutation(
    (partialUserConfigId: string) => {
      if (isSonarServerEnabled) {
        return convertedEmbeddedDeletePartialUserConfig(partialUserConfigId, requestOptions);
      }
      return deletePartialUserConfig({ partialUserConfigId, workspaceId }, requestOptions);
    },
    {
      onSuccess: () => {
        queryClient.invalidateQueries({ queryKey: partialUserConfigs.lists() });
      },
    }
  );
};
