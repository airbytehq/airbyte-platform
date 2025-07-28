import { useMutation, useQueryClient } from "@tanstack/react-query";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useNotificationService } from "hooks/services/Notification";
import { ConnectorFormValues } from "views/Connector/ConnectorForm";

import { createIntegrationsTemplatesConnections } from "../generated/SonarClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { ConnectionTemplateCreateRequest } from "../types/SonarClient";
import { useRequestOptions } from "../useRequestOptions";

const connectionTemplates = {
  all: [SCOPE_ORGANIZATION, "connectionTemplates"] as const,
  lists: () => [...connectionTemplates.all, "list"],
  detail: (connectionTemplateId: string) => [...connectionTemplates.all, "details", connectionTemplateId] as const,
};
export const useCreateConnectionTemplate = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const { registerNotification } = useNotificationService();
  const analyticsService = useAnalyticsService();

  return useMutation(
    ({
      values,
      destinationDefinitionId,
      organizationId,
    }: {
      values: ConnectorFormValues;
      destinationDefinitionId: string;
      organizationId: string;
    }) => {
      const connectionTemplate: ConnectionTemplateCreateRequest = {
        organization_id: organizationId,
        destination_name: values.name,
        destination_config: values.connectionConfiguration,
        destination_definition_id: destinationDefinitionId,
        destination_actor_definition_id: destinationDefinitionId,
      };

      return createIntegrationsTemplatesConnections(connectionTemplate, requestOptions);
    },
    {
      onSuccess: (response, request) => {
        analyticsService.track(Namespace.EMBEDDED, Action.CONNECTION_TEMPLATE_CREATED, {
          description: "User created a connection template",
          connectionTemplateId: response.id,
          destinationDefinitionId: request.destinationDefinitionId ?? response,
        });

        queryClient.invalidateQueries(connectionTemplates.lists());
        registerNotification({
          id: "connection-template-created",
          text: "Connection template created",
          type: "success",
        });
      },
      onError: (error, request) => {
        analyticsService.track(Namespace.EMBEDDED, Action.CONNECTION_TEMPLATE_CREATE_FAILED, {
          description: "User failed to create a connection template",
          destinationDefinitionId: request.destinationDefinitionId,
          error,
        });

        registerNotification({
          id: "connection-template-created",
          text: "Failed to create connection template",
          type: "error",
        });
      },
    }
  );
};
