import { useMutation, useQueryClient } from "@tanstack/react-query";

import { useRequestOptions } from "core/api/useRequestOptions";
import { upgradeDestinationVersion, upgradeSourceVersion } from "core/request/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { destinationsKeys } from "hooks/services/useDestinationHook";
import { sourcesKeys } from "hooks/services/useSourceHook";
import { destinationDefinitionSpecificationKeys } from "services/connector/DestinationDefinitionSpecificationService";
import { sourceDefinitionSpecificationKeys } from "services/connector/SourceDefinitionSpecificationService";

import { definitionKeys } from "./actorDefinitionVersions";

export const useUpgradeConnectorVersion = (
  type: "source" | "destination",
  connectorId: string,
  connectorDefinitionId: string
) => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const analyticsService = useAnalyticsService();
  const { trackError } = useAppMonitoringService();

  return useMutation(
    (connectorId: string) =>
      type === "source"
        ? upgradeSourceVersion({ sourceId: connectorId }, requestOptions)
        : upgradeDestinationVersion({ destinationId: connectorId }, requestOptions),
    {
      onSuccess: () => {
        analyticsService.track(Namespace.CONNECTOR, Action.UPGRADE_VERSION, {
          actionDescription: "Connector version upgraded",
          connector_id: connectorId,
          connector_definition_id: connectorDefinitionId,
        });
        queryClient.invalidateQueries(definitionKeys.detail(connectorId));
        if (type === "source") {
          queryClient.invalidateQueries(sourcesKeys.detail(connectorId));
          queryClient.invalidateQueries(sourceDefinitionSpecificationKeys.detail(connectorDefinitionId, connectorId));
        } else {
          queryClient.invalidateQueries(destinationsKeys.detail(connectorId));
          queryClient.invalidateQueries(
            destinationDefinitionSpecificationKeys.detail(connectorDefinitionId, connectorId)
          );
        }
      },
      onError: (error: Error) => {
        trackError(error);
      },
    }
  );
};
