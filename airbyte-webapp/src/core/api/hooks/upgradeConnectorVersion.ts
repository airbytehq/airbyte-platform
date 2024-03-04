import { useMutation, useQueryClient } from "@tanstack/react-query";

import { destinationsKeys, sourcesKeys } from "core/api";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useAppMonitoringService } from "hooks/services/AppMonitoringService";

import { definitionKeys } from "./actorDefinitionVersions";
import {
  sourceDefinitionSpecificationKeys,
  destinationDefinitionSpecificationKeys,
} from "./connectorDefinitionSpecification";
import { upgradeDestinationVersion, upgradeSourceVersion } from "../generated/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

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
