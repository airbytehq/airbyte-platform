import { useCallback } from "react";

import { Connector, ConnectorDefinition } from "core/domain/connector";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";

export const useAnalyticsTrackFunctions = (connectorType: "source" | "destination") => {
  const analytics = useAnalyticsService();

  const namespaceType = connectorType === "source" ? Namespace.SOURCE : Namespace.DESTINATION;

  const trackAction = useCallback(
    (connector: ConnectorDefinition | undefined, actionType: Action, actionDescription: string) => {
      if (!connector) {
        return;
      }
      analytics.track(namespaceType, actionType, {
        actionDescription,
        connectorType,
        connector: connector.name,
        connector_definition_id: Connector.id(connector),
      });
    },
    [analytics, namespaceType, connectorType]
  );

  const trackOAuthAttemp = useCallback(
    (connector: ConnectorDefinition | undefined) => {
      trackAction(connector, Action.OAUTH_ATTEMPT, "Connector OAuth flow - attempted");
    },
    [trackAction]
  );

  const trackOAuthSuccess = useCallback(
    (connector: ConnectorDefinition | undefined) => {
      trackAction(connector, Action.OAUTH_SUCCESS, "Connector OAuth flow - success");
    },
    [trackAction]
  );

  return { trackOAuthAttemp, trackOAuthSuccess };
};
