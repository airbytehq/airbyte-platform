import { useCallback } from "react";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";

export const useAnalyticsTrackFunctions = () => {
  const analytics = useAnalyticsService();

  const trackRequest = useCallback(
    ({
      sourceDefinitionId,
      connectorName,
      requestType,
    }: {
      sourceDefinitionId: string;
      connectorName: string;
      requestType: "schema" | "erd";
    }) => {
      const namespace = requestType === "schema" ? Namespace.SCHEMA : Namespace.ERD;

      analytics.track(namespace, Action.REQUEST, {
        actionDescription: `Requested source ${requestType}`,
        connector_source: connectorName,
        connector_source_definition_id: sourceDefinitionId,
        request_type: requestType,
      });
    },
    [analytics]
  );
  return { trackRequest };
};
