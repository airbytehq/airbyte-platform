import capitalize from "lodash/capitalize";
import { useCallback } from "react";

import { Action, Namespace } from "core/analytics";
import { useAnalyticsService } from "hooks/services/Analytics";

export const useTrackSelectConnector = (connectorType: "source" | "destination") => {
  const analytics = useAnalyticsService();

  const namespaceType = connectorType === "source" ? Namespace.SOURCE : Namespace.DESTINATION;

  return useCallback(
    (connectorId: string, connectorName: string) => {
      analytics.track(namespaceType, Action.SELECT, {
        actionDescription: `${capitalize(connectorType)} connector type selected`,
        [`connector_${connectorType}`]: connectorName,
        [`connector_${connectorType}_definition_id`]: connectorId,
      });
    },
    [analytics, connectorType, namespaceType]
  );
};
