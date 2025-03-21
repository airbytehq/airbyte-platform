import { useCallback, useEffect } from "react";

import { useAnalyticsService, Action, Namespace } from "core/services/analytics";
import { EventParams } from "core/services/analytics/types";

export const useTrackConnectionsGraph = () => {
  const analytics = useAnalyticsService();

  const trackConnectionGraphDrawerOpened = useCallback(
    (params: EventParams) => {
      analytics.track(Namespace.CONNECTIONS, Action.CONNECTIONS_GRAPH_DRAWER_OPENED, {
        actionDescription: "Connections graph drawer opened",
        ...params,
      });
    },
    [analytics]
  );

  return { trackConnectionGraphDrawerOpened };
};

export const useTrackConnectionsGraphLoaded = () => {
  const analytics = useAnalyticsService();
  useEffect(() => {
    analytics.track(Namespace.CONNECTIONS, Action.CONNECTIONS_GRAPH_DRAWER_LOADED, {
      actionDescription: "Connections graph loaded",
    });
  }, [analytics]);
};
