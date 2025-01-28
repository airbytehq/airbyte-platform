import capitalize from "lodash/capitalize";
import { useCallback } from "react";

import { EnterpriseSourceStubType } from "core/domain/connector";
import { useAnalyticsService, Action, Namespace } from "core/services/analytics";

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

export const useTrackSelectEnterpriseStub = () => {
  const analytics = useAnalyticsService();

  return useCallback(
    (enterpriseSourceStub: EnterpriseSourceStubType) => {
      analytics.track(Namespace.ENTERPRISE_SOURCE_STUB, Action.SELECT, {
        actionDescription: "Enterprise stub selected",
        ...enterpriseSourceStub,
      });
    },
    [analytics]
  );
};
