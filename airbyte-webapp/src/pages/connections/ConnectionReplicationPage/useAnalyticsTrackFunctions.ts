import { useCallback } from "react";

import { WebBackendConnectionRead } from "core/api/types/AirbyteClient";
import { Action, getFrequencyFromScheduleData, Namespace, useAnalyticsService } from "core/services/analytics";

/**
 * track schema edit action
 */
export const useAnalyticsTrackFunctions = () => {
  const analyticsService = useAnalyticsService();

  const trackSchemaEdit = useCallback(
    (connection: WebBackendConnectionRead) =>
      analyticsService.track(Namespace.CONNECTION, Action.EDIT_SCHEMA, {
        actionDescription: "Connection saved with catalog changes",
        connector_source: connection.source.sourceName,
        connector_source_definition_id: connection.source.sourceDefinitionId,
        connector_destination: connection.destination.destinationName,
        connector_destination_definition_id: connection.destination.destinationDefinitionId,
        frequency: getFrequencyFromScheduleData(connection.scheduleData),
      }),
    [analyticsService]
  );

  return { trackSchemaEdit };
};
