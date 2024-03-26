import { useCallback } from "react";

import { ErrorWithJobInfo } from "core/api";
import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";

/**
 * track discover schema failure action
 */
export const useAnalyticsTrackFunctions = () => {
  const analyticsService = useAnalyticsService();

  const trackFailure = useCallback(
    (source: SourceRead, destination: DestinationRead, schemaError: Error | ErrorWithJobInfo) => {
      const jobInfo = ErrorWithJobInfo.getJobInfo(schemaError);
      analyticsService.track(Namespace.CONNECTION, Action.DISCOVER_SCHEMA, {
        actionDescription: "Discover schema failure",
        connector_source_definition: source.sourceName,
        connector_source_definition_id: source.sourceDefinitionId,
        connector_destination_definition: destination.destinationName,
        connector_destination_definition_id: destination.destinationDefinitionId,
        failure_type: jobInfo?.failureReason?.failureType,
        failure_external_message: jobInfo?.failureReason?.externalMessage,
        failure_internal_message: jobInfo?.failureReason?.internalMessage,
      });
    },
    [analyticsService]
  );

  return { trackFailure };
};
