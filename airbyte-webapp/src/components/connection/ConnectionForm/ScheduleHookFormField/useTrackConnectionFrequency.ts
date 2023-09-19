import { useCallback } from "react";

import { ConnectionScheduleDataBasicSchedule } from "core/request/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import {
  ConnectionOrPartialConnection,
  useConnectionHookFormService,
} from "hooks/services/ConnectionForm/ConnectionHookFormService";

export const useTrackConnectionFrequency = (connection: ConnectionOrPartialConnection) => {
  const analyticsService = useAnalyticsService();
  const { mode } = useConnectionHookFormService();

  const trackDropdownSelect = useCallback(
    (value: ConnectionScheduleDataBasicSchedule) => {
      const enabledStreams = connection.syncCatalog.streams.filter((stream) => stream.config?.selected).length;

      if (value) {
        analyticsService.track(Namespace.CONNECTION, Action.FREQUENCY, {
          actionDescription: "Frequency selected",
          frequency: `${value.units} ${value.timeUnit}`,
          connector_source_definition: connection.source.sourceName,
          connector_source_definition_id: connection.source.sourceDefinitionId,
          connector_destination_definition: connection.destination.destinationName,
          connector_destination_definition_id: connection.destination.destinationDefinitionId,
          available_streams: connection.syncCatalog.streams.length,
          enabled_streams: enabledStreams,
          type: mode,
        });
      }
    },
    [
      analyticsService,
      connection.destination.destinationDefinitionId,
      connection.destination.destinationName,
      connection.source.sourceDefinitionId,
      connection.source.sourceName,
      connection.syncCatalog.streams,
      mode,
    ]
  );

  return { trackDropdownSelect };
};
