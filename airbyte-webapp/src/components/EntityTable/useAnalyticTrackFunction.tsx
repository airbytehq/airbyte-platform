import { ConnectionStatus, WebBackendConnectionRead } from "core/api/types/AirbyteClient";
import { Action, getFrequencyFromScheduleData, Namespace, useAnalyticsService } from "core/services/analytics";

// TODO: reuse the same function from PR-11981
export const useAnalyticsTrackFunctions = () => {
  const analyticsService = useAnalyticsService();

  const trackConnectionUpdate = (updatedConnection: WebBackendConnectionRead) => {
    const trackableAction = updatedConnection.status === ConnectionStatus.active ? Action.REENABLE : Action.DISABLE;

    analyticsService.track(Namespace.CONNECTION, trackableAction, {
      actionDescription: `${trackableAction} connection`,
      connector_source: updatedConnection.source?.sourceName,
      connector_source_definition_id: updatedConnection.source?.sourceDefinitionId,
      connector_destination: updatedConnection.destination?.destinationName,
      connector_destination_definition_id: updatedConnection.destination?.destinationDefinitionId,
      frequency: getFrequencyFromScheduleData(updatedConnection.scheduleData),
    });
  };

  return { trackConnectionUpdate };
};
