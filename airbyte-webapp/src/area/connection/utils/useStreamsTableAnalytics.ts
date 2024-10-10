import isEqual from "lodash/isEqual";
import { useEffect, useRef } from "react";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";

import { useGetConnectionSyncProgress } from "core/api";
import { ConnectionSyncProgressRead, ConnectionSyncStatus } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

export const useTrackSyncProgress = (connectionId: string, trackCountRef: React.MutableRefObject<number>) => {
  const { connection } = useConnectionFormService();
  const { status } = useConnectionStatus(connectionId);
  const { data: connectionSyncProgress } = useGetConnectionSyncProgress(
    connectionId,
    status === ConnectionSyncStatus.running
  );
  const analyticsService = useAnalyticsService();

  const prevSyncProgressRef = useRef<ConnectionSyncProgressRead | null>(null);

  useEffect(() => {
    if (!connectionSyncProgress || trackCountRef.current > 1) {
      return;
    }

    const hasProgressChanged = !isEqual(prevSyncProgressRef.current, connectionSyncProgress);

    if (hasProgressChanged) {
      analyticsService.track(Namespace.CONNECTION, Action.SYNC_PROGRESS, {
        connector_source_definition: connection.source.sourceName,
        connector_source_definition_id: connection.source.sourceDefinitionId,
        connector_destination_definition: connection.destination.destinationName,
        connector_destination_definition_id: connection.destination.destinationDefinitionId,
        job_id: connectionSyncProgress.jobId,
        records_emitted: connectionSyncProgress.recordsEmitted,
        records_committed: connectionSyncProgress.recordsCommitted,
      });

      trackCountRef.current++;
      prevSyncProgressRef.current = connectionSyncProgress;
    }
  }, [connectionSyncProgress, connection, analyticsService, trackCountRef]);

  return null;
};
