import dayjs from "dayjs";

import {
  useErrorMultiplierExperiment,
  useLateMultiplierExperiment,
} from "components/connection/StreamStatus/streamStatusUtils";
import { Status as ConnectionSyncStatus } from "components/EntityTable/types";

import { ConnectionScheduleType, WebBackendConnectionRead } from "core/request/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

import { ConnectionStatusIndicatorStatus } from "../ConnectionStatusIndicator";
import { useConnectionSyncContext } from "../ConnectionSync/ConnectionSyncContext";

export const isHandleableScheduledConnection = (scheduleType: ConnectionScheduleType | undefined) =>
  scheduleType === "basic";

// `late` here refers to how long past the last successful sync until it is flagged
export const isConnectionLate = (
  connection: WebBackendConnectionRead,
  lastSuccessfulSync: number | undefined,
  lateMultiplier: number
) => {
  // @TODO: after refactors & tests, I believe this can be removed
  // I think this is only here as a stop-gap when we don't have enough
  // job history (only last 10 jobs are loaded) to determine the last
  // successful sync, but that handling is in other places now
  if (lastSuccessfulSync === undefined) {
    return false;
  }

  return !!(
    isHandleableScheduledConnection(connection.scheduleType) &&
    connection.scheduleData?.basicSchedule?.units &&
    (lastSuccessfulSync ?? 0) * 1000 < // x1000 for a JS datetime
      dayjs()
        // Subtract 2x (default of lateMultiplier) the scheduled interval and compare it to last sync time
        .subtract(
          connection.scheduleData.basicSchedule.units * lateMultiplier,
          connection.scheduleData.basicSchedule.timeUnit
        )
        .valueOf()
  );
};

export const useConnectionStatus = () => {
  const lateMultiplier = useLateMultiplierExperiment();
  const errorMultiplier = useErrorMultiplierExperiment();

  // Disabling the schema changes as it's the only actionable error and is per-connection, not stream
  const { connection } = useConnectionEditService();
  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);

  const { connectionEnabled, connectionStatus, lastSuccessfulSync } = useConnectionSyncContext();

  if (!connectionEnabled) {
    return ConnectionStatusIndicatorStatus.Disabled;
  }

  if (connectionStatus === ConnectionSyncStatus.EMPTY) {
    return ConnectionStatusIndicatorStatus.Pending;
  }

  // The `error` value is based on the `connection.streamCentricUI.errorMultiplyer` experiment
  if (
    !hasBreakingSchemaChange &&
    connectionStatus === ConnectionSyncStatus.FAILED &&
    (!isHandleableScheduledConnection(connection.scheduleType) ||
      isConnectionLate(connection, lastSuccessfulSync, errorMultiplier) ||
      lastSuccessfulSync == null) // edge case: if the number of jobs we have loaded isn't enough to find the last successful sync
  ) {
    return ConnectionStatusIndicatorStatus.Error;
  }

  if (hasBreakingSchemaChange && isConnectionLate(connection, lastSuccessfulSync, lateMultiplier)) {
    return ConnectionStatusIndicatorStatus.ActionRequired;
  }

  if (connectionStatus === ConnectionSyncStatus.CANCELLED) {
    return ConnectionStatusIndicatorStatus.Cancelled;
  }

  // The `late` value is based on the `connection.streamCentricUI.late` experiment
  if (isConnectionLate(connection, lastSuccessfulSync, lateMultiplier)) {
    return ConnectionStatusIndicatorStatus.Late;
  } else if (isConnectionLate(connection, lastSuccessfulSync, 1)) {
    return ConnectionStatusIndicatorStatus.OnTrack;
  }

  if (connectionStatus === ConnectionSyncStatus.FAILED) {
    return ConnectionStatusIndicatorStatus.OnTrack;
  }

  return ConnectionStatusIndicatorStatus.OnTime;
};
