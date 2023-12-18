import dayjs from "dayjs";

import {
  useErrorMultiplierExperiment,
  useLateMultiplierExperiment,
} from "components/connection/StreamStatus/streamStatusUtils";

import { useGetConnection, useListConnectionsStatuses } from "core/api";
import {
  ConnectionScheduleType,
  ConnectionStatus,
  FailureReason,
  FailureType,
  JobStatus,
  WebBackendConnectionRead,
} from "core/api/types/AirbyteClient";
import { moveTimeToFutureByPeriod } from "core/utils/time";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";

import { ConnectionStatusIndicatorStatus } from "../ConnectionStatusIndicator";

export const isHandleableScheduledConnection = (scheduleType: ConnectionScheduleType | undefined) =>
  scheduleType === "basic";

// `late` here refers to how long past the last successful sync until it is flagged
export const isConnectionLate = (
  connection: WebBackendConnectionRead,
  lastSuccessfulSync: number | undefined,
  lateMultiplier: number
) => {
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

export interface UIConnectionStatus {
  // user-facing status reflecting the state of the connection
  status: ConnectionStatusIndicatorStatus;
  // status of the last completed sync job, useful for distinguishing between failed & delayed in OnTrack status
  lastSyncJobStatus: JobStatus | undefined;
  // unix timestamp of the last successful sync job
  lastSuccessfulSync: number | undefined;
  // expected time the next scheduled sync will start (basic schedule only)
  nextSync: number | undefined;
  // is the connection currently running a job
  isRunning: boolean;

  // for displaying error message and linking to the relevant logs
  failureReason?: FailureReason;
  lastSyncJobId?: number;
  lastSyncAttemptNumber?: number;
}

export const useConnectionStatus = (connectionId: string): UIConnectionStatus => {
  const connection = useGetConnection(connectionId);

  const connectionStatuses = useListConnectionsStatuses([connectionId]);
  const connectionStatus = connectionStatuses[0];

  const {
    isRunning,
    isLastCompletedJobReset,
    lastSyncJobStatus,
    lastSuccessfulSync,
    failureReason,
    lastSyncJobId,
    lastSyncAttemptNumber,
  } = connectionStatus;

  const hasConfigError = failureReason?.failureType === FailureType.config_error;

  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);

  // both default to 2; used as the connection schedule multiplier
  // to evaluate if a connection is OnTrack vs. Late/Error
  const lateMultiplier = useLateMultiplierExperiment();
  const errorMultiplier = useErrorMultiplierExperiment();

  // calculate the time we expect the next sync to start (basic schedule only)
  const latestSyncJobCreatedAt = connection.latestSyncJobCreatedAt;
  let nextSync;
  if (latestSyncJobCreatedAt && connection.scheduleData?.basicSchedule) {
    const latestSync = dayjs(latestSyncJobCreatedAt * 1000);
    nextSync = moveTimeToFutureByPeriod(
      latestSync.subtract(connection.scheduleData.basicSchedule.units, connection.scheduleData.basicSchedule.timeUnit),
      connection.scheduleData.basicSchedule.units,
      connection.scheduleData.basicSchedule.timeUnit
    ).valueOf();
  }

  if (hasBreakingSchemaChange || hasConfigError) {
    return {
      status: ConnectionStatusIndicatorStatus.ActionRequired,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
    };
  }

  if (connection.status !== ConnectionStatus.active) {
    return {
      status: ConnectionStatusIndicatorStatus.Disabled,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
    };
  }

  if (lastSyncJobStatus == null || isLastCompletedJobReset) {
    return {
      status: ConnectionStatusIndicatorStatus.Pending,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
    };
  }

  // The `error` value is based on the `connection.streamCentricUI.errorMultiplyer` experiment
  if (
    !hasBreakingSchemaChange &&
    lastSyncJobStatus === JobStatus.failed &&
    (!isHandleableScheduledConnection(connection.scheduleType) ||
      isConnectionLate(connection, lastSuccessfulSync, errorMultiplier) ||
      lastSuccessfulSync == null) // edge case: if the number of jobs we have loaded isn't enough to find the last successful sync
  ) {
    return {
      status: ConnectionStatusIndicatorStatus.Error,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
    };
  }

  // The `late` value is based on the `connection.streamCentricUI.late` experiment
  if (isConnectionLate(connection, lastSuccessfulSync, lateMultiplier)) {
    return { status: ConnectionStatusIndicatorStatus.Late, lastSyncJobStatus, nextSync, lastSuccessfulSync, isRunning };
  } else if (isConnectionLate(connection, lastSuccessfulSync, 1)) {
    return {
      status: ConnectionStatusIndicatorStatus.OnTrack,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
    };
  }

  if (lastSyncJobStatus === JobStatus.failed) {
    return {
      status: ConnectionStatusIndicatorStatus.OnTrack,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
    };
  }

  return { status: ConnectionStatusIndicatorStatus.OnTime, lastSyncJobStatus, nextSync, lastSuccessfulSync, isRunning };
};
