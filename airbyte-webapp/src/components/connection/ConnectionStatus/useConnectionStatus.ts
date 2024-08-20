import dayjs from "dayjs";

import { useGetConnection, useGetConnectionSyncProgress, useListConnectionsStatuses } from "core/api";
import { ConnectionStatus, FailureReason, FailureType, JobStatus } from "core/api/types/AirbyteClient";
import { moveTimeToFutureByPeriod } from "core/utils/time";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";

import { ConnectionStatusType } from "../ConnectionStatusIndicator";

export interface UIConnectionStatus {
  // user-facing status reflecting the state of the connection
  status: ConnectionStatusType;
  // status of the last completed sync job, useful for distinguishing failed jobs, first job since reset, etc.
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
  recordsExtracted: number | undefined;
  recordsLoaded: number | undefined;
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

  const { data: syncProgress } = useGetConnectionSyncProgress(connectionId, isRunning);

  const hasConfigError = failureReason?.failureType === FailureType.config_error;

  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);

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

  if (isRunning) {
    return {
      status: ConnectionStatusType.Syncing,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
      recordsExtracted: syncProgress?.recordsEmitted,
      recordsLoaded: syncProgress?.recordsCommitted,
    };
  }

  if (hasBreakingSchemaChange || hasConfigError) {
    return {
      status: ConnectionStatusType.Failed,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
      recordsExtracted: syncProgress?.recordsCommitted,
      recordsLoaded: syncProgress?.recordsEmitted,
    };
  }

  if (connection.status !== ConnectionStatus.active) {
    return {
      status: ConnectionStatusType.Paused,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
      recordsExtracted: syncProgress?.recordsCommitted,
      recordsLoaded: syncProgress?.recordsEmitted,
    };
  }

  if (lastSyncJobStatus == null || isLastCompletedJobReset) {
    return {
      status: ConnectionStatusType.Pending,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
      recordsExtracted: syncProgress?.recordsCommitted,
      recordsLoaded: syncProgress?.recordsEmitted,
    };
  }

  if (lastSyncJobStatus === JobStatus.failed) {
    return {
      status: ConnectionStatusType.Incomplete,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
      failureReason,
      lastSyncJobId,
      lastSyncAttemptNumber,
      recordsExtracted: syncProgress?.recordsCommitted,
      recordsLoaded: syncProgress?.recordsEmitted,
    };
  }

  return {
    status: ConnectionStatusType.Synced,
    lastSyncJobStatus,
    nextSync,
    lastSuccessfulSync,
    isRunning,
    recordsExtracted: syncProgress?.recordsCommitted,
    recordsLoaded: syncProgress?.recordsEmitted,
  };
};
