import dayjs from "dayjs";

import { useGetConnectionSyncProgress, useListConnectionsStatuses } from "core/api";
import { ConnectionSyncStatus, FailureReason } from "core/api/types/AirbyteClient";
import { moveTimeToFutureByPeriod } from "core/utils/time";

export interface UIConnectionStatus {
  // user-facing status reflecting the state of the connection
  status: ConnectionSyncStatus;
  // unix timestamp of the last successful sync job
  lastSuccessfulSync: number | undefined;
  // expected time the next scheduled sync will start (basic schedule only)
  nextSync: number | undefined;
  // for displaying error message and linking to the relevant logs
  failureReason?: FailureReason;
  lastSyncJobId?: number;
  lastSyncAttemptNumber?: number;
  recordsExtracted: number | undefined;
  recordsLoaded: number | undefined;
}

export const useConnectionStatus = (connectionId: string): UIConnectionStatus => {
  const connectionStatuses = useListConnectionsStatuses([connectionId]);
  const connectionStatus = connectionStatuses[0];

  const {
    connectionSyncStatus: status,
    lastSuccessfulSync,
    failureReason,
    lastSyncJobId,
    lastSyncAttemptNumber,
    lastSyncJobCreatedAt,
    scheduleData,
  } = connectionStatus;

  const { data: syncProgress } = useGetConnectionSyncProgress(connectionId, status === ConnectionSyncStatus.running);

  // calculate the time we expect the next sync to start (basic schedule only)
  let nextSync;
  if (lastSyncJobCreatedAt && scheduleData?.basicSchedule) {
    const latestSync = dayjs(lastSyncJobCreatedAt * 1000);
    nextSync = moveTimeToFutureByPeriod(
      latestSync.subtract(scheduleData.basicSchedule.units, scheduleData.basicSchedule.timeUnit),
      scheduleData.basicSchedule.units,
      scheduleData.basicSchedule.timeUnit
    ).valueOf();
  }

  return {
    status,
    nextSync,
    lastSuccessfulSync,
    failureReason,
    lastSyncJobId,
    lastSyncAttemptNumber,
    recordsExtracted: syncProgress?.recordsCommitted,
    recordsLoaded: syncProgress?.recordsEmitted,
  };
};
