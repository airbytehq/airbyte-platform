import dayjs from "dayjs";

import {
  useErrorMultiplierExperiment,
  useLateMultiplierExperiment,
} from "components/connection/StreamStatus/streamStatusUtils";
import { Status as ConnectionSyncStatus } from "components/EntityTable/types";
import { getConnectionSyncStatus } from "components/EntityTable/utils";

import { useListJobsForConnectionStatus } from "core/api";
import {
  ConnectionScheduleType,
  ConnectionStatus,
  JobConfigType,
  JobStatus,
  WebBackendConnectionRead,
} from "core/request/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useGetConnection } from "hooks/services/useConnectionHook";
import { moveTimeToFutureByPeriod } from "utils/time";

import { ConnectionStatusIndicatorStatus } from "../ConnectionStatusIndicator";
import { jobStatusesIndicatingFinishedExecution } from "../ConnectionSync/ConnectionSyncContext";

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

export interface UIConnectionStatus {
  // user-facing status reflecting the state of the connection
  status: ConnectionStatusIndicatorStatus;
  // status of the last completed sync job, useful for distinguishing between failed & delayed in OnTrack status
  lastSyncJobStatus: JobStatus | undefined;
  // expected time the next scheduled sync will start (basic schedule only)
  nextSync: dayjs.Dayjs | undefined;
}

export const useConnectionStatus = (connectionId: string): UIConnectionStatus => {
  const connection = useGetConnection(connectionId);

  // get the last N (10) jobs for this connection
  // to determine the connection's status
  const {
    data: { jobs },
  } = useListJobsForConnectionStatus(connectionId);

  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);

  // both default to 2; used as the connection schedule multiplier
  // to evaluate if a connection is OnTrack vs. Late/Error
  const lateMultiplier = useLateMultiplierExperiment();
  const errorMultiplier = useErrorMultiplierExperiment();

  // compute the connection sync status from the job history
  const lastCompletedSyncJob = jobs.find(
    ({ job }) =>
      job && job.configType === JobConfigType.sync && jobStatusesIndicatingFinishedExecution.includes(job.status)
  );
  const lastSyncJobStatus = lastCompletedSyncJob?.job?.status || connection.latestSyncJobStatus;
  const connectionSyncStatus = getConnectionSyncStatus(connection.status, lastSyncJobStatus);

  // find the last successful sync job & its timestamp
  const lastSuccessfulSyncJob = jobs.find(
    ({ job }) => job?.configType === JobConfigType.sync && job?.status === JobStatus.succeeded
  );
  const lastSuccessfulSync = lastSuccessfulSyncJob?.job?.createdAt;

  // calculate the time we expect the next sync to start (basic schedule only)
  const lastSyncJob = jobs.find(({ job }) => job?.configType === JobConfigType.sync);
  const latestSyncJobCreatedAt = lastSyncJob?.job?.createdAt;
  let nextSync;
  if (latestSyncJobCreatedAt && connection.scheduleData?.basicSchedule) {
    const latestSync = dayjs(latestSyncJobCreatedAt * 1000);
    nextSync = moveTimeToFutureByPeriod(
      latestSync.subtract(connection.scheduleData.basicSchedule.units, connection.scheduleData.basicSchedule.timeUnit),
      connection.scheduleData.basicSchedule.units,
      connection.scheduleData.basicSchedule.timeUnit
    );
  }

  if (hasBreakingSchemaChange) {
    return { status: ConnectionStatusIndicatorStatus.ActionRequired, lastSyncJobStatus, nextSync };
  }

  if (connection.status !== ConnectionStatus.active) {
    return { status: ConnectionStatusIndicatorStatus.Disabled, lastSyncJobStatus, nextSync };
  }

  if (connectionSyncStatus === ConnectionSyncStatus.EMPTY) {
    return { status: ConnectionStatusIndicatorStatus.Pending, lastSyncJobStatus, nextSync };
  }

  // The `error` value is based on the `connection.streamCentricUI.errorMultiplyer` experiment
  if (
    !hasBreakingSchemaChange &&
    connectionSyncStatus === ConnectionSyncStatus.FAILED &&
    (!isHandleableScheduledConnection(connection.scheduleType) ||
      isConnectionLate(connection, lastSuccessfulSync, errorMultiplier) ||
      lastSuccessfulSync == null) // edge case: if the number of jobs we have loaded isn't enough to find the last successful sync
  ) {
    return { status: ConnectionStatusIndicatorStatus.Error, lastSyncJobStatus, nextSync };
  }

  // The `late` value is based on the `connection.streamCentricUI.late` experiment
  if (isConnectionLate(connection, lastSuccessfulSync, lateMultiplier)) {
    return { status: ConnectionStatusIndicatorStatus.Late, lastSyncJobStatus, nextSync };
  } else if (isConnectionLate(connection, lastSuccessfulSync, 1)) {
    return { status: ConnectionStatusIndicatorStatus.OnTrack, lastSyncJobStatus, nextSync };
  }

  if (connectionSyncStatus === ConnectionSyncStatus.FAILED) {
    return { status: ConnectionStatusIndicatorStatus.OnTrack, lastSyncJobStatus, nextSync };
  }

  return { status: ConnectionStatusIndicatorStatus.OnTime, lastSyncJobStatus, nextSync };
};
