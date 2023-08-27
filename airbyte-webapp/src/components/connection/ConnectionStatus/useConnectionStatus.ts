import dayjs from "dayjs";

import {
  useErrorMultiplierExperiment,
  useLateMultiplierExperiment,
} from "components/connection/StreamStatus/streamStatusUtils";

import { useListJobsForConnectionStatus, useGetConnection } from "core/api";
import {
  ConnectionScheduleType,
  ConnectionStatus,
  FailureType,
  JobConfigType,
  JobStatus,
  JobWithAttemptsRead,
  WebBackendConnectionRead,
} from "core/api/types/AirbyteClient";
import { moveTimeToFutureByPeriod } from "core/utils/time";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";

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
  nextSync: dayjs.Dayjs | undefined;
  // is the connection currently running a job
  isRunning: boolean;
}

const getConfigErrorFromJobs = (jobs: JobWithAttemptsRead[]) => {
  const sortedAttempts = [...(jobs?.[0]?.attempts ?? [])].sort((a, b) => {
    if (a.createdAt < b.createdAt) {
      return 1;
    } else if (a.createdAt > b.createdAt) {
      return -1;
    }
    return 0;
  });
  const latestAttempt = sortedAttempts[0];
  const configErrorFailure = latestAttempt?.failureSummary?.failures.find(
    (failure) => failure.failureType === FailureType.config_error
  );
  return configErrorFailure;
};

export const useConnectionStatus = (connectionId: string): UIConnectionStatus => {
  const connection = useGetConnection(connectionId);

  // get the last N (10) jobs for this connection
  // to determine the connection's status
  const {
    data: { jobs },
  } = useListJobsForConnectionStatus(connectionId);

  const configError = getConfigErrorFromJobs(jobs);

  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);

  // both default to 2; used as the connection schedule multiplier
  // to evaluate if a connection is OnTrack vs. Late/Error
  const lateMultiplier = useLateMultiplierExperiment();
  const errorMultiplier = useErrorMultiplierExperiment();

  const isRunning = jobs[0]?.job?.status === JobStatus.running || jobs[0]?.job?.status === JobStatus.incomplete;

  const isLastCompletedJobReset =
    jobs[0]?.job?.resetConfig &&
    jobs[0]?.job?.configType === JobConfigType.reset_connection &&
    (jobs[0]?.job?.status === JobStatus.succeeded || jobs[0]?.job?.status === JobStatus.failed);

  // compute the connection sync status from the job history
  const lastCompletedSyncJob = jobs.find(
    ({ job }) =>
      job && job.configType === JobConfigType.sync && jobStatusesIndicatingFinishedExecution.includes(job.status)
  );
  const lastSyncJobStatus = lastCompletedSyncJob?.job?.status;

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

  if (hasBreakingSchemaChange || configError) {
    return {
      status: ConnectionStatusIndicatorStatus.ActionRequired,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
    };
  }

  if (connection.status !== ConnectionStatus.active) {
    return {
      status: ConnectionStatusIndicatorStatus.Disabled,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
    };
  }

  if (lastSyncJobStatus == null || isLastCompletedJobReset) {
    return {
      status: ConnectionStatusIndicatorStatus.Pending,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
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
    };
  }

  if (lastSyncJobStatus === JobStatus.failed) {
    return {
      status: ConnectionStatusIndicatorStatus.OnTrack,
      lastSyncJobStatus,
      nextSync,
      lastSuccessfulSync,
      isRunning,
    };
  }

  return { status: ConnectionStatusIndicatorStatus.OnTime, lastSyncJobStatus, nextSync, lastSuccessfulSync, isRunning };
};
