import dayjs from "dayjs";
import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";

import { Status as ConnectionSyncStatus } from "components/EntityTable/types";
import { getConnectionSyncStatus } from "components/EntityTable/utils";

import { useCancelJob } from "core/api";
import {
  JobRead,
  ConnectionStatus,
  ConnectionStream,
  JobWithAttemptsRead,
  JobCreatedAt,
  JobConfigType,
  JobStatus,
} from "core/request/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useResetConnection, useResetConnectionStream, useSyncConnection } from "hooks/services/useConnectionHook";
import { moveTimeToFutureByPeriod } from "utils/time";

interface ConnectionSyncContext {
  syncConnection: () => Promise<void>;
  connectionEnabled: boolean;
  syncStarting: boolean;
  jobSyncRunning: boolean;
  cancelJob: () => Promise<void>;
  cancelStarting: boolean;
  resetStreams: (streams?: ConnectionStream[]) => Promise<void>;
  resetStarting: boolean;
  jobResetRunning: boolean;
  activeJob?: JobRead;
  lastCompletedSyncJob?: JobWithAttemptsRead;
  connectionStatus: ConnectionSyncStatus;
  latestSyncJobCreatedAt?: JobCreatedAt;
  nextSync?: dayjs.Dayjs;
  lastSuccessfulSync?: number;
}

const jobStatusesIndicatingFinishedExecution: string[] = [JobStatus.succeeded, JobStatus.failed, JobStatus.incomplete];
const useConnectionSyncContextInit = (jobs: JobWithAttemptsRead[]): ConnectionSyncContext => {
  const { connection } = useConnectionEditService();
  const [activeJob, setActiveJob] = useState(jobs[0]?.job);
  const connectionEnabled = connection.status === ConnectionStatus.active;

  useEffect(() => {
    if (activeJob?.updatedAt && jobs?.[0]?.job?.updatedAt && activeJob.updatedAt <= jobs[0].job.updatedAt) {
      setActiveJob(jobs[0].job);
    }
  }, [activeJob, jobs]);

  const { mutateAsync: doSyncConnection, isLoading: syncStarting } = useSyncConnection();
  const syncConnection = useCallback(async () => {
    setActiveJob((await doSyncConnection(connection)).job);
  }, [connection, doSyncConnection]);

  const { mutateAsync: doCancelJob, isLoading: cancelStarting } = useCancelJob();
  const cancelJob = useCallback(async () => {
    const jobId = jobs?.[0]?.job?.id;
    if (jobId) {
      setActiveJob((await doCancelJob(jobId)).job);
    }
  }, [jobs, doCancelJob]);

  const { mutateAsync: doResetConnection, isLoading: resetStarting } = useResetConnection();
  const { mutateAsync: resetStream } = useResetConnectionStream(connection.connectionId);
  const resetStreams = useCallback(
    async (streams?: ConnectionStream[]) => {
      if (streams) {
        // Reset a set of streams.
        setActiveJob((await resetStream(streams)).job);
      } else {
        // Reset all selected streams
        setActiveJob((await doResetConnection(connection.connectionId)).job);
      }
    },
    [connection.connectionId, doResetConnection, resetStream]
  );

  const jobSyncRunning = useMemo(
    () => activeJob?.status === "running" && activeJob.configType === "sync",
    [activeJob?.configType, activeJob?.status]
  );
  const jobResetRunning = useMemo(
    () => activeJob?.status === "running" && activeJob.configType === "reset_connection",
    [activeJob?.configType, activeJob?.status]
  );

  const lastCompletedSyncJob = jobs.find(
    ({ job }) =>
      job && job.configType === JobConfigType.sync && jobStatusesIndicatingFinishedExecution.includes(job.status)
  );
  const lastSyncJobStatus = lastCompletedSyncJob?.job?.status || connection.latestSyncJobStatus;
  const connectionStatus = getConnectionSyncStatus(connection.status, lastSyncJobStatus);

  const lastSyncJob = jobs.find(({ job }) => job?.configType === JobConfigType.sync);
  const latestSyncJobCreatedAt = lastSyncJob?.job?.createdAt;

  const lastSuccessfulSyncJob = jobs.find(
    ({ job }) => job?.configType === JobConfigType.sync && job?.status === JobStatus.succeeded
  );
  const lastSuccessfulSync = lastSuccessfulSyncJob?.job?.createdAt;

  let nextSync;
  if (latestSyncJobCreatedAt && connection.scheduleData?.basicSchedule) {
    const latestSync = dayjs(latestSyncJobCreatedAt * 1000);
    nextSync = moveTimeToFutureByPeriod(
      latestSync.subtract(connection.scheduleData.basicSchedule.units, connection.scheduleData.basicSchedule.timeUnit),
      connection.scheduleData.basicSchedule.units,
      connection.scheduleData.basicSchedule.timeUnit
    );
  }

  return {
    syncConnection,
    connectionEnabled,
    syncStarting,
    jobSyncRunning,
    cancelJob,
    cancelStarting,
    resetStreams,
    resetStarting,
    jobResetRunning,
    activeJob,
    lastCompletedSyncJob,
    connectionStatus,
    latestSyncJobCreatedAt,
    nextSync,
    lastSuccessfulSync,
  };
};

const connectionSyncContext = createContext<ConnectionSyncContext | null>(null);

export const useConnectionSyncContext = () => {
  const context = useContext(connectionSyncContext);
  if (context === null) {
    throw new Error("useConnectionSyncContext must be used within a ConnectionSyncContextProvider");
  }
  return context;
};

export const ConnectionSyncContextProvider: React.FC<{
  jobs: JobWithAttemptsRead[];
}> = ({ jobs, children }) => {
  const context = useConnectionSyncContextInit(jobs);

  return <connectionSyncContext.Provider value={context}>{children}</connectionSyncContext.Provider>;
};
