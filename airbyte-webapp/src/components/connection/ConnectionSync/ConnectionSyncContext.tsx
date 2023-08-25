import { createContext, useCallback, useContext, useMemo } from "react";

import {
  useResetConnection,
  useResetConnectionStream,
  useSyncConnection,
  useCancelJob,
  useListJobsForConnectionStatus,
  useSetConnectionJobsData,
} from "core/api";
import {
  ConnectionStatus,
  ConnectionStream,
  JobWithAttemptsRead,
  JobConfigType,
  JobStatus,
  JobInfoRead,
  WebBackendConnectionRead,
} from "core/api/types/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";

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
  lastCompletedSyncJob?: JobWithAttemptsRead;
}

export const jobStatusesIndicatingFinishedExecution: string[] = [JobStatus.succeeded, JobStatus.failed];
const useConnectionSyncContextInit = (connection: WebBackendConnectionRead): ConnectionSyncContext => {
  const jobsPageSize = useExperiment("connection.streamCentricUI.numberOfLogsToLoad", 10);
  const {
    data: { jobs },
  } = useListJobsForConnectionStatus(connection.connectionId);
  const connectionEnabled = connection.status === ConnectionStatus.active;
  const setConnectionJobsData = useSetConnectionJobsData(connection.connectionId);

  const prependJob = useCallback(
    (newJob: JobInfoRead) => {
      setConnectionJobsData((prev) => {
        // if the new job id is already in the list, don't add it again
        if (prev?.jobs?.[0]?.job?.id === newJob.job.id) {
          return prev;
        }

        const newJobWithAttempts: JobWithAttemptsRead = {
          job: {
            ...newJob.job,
            // if the new job's status is pending, set to running so the UI updates immediately
            status: newJob.job.status === JobStatus.pending ? JobStatus.running : newJob.job.status,
          },
          attempts: [],
        };
        const jobs = [newJobWithAttempts, ...(prev?.jobs ?? [])];
        jobs.length = Math.min(jobs.length, jobsPageSize); // drop any jobs after the first $jobsPageSize
        return {
          jobs,
          totalJobCount: jobs.length,
        };
      });
    },
    [setConnectionJobsData, jobsPageSize]
  );

  const { mutateAsync: doSyncConnection, isLoading: syncStarting } = useSyncConnection();
  const syncConnection = useCallback(async () => {
    prependJob(await doSyncConnection(connection));
  }, [connection, doSyncConnection, prependJob]);

  const { mutateAsync: doCancelJob, isLoading: cancelStarting } = useCancelJob();
  const cancelJob = useCallback(async () => {
    const jobId = jobs?.[0]?.job?.id;
    if (jobId) {
      await doCancelJob(jobId);
      setConnectionJobsData((prev) => {
        // deep copy from previous data because
        // 1. we don't want to mutate the in-state objects
        // 2. react query doesn't see an uncloned object changed, preventing a UI update
        const jobs = structuredClone(prev?.jobs ?? []);
        if (jobs[0].job) {
          jobs[0].job.status = JobStatus.cancelled;
        }
        return {
          jobs,
          totalJobCount: jobs.length,
        };
      });
    }
  }, [jobs, doCancelJob, setConnectionJobsData]);

  const { mutateAsync: doResetConnection, isLoading: resetStarting } = useResetConnection();
  const { mutateAsync: resetStream } = useResetConnectionStream(connection.connectionId);
  const resetStreams = useCallback(
    async (streams?: ConnectionStream[]) => {
      if (streams) {
        // Reset a set of streams.
        prependJob(await resetStream(streams));
      } else {
        // Reset all selected streams
        prependJob(await doResetConnection(connection.connectionId));
      }
    },
    [connection.connectionId, doResetConnection, resetStream, prependJob]
  );

  const activeJob = jobs[0]?.job;
  const jobSyncRunning = useMemo(
    () =>
      activeJob?.configType === "sync" &&
      (activeJob?.status === JobStatus.running || activeJob?.status === JobStatus.incomplete),
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
    lastCompletedSyncJob,
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

export const ConnectionSyncContextProvider: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { connection } = useConnectionEditService();
  const context = useConnectionSyncContextInit(connection);

  return <connectionSyncContext.Provider value={context}>{children}</connectionSyncContext.Provider>;
};
