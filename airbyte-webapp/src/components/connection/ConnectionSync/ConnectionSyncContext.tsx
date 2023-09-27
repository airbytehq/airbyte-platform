import { createContext, useCallback, useContext, useMemo } from "react";

import {
  useResetConnection,
  useSyncConnection,
  useCancelJob,
  useListJobsForConnection,
  useSetConnectionJobsData,
  useSetConnectionRunState,
} from "core/api";
import {
  ConnectionStatus,
  ConnectionStream,
  JobWithAttemptsRead,
  JobStatus,
  JobInfoRead,
  WebBackendConnectionRead,
} from "core/api/types/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

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
}

export const jobStatusesIndicatingFinishedExecution: string[] = [
  JobStatus.succeeded,
  JobStatus.failed,
  JobStatus.cancelled,
];
const useConnectionSyncContextInit = (connection: WebBackendConnectionRead): ConnectionSyncContext => {
  const { connectionId } = connection;
  const jobsPageSize = 1;
  const {
    data: { jobs },
  } = useListJobsForConnection(connectionId);
  const connectionEnabled = connection.status === ConnectionStatus.active;
  const setConnectionJobsData = useSetConnectionJobsData(connectionId);
  const setConnectionStatusRunState = useSetConnectionRunState(connectionId);

  const prependJob = useCallback(
    (newJob: JobInfoRead) => {
      const isNewJobRunning = newJob.job.status === JobStatus.pending || newJob.job.status === JobStatus.running;
      setConnectionStatusRunState(isNewJobRunning);
      setConnectionJobsData((prev) => {
        // if the new job id is already in the list, don't add it again
        if (prev?.jobs?.[0]?.job?.id === newJob.job.id) {
          return prev;
        }

        const newJobWithAttempts: JobWithAttemptsRead = {
          job: {
            ...newJob.job,
            // if the new job's status is pending, set to running so the UI updates immediately
            status: isNewJobRunning ? JobStatus.running : newJob.job.status,
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
    [setConnectionJobsData, jobsPageSize, setConnectionStatusRunState]
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
      setConnectionStatusRunState(false);
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
  }, [jobs, doCancelJob, setConnectionJobsData, setConnectionStatusRunState]);

  const { mutateAsync: doResetConnection, isLoading: resetStarting } = useResetConnection();
  const resetStreams = useCallback(async () => {
    // Reset all streams
    prependJob(await doResetConnection(connectionId));
  }, [connectionId, doResetConnection, prependJob]);

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
