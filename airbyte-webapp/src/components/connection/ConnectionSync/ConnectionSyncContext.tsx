import { useQueryClient } from "@tanstack/react-query";
import { createContext, useCallback, useContext, useMemo } from "react";

import {
  useResetConnection,
  useResetConnectionStream,
  useSyncConnection,
  useCancelJob,
  useListJobsForConnectionStatus,
  jobsKeys,
  prependArtificialJobToStatus,
} from "core/api";
import {
  ConnectionStatus,
  ConnectionStream,
  JobWithAttemptsRead,
  JobConfigType,
  JobStatus,
  WebBackendConnectionRead,
  JobReadList,
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
  lastCompletedSyncJob?: JobWithAttemptsRead;
}

export const jobStatusesIndicatingFinishedExecution: string[] = [JobStatus.succeeded, JobStatus.failed];
const useConnectionSyncContextInit = (connection: WebBackendConnectionRead): ConnectionSyncContext => {
  const { jobs } = useListJobsForConnectionStatus(connection.connectionId);
  const connectionEnabled = connection.status === ConnectionStatus.active;
  const queryClient = useQueryClient();

  const { mutateAsync: doSyncConnection, isLoading: syncStarting } = useSyncConnection();
  const syncConnection = useCallback(async () => {
    doSyncConnection(connection);
  }, [connection, doSyncConnection]);

  const { mutateAsync: doCancelJob, isLoading: cancelStarting } = useCancelJob();
  const cancelJob = useCallback(async () => {
    const jobId = jobs?.[0]?.job?.id;
    if (jobId) {
      await doCancelJob(jobId);
      queryClient.setQueriesData<JobReadList>(
        jobsKeys.useListJobsForConnectionStatus(connection.connectionId),
        (prevJobList) =>
          prependArtificialJobToStatus(
            { configType: jobs?.[0]?.job?.configType ?? "sync", status: JobStatus.cancelled },
            prevJobList
          )
      );
    }
  }, [jobs, doCancelJob, connection.connectionId, queryClient]);

  const { mutateAsync: doResetConnection, isLoading: resetStarting } = useResetConnection();
  const { mutateAsync: resetStream } = useResetConnectionStream(connection.connectionId);
  const resetStreams = useCallback(
    async (streams?: ConnectionStream[]) => {
      if (streams) {
        // Reset a set of streams.
        await resetStream(streams);
      } else {
        // Reset all selected streams
        await doResetConnection(connection.connectionId);
      }
    },
    [connection.connectionId, doResetConnection, resetStream]
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
