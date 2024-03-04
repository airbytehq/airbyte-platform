import { useQueryClient } from "@tanstack/react-query";
import { createContext, useCallback, useContext, useMemo } from "react";

import {
  useResetConnection,
  useSyncConnection,
  useCancelJob,
  useListJobsForConnectionStatus,
  jobsKeys,
  prependArtificialJobToStatus,
  useResetConnectionStream,
} from "core/api";
import {
  ConnectionStatus,
  ConnectionStream,
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
  cancelJob: (() => Promise<void>) | undefined;
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
  const { jobs } = useListJobsForConnectionStatus(connection.connectionId);
  const mostRecentJob = jobs?.[0]?.job;
  const connectionEnabled = connection.status === ConnectionStatus.active;
  const queryClient = useQueryClient();

  const { mutateAsync: doSyncConnection, isLoading: syncStarting } = useSyncConnection();
  const syncConnection = useCallback(async () => {
    doSyncConnection(connection);
  }, [connection, doSyncConnection]);

  const { mutateAsync: doCancelJob, isLoading: cancelStarting } = useCancelJob();
  const cancelJob = useMemo(() => {
    const jobId = mostRecentJob?.id;
    if (!jobId) {
      return undefined;
    }
    return async () => {
      await doCancelJob(jobId);
      queryClient.setQueriesData<JobReadList>(
        jobsKeys.useListJobsForConnectionStatus(connection.connectionId),
        (prevJobList) =>
          prependArtificialJobToStatus(
            { configType: mostRecentJob?.configType ?? "sync", status: JobStatus.cancelled },
            prevJobList
          )
      );
    };
  }, [mostRecentJob, doCancelJob, connection.connectionId, queryClient]);

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

  const jobSyncRunning = useMemo(
    () =>
      mostRecentJob?.configType === "sync" &&
      (mostRecentJob?.status === JobStatus.running || mostRecentJob?.status === JobStatus.incomplete),
    [mostRecentJob?.configType, mostRecentJob?.status]
  );
  const jobResetRunning = useMemo(
    () => mostRecentJob?.status === "running" && mostRecentJob.configType === "reset_connection",
    [mostRecentJob?.configType, mostRecentJob?.status]
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
