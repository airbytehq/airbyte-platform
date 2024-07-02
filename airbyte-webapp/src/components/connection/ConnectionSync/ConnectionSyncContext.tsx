import { useQueryClient } from "@tanstack/react-query";
import { createContext, useCallback, useContext, useMemo } from "react";

import { isClearJob } from "area/connection/utils/jobs";
import {
  useSyncConnection,
  useCancelJob,
  useListJobsForConnectionStatus,
  jobsKeys,
  prependArtificialJobToStatus,
  useRefreshConnectionStreams,
  connectionsKeys,
  useClearConnection,
  useClearConnectionStream,
} from "core/api";
import {
  ConnectionStatus,
  ConnectionStream,
  JobStatus,
  WebBackendConnectionRead,
  JobReadList,
  RefreshMode,
} from "core/api/types/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";

interface ConnectionSyncContext {
  syncConnection: () => Promise<void>;
  isSyncConnectionAvailable: boolean;
  connectionEnabled: boolean;
  syncStarting: boolean;
  jobSyncRunning: boolean;
  cancelJob: (() => Promise<void>) | undefined;
  cancelStarting: boolean;
  refreshStreams: ({
    streams,
    refreshMode,
  }: {
    streams?: ConnectionStream[];
    refreshMode: RefreshMode;
  }) => Promise<void>;
  refreshStarting: boolean;
  clearStreams: (streams?: ConnectionStream[]) => Promise<void>;
  clearStarting: boolean;
  jobClearRunning: boolean;
  jobRefreshRunning: boolean;
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
      queryClient.invalidateQueries(connectionsKeys.syncProgress(connection.connectionId));
    };
  }, [mostRecentJob, doCancelJob, connection.connectionId, queryClient]);

  const { mutateAsync: doResetConnection, isLoading: clearStarting } = useClearConnection();
  const { mutateAsync: resetStream } = useClearConnectionStream(connection.connectionId);
  const { mutateAsync: refreshStreams, isLoading: refreshStarting } = useRefreshConnectionStreams(
    connection.connectionId
  );

  const clearStreams = useCallback(
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
  const jobClearRunning = mostRecentJob?.status === "running" && isClearJob(mostRecentJob);

  const jobRefreshRunning = mostRecentJob?.status === "running" && mostRecentJob.configType === "refresh";

  const isSyncConnectionAvailable = useMemo(
    () =>
      !syncStarting &&
      !cancelStarting &&
      !clearStarting &&
      !refreshStarting &&
      !jobSyncRunning &&
      !jobClearRunning &&
      !jobRefreshRunning &&
      connectionEnabled,
    [
      cancelStarting,
      clearStarting,
      connectionEnabled,
      jobClearRunning,
      jobRefreshRunning,
      jobSyncRunning,
      refreshStarting,
      syncStarting,
    ]
  );

  return {
    syncConnection,
    isSyncConnectionAvailable,
    connectionEnabled,
    syncStarting,
    jobSyncRunning,
    cancelJob,
    cancelStarting,
    refreshStreams,
    refreshStarting,
    jobRefreshRunning,
    clearStreams,
    clearStarting,
    jobClearRunning,
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
