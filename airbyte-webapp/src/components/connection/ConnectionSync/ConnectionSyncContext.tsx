import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";

import { JobRead, ConnectionStatus, ConnectionStream, JobWithAttemptsRead } from "core/request/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useResetConnection, useResetConnectionStream, useSyncConnection } from "hooks/services/useConnectionHook";
import { useCancelJob } from "services/job/JobService";

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
}

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
