import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";

import { ConnectionStream, JobWithAttemptsRead } from "core/request/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useResetConnection, useResetConnectionStream, useSyncConnection } from "hooks/services/useConnectionHook";
import { useCancelJob } from "services/job/JobService";

const useConnectionSyncContextInit = (jobs: JobWithAttemptsRead[]) => {
  const { connection } = useConnectionEditService();
  const [activeJob, setActiveJob] = useState(jobs[0]?.job);

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

const ConnectionSyncContext = createContext<ReturnType<typeof useConnectionSyncContextInit> | null>(null);

export const useConnectionSyncContext = () => {
  const context = useContext(ConnectionSyncContext);
  if (context === null) {
    throw new Error("useConnectionSyncContext must be used within a ConnectionSyncContextProvider");
  }
  return context;
};

export const ConnectionSyncContextProvider: React.FC<{
  jobs: JobWithAttemptsRead[];
}> = ({ jobs, children }) => {
  const connectionSyncContext = useConnectionSyncContextInit(jobs);

  return <ConnectionSyncContext.Provider value={connectionSyncContext}>{children}</ConnectionSyncContext.Provider>;
};
