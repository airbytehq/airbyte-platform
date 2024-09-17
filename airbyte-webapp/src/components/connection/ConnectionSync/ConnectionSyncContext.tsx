import { useQueryClient } from "@tanstack/react-query";
import { createContext, useCallback, useContext, useMemo } from "react";

import { isClearJob } from "area/connection/utils/jobs";
import { useInitialStreamSync } from "area/connection/utils/useInitialStreamSync";
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
  useCurrentConnection,
} from "core/api";
import {
  ConnectionStatus,
  ConnectionStream,
  JobStatus,
  WebBackendConnectionRead,
  JobReadList,
  RefreshMode,
} from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";

import { CancelJobModalBody } from "./CancelJobModalBody";

interface ConnectionSyncContext {
  syncConnection: () => Promise<void>;
  isSyncConnectionAvailable: boolean;
  connectionEnabled: boolean;
  syncStarting: boolean;
  jobSyncRunning: boolean;
  cancelJob: (() => void) | undefined;
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
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const analyticsService = useAnalyticsService();
  const { streamsSyncingForFirstTime, isConnectionInitialSync } = useInitialStreamSync(connection.connectionId);

  const { mutateAsync: doSyncConnection, isLoading: syncStarting } = useSyncConnection();
  const syncConnection = useCallback(async () => {
    doSyncConnection(connection);
  }, [connection, doSyncConnection]);

  const { mutateAsync: doCancelJob, isLoading: isCancelLoading } = useCancelJob();
  const cancelStarting = isCancelLoading || (mostRecentJob?.status !== "running" && mostRecentJob?.id === 999999999);

  const cancelJobWithConfirmationModal = useCallback(() => {
    const jobId = mostRecentJob?.id;

    if (!jobId) {
      return undefined;
    }

    const isClear = mostRecentJob?.configType === "clear" || mostRecentJob?.configType === "reset_connection";

    const handleCancel = () => {
      doCancelJob(jobId);

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

    return openConfirmationModal({
      title: "connection.actions.cancel.confirm.title",
      text: (
        <CancelJobModalBody
          streamsSyncingForFirstTime={streamsSyncingForFirstTime}
          isConnectionInitialSync={isConnectionInitialSync}
          configType={mostRecentJob?.configType}
        />
      ),

      submitButtonText: isClear
        ? "connection.actions.cancel.clear.confirm.submit"
        : "connection.actions.cancel.confirm.submit",
      cancelButtonText: isClear
        ? "connection.actions.cancel.clear.confirm.cancel"
        : "connection.actions.cancel.confirm.cancel",

      onCancel: () => {
        analyticsService.track(Namespace.CONNECTION, Action.DECLINED_CANCEL_SYNC, {
          actionDescription: "Closed modal without cancelling sync",
          connector_source: connection.source?.sourceName,
          connector_source_definition_id: connection.source?.sourceDefinitionId,
          connector_destination: connection.destination?.destinationName,
          connector_destination_definition_id: connection.destination?.destinationDefinitionId,
          job_id: jobId,
          config_type: mostRecentJob.configType,
        });
      },
      onSubmit: async () => {
        analyticsService.track(Namespace.CONNECTION, Action.CONFIRMED_CANCEL_SYNC, {
          actionDescription: "Canceled sync from confirmation modal",
          connector_source: connection.source?.sourceName,
          connector_source_definition_id: connection.source?.sourceDefinitionId,
          connector_destination: connection.destination?.destinationName,
          connector_destination_definition_id: connection.destination?.destinationDefinitionId,
          job_id: jobId,
          config_type: mostRecentJob.configType,
        });
        handleCancel();
        closeConfirmationModal();
      },
    });
  }, [
    mostRecentJob,
    openConfirmationModal,
    connection.connectionId,
    connection.source?.sourceName,
    connection.source?.sourceDefinitionId,
    connection.destination?.destinationName,
    connection.destination?.destinationDefinitionId,
    streamsSyncingForFirstTime,
    isConnectionInitialSync,
    doCancelJob,
    queryClient,
    analyticsService,
    closeConfirmationModal,
  ]);

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
    cancelJob: cancelJobWithConfirmationModal,
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
  const connection = useCurrentConnection();
  const context = useConnectionSyncContextInit(connection);

  return <connectionSyncContext.Provider value={context}>{children}</connectionSyncContext.Provider>;
};
