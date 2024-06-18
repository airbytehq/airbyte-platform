/**
 * Returns the current state of streams needed to render the Active Streams UI.
 *
 * Pulls in the stream statuses, historical per-stream data, and current sync progress (if any)
 * and determines what to render.
 */

import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";

import { connectionsKeys } from "core/api";
import { JobConfigType, StreamStatusJobType, StreamStatusRunState } from "core/api/types/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";

import { getStreamKey } from "./computeStreamStatus";
import { useHistoricalStreamData } from "./useStreamsHistoricalData";
import { useStreamsStatuses } from "./useStreamsStatuses";
import { useStreamsSyncProgress } from "./useStreamsSyncProgress";

interface UIStreamState {
  streamName: string;
  streamNamespace?: string;
  activeJobConfigType?: JobConfigType;
  activeJobStartedAt?: number; // date?
  dataFreshAsOf?: number; // date?
  recordsExtracted?: number;
  recordsLoaded?: number;
  bytesLoaded?: number;
  status: ConnectionStatusIndicatorStatus;
  isLoadingHistoricalData: boolean;
}

export const useUiStreamStates = (connectionId: string): UIStreamState[] => {
  const connectionStatus = useConnectionStatus(connectionId);
  const [wasRunning, setWasRunning] = useState<boolean>(connectionStatus.isRunning);
  const [isFetchingPostJob, setIsFetchingPostJob] = useState<boolean>(false);
  const isSyncProgressEnabled = useExperiment("connection.syncProgress", false);

  const queryClient = useQueryClient();

  const { streamStatuses, enabledStreams } = useStreamsStatuses(connectionId);
  const syncProgress = useStreamsSyncProgress(connectionId, connectionStatus.isRunning, isSyncProgressEnabled);
  const isClearOrResetJob = (configType?: JobConfigType) =>
    configType === JobConfigType.clear || configType === JobConfigType.reset_connection;

  const streamsToList = enabledStreams
    .map((stream) => {
      return { streamName: stream.stream?.name ?? "", streamNamespace: stream.stream?.namespace };
    })
    .sort((a, b) => a.streamName.localeCompare(b.streamName));

  const { historicalStreamsData, isFetching: isLoadingHistoricalData } = useHistoricalStreamData(connectionId);
  // if we just finished a job, re-fetch the historical data and set wasRunning to false
  useEffect(() => {
    if (wasRunning && !connectionStatus.isRunning) {
      setIsFetchingPostJob(true);
      queryClient.invalidateQueries(connectionsKeys.lastJobPerStream(connectionId));
      queryClient.invalidateQueries(connectionsKeys.uptimeHistory(connectionId));
      queryClient.invalidateQueries(connectionsKeys.dataHistory(connectionId));
    }
    setWasRunning(connectionStatus.isRunning);
  }, [wasRunning, connectionStatus.isRunning, queryClient, connectionId]);

  // after we've fetched the data
  useEffect(() => {
    if (isFetchingPostJob && !isLoadingHistoricalData) {
      queryClient.invalidateQueries(connectionsKeys.syncProgress(connectionId));
      setIsFetchingPostJob(false);
    }
  }, [wasRunning, connectionStatus.isRunning, queryClient, connectionId, isFetchingPostJob, isLoadingHistoricalData]);

  const uiStreamStates = streamsToList.map((streamItem) => {
    // initialize the state as undefined
    const uiState: UIStreamState = {
      streamName: streamItem.streamName,
      streamNamespace: streamItem.streamNamespace,
      activeJobConfigType: undefined,
      activeJobStartedAt: undefined,
      dataFreshAsOf: undefined,
      recordsExtracted: undefined,
      recordsLoaded: undefined,
      bytesLoaded: undefined,
      status: ConnectionStatusIndicatorStatus.Pending,
      isLoadingHistoricalData,
    };

    const key = getStreamKey({ name: streamItem.streamName, namespace: streamItem.streamNamespace });
    const streamStatus = streamStatuses.get(key);
    const syncProgressItem = syncProgress.get(key);
    const historicalItem = historicalStreamsData.get(key);

    if (streamStatus?.status) {
      uiState.status = streamStatus.status;
    }

    // only pull from syncProgress OR historicalData for the latestSync related data
    if (syncProgressItem) {
      // uiState.activeJobConfigType = syncProgressItem.jobConfigType; TODO: once added to API!
      // also, for clear jobs, we should not show anything in this column
      uiState.recordsExtracted = syncProgressItem.recordsEmitted;
      uiState.recordsLoaded = syncProgressItem.recordsCommitted;
      uiState.activeJobStartedAt = streamStatus?.relevantHistory[0]?.transitionedAt;
    } else if (historicalItem && !isClearOrResetJob(historicalItem.configType)) {
      uiState.recordsLoaded = historicalItem.recordsCommitted;
      uiState.bytesLoaded = historicalItem.bytesCommitted;
    }

    const lastSuccessfulClear = streamStatus?.relevantHistory?.find(
      (status) => status.jobType === StreamStatusJobType.RESET && status.runState === StreamStatusRunState.COMPLETE
    );
    const lastSuccessfulSync = streamStatus?.relevantHistory?.find(
      (status) => status.jobType === StreamStatusJobType.SYNC && status.runState === StreamStatusRunState.COMPLETE
    );

    uiState.dataFreshAsOf =
      // has the stream successfully cleared since it successfully synced? then it's not fresh
      // note: refresh jobs will register as StreamStatusJobType.SYNC, so this includes them (which it should)
      (lastSuccessfulClear?.transitionedAt ?? 0) > (lastSuccessfulSync?.transitionedAt ?? 0) || !!syncProgressItem
        ? undefined
        : streamStatus?.lastSuccessfulSyncAt;

    return uiState;
  });

  return uiStreamStates;
};
