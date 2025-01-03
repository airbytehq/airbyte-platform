/**
 * Returns the current state of streams needed to render the Active Streams UI.
 *
 * Pulls in the stream statuses, historical per-stream data, and current sync progress (if any)
 * and determines what to render.
 */

import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { StreamStatusType } from "components/connection/StreamStatusIndicator";

import { connectionsKeys, useCurrentConnection, useGetConnectionSyncProgress } from "core/api";
import {
  AirbyteStreamAndConfiguration,
  ConnectionSyncStatus,
  JobConfigType,
  StreamStatusJobType,
  StreamStatusRunState,
} from "core/api/types/AirbyteClient";
import { useStreamsListContext } from "pages/connections/StreamStatusPage/StreamsListContext";

import { getStreamKey } from "./computeStreamStatus";
import { useHistoricalStreamData } from "./useStreamsHistoricalData";
import { useStreamsStatuses } from "./useStreamsStatuses";
import { useStreamsSyncProgress } from "./useStreamsSyncProgress";

interface BaseUIStreamState {
  streamName: string;
  streamNameWithPrefix: string;
  streamNamespace?: string;
  catalogStream?: AirbyteStreamAndConfiguration;
  activeJobConfigType?: JobConfigType;
  activeJobStartedAt?: number;
  dataFreshAsOf?: number;
  recordsExtracted?: number;
  recordsLoaded?: number;
  bytesExtracted?: number;
  bytesLoaded?: number;
  status: Exclude<StreamStatusType, "rateLimited">;
  isLoadingHistoricalData: boolean;
}

export interface RateLimitedUIStreamState extends Omit<BaseUIStreamState, "status"> {
  status: StreamStatusType.RateLimited;
  quotaReset?: number;
}

export type UIStreamState = BaseUIStreamState | RateLimitedUIStreamState;

export const useUiStreamStates = (connectionId: string): UIStreamState[] => {
  const { prefix, syncCatalog } = useCurrentConnection();
  const connectionStatus = useConnectionStatus(connectionId);
  const { enabledStreamsByName } = useStreamsListContext();
  const [wasRunning, setWasRunning] = useState<boolean>(connectionStatus.status === ConnectionSyncStatus.running);
  const [isFetchingPostJob, setIsFetchingPostJob] = useState<boolean>(false);
  const { data: connectionSyncProgress } = useGetConnectionSyncProgress(
    connectionId,
    connectionStatus.status === ConnectionSyncStatus.running
  );
  const currentJobId = connectionSyncProgress?.jobId;

  const queryClient = useQueryClient();

  const { streamStatuses } = useStreamsStatuses(connectionId);
  const syncProgress = useStreamsSyncProgress(connectionId, connectionStatus.status === ConnectionSyncStatus.running);
  const isClearOrResetJob = (configType?: JobConfigType) =>
    configType === JobConfigType.clear || configType === JobConfigType.reset_connection;

  const { historicalStreamsData, isFetching: isLoadingHistoricalData } = useHistoricalStreamData(connectionId);

  // if we just finished a job, re-fetch the historical data and set wasRunning to false
  useEffect(() => {
    if (wasRunning && connectionStatus.status !== ConnectionSyncStatus.running) {
      setIsFetchingPostJob(true);
      queryClient.invalidateQueries(connectionsKeys.lastJobPerStream(connectionId));
      queryClient.invalidateQueries(connectionsKeys.uptimeHistory(connectionId));
      queryClient.invalidateQueries(connectionsKeys.dataHistory(connectionId));
    }
    setWasRunning(connectionStatus.status === ConnectionSyncStatus.running);
  }, [wasRunning, connectionStatus.status, queryClient, connectionId]);

  // after we've fetched the data
  useEffect(() => {
    if (isFetchingPostJob && !isLoadingHistoricalData) {
      queryClient.invalidateQueries(connectionsKeys.syncProgress(connectionId));
      setIsFetchingPostJob(false);
    }
  }, [wasRunning, connectionStatus.status, queryClient, connectionId, isFetchingPostJob, isLoadingHistoricalData]);

  const uiStreamStates = enabledStreamsByName.map((streamItem) => {
    // initialize the state as undefined
    const uiState: UIStreamState = {
      streamName: streamItem.streamName,
      streamNameWithPrefix: `${prefix}${streamItem.streamName}`,
      streamNamespace: streamItem.streamNamespace,
      catalogStream: syncCatalog.streams.find(
        (catalogStream) =>
          catalogStream.stream?.name === streamItem.streamName &&
          catalogStream.stream?.namespace === streamItem.streamNamespace
      ),
      activeJobConfigType: undefined,
      activeJobStartedAt: undefined,
      dataFreshAsOf: undefined,
      recordsExtracted: undefined,
      recordsLoaded: undefined,
      bytesExtracted: undefined,
      bytesLoaded: undefined,
      status: StreamStatusType.Pending as StreamStatusType, // cast so TS keeps the wider UIStreamState union instead of narrowing to BaseUIStreamState
      isLoadingHistoricalData,
    };

    const key = getStreamKey({ name: streamItem.streamName, namespace: streamItem.streamNamespace });
    const streamStatus = streamStatuses.get(key);
    const syncProgressItem = syncProgress.get(key);
    const historicalItem = historicalStreamsData.get(key);

    if (streamStatus?.status) {
      uiState.status = streamStatus.status;
      if (uiState.status === StreamStatusType.RateLimited) {
        uiState.quotaReset = streamStatus.relevantHistory.at(0)?.metadata?.quotaReset;
      }
    }
    // only pull from syncProgress OR historicalData for the latestSync related data
    if (syncProgressItem) {
      // also, for clear jobs, we should not show anything in this column
      uiState.recordsExtracted = syncProgressItem.recordsEmitted;
      uiState.bytesExtracted = syncProgressItem.bytesEmitted;
      uiState.bytesLoaded = syncProgressItem.bytesCommitted;
      uiState.recordsLoaded = syncProgressItem.recordsCommitted;
      uiState.activeJobStartedAt =
        currentJobId === streamStatus?.relevantHistory[0]?.jobId
          ? streamStatus?.relevantHistory[0]?.transitionedAt
          : undefined;
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
