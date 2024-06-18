import { useRef } from "react";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import {
  StreamWithStatus,
  useErrorMultiplierExperiment,
  useLateMultiplierExperiment,
} from "components/connection/StreamStatus/streamStatusUtils";

import { useListStreamsStatuses, useGetConnection } from "core/api";
import { StreamStatusJobType, StreamStatusRead } from "core/api/types/AirbyteClient";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useExperiment } from "hooks/services/Experiment";

import {
  AirbyteStreamAndConfigurationWithEnforcedStream,
  computeStreamStatus,
  getStreamKey,
} from "./computeStreamStatus";
import { useStreamsSyncProgress } from "./useStreamsSyncProgress";

const createEmptyStreamsStatuses = (): ReturnType<typeof useListStreamsStatuses> => ({ streamStatuses: [] });

export const sortStreamStatuses = (a: StreamStatusRead, b: StreamStatusRead) => {
  if (a.transitionedAt > b.transitionedAt) {
    return -1;
  }
  if (a.transitionedAt < b.transitionedAt) {
    return 1;
  }
  return 0;
};

export const useStreamsStatuses = (
  connectionId: string
): {
  streamStatuses: Map<string, StreamWithStatus>;
  enabledStreams: AirbyteStreamAndConfigurationWithEnforcedStream[];
} => {
  const doUseStreamStatuses = useExperiment("connection.streamCentricUI.v2", false);
  // memoizing the function to call to get per-stream statuses as
  // otherwise breaks the Rules of Hooks by introducing a conditional;
  // using ref here as react doesn't guarantee `useMemo` won't drop the reference
  // using ref here as react doesn't guarantee `useMemo` won't drop the reference
  // and we need to be sure of it in this case
  const { current: useStreamStatusesFunction } = useRef(
    doUseStreamStatuses ? useListStreamsStatuses : createEmptyStreamsStatuses
  );
  const data = useStreamStatusesFunction({ connectionId });

  const connection = useGetConnection(connectionId);
  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);
  const showSyncProgress = useExperiment("connection.syncProgress", false);
  const lateMultiplier = useLateMultiplierExperiment();
  const errorMultiplier = useErrorMultiplierExperiment();
  const connectionStatus = useConnectionStatus(connectionId);
  const isConnectionDisabled = connectionStatus.status === ConnectionStatusIndicatorStatus.Paused;

  // TODO: Ideally we can pull this from the stream status endpoint directly once the "pending" status has been updated to reflect the correct status
  // for now, we'll use this
  const syncProgressMap = useStreamsSyncProgress(connectionId, connectionStatus.isRunning, showSyncProgress);

  const enabledStreams: AirbyteStreamAndConfigurationWithEnforcedStream[] = connection.syncCatalog.streams.filter(
    (stream) =>
      (showSyncProgress && !!stream.stream && syncProgressMap.has(getStreamKey(stream.stream))) ||
      (stream.config?.selected && stream.stream)
  ) as AirbyteStreamAndConfigurationWithEnforcedStream[];
  const streamStatuses = new Map<string, StreamWithStatus>();

  if (data.streamStatuses) {
    // if there are no stream statuses, we fallback to connection status
    // in the case of a new connection that _will_ have stream statuses
    // this Just Works as the connection status will be Pending
    // and that's correct for new streams; as soon as one statuses exists
    // each stream status will be initialized with Pending - instead of the
    // connection status - and it has its individual stream status computed
    const hasPerStreamStatuses =
      data.streamStatuses.length > 0 &&
      // and not all stream statuses are RESET, as the platform emits RESET statuses
      // but unknown if connector is emitting other statuses
      !data.streamStatuses.every((status) => status.jobType === StreamStatusJobType.RESET);

    enabledStreams.forEach((enabledStream) => {
      const streamKey = getStreamKey(enabledStream.stream);
      const syncProgressItem = syncProgressMap.get(streamKey);
      const streamStatus: StreamWithStatus = {
        streamName: enabledStream.stream.name,
        streamNamespace: enabledStream.stream.namespace === "" ? undefined : enabledStream.stream.namespace,
        status: isConnectionDisabled ? ConnectionStatusIndicatorStatus.Paused : ConnectionStatusIndicatorStatus.Pending,
        isRunning: false,
        relevantHistory: [],
      };

      if (!hasPerStreamStatuses) {
        streamStatus.status = connectionStatus.status;
        streamStatus.isRunning = showSyncProgress ? !!syncProgressItem : connectionStatus.isRunning;
        streamStatus.isRunning = showSyncProgress ? !!syncProgressItem : connectionStatus.isRunning;
        streamStatus.lastSuccessfulSyncAt = connectionStatus.lastSuccessfulSync
          ? connectionStatus.lastSuccessfulSync * 1000 // unix timestamp in seconds -> milliseconds
          : undefined;
      }

      streamStatuses.set(streamKey, streamStatus);
    });

    if (hasPerStreamStatuses && !isConnectionDisabled) {
      // push each stream status entry into to the corresponding stream's history
      data.streamStatuses.forEach((streamStatus) => {
        const streamKey = getStreamKey(streamStatus);
        const mappedStreamStatus = streamStatuses.get(streamKey);
        if (mappedStreamStatus) {
          mappedStreamStatus.relevantHistory.push(streamStatus);
        }
      });
      // compute the final status for each stream
      enabledStreams.forEach((enabledStream) => {
        const streamKey = getStreamKey(enabledStream.stream);
        const mappedStreamStatus = streamStatuses.get(streamKey);
        const syncProgressItem = syncProgressMap.get(streamKey);

        if (mappedStreamStatus) {
          mappedStreamStatus.relevantHistory.sort(sortStreamStatuses); // put the histories are in order

          const detectedStatus = computeStreamStatus({
            statuses: mappedStreamStatus.relevantHistory,
            scheduleType: connection.scheduleType,
            scheduleData: connection.scheduleData,
            hasBreakingSchemaChange,
            lateMultiplier,
            errorMultiplier,
            showSyncProgress,
            isSyncing: showSyncProgress && !!syncProgressItem ? true : false,
            recordsExtracted: syncProgressMap.get(streamKey)?.recordsEmitted,
            runningJobConfigType: syncProgressItem?.configType,
          });

          if (detectedStatus.status != null) {
            mappedStreamStatus.status = detectedStatus.status;
          }

          mappedStreamStatus.isRunning =
            detectedStatus.status === ConnectionStatusIndicatorStatus.Syncing ||
            detectedStatus.status === ConnectionStatusIndicatorStatus.Queued;

          mappedStreamStatus.lastSuccessfulSyncAt = detectedStatus.lastSuccessfulSync?.transitionedAt;
        }
      });
    }

    if (isConnectionDisabled) {
      data.streamStatuses.forEach((streamStatus) => {
        const streamKey = getStreamKey(streamStatus);
        const mappedStreamStatus = streamStatuses.get(streamKey);
        if (mappedStreamStatus) {
          mappedStreamStatus.status = ConnectionStatusIndicatorStatus.Paused;
        }
      });
    }
  }

  return {
    streamStatuses,
    enabledStreams,
  };
};
