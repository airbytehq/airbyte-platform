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
  // using ref here as react doesn't guaruntee `useMemo` won't drop the reference
  // and we need to be sure of it in this case
  const { current: useStreamStatusesFunction } = useRef(
    doUseStreamStatuses ? useListStreamsStatuses : createEmptyStreamsStatuses
  );

  const data = useStreamStatusesFunction({ connectionId });

  const connection = useGetConnection(connectionId);
  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);
  const lateMultiplier = useLateMultiplierExperiment();
  const errorMultiplier = useErrorMultiplierExperiment();

  const connectionStatus = useConnectionStatus(connectionId);
  const isConnectionDisabled = connectionStatus.status === ConnectionStatusIndicatorStatus.Disabled;

  const enabledStreams: AirbyteStreamAndConfigurationWithEnforcedStream[] = connection.syncCatalog.streams.filter(
    (stream) => stream.config?.selected && stream.stream
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
      // there are stream statuses
      data.streamStatuses.length > 0 &&
      // and not all stream statuses are RESET, as the platform emits RESET statuses
      // but unknown if connector is emitting other statuses
      !data.streamStatuses.every((status) => status.jobType === StreamStatusJobType.RESET);

    // map stream statuses to enabled streams
    // naively, this is O(m*n) where m=enabledStreams and n=streamStatuses
    // for enabledStream:
    //   for streamStatus:
    //     if streamStatus.streamName === enabledStream.name then processStatusForStream
    //
    // instead, we can reduce each stream status into a map of <Stream, StatusEntry[]>
    // and process each stream's history in one shot

    // initialize enabled streams as Pending with 0 history
    // (with fallback to connection status when hasPerStreamStatuses === false)
    enabledStreams.reduce((streamStatuses, enabledStream) => {
      const streamKey = getStreamKey(enabledStream.stream);

      const streamStatus: StreamWithStatus = {
        streamName: enabledStream.stream.name,
        streamNamespace: enabledStream.stream.namespace === "" ? undefined : enabledStream.stream.namespace,
        status: isConnectionDisabled
          ? ConnectionStatusIndicatorStatus.Disabled
          : ConnectionStatusIndicatorStatus.Pending,
        isRunning: false,
        relevantHistory: [],
      };

      if (hasPerStreamStatuses === false) {
        streamStatus.status = connectionStatus.status;
        streamStatus.isRunning = connectionStatus.isRunning;
        streamStatus.lastSuccessfulSyncAt = connectionStatus.lastSuccessfulSync
          ? connectionStatus.lastSuccessfulSync * 1000 // unix timestamp in seconds -> milliseconds
          : undefined;
      }

      streamStatuses.set(streamKey, streamStatus);

      return streamStatuses;
    }, streamStatuses);

    if (hasPerStreamStatuses && !isConnectionDisabled) {
      // push each stream status entry into to the corresponding stream's history
      data.streamStatuses.reduce((streamStatuses, streamStatus) => {
        const streamKey = getStreamKey(streamStatus);
        const mappedStreamStatus = streamStatuses.get(streamKey);
        if (mappedStreamStatus) {
          mappedStreamStatus.relevantHistory.push(streamStatus); // intentionally mutate the array inside the map
        }
        return streamStatuses;
      }, streamStatuses);

      // compute the final status for each stream
      enabledStreams.reduce((streamStatuses, enabledStream) => {
        const streamKey = getStreamKey(enabledStream.stream);
        const mappedStreamStatus = streamStatuses.get(streamKey);

        if (mappedStreamStatus) {
          mappedStreamStatus.relevantHistory.sort(sortStreamStatuses); // put the histories are in order
          const detectedStatus = computeStreamStatus({
            statuses: mappedStreamStatus.relevantHistory,
            scheduleType: connection.scheduleType,
            scheduleData: connection.scheduleData,
            hasBreakingSchemaChange,
            lateMultiplier,
            errorMultiplier,
          });

          if (detectedStatus.status != null) {
            // we have enough history to determine the final status
            // otherwise it will be left in Pending
            mappedStreamStatus.status = detectedStatus.status;
          }
          mappedStreamStatus.isRunning = detectedStatus.isRunning;
          mappedStreamStatus.lastSuccessfulSyncAt = detectedStatus.lastSuccessfulSync?.transitionedAt;
        }

        return streamStatuses;
      }, streamStatuses);
    }
  }

  return {
    streamStatuses,
    enabledStreams,
  };
};
