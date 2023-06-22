import { useRef } from "react";

import { useConnectionStatus } from "components/connection/ConnectionStatus/useConnectionStatus";
import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import {
  StreamWithStatus,
  useErrorMultiplierExperiment,
  useLateMultiplierExperiment,
} from "components/connection/StreamStatus/streamStatusUtils";

import { useListStreamsStatuses } from "core/api";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useExperiment } from "hooks/services/Experiment";
import { useGetConnection } from "hooks/services/useConnectionHook";

import {
  AirbyteStreamAndConfigurationWithEnforcedStream,
  computeStreamStatus,
  getStreamKey,
} from "./computeStreamStatus";

const createEmptyStreamsStatuses = (): ReturnType<typeof useListStreamsStatuses> => ({ streamStatuses: [] });

export const useStreamsStatuses = ({
  workspaceId,
  connectionId,
}: {
  workspaceId: string;
  connectionId: string;
}): {
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

  const data = useStreamStatusesFunction({
    workspaceId,
    connectionId,
    pagination: {
      // obnoxiously high for now, the endpoint returns statuses by timestamp ASC so most recent is at the end
      // we are using this to validate the approach and inform API changes
      // while in this state, we are not enabling the flag outside of well-known workspaces
      pageSize: 25000,
      rowOffset: 0,
    },
  });

  const connection = useGetConnection(connectionId);
  const { hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);
  const lateMultiplier = useLateMultiplierExperiment();
  const errorMultiplier = useErrorMultiplierExperiment();

  const connectionStatus = useConnectionStatus(connectionId);

  const enabledStreams: AirbyteStreamAndConfigurationWithEnforcedStream[] = connection.syncCatalog.streams.filter(
    (stream) => stream.config?.selected && stream.stream
  ) as AirbyteStreamAndConfigurationWithEnforcedStream[];

  const streamStatuses = new Map<string, StreamWithStatus>();

  if (data.streamStatuses) {
    // using transitionedAt, sort most recent first
    const orderedStreamStatuses = [...data.streamStatuses].sort((a, b) => {
      if (a.transitionedAt > b.transitionedAt) {
        return -1;
      }
      if (a.transitionedAt < b.transitionedAt) {
        return 1;
      }
      return 0;
    });

    // if there are no stream statuses, we fallback to connection status
    // in the case of a new connection that _will_ have stream statuses
    // this Just Works as the connection status will be Pending
    // and that's correct for new streams; as soon as one statuses exists
    // each stream status will be initialized with Pending - instead of the
    // connection status - and it has its individual stream status computed
    const hasPerStreamStatuses = orderedStreamStatuses.length > 0;

    // map stream statuses to enabled streams
    // naively, this is O(m*n) where m=enabledStreams and n=streamStatuses
    // for enabledStream:
    //   for streamStatus:
    //     if streamStatus.streamName === enabledStream.name then processStatusForStream
    //
    // instead we can approach O(n) where n=streamStatuses
    // by keeping track of # of "finalized" streams:
    // we can stop processing when finalizedStreamCount === enabledStreams.length
    // OR reaching the end of streamStatuses
    //
    // a stream is "finalized" when enough statuses have been processed to determine its final state:
    // 1. encountering a successful sync (SUCCESS)
    // 2. encountering a reset of the stream (PENDING)
    // 3. encountered enough historical sync data to determine if a failure/error is systemic or transient

    // initialize enabled streams as Pending with 0 history
    // (with fallback to connection status when hasPerStreamStatuses === false)
    for (let i = 0; i < enabledStreams.length; i++) {
      const enabledStream = enabledStreams[i];
      const streamKey = getStreamKey(enabledStream.stream);

      const streamStatus: StreamWithStatus = {
        streamName: enabledStream.stream.name,
        streamNamespace: enabledStream.stream.namespace === "" ? undefined : enabledStream.stream.namespace,
        status: ConnectionStatusIndicatorStatus.Pending,
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
    }

    const finalizedStreams = new Set<string>();

    for (let i = 0; i < orderedStreamStatuses.length; i++) {
      const streamStatus = orderedStreamStatuses[i];
      const streamKey = getStreamKey(streamStatus);

      if (finalizedStreams.has(streamKey)) {
        // no need to process this status
        continue;
      }

      const mappedStreamStatus = streamStatuses.get(streamKey);
      if (mappedStreamStatus) {
        mappedStreamStatus.relevantHistory.push(streamStatus); // intentionally mutate the array inside the map
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
          mappedStreamStatus.status = detectedStatus.status;
          mappedStreamStatus.isRunning = detectedStatus.isRunning;
          mappedStreamStatus.lastSuccessfulSyncAt = detectedStatus.lastSuccessfulSync?.transitionedAt;
          finalizedStreams.add(streamKey);
        }
      }
    }
  }

  return {
    streamStatuses,
    enabledStreams,
  };
};
