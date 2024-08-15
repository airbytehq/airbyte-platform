import { AirbyteStreamAndConfigurationWithEnforcedStream, getStreamKey } from "area/connection/utils";
import { StreamStatusRead } from "core/api/types/AirbyteClient";
import { naturalComparatorBy } from "core/utils/objects";

import { ConnectionStatusIndicatorStatus } from "../ConnectionStatusIndicator";

export interface StreamWithStatus {
  streamName: string;
  streamNamespace?: string;
  status: ConnectionStatusIndicatorStatus;
  isRunning: boolean;
  relevantHistory: StreamStatusRead[];
  lastSuccessfulSyncAt?: StreamStatusRead["transitionedAt"];
}

type StreamMapping = Record<ConnectionStatusIndicatorStatus, StreamWithStatus[]>;

/** deprecated... will remove with sync progress project */
export const sortStreamsByStatus = (
  enabledStreams: AirbyteStreamAndConfigurationWithEnforcedStream[],
  streamStatuses: Map<string, StreamWithStatus>
): StreamMapping => {
  const mappedStreams = enabledStreams.reduce<Record<ConnectionStatusIndicatorStatus, StreamWithStatus[]>>(
    (sortedStreams, { stream }) => {
      const streamKey = getStreamKey(stream);
      const streamStatus = streamStatuses.get(streamKey);
      if (streamStatus) {
        sortedStreams[streamStatus.status].push(streamStatus);
      }
      return sortedStreams;
    },
    // This is the intended display order thanks to Javascript object insertion order!
    {
      [ConnectionStatusIndicatorStatus.Failed]: [],
      [ConnectionStatusIndicatorStatus.Incomplete]: [],
      [ConnectionStatusIndicatorStatus.Pending]: [],
      [ConnectionStatusIndicatorStatus.Synced]: [],
      [ConnectionStatusIndicatorStatus.Paused]: [],
      [ConnectionStatusIndicatorStatus.RateLimited]: [],
      [ConnectionStatusIndicatorStatus.Syncing]: [],
      [ConnectionStatusIndicatorStatus.Clearing]: [],
      [ConnectionStatusIndicatorStatus.Refreshing]: [],
      [ConnectionStatusIndicatorStatus.Queued]: [],
      [ConnectionStatusIndicatorStatus.QueuedForNextSync]: [],
    }
  );

  // sort each bucket
  const bucketSorterByName = naturalComparatorBy<StreamWithStatus>(({ streamName }) => streamName);
  const bucketSorterByRunning = (a: StreamWithStatus, b: StreamWithStatus) => {
    if (a.isRunning && !b.isRunning) {
      return -1;
    }
    if (!a.isRunning && b.isRunning) {
      return 1;
    }
    return 0;
  };
  Object.entries(mappedStreams).forEach(([_key, configuredStreams]) => {
    configuredStreams.sort(bucketSorterByName); // alphabetize streams in bucket
    configuredStreams.sort(bucketSorterByRunning); // then sort running jobs to top of bucket
  });

  return mappedStreams;
};

export const sortStreamsAlphabetically = (
  enabledStreams: AirbyteStreamAndConfigurationWithEnforcedStream[],
  streamStatuses: Map<string, StreamWithStatus>
): StreamWithStatus[] => {
  // Collect all streams into a single array
  const allStreams = enabledStreams.reduce<StreamWithStatus[]>((streams, { stream }) => {
    const streamKey = getStreamKey(stream);
    const streamStatus = streamStatuses.get(streamKey);
    if (streamStatus) {
      streams.push(streamStatus);
    }
    return streams;
  }, []);

  // Define a comparator for sorting by stream name
  const comparatorByName = (a: StreamWithStatus, b: StreamWithStatus) => {
    return a.streamName.localeCompare(b.streamName);
  };

  // Sort the array by name
  allStreams.sort(comparatorByName);

  return allStreams;
};
