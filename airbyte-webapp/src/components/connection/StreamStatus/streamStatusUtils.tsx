import { AirbyteStreamAndConfigurationWithEnforcedStream, getStreamKey } from "area/connection/utils";
import { StreamStatusRead } from "core/api/types/AirbyteClient";
import { naturalComparatorBy } from "core/utils/objects";
import { useExperiment } from "hooks/services/Experiment";

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

export const sortStreams = (
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
      [ConnectionStatusIndicatorStatus.ActionRequired]: [],
      [ConnectionStatusIndicatorStatus.Error]: [],
      [ConnectionStatusIndicatorStatus.Late]: [],
      [ConnectionStatusIndicatorStatus.Pending]: [],
      [ConnectionStatusIndicatorStatus.OnTrack]: [],
      [ConnectionStatusIndicatorStatus.OnTime]: [],
      [ConnectionStatusIndicatorStatus.Disabled]: [],
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

export const useLateMultiplierExperiment = () => useExperiment("connection.streamCentricUI.lateMultiplier", 2);
export const useErrorMultiplierExperiment = () => useExperiment("connection.streamCentricUI.errorMultiplier", 2);
