import dayjs from "dayjs";
import { useCallback } from "react";

import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";

import { AirbyteStreamWithStatusAndConfiguration, FakeStreamConfigWithStatus } from "./getStreamsWithStatus";

export const enum StreamStatusType {
  ActionRequired = "actionRequired",
  UpToDate = "upToDate",
  Disabled = "disabled",
  Error = "error",
  Late = "late",
  Pending = "pending",
}

// `late` here refers to how long past the last successful sync until it is flagged
const isStreamLate = (streamConfig: FakeStreamConfigWithStatus, lateMultiplier: number) => {
  return (
    // This can be undefined due to historical data, but should always be present
    streamConfig?.scheduleType &&
    !["cron", "manual"].includes(streamConfig.scheduleType) &&
    streamConfig.scheduleData?.basicSchedule?.units &&
    (streamConfig.lastSuccessfulSync ?? 0) * 1000 < // x1000 for a JS datetime
      dayjs()
        // Subtract 2x (or the late value override) the scheduled interval and compare it to last sync time
        .subtract(
          streamConfig.scheduleData.basicSchedule.units * lateMultiplier,
          streamConfig.scheduleData.basicSchedule.timeUnit
        )
        .valueOf()
  );
};

const isStreamSelected = (
  streamConfig: FakeStreamConfigWithStatus | undefined
): streamConfig is Exclude<FakeStreamConfigWithStatus, undefined> => !!streamConfig?.selected;

const isUnscheduledStream = (streamConfig: FakeStreamConfigWithStatus | undefined) =>
  ["cron", "manual", undefined].includes(streamConfig?.scheduleType);

const streamHasBeenSynced = (streamConfig: FakeStreamConfigWithStatus) =>
  streamConfig.latestAttemptCreatedAt !== undefined;

const streamHasError = (streamConfig: FakeStreamConfigWithStatus) =>
  streamConfig.latestAttemptStatus === "failed" || streamConfig.jobStatus === "failed";

// Status calculation logic comes from this ticket: https://github.com/airbytehq/airbyte/issues/23912
export const useGetStreamStatus = () => {
  const { connection } = useConnectionEditService();
  const lateMultiplier = useLateMultiplierExperiment();
  const errorMultiplier = useErrorMultiplierExperiment();
  const { hasSchemaChanges, hasBreakingSchemaChange } = useSchemaChanges(connection.schemaChange);

  return useCallback<(streamConfig?: FakeStreamConfigWithStatus) => StreamStatusType>(
    (streamConfig) => {
      if (!isStreamSelected(streamConfig)) {
        return StreamStatusType.Disabled;
      }

      if (!streamHasBeenSynced(streamConfig)) {
        return StreamStatusType.Pending;
      }

      // The `error` value is based on the `connection.streamCentricUI.error` experiment
      if (
        !hasBreakingSchemaChange &&
        streamHasError(streamConfig) &&
        (isUnscheduledStream(streamConfig) || isStreamLate(streamConfig, errorMultiplier))
      ) {
        return StreamStatusType.Error;
      }

      if (hasSchemaChanges) {
        return StreamStatusType.ActionRequired;
      }

      // The `late` value is based on the `connection.streamCentricUI.late` experiment
      if (isStreamLate(streamConfig, lateMultiplier)) {
        return StreamStatusType.Late;
      }

      return StreamStatusType.UpToDate;
    },
    [errorMultiplier, hasBreakingSchemaChange, hasSchemaChanges, lateMultiplier]
  );
};

export const useSortStreams = (
  streams: AirbyteStreamWithStatusAndConfiguration[]
): Record<StreamStatusType, AirbyteStreamWithStatusAndConfiguration[]> => {
  const getStreamStatus = useGetStreamStatus();
  return streams.reduce<Record<StreamStatusType, AirbyteStreamWithStatusAndConfiguration[]>>(
    (sortedStreams, stream) => {
      sortedStreams[getStreamStatus(stream.config)].push(stream);
      return sortedStreams;
    },
    // This is the intended display order thanks to Javascript object insertion order!
    {
      [StreamStatusType.ActionRequired]: [],
      [StreamStatusType.Error]: [],
      [StreamStatusType.Late]: [],
      [StreamStatusType.Pending]: [],
      [StreamStatusType.UpToDate]: [],
      [StreamStatusType.Disabled]: [],
    }
  );
};

export const filterEmptyStreamStatuses = (
  streams: Record<StreamStatusType, AirbyteStreamWithStatusAndConfiguration[]>
) => Object.entries(streams).filter(([, streams]) => !!streams.length);

export const useLateMultiplierExperiment = () => useExperiment("connection.streamCentricUI.lateMultiplier", 2);
export const useErrorMultiplierExperiment = () => useExperiment("connection.streamCentricUI.errorMultiplier", 2);
