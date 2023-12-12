import dayjs from "dayjs";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";

import {
  AirbyteStream,
  AirbyteStreamAndConfiguration,
  ConnectionScheduleData,
  ConnectionScheduleType,
  ConnectionSyncResultRead,
  StreamStatusIncompleteRunCause,
  StreamStatusJobType,
  StreamStatusRead,
  StreamStatusRunState,
} from "core/api/types/AirbyteClient";

export type AirbyteStreamAndConfigurationWithEnforcedStream = AirbyteStreamAndConfiguration & { stream: AirbyteStream };

export function getStreamKey(streamStatus: StreamStatusRead | AirbyteStream | ConnectionSyncResultRead) {
  let name: string;
  let namespace: string | undefined;

  if ("connectionId" in streamStatus) {
    // StreamStatusRead
    name = streamStatus.streamName;
    namespace = streamStatus.streamNamespace;
  } else if ("streamName" in streamStatus) {
    // ConnectionSyncResultRead
    name = streamStatus.streamName;
    namespace = streamStatus.streamNamespace;
  } else {
    // AirbyteStream
    name = streamStatus.name;
    namespace = streamStatus.namespace;
  }

  if (namespace === undefined) {
    // opting to unify `undefined` into ""
    // instead of the string literal "undefined" which
    // could conflict with a namespace called "undefined"
    namespace = "";
  }

  return `${name}-${namespace}`;
}

export const isStreamLate = (
  streamStatuses: StreamStatusRead[],
  scheduleType: ConnectionScheduleType | undefined,
  scheduleData: ConnectionScheduleData | undefined,
  missedScheduleCount: number
) => {
  if (scheduleType !== ConnectionScheduleType.basic) {
    // only basic schedules can be OnTrack
    return false;
  }

  const { timeUnit, units } = scheduleData?.basicSchedule ?? {};

  if (timeUnit == null || units == null) {
    // shouldn't be possible, but lacking type safety for this case
    return false;
  }

  const { transitionedAt: lastSuccessfulSync } =
    streamStatuses.find(
      ({ jobType, runState }) => jobType === StreamStatusJobType.SYNC && runState === StreamStatusRunState.COMPLETE
    ) ?? {};

  if (lastSuccessfulSync == null) {
    // connections with no successful syncs are either Pending or Failed
    return false;
  }

  return (
    lastSuccessfulSync <
    dayjs()
      // Subtract time for N (missedScheduleCount) runs and compare it to last sync time
      .subtract(units * missedScheduleCount, timeUnit)
      .valueOf()
  );
};

interface UIStreamStatus {
  status: ConnectionStatusIndicatorStatus | undefined;
  isRunning: boolean;
  lastSuccessfulSync: StreamStatusRead | undefined;
}

export const computeStreamStatus = ({
  statuses,
  scheduleType,
  scheduleData,
  hasBreakingSchemaChange,
  lateMultiplier,
  errorMultiplier,
}: {
  statuses: StreamStatusRead[];
  scheduleType?: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
  hasBreakingSchemaChange: boolean;
  lateMultiplier: number;
  errorMultiplier: number;
}): UIStreamStatus => {
  if (statuses == null || statuses.length === 0) {
    return { status: undefined, isRunning: false, lastSuccessfulSync: undefined };
  }

  const isRunning = statuses[0].runState === StreamStatusRunState.RUNNING;

  const lastSuccessfulSync = statuses.find(
    ({ jobType, runState }) => jobType === StreamStatusJobType.SYNC && runState === StreamStatusRunState.COMPLETE
  );

  // breaking schema change means user action is required
  if (hasBreakingSchemaChange) {
    return { status: ConnectionStatusIndicatorStatus.ActionRequired, isRunning, lastSuccessfulSync };
  }

  if (statuses[0].runState === StreamStatusRunState.PENDING) {
    return { status: ConnectionStatusIndicatorStatus.Pending, isRunning, lastSuccessfulSync };
  }

  // reset streams are pending, regardless of the run state
  if (statuses[0].jobType === StreamStatusJobType.RESET) {
    return { status: ConnectionStatusIndicatorStatus.Pending, isRunning, lastSuccessfulSync };
  }

  // if the most recent sync failed, is there a previous successful sync within errorMultiplier (2x) schedule frequency?
  if (
    statuses[0].jobType === StreamStatusJobType.SYNC &&
    statuses[0].runState === StreamStatusRunState.INCOMPLETE &&
    statuses[0].incompleteRunCause === StreamStatusIncompleteRunCause.FAILED &&
    !isStreamLate(statuses, scheduleType, scheduleData, errorMultiplier)
  ) {
    return { status: ConnectionStatusIndicatorStatus.OnTrack, isRunning, lastSuccessfulSync };
  }

  // is there a previous successful sync outside lateMultipler (2x) schedule frequency?
  if (isStreamLate(statuses, scheduleType, scheduleData, lateMultiplier)) {
    return { status: ConnectionStatusIndicatorStatus.Late, isRunning, lastSuccessfulSync };
  }

  // is there a previous successful sync outside 1 schedule frequency?
  if (isStreamLate(statuses, scheduleType, scheduleData, 1)) {
    return { status: ConnectionStatusIndicatorStatus.OnTrack, isRunning, lastSuccessfulSync };
  }

  // is there are error?
  if (
    statuses[0].jobType === StreamStatusJobType.SYNC &&
    statuses[0].runState === StreamStatusRunState.INCOMPLETE &&
    statuses[0].incompleteRunCause === StreamStatusIncompleteRunCause.FAILED
  ) {
    return { status: ConnectionStatusIndicatorStatus.Error, isRunning, lastSuccessfulSync };
  }

  // completed syncs that haven't been flagged as ontrack/late are on time
  if (lastSuccessfulSync) {
    return { status: ConnectionStatusIndicatorStatus.OnTime, isRunning, lastSuccessfulSync };
  }

  return { status: undefined, isRunning, lastSuccessfulSync };
};
