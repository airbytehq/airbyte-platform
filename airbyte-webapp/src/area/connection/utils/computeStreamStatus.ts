import dayjs from "dayjs";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";

import {
  AirbyteStream,
  AirbyteStreamAndConfiguration,
  ConnectionScheduleData,
  ConnectionScheduleType,
  ConnectionSyncResultRead,
  JobConfigType,
  StreamStatusIncompleteRunCause,
  StreamStatusJobType,
  StreamStatusRead,
  StreamStatusRunState,
} from "core/api/types/AirbyteClient";

export type AirbyteStreamAndConfigurationWithEnforcedStream = AirbyteStreamAndConfiguration & { stream: AirbyteStream };

export const activeStatuses: readonly ConnectionStatusIndicatorStatus[] = [
  ConnectionStatusIndicatorStatus.Syncing,
  ConnectionStatusIndicatorStatus.Queued,
  ConnectionStatusIndicatorStatus.Clearing,
  ConnectionStatusIndicatorStatus.Refreshing,
] as const;

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

interface BaseUIStreamStatus {
  status: Exclude<ConnectionStatusIndicatorStatus, "rateLimited"> | undefined;
  lastSuccessfulSync: StreamStatusRead | undefined;
  isRunning: boolean; // isRunning can be removed once sync progress has been fully rolled out, as it can be inferred from the status.  until then, it is required for the v1 stream status ui.
}
interface RateLimitedUIStreamStatus {
  status: ConnectionStatusIndicatorStatus.RateLimited;
  lastSuccessfulSync: StreamStatusRead | undefined;
  isRunning: true;
  quotaReset?: number;
}

type UIStreamStatus = BaseUIStreamStatus | RateLimitedUIStreamStatus;

export const computeStreamStatus = ({
  statuses,
  hasBreakingSchemaChange,
  isSyncing,
  recordsExtracted,
  runningJobConfigType,
  isRateLimitedUiEnabled = true, // default to `true` here so unit tests execute against this side, this value is set from the one actual call site
}: {
  statuses: StreamStatusRead[];
  scheduleType?: ConnectionScheduleType;
  scheduleData?: ConnectionScheduleData;
  hasBreakingSchemaChange: boolean;
  isSyncing: boolean;
  recordsExtracted?: number;
  runningJobConfigType?: string;
  isRateLimitedUiEnabled?: boolean;
}): UIStreamStatus => {
  // no statuses
  if (statuses == null || statuses.length === 0) {
    return {
      status: ConnectionStatusIndicatorStatus.QueuedForNextSync,
      isRunning: false,
      lastSuccessfulSync: undefined,
    };
  }

  const isRunning =
    isSyncing ||
    statuses[0].runState === StreamStatusRunState.RUNNING ||
    statuses[0].runState === StreamStatusRunState.RATE_LIMITED;

  const lastSuccessfulSync = statuses.find(
    ({ jobType, runState }) => jobType === StreamStatusJobType.SYNC && runState === StreamStatusRunState.COMPLETE
  );

  // queued for next sync
  if (
    !isRunning &&
    (statuses[0].runState === StreamStatusRunState.PENDING || statuses[0].jobType === StreamStatusJobType.RESET)
  ) {
    return {
      status: ConnectionStatusIndicatorStatus.QueuedForNextSync,
      isRunning,
      lastSuccessfulSync,
    };
  }

  if (isRateLimitedUiEnabled && statuses[0].runState === StreamStatusRunState.RATE_LIMITED) {
    return {
      status: ConnectionStatusIndicatorStatus.RateLimited,
      isRunning: true,
      lastSuccessfulSync,
      quotaReset: statuses[0].metadata?.quotaReset,
    };
  }

  // queued
  if (isRunning) {
    if (runningJobConfigType === JobConfigType.reset_connection || runningJobConfigType === JobConfigType.clear) {
      return {
        status: ConnectionStatusIndicatorStatus.Clearing,
        isRunning,
        lastSuccessfulSync,
      };
    }

    if (!recordsExtracted || recordsExtracted === 0) {
      return {
        status: ConnectionStatusIndicatorStatus.Queued,
        isRunning,
        lastSuccessfulSync,
      };
    }
    if (recordsExtracted && recordsExtracted > 0) {
      // syncing or refreshing
      if (runningJobConfigType === "sync") {
        return {
          status: ConnectionStatusIndicatorStatus.Syncing,
          isRunning,
          lastSuccessfulSync,
        };
      } else if (runningJobConfigType === "refresh") {
        return {
          status: ConnectionStatusIndicatorStatus.Refreshing,
          isRunning,
          lastSuccessfulSync,
        };
      }
    }
  }

  // breaking schema change means user action is required
  if (hasBreakingSchemaChange) {
    return {
      status: ConnectionStatusIndicatorStatus.Failed,
      isRunning,
      lastSuccessfulSync,
    };
  }

  if (statuses[0].runState === StreamStatusRunState.PENDING) {
    return {
      status: ConnectionStatusIndicatorStatus.Pending,
      isRunning,
      lastSuccessfulSync,
    };
  }

  // reset streams are pending, regardless of the run state
  if (statuses[0].jobType === StreamStatusJobType.RESET) {
    return {
      status: ConnectionStatusIndicatorStatus.Pending,
      isRunning,
      lastSuccessfulSync,
    };
  }

  // is there are error?
  if (
    statuses[0].jobType === StreamStatusJobType.SYNC &&
    statuses[0].runState === StreamStatusRunState.INCOMPLETE &&
    statuses[0].incompleteRunCause === StreamStatusIncompleteRunCause.FAILED
  ) {
    return {
      status: ConnectionStatusIndicatorStatus.Incomplete,
      isRunning,
      lastSuccessfulSync,
    };
  }

  // completed syncs that haven't been flagged as ontrack/late are on time
  if (lastSuccessfulSync) {
    return {
      status: ConnectionStatusIndicatorStatus.Synced,
      isRunning,
      lastSuccessfulSync,
    };
  }

  return {
    status: undefined,
    isRunning,
    lastSuccessfulSync,
  };
};
